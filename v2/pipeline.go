package goetl

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// Emit hands a Batch to the next stage. It retains the batch on the caller's
// behalf, so the caller keeps ownership of its own reference and should release
// it when done. Emit returns an error if the pipeline is shutting down, and
// callers must propagate that error rather than continuing to produce.
type Emit func(*Batch) error

// Source produces batches at the head of a pipeline. Read returns when the
// source is exhausted, or early with an error. Read must stop and return
// ctx.Err() if the context is cancelled.
type Source interface {
	Read(ctx context.Context, emit Emit) error
}

// Processor transforms batches.
//
// Process is called once per incoming batch. The batch is borrowed: it is valid
// for the duration of the call, and the processor must not release it. To keep
// a batch beyond the call (for buffering or joining), Retain it.
//
// Flush is called exactly once, after the upstream stage has closed and every
// Process call has returned. Use it to emit buffered state. Unlike goetl v1's
// Finish, Flush is guaranteed to run exactly once on every stage, including the
// first.
type Processor interface {
	Process(ctx context.Context, b *Batch, emit Emit) error
	Flush(ctx context.Context, emit Emit) error
}

// NopFlush can be embedded by processors that hold no buffered state.
type NopFlush struct{}

// Flush implements the tail of Processor for stateless processors.
func (NopFlush) Flush(context.Context, Emit) error { return nil }

// DefaultBufferLength is the channel depth between stages. Unlike v1, whose
// documented default of 8 was actually 0, this value is really applied.
const DefaultBufferLength = 8

// Pipeline is a linear chain of stages: one Source followed by zero or more
// Processors. Each stage runs in its own goroutine, connected by bounded
// channels, so stages execute concurrently and backpressure propagates upstream.
type Pipeline struct {
	// BufferLength sets the channel depth between stages. Zero means
	// DefaultBufferLength; set explicitly to 0 via Unbuffered if you want
	// rendezvous semantics.
	BufferLength int

	src   Source
	procs []Processor
}

// New builds a pipeline from a source and an ordered chain of processors.
func New(src Source, procs ...Processor) *Pipeline {
	return &Pipeline{src: src, procs: procs}
}

// Run executes the pipeline and blocks until every stage has finished or one of
// them fails.
//
// Error handling is conventional Go: the first non-nil error from any stage is
// returned, and the context passed to every other stage is cancelled so they can
// unwind. There is no kill channel, no fire-and-continue, and no way for a stage
// to signal failure and keep running.
func (p *Pipeline) Run(ctx context.Context) error {
	buf := p.BufferLength
	if buf == 0 {
		buf = DefaultBufferLength
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	// chans[i] carries batches into stage i.
	chans := make([]chan *Batch, len(p.procs))
	for i := range chans {
		chans[i] = make(chan *Batch, buf)
	}

	// emitter returns an Emit that retains and forwards onto ch, or drops the
	// batch when ch is nil (the tail of the pipeline).
	emitter := func(ch chan *Batch) Emit {
		if ch == nil {
			return func(*Batch) error { return nil }
		}
		return func(b *Batch) error {
			b.Retain()
			select {
			case ch <- b:
				return nil
			case <-ctx.Done():
				b.Release()
				return ctx.Err()
			}
		}
	}

	// out(i) is the channel that stage i writes into: the next stage's input,
	// or nil if stage i is last.
	out := func(i int) chan *Batch {
		if i+1 < len(chans) {
			return chans[i+1]
		}
		return nil
	}

	// Source stage.
	g.Go(func() error {
		var ch chan *Batch
		if len(chans) > 0 {
			ch = chans[0]
			defer close(ch)
		}
		if err := p.src.Read(ctx, emitter(ch)); err != nil {
			return fmt.Errorf("source %T: %w", p.src, err)
		}
		return nil
	})

	// Processor stages.
	for i, proc := range p.procs {
		in, emit := chans[i], emitter(out(i))
		g.Go(func() error {
			if ch := out(i); ch != nil {
				defer close(ch)
			}
			// Drain on exit so upstream senders never block on a dead stage.
			defer func() {
				for b := range in {
					b.Release()
				}
			}()

			for b := range in {
				err := proc.Process(ctx, b, emit)
				b.Release()
				if err != nil {
					return fmt.Errorf("stage %d %T: %w", i+1, proc, err)
				}
			}
			if err := proc.Flush(ctx, emit); err != nil {
				return fmt.Errorf("stage %d %T flush: %w", i+1, proc, err)
			}
			return nil
		})
	}

	return g.Wait()
}
