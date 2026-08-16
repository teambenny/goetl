package goetl_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	goetl "github.com/teambenny/goetl/v2"
)

// ---------- correctness ----------

func TestPipelineRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	src := &goetl.GenSource{Rows: 10, PerBatch: 4}
	p := goetl.New(src, goetl.NewCSVWriter(&buf))

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 11 { // header + 10 rows
		t.Fatalf("got %d lines, want 11:\n%s", len(lines), buf.String())
	}
	if lines[0] != "id,region,year,amount" {
		t.Errorf("header = %q", lines[0])
	}
	if lines[1] != "0,west,2020,0.5" {
		t.Errorf("first row = %q", lines[1])
	}
}

func TestColumnTransformMutatesInPlace(t *testing.T) {
	var buf bytes.Buffer
	double := goetl.NewColumnTransform("double", func(b *goetl.Batch) error {
		amounts, err := goetl.Column[float64](b, "amount")
		if err != nil {
			return err
		}
		for i := range amounts {
			amounts[i] *= 2
		}
		return nil
	})

	p := goetl.New(&goetl.GenSource{Rows: 3, PerBatch: 3}, double, goetl.NewCSVWriter(&buf))
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// amount for row 0 is 0.5; doubled is 1.
	if got := strings.Split(buf.String(), "\n")[1]; got != "0,west,2020,1" {
		t.Errorf("row 0 = %q, want amount doubled", got)
	}
}

func TestRowTransformSeesEveryRow(t *testing.T) {
	var seen []int64
	rt := goetl.NewRowTransform("collect", func(r goetl.Row) error {
		id, err := r.Int64("id")
		if err != nil {
			return err
		}
		seen = append(seen, id)
		return nil
	})
	p := goetl.New(&goetl.GenSource{Rows: 9, PerBatch: 4}, rt, &goetl.Discard{})
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 9 {
		t.Fatalf("saw %d rows, want 9", len(seen))
	}
	for i, id := range seen {
		if id != int64(i) {
			t.Fatalf("row %d has id %d; order not preserved", i, id)
		}
	}
}

// Errors must propagate and cancel the rest of the pipeline, with no
// fire-and-continue and no hang. This is the class of bug that killChan caused
// in v1.
func TestErrorPropagates(t *testing.T) {
	sentinel := errors.New("boom")
	bad := goetl.NewColumnTransform("bad", func(b *goetl.Batch) error { return sentinel })

	p := goetl.New(&goetl.GenSource{Rows: 100000, PerBatch: 16}, bad, &goetl.Discard{})
	err := p.Run(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want wrapped %v", err, sentinel)
	}
}

func TestContextCancelStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stop := goetl.NewColumnTransform("stop", func(b *goetl.Batch) error {
		cancel()
		return nil
	})
	p := goetl.New(&goetl.GenSource{Rows: 1000000, PerBatch: 16}, stop, &goetl.Discard{})
	if err := p.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

// Flush must run exactly once per stage. v1 called Finish twice on stage 1.
type flushCounter struct {
	goetl.Passthrough
	n int
}

func (f *flushCounter) Flush(ctx context.Context, emit goetl.Emit) error {
	f.n++
	return nil
}

func TestFlushExactlyOnce(t *testing.T) {
	f1, f2 := &flushCounter{}, &flushCounter{}
	p := goetl.New(&goetl.GenSource{Rows: 20, PerBatch: 4}, f1, f2, &goetl.Discard{})
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f1.n != 1 || f2.n != 1 {
		t.Fatalf("flush counts = %d, %d; want 1, 1", f1.n, f2.n)
	}
}

// Memory must not grow with the number of batches processed. v1 retained ~314
// bytes per payload forever; this checks the allocator is fully drained.
func TestNoPerBatchRetention(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	src := &goetl.GenSource{Rows: 200000, PerBatch: 1024, Alloc: alloc}
	p := goetl.New(src, &goetl.Passthrough{}, &goetl.Discard{})
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// CheckedAllocator fails the test if any Arrow buffer was not released.
	alloc.AssertSize(t, 0)
}

// ---------- benchmarks ----------
//
// b.N is the ROW count, so ns/op reads as nanoseconds per row. goetl v1
// measured 10,947 ns/row for a 3-stage pipeline doing a JSON round trip.

func benchRows(b *testing.B, procs ...goetl.Processor) {
	b.ReportAllocs()
	src := &goetl.GenSource{Rows: b.N, PerBatch: 4096}
	p := goetl.New(src, procs...)
	b.ResetTimer()
	if err := p.Run(context.Background()); err != nil {
		b.Fatal(err)
	}
}

// Bare runtime cost: source -> passthrough -> discard, no data touched.
func BenchmarkRuntimeOverhead(b *testing.B) {
	benchRows(b, &goetl.Passthrough{}, &goetl.Discard{})
}

// Tier 1: a Go transform over whole columns.
func BenchmarkTier1Column(b *testing.B) {
	t := goetl.NewColumnTransform("markup", func(bt *goetl.Batch) error {
		amounts, err := goetl.Column[float64](bt, "amount")
		if err != nil {
			return err
		}
		regions, err := goetl.StringColumn(bt, "region")
		if err != nil {
			return err
		}
		for i := range amounts {
			rate := 1.08
			if regions.Value(i) == "west" {
				rate = 1.095
			}
			amounts[i] *= rate
		}
		return nil
	})
	benchRows(b, t, &goetl.Discard{})
}

// Tier 2: the same logic expressed row-wise.
func BenchmarkTier2Row(b *testing.B) {
	t := goetl.NewRowTransform("markup", func(r goetl.Row) error {
		region, err := r.String("region")
		if err != nil {
			return err
		}
		if _, err := r.Float64("amount"); err != nil {
			return err
		}
		_ = region
		return nil
	})
	benchRows(b, t, &goetl.Discard{})
}

// End-to-end with a real sink: transform then CSV encode.
func BenchmarkTier1PlusCSV(b *testing.B) {
	t := goetl.NewColumnTransform("markup", func(bt *goetl.Batch) error {
		amounts, err := goetl.Column[float64](bt, "amount")
		if err != nil {
			return err
		}
		for i := range amounts {
			amounts[i] *= 1.08
		}
		return nil
	})
	benchRows(b, t, goetl.NewCSVWriter(discardWriter{}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
