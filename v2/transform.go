package goetl

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ColumnFunc transforms a batch by operating on whole columns. This is the fast
// path: the function receives the batch and mutates column slices in place or
// builds new columns, with no per-row dispatch.
type ColumnFunc func(*Batch) error

// ColumnTransform applies a ColumnFunc to each batch. Tier 1 in the performance
// hierarchy: loops inside fn run at hand-written-slice-loop speed.
type ColumnTransform struct {
	NopFlush
	Name string
	Fn   ColumnFunc
}

// NewColumnTransform builds a Tier 1 transform.
func NewColumnTransform(name string, fn ColumnFunc) *ColumnTransform {
	return &ColumnTransform{Name: name, Fn: fn}
}

// Process implements Processor.
func (t *ColumnTransform) Process(ctx context.Context, b *Batch, emit Emit) error {
	if err := t.Fn(b); err != nil {
		return err
	}
	return emit(b)
}

func (t *ColumnTransform) String() string { return "ColumnTransform(" + t.Name + ")" }

// Row is a cursor over one row of a batch. Obtained from RowTransform, it is
// only valid until the next call to Next.
type Row struct {
	b   *Batch
	idx int
}

// Index returns the row's position within the batch.
func (r Row) Index() int { return r.idx }

// Float64 reads a float64 field by column name. Each call performs a schema
// lookup and an interface dispatch, which is the cost this tier is named for.
func (r Row) Float64(name string) (float64, error) {
	vals, err := Column[float64](r.b, name)
	if err != nil {
		return 0, err
	}
	return vals[r.idx], nil
}

// Int64 reads an int64 field by column name.
func (r Row) Int64(name string) (int64, error) {
	vals, err := Column[int64](r.b, name)
	if err != nil {
		return 0, err
	}
	return vals[r.idx], nil
}

// String reads a string field by column name.
func (r Row) String(name string) (string, error) {
	arr, err := StringColumn(r.b, name)
	if err != nil {
		return "", err
	}
	return arr.Value(r.idx), nil
}

// RowFunc is called once per row. Returning an error aborts the batch.
type RowFunc func(Row) error

// RowTransform applies a RowFunc to every row of every batch.
//
// This is Tier 2, the deliberately slower path. Prefer ColumnTransform: this
// type exists for logic that genuinely needs whole-row context, and it costs
// roughly an order of magnitude more per row. It is still far cheaper than
// goetl v1, which paid a JSON round trip and a map allocation per payload.
type RowTransform struct {
	NopFlush
	Name string
	Fn   RowFunc
}

// NewRowTransform builds a Tier 2 transform.
func NewRowTransform(name string, fn RowFunc) *RowTransform {
	return &RowTransform{Name: name, Fn: fn}
}

// Process implements Processor.
func (t *RowTransform) Process(ctx context.Context, b *Batch, emit Emit) error {
	n := b.NumRows()
	for i := 0; i < n; i++ {
		if err := t.Fn(Row{b: b, idx: i}); err != nil {
			return err
		}
	}
	return emit(b)
}

func (t *RowTransform) String() string { return "RowTransform(" + t.Name + ")" }

// Passthrough forwards batches unchanged. Used to measure bare runtime overhead.
type Passthrough struct {
	NopFlush
	_ byte // avoid zero-size struct address aliasing
}

// Process implements Processor.
func (p *Passthrough) Process(ctx context.Context, b *Batch, emit Emit) error {
	return emit(b)
}

func (p *Passthrough) String() string { return "Passthrough" }

// Discard consumes batches and emits nothing. Used as a terminal stage in
// benchmarks and for pipelines whose side effects happen upstream.
type Discard struct {
	NopFlush
	Rows int64
}

// Process implements Processor.
func (d *Discard) Process(ctx context.Context, b *Batch, emit Emit) error {
	d.Rows += int64(b.NumRows())
	return nil
}

func (d *Discard) String() string { return "Discard" }

// AppendFloat64Column adds a float64 column to a batch, returning a new Batch
// that shares the existing columns and owns the new one. The original batch is
// unchanged and its columns are retained, not copied.
func AppendFloat64Column(b *Batch, name string, vals []float64, alloc memory.Allocator) *Batch {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	bldr := array.NewFloat64Builder(alloc)
	defer bldr.Release()
	bldr.AppendValues(vals, nil)
	arr := bldr.NewArray()
	defer arr.Release()

	old := b.Record()
	fields := append(append([]arrow.Field{}, old.Schema().Fields()...),
		arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64})
	cols := append(append([]arrow.Array{}, old.Columns()...), arr)

	rec := array.NewRecord(arrow.NewSchema(fields, nil), cols, old.NumRows())
	return NewBatch(rec)
}
