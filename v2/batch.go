// Package goetl is a columnar ETL pipeline runtime built on Apache Arrow.
//
// Data moves between stages as Batch values, which wrap an arrow.Record: a
// chunk of rows stored column-by-column. Processors operate on whole columns
// (contiguous typed slices) rather than row-at-a-time, which is what makes the
// inner loops fast. A row-wise view is available for logic that genuinely needs
// it, but it is deliberately named to signal the cost.
package goetl

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Batch is a columnar chunk of rows. It wraps an arrow.Record and carries no
// additional copies of the data: every accessor below returns a view into the
// Arrow buffers.
//
// Batches are reference counted. A processor that holds a Batch beyond the
// Process call that received it must Retain it, and Release it when done.
type Batch struct {
	rec arrow.Record
}

// NewBatch wraps an arrow.Record. It takes ownership of the caller's reference.
func NewBatch(rec arrow.Record) *Batch { return &Batch{rec: rec} }

// Record exposes the underlying Arrow record for zero-copy handoff to other
// Arrow-native systems (DuckDB, Parquet writers, Flight).
func (b *Batch) Record() arrow.Record { return b.rec }

// Schema returns the batch schema.
func (b *Batch) Schema() *arrow.Schema { return b.rec.Schema() }

// NumRows returns the number of rows in the batch.
func (b *Batch) NumRows() int { return int(b.rec.NumRows()) }

// NumCols returns the number of columns in the batch.
func (b *Batch) NumCols() int { return int(b.rec.NumCols()) }

// Retain increments the reference count.
func (b *Batch) Retain() { b.rec.Retain() }

// Release decrements the reference count, freeing buffers at zero.
func (b *Batch) Release() { b.rec.Release() }

// column looks up a column by name and returns the Arrow array.
func (b *Batch) column(name string) (arrow.Array, error) {
	idx := b.rec.Schema().FieldIndices(name)
	if len(idx) == 0 {
		return nil, fmt.Errorf("goetl: no column %q in schema %v", name, b.rec.Schema().Fields())
	}
	if len(idx) > 1 {
		return nil, fmt.Errorf("goetl: column %q is ambiguous (%d matches)", name, len(idx))
	}
	return b.rec.Column(idx[0]), nil
}

// Numeric constrains the types that can be read as a flat Go slice.
type Numeric interface {
	~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// Column returns the named column as a contiguous Go slice aliasing the Arrow
// buffer. No copy is made and no allocation occurs, so writing to the returned
// slice mutates the batch in place.
//
// This is the fast path. Loops over the returned slice compile to the same code
// as a hand-written loop over a []float64, which is the entire point of the
// columnar layout.
//
// Null entries read as the zero value; use NullMask if you need to distinguish
// them.
func Column[T Numeric](b *Batch, name string) ([]T, error) {
	arr, err := b.column(name)
	if err != nil {
		return nil, err
	}
	vals, ok := numericValues[T](arr)
	if !ok {
		var zero T
		return nil, fmt.Errorf("goetl: column %q is %s, not %T", name, arr.DataType(), zero)
	}
	return vals, nil
}

// numericValues asserts the concrete Arrow array type and returns its value
// slice, checking that the element type matches T.
func numericValues[T Numeric](arr arrow.Array) ([]T, bool) {
	switch a := arr.(type) {
	case *array.Int8:
		return asSlice[T, int8](a.Int8Values())
	case *array.Int16:
		return asSlice[T, int16](a.Int16Values())
	case *array.Int32:
		return asSlice[T, int32](a.Int32Values())
	case *array.Int64:
		return asSlice[T, int64](a.Int64Values())
	case *array.Uint8:
		return asSlice[T, uint8](a.Uint8Values())
	case *array.Uint16:
		return asSlice[T, uint16](a.Uint16Values())
	case *array.Uint32:
		return asSlice[T, uint32](a.Uint32Values())
	case *array.Uint64:
		return asSlice[T, uint64](a.Uint64Values())
	case *array.Float32:
		return asSlice[T, float32](a.Float32Values())
	case *array.Float64:
		return asSlice[T, float64](a.Float64Values())
	}
	return nil, false
}

// asSlice reinterprets []S as []T when S and T are the same underlying type.
// This is a compile-time-shaped check done at runtime: it succeeds only when
// the caller asked for the type the column actually holds, so no conversion or
// copy ever happens.
func asSlice[T Numeric, S Numeric](vals []S) ([]T, bool) {
	if out, ok := any(vals).([]T); ok {
		return out, true
	}
	return nil, false
}

// StringColumn returns the named column as an Arrow string array. Strings are
// stored as an offsets buffer plus a data buffer, so there is no []string to
// alias; call Value(i) to get a string header pointing into the data buffer.
func StringColumn(b *Batch, name string) (*array.String, error) {
	arr, err := b.column(name)
	if err != nil {
		return nil, err
	}
	s, ok := arr.(*array.String)
	if !ok {
		return nil, fmt.Errorf("goetl: column %q is %s, not string", name, arr.DataType())
	}
	return s, nil
}

// NullMask reports which rows of the named column are null. It returns nil when
// the column has no nulls at all, which is the common case and lets callers skip
// null handling entirely.
func NullMask(b *Batch, name string) ([]bool, error) {
	arr, err := b.column(name)
	if err != nil {
		return nil, err
	}
	if arr.NullN() == 0 {
		return nil, nil
	}
	mask := make([]bool, arr.Len())
	for i := range mask {
		mask[i] = arr.IsNull(i)
	}
	return mask, nil
}

// String renders a short description of the batch, for logs and errors.
func (b *Batch) String() string {
	return fmt.Sprintf("Batch(%d rows x %d cols)", b.NumRows(), b.NumCols())
}
