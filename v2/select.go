package goetl

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Filter returns a new Batch containing only the rows where keep[i] is true.
//
// Arrow arrays are fixed length, so dropping rows means building a new batch.
// Without this helper every filtering processor has to hand-write a type switch
// over its columns, which is the single most tedious shape in a columnar API.
//
// The returned Batch is owned by the caller and must be released. When nothing
// is dropped the input is retained and returned unchanged, so a filter that
// matches everything costs no copy.
func Filter(b *Batch, keep []bool, alloc memory.Allocator) (*Batch, error) {
	if len(keep) != b.NumRows() {
		return nil, fmt.Errorf("goetl: keep has %d entries, batch has %d rows", len(keep), b.NumRows())
	}
	n := 0
	for _, k := range keep {
		if k {
			n++
		}
	}
	if n == b.NumRows() {
		b.Retain()
		return b, nil
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}

	rec := b.Record()
	bldr := array.NewRecordBuilder(alloc, rec.Schema())
	defer bldr.Release()

	for j := 0; j < int(rec.NumCols()); j++ {
		if err := appendFiltered(bldr.Field(j), rec.Column(j), keep); err != nil {
			return nil, fmt.Errorf("column %q: %w", rec.Schema().Field(j).Name, err)
		}
	}
	return NewBatch(bldr.NewRecord()), nil
}

// appendFiltered copies the kept rows of src into dst.
func appendFiltered(dst array.Builder, src arrow.Array, keep []bool) error {
	switch a := src.(type) {
	case *array.Int64:
		bl := dst.(*array.Int64Builder)
		bl.Reserve(len(keep))
		for i, k := range keep {
			switch {
			case !k:
			case a.IsNull(i):
				bl.AppendNull()
			default:
				bl.Append(a.Value(i))
			}
		}
	case *array.Uint64:
		bl := dst.(*array.Uint64Builder)
		bl.Reserve(len(keep))
		for i, k := range keep {
			switch {
			case !k:
			case a.IsNull(i):
				bl.AppendNull()
			default:
				bl.Append(a.Value(i))
			}
		}
	case *array.Float64:
		bl := dst.(*array.Float64Builder)
		bl.Reserve(len(keep))
		for i, k := range keep {
			switch {
			case !k:
			case a.IsNull(i):
				bl.AppendNull()
			default:
				bl.Append(a.Value(i))
			}
		}
	case *array.Boolean:
		bl := dst.(*array.BooleanBuilder)
		bl.Reserve(len(keep))
		for i, k := range keep {
			switch {
			case !k:
			case a.IsNull(i):
				bl.AppendNull()
			default:
				bl.Append(a.Value(i))
			}
		}
	case *array.String:
		bl := dst.(*array.StringBuilder)
		bl.Reserve(len(keep))
		for i, k := range keep {
			switch {
			case !k:
			case a.IsNull(i):
				bl.AppendNull()
			default:
				bl.Append(a.Value(i))
			}
		}
	default:
		return fmt.Errorf("unsupported type %s", src.DataType())
	}
	return nil
}

// FilterFunc decides which rows of a batch to keep. It is called once per
// batch, not once per row, so the predicate can read whole columns.
type FilterFunc func(*Batch) ([]bool, error)

// FilterTransform drops rows using a caller-supplied predicate.
type FilterTransform struct {
	NopFlush
	Name  string
	Fn    FilterFunc
	Alloc memory.Allocator
}

// NewFilter builds a filtering processor.
func NewFilter(name string, fn FilterFunc) *FilterTransform {
	return &FilterTransform{Name: name, Fn: fn}
}

// Process implements Processor.
func (f *FilterTransform) Process(ctx context.Context, b *Batch, emit Emit) error {
	keep, err := f.Fn(b)
	if err != nil {
		return err
	}
	out, err := Filter(b, keep, f.Alloc)
	if err != nil {
		return err
	}
	defer out.Release()
	if out.NumRows() == 0 {
		return nil
	}
	return emit(out)
}

func (f *FilterTransform) String() string { return "Filter(" + f.Name + ")" }

// AppendColumn returns a new Batch with one column added, sharing the existing
// columns rather than copying them. The new column must have the same length as
// the batch. The caller owns the result and must release it.
func AppendColumn(b *Batch, field arrow.Field, col arrow.Array) (*Batch, error) {
	if col.Len() != b.NumRows() {
		return nil, fmt.Errorf("goetl: column %q has %d rows, batch has %d",
			field.Name, col.Len(), b.NumRows())
	}
	old := b.Record()
	fields := append(append([]arrow.Field{}, old.Schema().Fields()...), field)
	cols := append(append([]arrow.Array{}, old.Columns()...), col)
	return NewBatch(array.NewRecord(arrow.NewSchema(fields, nil), cols, old.NumRows())), nil
}
