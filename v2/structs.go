package goetl

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Struct binding: the ergonomic on-ramp.
//
// The columnar API is fast but demands that you know each column's name and
// type at every access, handle errors per lookup, and use Arrow builders to
// change a schema. v1 let you write `d.Parse(&rows)` and work with ordinary Go
// structs, which is a large part of why it was pleasant.
//
// These functions restore that. They are deliberately the slow path -- a batch
// is decoded into []T and re-encoded -- so the tradeoff is explicit: start with
// structs, move the hot stage to Column[T] when a benchmark says to.

// fieldMap describes how a struct's exported fields map onto column names.
// Column name comes from the `goetl` tag, else the field name.
type fieldMap struct {
	index []int    // struct field index per column
	names []string // column name per column
	types []reflect.Type
}

func mapStruct(t reflect.Type) (*fieldMap, error) {
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("goetl: %s is not a struct", t)
	}
	m := &fieldMap{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue // unexported
		}
		name := f.Name
		if tag := f.Tag.Get("goetl"); tag != "" {
			if tag == "-" {
				continue
			}
			name = strings.Split(tag, ",")[0]
		}
		m.index = append(m.index, i)
		m.names = append(m.names, name)
		m.types = append(m.types, f.Type)
	}
	if len(m.index) == 0 {
		return nil, fmt.Errorf("goetl: %s has no exported fields", t)
	}
	return m, nil
}

// Structs decodes a batch into a slice of T.
//
// Columns are matched to fields by the `goetl` struct tag, falling back to the
// field name. Missing columns are an error; extra columns are ignored.
func Structs[T any](b *Batch) ([]T, error) {
	var zero T
	rt := reflect.TypeOf(zero)
	m, err := mapStruct(rt)
	if err != nil {
		return nil, err
	}

	n := b.NumRows()
	out := make([]T, n)
	rv := reflect.ValueOf(out)

	for c, name := range m.names {
		arr, err := b.column(name)
		if err != nil {
			return nil, err
		}
		fi := m.index[c]
		if err := scatterColumn(rv, fi, arr, n); err != nil {
			return nil, fmt.Errorf("column %q: %w", name, err)
		}
	}
	return out, nil
}

// scatterColumn writes one Arrow column across the struct slice.
func scatterColumn(slice reflect.Value, fieldIdx int, arr arrow.Array, n int) error {
	switch a := arr.(type) {
	case *array.Int64:
		v := a.Int64Values()
		for i := 0; i < n; i++ {
			f := slice.Index(i).Field(fieldIdx)
			if !f.CanInt() {
				return fmt.Errorf("field is %s, column is int64", f.Type())
			}
			if !a.IsNull(i) {
				f.SetInt(v[i])
			}
		}
	case *array.Float64:
		v := a.Float64Values()
		for i := 0; i < n; i++ {
			f := slice.Index(i).Field(fieldIdx)
			if !f.CanFloat() {
				return fmt.Errorf("field is %s, column is float64", f.Type())
			}
			if !a.IsNull(i) {
				f.SetFloat(v[i])
			}
		}
	case *array.String:
		for i := 0; i < n; i++ {
			f := slice.Index(i).Field(fieldIdx)
			if f.Kind() != reflect.String {
				return fmt.Errorf("field is %s, column is string", f.Type())
			}
			if !a.IsNull(i) {
				f.SetString(a.Value(i))
			}
		}
	case *array.Boolean:
		for i := 0; i < n; i++ {
			f := slice.Index(i).Field(fieldIdx)
			if f.Kind() != reflect.Bool {
				return fmt.Errorf("field is %s, column is bool", f.Type())
			}
			if !a.IsNull(i) {
				f.SetBool(a.Value(i))
			}
		}
	default:
		return fmt.Errorf("unsupported type %s", arr.DataType())
	}
	return nil
}

// FromStructs builds a Batch from a slice of T. The caller owns the result and
// must release it.
func FromStructs[T any](vals []T, alloc memory.Allocator) (*Batch, error) {
	var zero T
	rt := reflect.TypeOf(zero)
	m, err := mapStruct(rt)
	if err != nil {
		return nil, err
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}

	fields := make([]arrow.Field, len(m.names))
	for c, name := range m.names {
		dt, err := arrowTypeForGo(m.types[c])
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", name, err)
		}
		fields[c] = arrow.Field{Name: name, Type: dt}
	}

	bldr := array.NewRecordBuilder(alloc, arrow.NewSchema(fields, nil))
	defer bldr.Release()

	rv := reflect.ValueOf(vals)
	for c := range m.names {
		fi := m.index[c]
		fb := bldr.Field(c)
		for i := 0; i < len(vals); i++ {
			f := rv.Index(i).Field(fi)
			switch b := fb.(type) {
			case *array.Int64Builder:
				b.Append(f.Int())
			case *array.Float64Builder:
				b.Append(f.Float())
			case *array.StringBuilder:
				b.Append(f.String())
			case *array.BooleanBuilder:
				b.Append(f.Bool())
			default:
				return nil, fmt.Errorf("field %q: unsupported builder %T", m.names[c], fb)
			}
		}
	}
	return NewBatch(bldr.NewRecord()), nil
}

// arrowTypeForGo maps a Go field type onto an Arrow type.
func arrowTypeForGo(t reflect.Type) (arrow.DataType, error) {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return arrow.PrimitiveTypes.Int64, nil
	case reflect.Float32, reflect.Float64:
		return arrow.PrimitiveTypes.Float64, nil
	case reflect.String:
		return arrow.BinaryTypes.String, nil
	case reflect.Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	}
	return nil, fmt.Errorf("unsupported Go type %s", t)
}

// StructFunc transforms a batch as a slice of Go structs. Returning a slice of
// different length is allowed, so this covers filtering and expansion too.
type StructFunc[T any] func([]T) ([]T, error)

// structTransform is the Processor returned by NewStructTransform.
type structTransform[T any] struct {
	NopFlush
	name  string
	fn    StructFunc[T]
	alloc memory.Allocator
}

// NewStructTransform builds a processor that works in ordinary Go structs,
// closely mirroring v1's FuncTransformer.
//
// This is the friendly path, not the fast one: each batch is decoded into []T
// and re-encoded. Reach for ColumnTransform once a stage matters.
func NewStructTransform[T any](name string, fn StructFunc[T]) Processor {
	return &structTransform[T]{name: name, fn: fn}
}

// Process implements Processor.
func (t *structTransform[T]) Process(ctx context.Context, b *Batch, emit Emit) error {
	in, err := Structs[T](b)
	if err != nil {
		return err
	}
	out, err := t.fn(in)
	if err != nil {
		return err
	}
	if len(out) == 0 {
		return nil
	}
	nb, err := FromStructs(out, t.alloc)
	if err != nil {
		return err
	}
	defer nb.Release()
	return emit(nb)
}

func (t *structTransform[T]) String() string { return "StructTransform(" + t.name + ")" }
