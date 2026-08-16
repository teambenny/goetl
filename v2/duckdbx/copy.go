//go:build duckdb_arrow

package duckdbx

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// copyRecord deep-copies an Arrow record into Go-owned memory.
//
// This is required, not an optimization choice. DuckDB's Arrow result records
// alias memory owned by the result stream, and that memory is invalidated when
// the stream advances or is released. A pipeline hands batches to the next
// stage through a channel, so a batch is read after Process has returned and
// released the stream: without this copy the downstream stage reads freed
// memory.
//
// Note the asymmetry. Go -> DuckDB (RegisterView) is genuinely zero-copy, as
// TestZeroCopyHandoff shows. DuckDB -> Go costs one copy of the result, which
// is normally far smaller than the input when the query aggregates.
func copyRecord(rec arrow.Record, mem memory.Allocator) arrow.Record {
	cols := make([]arrow.Array, rec.NumCols())
	for i := range cols {
		cols[i] = copyArray(rec.Column(i), mem)
	}
	out := array.NewRecord(rec.Schema(), cols, rec.NumRows())
	for _, c := range cols {
		c.Release()
	}
	return out
}

func copyArray(arr arrow.Array, mem memory.Allocator) arrow.Array {
	d := copyArrayData(arr.Data(), mem)
	defer d.Release()
	return array.MakeFromData(d)
}

func copyArrayData(d arrow.ArrayData, mem memory.Allocator) arrow.ArrayData {
	bufs := make([]*memory.Buffer, len(d.Buffers()))
	for i, b := range d.Buffers() {
		if b == nil || b.Len() == 0 {
			continue
		}
		nb := memory.NewResizableBuffer(mem)
		nb.Resize(b.Len())
		copy(nb.Bytes(), b.Bytes())
		bufs[i] = nb
	}
	children := make([]arrow.ArrayData, len(d.Children()))
	for i, c := range d.Children() {
		children[i] = copyArrayData(c, mem)
	}

	nd := array.NewData(d.DataType(), d.Len(), bufs, children, d.NullN(), d.Offset())

	for _, b := range bufs {
		if b != nil {
			b.Release()
		}
	}
	for _, c := range children {
		c.Release()
	}
	return nd
}
