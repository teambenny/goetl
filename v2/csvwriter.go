package goetl

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// CSVWriter writes batches to an io.Writer as CSV.
//
// Encoding and quoting are delegated to encoding/csv. An earlier version wrote
// bytes directly and was 1.7x faster with zero allocations, but that meant
// owning an RFC 4180 implementation whose failure mode is silently malformed
// output. The standard library is the right call for a format where
// correctness matters more than throughput; see the README for the measured
// cost of that decision, and use a columnar sink when throughput is the goal.
//
// The one optimization kept is resolving each column's Arrow type once per
// batch rather than per cell. That costs nothing to maintain — it is a type
// switch hoisted out of a loop, not a reimplementation of anything.
type CSVWriter struct {
	w      *csv.Writer
	bw     *bufio.Writer
	header bool
	fmts   []func(int) string
	row    []string

	// WriteHeader emits a header row derived from the first batch's schema.
	WriteHeader bool

	// Comma is the field delimiter. Zero means ','.
	Comma rune
}

// NewCSVWriter wraps an io.Writer.
func NewCSVWriter(w io.Writer) *CSVWriter {
	bw := bufio.NewWriterSize(w, 64<<10)
	return &CSVWriter{w: csv.NewWriter(bw), bw: bw, WriteHeader: true}
}

// Process implements Processor.
func (c *CSVWriter) Process(ctx context.Context, b *Batch, emit Emit) error {
	rec := b.Record()
	ncols := int(rec.NumCols())
	nrows := int(rec.NumRows())

	if c.Comma != 0 {
		c.w.Comma = c.Comma
	}

	if c.WriteHeader && !c.header {
		hdr := make([]string, ncols)
		for i := range hdr {
			hdr[i] = rec.Schema().Field(i).Name
		}
		if err := c.w.Write(hdr); err != nil {
			return err
		}
		c.header = true
	}

	// Resolve one formatter per column per batch, so the per-cell path is a
	// direct typed index rather than an interface type switch.
	if len(c.fmts) != ncols {
		c.fmts = make([]func(int) string, ncols)
		c.row = make([]string, ncols)
	}
	for j := 0; j < ncols; j++ {
		f, err := formatterFor(rec.Column(j))
		if err != nil {
			return fmt.Errorf("column %q: %w", rec.Schema().Field(j).Name, err)
		}
		c.fmts[j] = f
	}

	for i := 0; i < nrows; i++ {
		for j := 0; j < ncols; j++ {
			c.row[j] = c.fmts[j](i)
		}
		if err := c.w.Write(c.row); err != nil {
			return err
		}
	}
	return nil
}

// Flush implements Processor, draining the csv and bufio buffers.
func (c *CSVWriter) Flush(ctx context.Context, emit Emit) error {
	c.w.Flush()
	if err := c.w.Error(); err != nil {
		return err
	}
	return c.bw.Flush()
}

func (c *CSVWriter) String() string { return "CSVWriter" }

// formatterFor resolves an Arrow array to a per-row string formatter.
// Null entries render as the empty field.
func formatterFor(arr arrow.Array) (func(int) string, error) {
	switch a := arr.(type) {
	case *array.Int64:
		v := a.Int64Values()
		if a.NullN() == 0 {
			return func(i int) string { return strconv.FormatInt(v[i], 10) }, nil
		}
		return func(i int) string {
			if a.IsNull(i) {
				return ""
			}
			return strconv.FormatInt(v[i], 10)
		}, nil
	case *array.Uint64:
		v := a.Uint64Values()
		return func(i int) string {
			if a.IsNull(i) {
				return ""
			}
			return strconv.FormatUint(v[i], 10)
		}, nil
	case *array.Float64:
		v := a.Float64Values()
		if a.NullN() == 0 {
			return func(i int) string { return strconv.FormatFloat(v[i], 'g', -1, 64) }, nil
		}
		return func(i int) string {
			if a.IsNull(i) {
				return ""
			}
			return strconv.FormatFloat(v[i], 'g', -1, 64)
		}, nil
	case *array.Boolean:
		return func(i int) string {
			if a.IsNull(i) {
				return ""
			}
			return strconv.FormatBool(a.Value(i))
		}, nil
	case *array.String:
		return func(i int) string {
			if a.IsNull(i) {
				return ""
			}
			return a.Value(i)
		}, nil
	}
	return nil, fmt.Errorf("unsupported type %s", arr.DataType())
}
