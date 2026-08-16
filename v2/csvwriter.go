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
// Values are formatted column-wise: for each column the concrete Arrow array
// type is resolved once per batch, not once per cell, so the per-value cost is
// a direct typed read and a strconv call with no interface dispatch.
type CSVWriter struct {
	w      *csv.Writer
	bw     *bufio.Writer
	header bool

	// WriteHeader emits a header row derived from the first batch's schema.
	WriteHeader bool
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

	// Format each column into a dense []string once, resolving the array type
	// a single time per column rather than per cell.
	cols := make([][]string, ncols)
	for j := 0; j < ncols; j++ {
		s, err := formatColumn(rec.Column(j), nrows)
		if err != nil {
			return fmt.Errorf("column %q: %w", rec.Schema().Field(j).Name, err)
		}
		cols[j] = s
	}

	row := make([]string, ncols)
	for i := 0; i < nrows; i++ {
		for j := 0; j < ncols; j++ {
			row[j] = cols[j][i]
		}
		if err := c.w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

// Flush implements Processor, draining the CSV and bufio buffers.
func (c *CSVWriter) Flush(ctx context.Context, emit Emit) error {
	c.w.Flush()
	if err := c.w.Error(); err != nil {
		return err
	}
	return c.bw.Flush()
}

func (c *CSVWriter) String() string { return "CSVWriter" }

// formatColumn renders one Arrow column as strings.
func formatColumn(arr arrow.Array, n int) ([]string, error) {
	out := make([]string, n)
	switch a := arr.(type) {
	case *array.Int64:
		v := a.Int64Values()
		for i := 0; i < n; i++ {
			if a.IsNull(i) {
				continue
			}
			out[i] = strconv.FormatInt(v[i], 10)
		}
	case *array.Uint64:
		v := a.Uint64Values()
		for i := 0; i < n; i++ {
			if a.IsNull(i) {
				continue
			}
			out[i] = strconv.FormatUint(v[i], 10)
		}
	case *array.Float64:
		v := a.Float64Values()
		for i := 0; i < n; i++ {
			if a.IsNull(i) {
				continue
			}
			out[i] = strconv.FormatFloat(v[i], 'g', -1, 64)
		}
	case *array.Boolean:
		for i := 0; i < n; i++ {
			if a.IsNull(i) {
				continue
			}
			out[i] = strconv.FormatBool(a.Value(i))
		}
	case *array.String:
		for i := 0; i < n; i++ {
			if a.IsNull(i) {
				continue
			}
			out[i] = a.Value(i)
		}
	default:
		return nil, fmt.Errorf("unsupported type %s", arr.DataType())
	}
	return out, nil
}
