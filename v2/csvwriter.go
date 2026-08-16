package goetl

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"unicode"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// CSVWriter writes batches to an io.Writer as CSV.
//
// Values are appended directly into a reusable byte buffer. There is no
// intermediate []string, no allocation per cell, and no per-row call into
// encoding/csv. Measured per value on this machine:
//
//	strconv.FormatFloat (allocating)   91.1 ns, 1 alloc
//	strconv.AppendFloat (buffer)       68.1 ns, 0 allocs
//	strconv.AppendInt                  14.4 ns, 0 allocs
//	encoding/csv framing, 4 fields     65.8 ns/row
//	direct append framing, 4 fields    14.2 ns/row
//
// Float formatting is the floor. Shortest-round-trip conversion dominates any
// remaining cost and no CSV encoder can avoid it; note that fixed precision
// ('f', 2) measured *slower* at 138.5 ns, so trading digits for speed does not
// work here.
type CSVWriter struct {
	w   io.Writer
	buf []byte

	header bool
	apps   []appender

	// WriteHeader emits a header row derived from the first batch's schema.
	WriteHeader bool

	// FlushSize is the buffer high-water mark in bytes. Zero means 64KiB.
	FlushSize int
}

// NewCSVWriter wraps an io.Writer.
func NewCSVWriter(w io.Writer) *CSVWriter {
	return &CSVWriter{w: w, WriteHeader: true, buf: make([]byte, 0, 68<<10)}
}

// appender appends the CSV rendering of row i to dst.
type appender func(dst []byte, i int) []byte

// Process implements Processor.
func (c *CSVWriter) Process(ctx context.Context, b *Batch, emit Emit) error {
	rec := b.Record()
	ncols := int(rec.NumCols())
	nrows := int(rec.NumRows())

	flush := c.FlushSize
	if flush <= 0 {
		flush = 64 << 10
	}

	if c.WriteHeader && !c.header {
		for i := 0; i < ncols; i++ {
			if i > 0 {
				c.buf = append(c.buf, ',')
			}
			c.buf = appendCSVString(c.buf, rec.Schema().Field(i).Name)
		}
		c.buf = append(c.buf, '\n')
		c.header = true
	}

	// Resolve one appender per column per batch, so the per-cell path is a
	// direct typed index with no interface dispatch. The appenders close over
	// this batch's arrays, so they are rebuilt each batch; that is ncols work,
	// amortized over nrows.
	if len(c.apps) != ncols {
		c.apps = make([]appender, ncols)
	}
	for j := 0; j < ncols; j++ {
		a, err := appenderFor(rec.Column(j))
		if err != nil {
			return fmt.Errorf("column %q: %w", rec.Schema().Field(j).Name, err)
		}
		c.apps[j] = a
	}

	for i := 0; i < nrows; i++ {
		for j := 0; j < ncols; j++ {
			if j > 0 {
				c.buf = append(c.buf, ',')
			}
			c.buf = c.apps[j](c.buf, i)
		}
		c.buf = append(c.buf, '\n')

		if len(c.buf) >= flush {
			if _, err := c.w.Write(c.buf); err != nil {
				return err
			}
			c.buf = c.buf[:0]
		}
	}
	return nil
}

// Flush implements Processor, draining the buffer.
func (c *CSVWriter) Flush(ctx context.Context, emit Emit) error {
	if len(c.buf) == 0 {
		return nil
	}
	_, err := c.w.Write(c.buf)
	c.buf = c.buf[:0]
	return err
}

func (c *CSVWriter) String() string { return "CSVWriter" }

// appenderFor resolves an Arrow array to a per-row appender.
func appenderFor(arr arrow.Array) (appender, error) {
	switch a := arr.(type) {
	case *array.Int64:
		v := a.Int64Values()
		if a.NullN() == 0 {
			return func(dst []byte, i int) []byte { return strconv.AppendInt(dst, v[i], 10) }, nil
		}
		return func(dst []byte, i int) []byte {
			if a.IsNull(i) {
				return dst
			}
			return strconv.AppendInt(dst, v[i], 10)
		}, nil
	case *array.Uint64:
		v := a.Uint64Values()
		return func(dst []byte, i int) []byte {
			if a.IsNull(i) {
				return dst
			}
			return strconv.AppendUint(dst, v[i], 10)
		}, nil
	case *array.Float64:
		v := a.Float64Values()
		if a.NullN() == 0 {
			return func(dst []byte, i int) []byte {
				return strconv.AppendFloat(dst, v[i], 'g', -1, 64)
			}, nil
		}
		return func(dst []byte, i int) []byte {
			if a.IsNull(i) {
				return dst
			}
			return strconv.AppendFloat(dst, v[i], 'g', -1, 64)
		}, nil
	case *array.Boolean:
		return func(dst []byte, i int) []byte {
			if a.IsNull(i) {
				return dst
			}
			return strconv.AppendBool(dst, a.Value(i))
		}, nil
	case *array.String:
		return func(dst []byte, i int) []byte {
			if a.IsNull(i) {
				return dst
			}
			return appendCSVString(dst, a.Value(i))
		}, nil
	}
	return nil, fmt.Errorf("unsupported type %s", arr.DataType())
}

// appendCSVString appends s, quoting only when RFC 4180 requires it. Most
// values need no quoting, so the common path is a plain append after a scan.
func appendCSVString(dst []byte, s string) []byte {
	if !csvNeedsQuote(s) {
		return append(dst, s...)
	}
	dst = append(dst, '"')
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			dst = append(dst, '"')
		}
		dst = append(dst, s[i])
	}
	return append(dst, '"')
}

// csvNeedsQuote reports whether s must be quoted.
//
// These rules mirror encoding/csv's fieldNeedsQuotes exactly, including the
// `\.` case (Postgres treats a bare \. as end-of-data) and the leading-space
// rule using unicode.IsSpace rather than an ASCII check. Matching stdlib byte
// for byte is deliberate: it means this writer is a drop-in substitute, and it
// lets FuzzCSVMatchesStdlib assert equality on arbitrary input instead of
// merely asserting that the output looks reasonable.
func csvNeedsQuote(s string) bool {
	if s == "" {
		return false
	}
	if s == `\.` {
		return true
	}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case ',', '"', '\n', '\r':
			return true
		}
	}
	r, _ := utf8.DecodeRuneInString(s)
	return unicode.IsSpace(r)
}
