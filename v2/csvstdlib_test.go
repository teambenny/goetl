package goetl_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	goetl "github.com/teambenny/goetl/v2"
)

// stdlibCSVWriter is a fair encoding/csv implementation of the same job: the
// best version a careful person would write. It resolves each column's type
// once per batch (that optimization is orthogonal to the encoder choice) and
// reuses the record slice across rows.
//
// It cannot avoid allocating a string per numeric cell, because
// csv.Writer.Write takes []string and strconv must produce one. That is the
// structural cost being measured, not an unfair handicap.
type stdlibCSVWriter struct {
	goetl.NopFlush
	w      *csv.Writer
	bw     *bufio.Writer
	header bool
}

func newStdlibCSVWriter(w *nullWriter) *stdlibCSVWriter {
	bw := bufio.NewWriterSize(w, 64<<10)
	return &stdlibCSVWriter{w: csv.NewWriter(bw), bw: bw}
}

func (c *stdlibCSVWriter) Process(ctx context.Context, b *goetl.Batch, emit goetl.Emit) error {
	rec := b.Record()
	ncols, nrows := int(rec.NumCols()), int(rec.NumRows())

	if !c.header {
		hdr := make([]string, ncols)
		for i := range hdr {
			hdr[i] = rec.Schema().Field(i).Name
		}
		if err := c.w.Write(hdr); err != nil {
			return err
		}
		c.header = true
	}

	fmts := make([]func(int) string, ncols)
	for j := 0; j < ncols; j++ {
		switch a := rec.Column(j).(type) {
		case *array.Int64:
			v := a.Int64Values()
			fmts[j] = func(i int) string { return strconv.FormatInt(v[i], 10) }
		case *array.Float64:
			v := a.Float64Values()
			fmts[j] = func(i int) string { return strconv.FormatFloat(v[i], 'g', -1, 64) }
		case *array.String:
			fmts[j] = func(i int) string { return a.Value(i) }
		default:
			return fmt.Errorf("unsupported %s", rec.Column(j).DataType())
		}
	}

	row := make([]string, ncols)
	for i := 0; i < nrows; i++ {
		for j := 0; j < ncols; j++ {
			row[j] = fmts[j](i)
		}
		if err := c.w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func (c *stdlibCSVWriter) FlushAll() error { c.w.Flush(); return c.bw.Flush() }

type nullWriter struct{}

func (nullWriter) Write(p []byte) (int, error) { return len(p), nil }

// ---------- head to head ----------

func BenchmarkCSVStdlib(b *testing.B) {
	b.ReportAllocs()
	w := newStdlibCSVWriter(&nullWriter{})
	p := goetl.New(&goetl.GenSource{Rows: b.N, PerBatch: 4096}, w)
	b.ResetTimer()
	if err := p.Run(context.Background()); err != nil {
		b.Fatal(err)
	}
	w.FlushAll()
}

func BenchmarkCSVDirect(b *testing.B) {
	b.ReportAllocs()
	p := goetl.New(&goetl.GenSource{Rows: b.N, PerBatch: 4096}, goetl.NewCSVWriter(nullWriter{}))
	b.ResetTimer()
	if err := p.Run(context.Background()); err != nil {
		b.Fatal(err)
	}
}

// ---------- differential correctness ----------

// writeOurs renders vals through the goetl CSV writer.
func writeOurs(t *testing.T, vals []string) string {
	t.Helper()
	var buf bytes.Buffer
	w := goetl.NewCSVWriter(&buf)
	w.WriteHeader = false
	if err := goetl.New(&strSource{vals: vals}, w).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

// writeStdlib renders the same values through encoding/csv, one field per row,
// which is the shape strSource produces.
func writeStdlib(t *testing.T, vals []string) string {
	t.Helper()
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	for _, v := range vals {
		if err := w.Write([]string{v}); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

// FuzzCSVMatchesStdlib asserts byte-for-byte equality with encoding/csv on
// arbitrary input. This is what makes the hand-rolled encoder maintainable:
// the correctness question is delegated to the standard library rather than to
// a hand-written list of edge cases.
func FuzzCSVMatchesStdlib(f *testing.F) {
	for _, s := range []string{
		"", "plain", "a,b", `a"b`, "a\nb", "a\rb", " lead", "trail ",
		`\.`, `\`, "\t", "\u00a0nbsp", "日本語", `""`, ",", "\n",
		"a\r\nb", "  ", "0.5", "-1e-322",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		// encoding/csv rejects fields containing invalid UTF-8 in some paths;
		// restrict to what both accept.
		if !utf8ValidString(s) {
			t.Skip()
		}
		vals := []string{s, "after"}
		ours, std := writeOurs(t, vals), writeStdlib(t, vals)
		if ours != std {
			t.Fatalf("mismatch for %q:\n ours: %q\n std:  %q", s, ours, std)
		}
	})
}

func utf8ValidString(s string) bool {
	for _, r := range s {
		if r == 0xFFFD {
			return false
		}
	}
	return true
}
