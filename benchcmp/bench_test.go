// Package benchcmp benchmarks goetl v1 and v2 against each other.
//
// Both versions are imported into one module and run in one test binary, so
// the comparison is on the same machine, the same Go toolchain, and the same
// process. Earlier v2 numbers were measured against a scratch v1 harness built
// with a different toolchain; this removes that confound and puts the baseline
// in the repository instead of a temp directory.
//
// The job is identical in both: produce N rows of
// (id int64, region string, year int64, amount float64), apply a
// region-dependent markup to amount, and write CSV to a discarding writer.
//
// b.N is the ROW count in every benchmark, so ns/op reads as ns/row.
package benchcmp

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	v1 "github.com/teambenny/goetl"
	v1data "github.com/teambenny/goetl/etldata"
	v1logger "github.com/teambenny/goetl/logger"
	v1proc "github.com/teambenny/goetl/processors"

	v2 "github.com/teambenny/goetl/v2"
)

const perBatch = 4096

var regions = [4]string{"west", "east", "north", "south"}

func markup(region string) float64 {
	if region == "west" {
		return 1.095
	}
	return 1.08
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// ---------- v1 ----------

type row struct {
	ID     int64   `json:"id"`
	Region string  `json:"region"`
	Year   int64   `json:"year"`
	Amount float64 `json:"amount"`
}

// v1Source emits JSON payloads of rowsPerPayload rows each.
type v1Source struct {
	rows           int
	rowsPerPayload int
}

func (s *v1Source) ProcessData(d v1data.Payload, out chan v1data.Payload, kill chan error) {
	for sent := 0; sent < s.rows; sent += s.rowsPerPayload {
		n := s.rowsPerPayload
		if r := s.rows - sent; r < n {
			n = r
		}
		batch := make([]row, n)
		for i := range batch {
			k := sent + i
			batch[i] = row{
				ID:     int64(k),
				Region: regions[k%4],
				Year:   int64(2020 + k%6),
				Amount: float64(k%1000) + 0.5,
			}
		}
		d, err := v1data.NewJSON(batch)
		if err != nil {
			kill <- err
			return
		}
		out <- d
	}
}

func (s *v1Source) Finish(out chan v1data.Payload, kill chan error) {}
func (s *v1Source) String() string                                  { return "v1Source" }

// runV1 executes the job on v1. rowsPerPayload lets the benchmark be generous
// to v1 by amortizing its per-payload cost over a large batch.
func runV1(b *testing.B, rows, rowsPerPayload int) {
	v1logger.LogLevel = v1logger.LevelSilent // do not measure v1's per-payload logging

	transform := v1proc.NewFuncTransformer(func(d v1data.Payload) v1data.Payload {
		var rs []row
		if err := d.Parse(&rs); err != nil {
			panic(err)
		}
		for i := range rs {
			rs[i].Amount *= markup(rs[i].Region)
		}
		out, err := v1data.NewJSON(rs)
		if err != nil {
			panic(err)
		}
		return out
	})

	p := v1.NewPipeline(&v1Source{rows: rows, rowsPerPayload: rowsPerPayload}, transform,
		v1proc.NewCSVWriter(discard{}))
	p.BufferLength = 8 // v1's documented default, which it does not actually apply

	if err := <-p.Run(); err != nil {
		b.Fatal(err)
	}
}

// ---------- v2 ----------

func runV2(b *testing.B, rows int) {
	transform := v2.NewColumnTransform("markup", func(bt *v2.Batch) error {
		amounts, err := v2.Column[float64](bt, "amount")
		if err != nil {
			return err
		}
		region, err := v2.StringColumn(bt, "region")
		if err != nil {
			return err
		}
		for i := range amounts {
			amounts[i] *= markup(region.Value(i))
		}
		return nil
	})

	p := v2.New(&v2.GenSource{Rows: rows, PerBatch: perBatch}, transform, v2.NewCSVWriter(discard{}))
	if err := p.Run(context.Background()); err != nil {
		b.Fatal(err)
	}
}

// ---------- head to head ----------

// v1 at one row per payload: the shape produced by line-oriented sources
// (IoReader, FileReader, S3Reader) and by any per-record stream.
func BenchmarkV1_1RowPerPayload(b *testing.B) {
	b.ReportAllocs()
	runV1(b, b.N, 1)
}

// v1 at 4096 rows per payload: v1's best case, matching v2's batch size, so
// per-payload overhead is amortized exactly as far as v2 amortizes per-batch
// overhead. This is the fair comparison.
func BenchmarkV1_4096RowsPerPayload(b *testing.B) {
	b.ReportAllocs()
	runV1(b, b.N, perBatch)
}

func BenchmarkV2_4096RowsPerBatch(b *testing.B) {
	b.ReportAllocs()
	runV2(b, b.N)
}

// ---------- correctness: both must produce the same output ----------

// TestSameOutput guards the comparison: if the two pipelines did different
// work, the benchmark would be meaningless.
func TestSameOutput(t *testing.T) {
	v1logger.LogLevel = v1logger.LevelSilent

	const rows = 100

	var v1buf capture
	transform := v1proc.NewFuncTransformer(func(d v1data.Payload) v1data.Payload {
		var rs []row
		if err := d.Parse(&rs); err != nil {
			t.Fatal(err)
		}
		for i := range rs {
			rs[i].Amount *= markup(rs[i].Region)
		}
		out, err := v1data.NewJSON(rs)
		if err != nil {
			t.Fatal(err)
		}
		return out
	})
	p1 := v1.NewPipeline(&v1Source{rows: rows, rowsPerPayload: 32}, transform,
		v1proc.NewCSVWriter(&v1buf))
	p1.BufferLength = 8
	if err := <-p1.Run(); err != nil {
		t.Fatal(err)
	}

	var v2buf capture
	t2 := v2.NewColumnTransform("markup", func(bt *v2.Batch) error {
		amounts, _ := v2.Column[float64](bt, "amount")
		region, _ := v2.StringColumn(bt, "region")
		for i := range amounts {
			amounts[i] *= markup(region.Value(i))
		}
		return nil
	})
	p2 := v2.New(&v2.GenSource{Rows: rows, PerBatch: 32}, t2, v2.NewCSVWriter(&v2buf))
	if err := p2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// v1 sorts columns alphabetically and v2 preserves schema order, so compare
	// the parsed values rather than raw bytes.
	a := parseCSV(t, v1buf.String(), true)
	c := parseCSV(t, v2buf.String(), false)
	if len(a) != rows || len(c) != rows {
		t.Fatalf("row counts: v1=%d v2=%d, want %d", len(a), len(c), rows)
	}
	for i := range a {
		if fmt.Sprintf("%.6f", a[i]) != fmt.Sprintf("%.6f", c[i]) {
			t.Fatalf("row %d amount: v1=%v v2=%v", i, a[i], c[i])
		}
	}
	t.Logf("v1 and v2 produced identical amounts across %d rows", rows)
}

type capture struct{ b []byte }

func (c *capture) Write(p []byte) (int, error) { c.b = append(c.b, p...); return len(p), nil }
func (c *capture) String() string              { return string(c.b) }

// parseCSV pulls the amount column out. v1 sorts headers alphabetically
// (amount,id,region,year) so amount is index 0; v2 keeps schema order
// (id,region,year,amount) so amount is index 3.
func parseCSV(t *testing.T, s string, v1Order bool) []float64 {
	t.Helper()
	var out []float64
	idx := 3
	if v1Order {
		idx = 0
	}
	for i, line := range splitLines(s) {
		if i == 0 || line == "" {
			continue // header
		}
		fields := splitFields(line)
		if len(fields) <= idx {
			t.Fatalf("line %d has %d fields: %q", i, len(fields), line)
		}
		var f float64
		if err := json.Unmarshal([]byte(fields[idx]), &f); err != nil {
			t.Fatalf("line %d field %q: %v", i, fields[idx], err)
		}
		out = append(out, f)
	}
	return out
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

// splitFields splits a CSV line, stripping the quotes v1's writer always emits.
func splitFields(line string) []string {
	var out []string
	var cur []byte
	inQuote := false
	for i := 0; i < len(line); i++ {
		switch {
		case line[i] == '"':
			inQuote = !inQuote
		case line[i] == ',' && !inQuote:
			out = append(out, string(cur))
			cur = cur[:0]
		default:
			cur = append(cur, line[i])
		}
	}
	return append(out, string(cur))
}
