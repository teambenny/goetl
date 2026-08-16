//go:build duckdb_arrow

package duckdbx_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/marcboeker/go-duckdb/v2"

	goetl "github.com/teambenny/goetl/v2"
	"github.com/teambenny/goetl/v2/duckdbx"
)

func openArrow(t testing.TB) (*sql.DB, *duckdb.Arrow) {
	t.Helper()
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ar *duckdb.Arrow
	if err := conn.Raw(func(dc any) error {
		var e error
		ar, e = duckdb.NewArrowFromConn(dc.(driver.Conn))
		return e
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); db.Close() })
	return db, ar
}

// numericView registers a float64-only Arrow view named "probe".
func numericView(t testing.TB, ar *duckdb.Arrow, vals []float64) (arrow.Record, func()) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	t.Cleanup(b.Release)
	b.Field(0).(*array.Float64Builder).AppendValues(vals, nil)
	rec := b.NewRecord()
	t.Cleanup(rec.Release)

	rdr, err := array.NewRecordReader(schema, []arrow.Record{rec})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rdr.Release)

	release, err := ar.RegisterView(rdr, "probe")
	if err != nil {
		t.Fatal(err)
	}
	return rec, release
}

// TestZeroCopyHandoff establishes that DuckDB reads the Go-owned Arrow buffers
// directly rather than snapshotting them.
//
// Method: register the view, then mutate the underlying Go buffer BEFORE
// running the query. A copy would still sum to 60; reading our live memory
// sums to 1049.
func TestZeroCopyHandoff(t *testing.T) {
	_, ar := openArrow(t)
	rec, release := numericView(t, ar, []float64{10, 20, 30})
	defer release()

	rec.Column(0).(*array.Float64).Float64Values()[0] = 999

	res, err := ar.QueryContext(context.Background(), `SELECT SUM(v) AS s FROM probe`)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Release()

	var sum float64
	for res.Next() {
		sum = res.Record().Column(0).(*array.Float64).Value(0)
	}
	if err := res.Err(); err != nil {
		t.Fatal(err)
	}

	const copied, zeroCopy = 60.0, 1049.0
	switch sum {
	case zeroCopy:
		t.Logf("zero-copy confirmed: DuckDB read the live Go buffer (sum=%v)", sum)
	case copied:
		t.Fatalf("not zero-copy: DuckDB snapshotted at RegisterView (sum=%v)", sum)
	default:
		t.Fatalf("unexpected sum %v", sum)
	}
}

// TestVarcharInViewCorruptsNumericResults records the most dangerous of the
// go-duckdb v2.4.3 Arrow limitations: a view that merely CONTAINS a VARCHAR
// column returns silently wrong numbers for queries that never reference it.
//
// GenSource is (id int64, region string, year int64, amount float64). Summing
// amount over 8 rows must be 32. With region present in the view the query
// returns a denormal (~1.4e-322) instead, with no error and no crash.
//
// The identical pipeline over a numeric-only source returns 32 correctly, which
// TestNumericOnlyViewIsCorrect pins down. Silent corruption is worse than the
// segfault, because nothing surfaces it.
//
// Unskip to check whether a newer go-duckdb has fixed this.
func TestVarcharInViewCorruptsNumericResults(t *testing.T) {
	t.Skip("go-duckdb v2.4.3 silently corrupts results when the view contains a VARCHAR; see doc comment")

	ctx := context.Background()
	tr, closeFn, err := duckdbx.Open(ctx, `SELECT SUM(amount) AS total FROM batch`)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()

	var totals []float64
	collect := goetl.NewColumnTransform("collect", func(b *goetl.Batch) error {
		vals, err := goetl.Column[float64](b, "total")
		if err != nil {
			return err
		}
		totals = append(totals, vals...)
		return nil
	})

	p := goetl.New(&goetl.GenSource{Rows: 8, PerBatch: 8}, tr, collect, &goetl.Discard{})
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(totals) != 1 || totals[0] != 32 {
		t.Fatalf("totals = %v, want [32]", totals)
	}
}

// TestVarcharFromViewIsBroken documents a hard limitation in go-duckdb v2.4.3:
// any query that READS a VARCHAR column out of a registered Arrow view
// segfaults inside duckdb_execute_prepared_arrow.
//
// Verified boundary, on go-duckdb v2.4.3 / duckdb-go-bindings v0.1.21:
//
//	SELECT SUM(amount) FROM batch                   -- ok, never touches the string
//	SELECT 'literal' AS s                           -- ok, string not from a view
//	SELECT region FROM batch                        -- SIGSEGV
//	SELECT SUM(amount) FROM batch GROUP BY region   -- SIGSEGV
//	SELECT region, SUM(amount) FROM batch GROUP BY region -- SIGSEGV
//
// Numeric columns from a view are fine, and are genuinely zero-copy (see
// TestZeroCopyHandoff). Registering a view that CONTAINS a string column is
// also fine, as long as no query reads that column.
//
// This test is skipped because it crashes the whole test binary rather than
// failing. Unskip it to check whether a newer go-duckdb has fixed the issue.
func TestVarcharFromViewIsBroken(t *testing.T) {
	t.Skip("go-duckdb v2.4.3 segfaults reading VARCHAR from an Arrow view; see doc comment")

	_, ar := openArrow(t)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"a", "b"}, nil)
	rec := b.NewRecord()
	defer rec.Release()
	rdr, _ := array.NewRecordReader(schema, []arrow.Record{rec})
	defer rdr.Release()
	release, err := ar.RegisterView(rdr, "probe")
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	res, err := ar.QueryContext(context.Background(), `SELECT s FROM probe`)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Release()
	for res.Next() {
	}
	if err := res.Err(); err != nil {
		t.Fatal(err)
	}
}

// BenchmarkSQLTransform measures per-row cost of a DuckDB aggregation inside
// the pipeline. b.N is the row count, so ns/op reads as ns/row.
func BenchmarkSQLTransform(b *testing.B) {
	ctx := context.Background()
	tr, closeFn, err := duckdbx.Open(ctx, `SELECT SUM(amount) AS total FROM batch`)
	if err != nil {
		b.Fatal(err)
	}
	defer closeFn()

	b.ReportAllocs()
	// numeric-only source: a VARCHAR in the view corrupts results (see
	// TestVarcharInViewCorruptsNumericResults).
	p := goetl.New(&numSource{rows: b.N, per: 4096}, tr, &goetl.Discard{})
	b.ResetTimer()
	if err := p.Run(ctx); err != nil {
		b.Fatal(err)
	}
}

// numSource emits numeric-only batches, to test whether a VARCHAR column in the
// registered view corrupts results for queries that never reference it.
type numSource struct{ rows, per int }

func (s *numSource) Read(ctx context.Context, emit goetl.Emit) error {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "amount", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	for sent := 0; sent < s.rows; sent += s.per {
		n := s.per
		if r := s.rows - sent; r < n {
			n = r
		}
		bl := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
		f := bl.Field(0).(*array.Float64Builder)
		for i := 0; i < n; i++ {
			f.Append(float64((sent+i)%1000) + 0.5)
		}
		b := goetl.NewBatch(bl.NewRecord())
		bl.Release()
		err := emit(b)
		b.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

func TestNumericOnlyViewIsCorrect(t *testing.T) {
	ctx := context.Background()
	tr, closeFn, err := duckdbx.Open(ctx, `SELECT SUM(amount) AS total FROM batch`)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()

	var totals []float64
	collect := goetl.NewColumnTransform("collect", func(b *goetl.Batch) error {
		v, err := goetl.Column[float64](b, "total")
		if err != nil {
			return err
		}
		totals = append(totals, v...)
		return nil
	})

	// 0.5 + 1.5 + ... + 7.5 = 32
	p := goetl.New(&numSource{rows: 8, per: 8}, tr, collect, &goetl.Discard{})
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(totals) != 1 || totals[0] != 32 {
		t.Fatalf("totals = %v, want [32] (numeric-only view)", totals)
	}
	t.Logf("numeric-only view correct: %v", totals)
}
