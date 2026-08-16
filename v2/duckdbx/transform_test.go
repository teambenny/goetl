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
	"github.com/duckdb/duckdb-go/v2"

	goetl "github.com/teambenny/goetl/v2"
	"github.com/teambenny/goetl/v2/duckdbx"
)

// Arrow-view support in duckdb-go is only sound for numeric-only schemas.
// Measured on both duckdb-go v2.4.3 and v2.10505.0 (DuckDB 1.5.5):
//
//	case                                       v2.4.3    v2.10505.0
//	numeric-only view, numeric query           correct   correct
//	view CONTAINS varchar, numeric-only query  corrupt   corrupt
//	read varchar from view                     SIGSEGV   corrupt strings
//	GROUP BY varchar from view                 SIGSEGV   corrupt strings
//
// Upgrading removes the segfault but replaces it with silent corruption, which
// is the more dangerous failure. Tracked upstream as duckdb/duckdb-go#24
// (open), migrated from marcboeker/go-duckdb#513.
//
// The broken cases are skipped so the suite stays green; unskip them to
// re-check against a newer release.

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

// registerView registers rec as an Arrow view named "probe".
func registerView(t testing.TB, ar *duckdb.Arrow, rec arrow.Record) func() {
	t.Helper()
	rdr, err := array.NewRecordReader(rec.Schema(), []arrow.Record{rec})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rdr.Release)
	release, err := ar.RegisterView(rdr, "probe")
	if err != nil {
		t.Fatal(err)
	}
	return release
}

func float64Record(t testing.TB, name string, vals []float64) arrow.Record {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: name, Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	t.Cleanup(b.Release)
	b.Field(0).(*array.Float64Builder).AppendValues(vals, nil)
	rec := b.NewRecord()
	t.Cleanup(rec.Release)
	return rec
}

// ---------- what works ----------

// TestZeroCopyHandoff establishes that DuckDB reads Go-owned Arrow buffers
// directly rather than snapshotting them.
//
// Method: register the view, then mutate the Go buffer BEFORE querying. A copy
// would still sum to 60; reading live memory sums to 1049.
func TestZeroCopyHandoff(t *testing.T) {
	_, ar := openArrow(t)
	rec := float64Record(t, "v", []float64{10, 20, 30})
	defer registerView(t, ar, rec)()

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

	switch sum {
	case 1049:
		t.Logf("zero-copy confirmed: DuckDB read the live Go buffer (sum=%v)", sum)
	case 60:
		t.Fatalf("not zero-copy: DuckDB snapshotted at RegisterView (sum=%v)", sum)
	default:
		t.Fatalf("unexpected sum %v", sum)
	}
}

// TestNumericOnlyViewIsCorrect is the supported path: no strings anywhere in
// the view schema.
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
		t.Fatalf("totals = %v, want [32]", totals)
	}
	t.Logf("numeric-only view correct: %v", totals)
}

// ---------- known broken: duckdb/duckdb-go#24 ----------

// TestVarcharInViewCorruptsNumericResults: a view that merely CONTAINS a
// VARCHAR returns wrong numbers for queries that never reference it.
//
// GenSource is (id int64, region string, year int64, amount float64). Summing
// amount over 8 rows must be 32; it returns a denormal (~1.4e-322) instead,
// with no error. The identical pipeline over numSource returns 32.
//
// This narrows upstream #24, whose reproducer also carries a string column but
// attributes the failure to aggregation rather than to the string's presence.
func TestVarcharInViewCorruptsNumericResults(t *testing.T) {
	t.Skip("duckdb/duckdb-go#24: varchar in view corrupts unrelated numeric results (v2.4.3 and v2.10505.0)")

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

	p := goetl.New(&goetl.GenSource{Rows: 8, PerBatch: 8}, tr, collect, &goetl.Discard{})
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(totals) != 1 || totals[0] != 32 {
		t.Fatalf("totals = %v, want [32]", totals)
	}
}

// TestGroupByStringFromView: grouping by a VARCHAR out of an Arrow view.
// SIGSEGV on v2.4.3; on v2.10505.0 it survives but the group keys come back as
// corrupt bytes (observed map[<binary garbage>:6] instead of
// map[east:6 west:4]).
func TestGroupByStringFromView(t *testing.T) {
	t.Skip("duckdb/duckdb-go#24: varchar read from an Arrow view returns corrupt strings (segfaults on v2.4.3)")

	_, ar := openArrow(t)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String},
		{Name: "amount", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"west", "east", "west", "east"}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4}, nil)
	rec := b.NewRecord()
	defer rec.Release()
	defer registerView(t, ar, rec)()

	res, err := ar.QueryContext(context.Background(),
		`SELECT region, SUM(amount) AS total FROM probe GROUP BY region ORDER BY region`)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Release()

	got := map[string]float64{}
	for res.Next() {
		r := res.Record()
		regions := r.Column(0).(*array.String)
		totals := r.Column(1).(*array.Float64)
		for i := 0; i < int(r.NumRows()); i++ {
			got[regions.Value(i)] = totals.Value(i)
		}
	}
	if err := res.Err(); err != nil {
		t.Fatal(err)
	}
	if got["east"] != 6 || got["west"] != 4 {
		t.Fatalf("got %v, want map[east:6 west:4]", got)
	}
}

// ---------- fixtures and benchmarks ----------

// numSource emits numeric-only batches, avoiding the varchar defect.
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
	p := goetl.New(&numSource{rows: b.N, per: 4096}, tr, &goetl.Discard{})
	b.ResetTimer()
	if err := p.Run(ctx); err != nil {
		b.Fatal(err)
	}
}
