//go:build duckdb_arrow

package goetl_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/duckdb/duckdb-go/v2"

	goetl "github.com/teambenny/goetl/v2"
)

// Round-trip tests run against an embedded DuckDB, which accepts $N
// placeholders and ON CONFLICT ... DO UPDATE, so the Postgres dialect can be
// exercised for real. They are behind the cgo tag; the core stays pure Go.

func openTestDB(t testing.TB) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// nullSource emits one batch of two rows where each column has one null.
type nullSource struct{}

func (nullSource) Read(ctx context.Context, emit goetl.Emit) error {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	bl := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer bl.Release()
	bl.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 0}, []bool{true, false})
	bl.Field(1).(*array.StringBuilder).AppendValues([]string{"", "x"}, []bool{false, true})
	b := goetl.NewBatch(bl.NewRecord())
	defer b.Release()
	return emit(b)
}

func TestSQLWriterRoundTrip(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx,
		`CREATE TABLE sales (id BIGINT, region VARCHAR, year BIGINT, amount DOUBLE)`); err != nil {
		t.Fatal(err)
	}

	w := goetl.NewSQLWriter(db, "sales", goetl.PostgresDialect{})
	w.MaxRows = 100
	defer w.Close()

	const rows = 250 // exercises batch splitting: 100 + 100 + 50
	p := goetl.New(&goetl.GenSource{Rows: rows, PerBatch: 64}, w)
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}

	var n int
	var sum float64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), SUM(amount) FROM sales`).Scan(&n, &sum); err != nil {
		t.Fatal(err)
	}
	if n != rows {
		t.Fatalf("row count = %d, want %d", n, rows)
	}
	// GenSource amount is (k%1000)+0.5 for k in [0,250): sum = sum(0..249) + 125
	want := float64(249*250/2) + 0.5*rows
	if sum != want {
		t.Fatalf("sum = %v, want %v", sum, want)
	}

	// Strings must survive intact.
	var region string
	if err := db.QueryRowContext(ctx, `SELECT region FROM sales WHERE id = 0`).Scan(&region); err != nil {
		t.Fatal(err)
	}
	if region != "west" {
		t.Fatalf("region = %q, want %q", region, "west")
	}
}

func TestSQLWriterUpsert(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx,
		`CREATE TABLE sales (id BIGINT PRIMARY KEY, region VARCHAR, year BIGINT, amount DOUBLE)`); err != nil {
		t.Fatal(err)
	}

	write := func() error {
		w := goetl.NewSQLWriter(db, "sales", goetl.PostgresDialect{})
		w.ConflictTarget = "id"
		defer w.Close()
		return goetl.New(&goetl.GenSource{Rows: 10, PerBatch: 10}, w).Run(ctx)
	}
	if err := write(); err != nil {
		t.Fatal(err)
	}
	// Writing the same rows again must update, not duplicate or error.
	if err := write(); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sales`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatalf("row count = %d after two writes, want 10 (upsert)", n)
	}
}

func TestSQLWriterNulls(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `CREATE TABLE t (a BIGINT, b VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	w := goetl.NewSQLWriter(db, "t", goetl.PostgresDialect{})
	defer w.Close()

	if err := goetl.New(&nullSource{}, w).Run(ctx); err != nil {
		t.Fatal(err)
	}

	var nullA, nullB int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FILTER (WHERE a IS NULL), COUNT(*) FILTER (WHERE b IS NULL) FROM t`).
		Scan(&nullA, &nullB); err != nil {
		t.Fatal(err)
	}
	if nullA != 1 || nullB != 1 {
		t.Fatalf("nulls = (%d,%d), want (1,1)", nullA, nullB)
	}
}

func BenchmarkSQLWriter(b *testing.B) {
	db := openTestDB(b)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx,
		`CREATE TABLE sales (id BIGINT, region VARCHAR, year BIGINT, amount DOUBLE)`); err != nil {
		b.Fatal(err)
	}
	w := goetl.NewSQLWriter(db, "sales", goetl.PostgresDialect{})
	w.MaxRows = 1000
	defer w.Close()

	b.ReportAllocs()
	p := goetl.New(&goetl.GenSource{Rows: b.N, PerBatch: 4096}, w)
	b.ResetTimer()
	if err := p.Run(ctx); err != nil {
		b.Fatal(err)
	}
}
