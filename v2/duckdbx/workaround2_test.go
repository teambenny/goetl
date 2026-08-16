//go:build duckdb_arrow

package duckdbx_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
)

// openArrowConn returns the *sql.Conn too, so DDL and queries run on the same
// connection that holds the registered view.
func openArrowConn(t testing.TB) (*sql.Conn, *duckdb.Arrow) {
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
	return conn, ar
}

// Materialize the Arrow view into a real table on the SAME connection, then
// query the table. If this is reliable it is the upstream-suggested escape
// hatch: it costs a full copy of the input and gives up streaming.
func TestWorkaroundMaterializeSameConn(t *testing.T) {
	ctx := context.Background()
	conn, ar := openArrowConn(t)
	defer registerView(t, ar, stringRecord(t))()

	if _, err := conn.ExecContext(ctx, `CREATE TEMP TABLE mat AS SELECT * FROM probe`); err != nil {
		t.Fatal(err)
	}

	// Direct aggregation over the materialized table -- the shape that is
	// corrupt when run against the Arrow view directly.
	var total float64
	if err := conn.QueryRowContext(ctx, `SELECT SUM(amount) FROM mat`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 10 {
		t.Fatalf("direct SUM after materialize = %v, want 10", total)
	}
	t.Logf("materialized direct SUM correct: %v", total)

	// And the grouped form, reading strings back out.
	rows, err := conn.QueryContext(ctx, `SELECT region, SUM(amount) FROM mat GROUP BY region`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]float64{}
	for rows.Next() {
		var r string
		var v float64
		if err := rows.Scan(&r, &v); err != nil {
			t.Fatal(err)
		}
		got[r] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if got["east"] != 6 || got["west"] != 4 {
		t.Fatalf("got %v, want map[east:6 west:4]", got)
	}
	t.Logf("materialized GROUP BY correct: %v", got)
}

// Control: direct aggregation straight off the Arrow view, same connection,
// via database/sql. This is upstream #24's exact reported shape.
func TestDirectAggregationOnView(t *testing.T) {
	t.Skip("duckdb/duckdb-go#24: direct aggregation on an Arrow view returns corrupt values")

	ctx := context.Background()
	conn, ar := openArrowConn(t)
	defer registerView(t, ar, stringRecord(t))()

	var total float64
	if err := conn.QueryRowContext(ctx, `SELECT SUM(amount) FROM probe`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 10 {
		t.Fatalf("direct SUM on view = %v, want 10 (upstream duckdb-go#24)", total)
	}
	t.Logf("direct SUM on view correct: %v", total)
}

// BenchmarkMaterializePerBatch measures the reliable-but-copying path: register
// the Arrow view, materialize it into a temp table, aggregate, drop. b.N is the
// row count, so ns/op is ns/row -- comparable to BenchmarkSQLTransform, which
// queries the Arrow view directly (fast, but only sound for numeric-only
// schemas).
func BenchmarkMaterializePerBatch(b *testing.B) {
	ctx := context.Background()
	conn, ar := openArrowConn(b)

	const perBatch = 4096
	rec := float64Record(b, "amount", func() []float64 {
		v := make([]float64, perBatch)
		for i := range v {
			v[i] = float64(i%1000) + 0.5
		}
		return v
	}())

	b.ReportAllocs()
	b.ResetTimer()
	for sent := 0; sent < b.N; sent += perBatch {
		release := registerView(b, ar, rec)
		if _, err := conn.ExecContext(ctx, `CREATE OR REPLACE TEMP TABLE mat AS SELECT * FROM probe`); err != nil {
			b.Fatal(err)
		}
		var total float64
		if err := conn.QueryRowContext(ctx, `SELECT SUM(amount) FROM mat`).Scan(&total); err != nil {
			b.Fatal(err)
		}
		release()
	}
}
