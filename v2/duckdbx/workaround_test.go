//go:build duckdb_arrow

package duckdbx_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/duckdb/duckdb-go/v2"
)

// stringRecord builds (region VARCHAR, amount DOUBLE) with west=1+3, east=2+4.
func stringRecord(t testing.TB) arrow.Record {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String},
		{Name: "amount", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	t.Cleanup(b.Release)
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"west", "east", "west", "east"}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4}, nil)
	rec := b.NewRecord()
	t.Cleanup(rec.Release)
	return rec
}

// WORKAROUND A: register the Arrow view (the zero-copy IN direction), but read
// results back through database/sql instead of the Arrow OUT path.
//
// This costs nothing architecturally, because DuckDB -> Go can never be
// zero-copy anyway: result records alias stream-owned memory and must be copied
// regardless. If this is correct, the varchar defect is a routing detail rather
// than a blocker.
func TestWorkaroundDatabaseSQLResults(t *testing.T) {
	db, ar := openArrow(t)
	defer registerView(t, ar, stringRecord(t))()

	rows, err := db.QueryContext(context.Background(),
		`SELECT region, SUM(amount) AS total FROM probe GROUP BY region ORDER BY region`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	got := map[string]float64{}
	for rows.Next() {
		var region string
		var total float64
		if err := rows.Scan(&region, &total); err != nil {
			t.Fatal(err)
		}
		got[region] = total
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if got["east"] != 6 || got["west"] != 4 {
		t.Fatalf("got %v, want map[east:6 west:4]", got)
	}
	t.Logf("database/sql results correct with varchar from Arrow view: %v", got)
}

// WORKAROUND C: does the numeric corruption also disappear on the
// database/sql path? Sums amount from a view that contains a varchar, without
// referencing the varchar -- the case that silently returns a denormal through
// the Arrow path.
func TestWorkaroundNumericFromMixedView(t *testing.T) {
	t.Skip("duckdb/duckdb-go#24: direct aggregation on an Arrow view is corrupt on the database/sql path too")

	db, ar := openArrow(t)
	defer registerView(t, ar, stringRecord(t))()

	var total float64
	if err := db.QueryRowContext(context.Background(),
		`SELECT SUM(amount) FROM probe`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 10 {
		t.Fatalf("total = %v, want 10", total)
	}
	t.Logf("database/sql numeric-from-mixed-view correct: %v", total)
}

var _ = duckdb.Arrow{}
