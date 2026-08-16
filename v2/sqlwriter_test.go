package goetl_test

import (
	"strings"
	"testing"

	goetl "github.com/teambenny/goetl/v2"
)

// ---------- SQL generation ----------
//
// Generation is tested exactly and without a database, because this is where
// v1's defects lived: O(n^2) concatenation, and an ON DUPLICATE KEY clause that
// quoted update columns but not the column list.

func newTestWriter(d goetl.Dialect, target string, cols ...string) *goetl.SQLWriter {
	w := goetl.NewSQLWriter(nil, "sales", d)
	w.ConflictTarget = target
	goetl.SetColumnsForTest(w, cols)
	return w
}

func TestBuildInsertPostgres(t *testing.T) {
	w := newTestWriter(goetl.PostgresDialect{}, "id", "id", "amount")
	got := goetl.BuildInsertForTest(w, 2)
	want := `INSERT INTO sales ("id","amount") VALUES ($1,$2),($3,$4)` +
		` ON CONFLICT (id) DO UPDATE SET "id"=EXCLUDED."id","amount"=EXCLUDED."amount"`
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestBuildInsertMySQL(t *testing.T) {
	w := newTestWriter(goetl.MySQLDialect{}, "", "id", "amount")
	got := goetl.BuildInsertForTest(w, 2)
	want := "INSERT INTO sales (`id`,`amount`) VALUES (?,?),(?,?)" +
		" ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`amount`=VALUES(`amount`)"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestBuildInsertPlainWhenNoConflictTarget(t *testing.T) {
	w := newTestWriter(goetl.PostgresDialect{}, "", "id")
	got := goetl.BuildInsertForTest(w, 1)
	if strings.Contains(got, "CONFLICT") {
		t.Errorf("expected plain INSERT, got: %s", got)
	}
}

func TestBuildInsertUpdateColumnsSubset(t *testing.T) {
	w := newTestWriter(goetl.PostgresDialect{}, "id", "id", "amount", "note")
	w.UpdateColumns = []string{"amount"}
	got := goetl.BuildInsertForTest(w, 1)
	if !strings.HasSuffix(got, `DO UPDATE SET "amount"=EXCLUDED."amount"`) {
		t.Errorf("update columns not honored: %s", got)
	}
}

// Identifiers must be quoted, not interpolated raw. v1 wrote the column list
// unquoted, so a column named "order" or "select" produced invalid SQL.
func TestIdentifierQuotingHandlesReservedWords(t *testing.T) {
	w := newTestWriter(goetl.PostgresDialect{}, "", "order", "select")
	got := goetl.BuildInsertForTest(w, 1)
	if !strings.Contains(got, `("order","select")`) {
		t.Errorf("reserved words not quoted: %s", got)
	}
}

// Generation must be linear in the batch size, not quadratic. 5000 rows is
// where v1's repeated concatenation became noticeable.
func BenchmarkBuildInsert5000(b *testing.B) {
	w := newTestWriter(goetl.PostgresDialect{}, "id", "id", "region", "year", "amount")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = goetl.BuildInsertForTest(w, 5000)
	}
}

// Linearity check. v1 built this statement by repeated concatenation inside
// nested loops, which is quadratic in the row count. These two benchmarks
// differ by 10x in n; if generation is linear their ns/op must differ by ~10x,
// not ~100x.
func BenchmarkBuildInsert500(b *testing.B) {
	w := newTestWriter(goetl.PostgresDialect{}, "id", "id", "region", "year", "amount")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = goetl.BuildInsertForTest(w, 500)
	}
}
