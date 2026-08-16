package goetl

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Dialect captures the differences between SQL flavors that matter for bulk
// INSERT. v1 had a near-duplicate writer per database; here the shared work
// lives in SQLWriter and only the flavor-specific fragments are per-dialect.
// Methods append to dst and return it, so rendering a large statement does not
// allocate a string per placeholder.
type Dialect interface {
	// AppendPlaceholder appends the n-th bind parameter, 1-indexed.
	AppendPlaceholder(dst []byte, n int) []byte
	// AppendQuoted appends a quoted identifier.
	AppendQuoted(dst []byte, ident string) []byte
	// AppendUpsert appends the conflict clause, or nothing for a plain insert.
	// updateCols are the columns to overwrite on conflict.
	AppendUpsert(dst []byte, conflictTarget string, updateCols []string) []byte
}

// PostgresDialect renders $N placeholders and ON CONFLICT ... DO UPDATE.
type PostgresDialect struct{}

// AppendPlaceholder implements Dialect.
func (PostgresDialect) AppendPlaceholder(dst []byte, n int) []byte {
	return strconv.AppendInt(append(dst, '$'), int64(n), 10)
}

// AppendQuoted implements Dialect.
func (PostgresDialect) AppendQuoted(dst []byte, id string) []byte {
	return appendQuoted(dst, id, '"')
}

// AppendUpsert implements Dialect.
func (d PostgresDialect) AppendUpsert(dst []byte, target string, cols []string) []byte {
	if target == "" || len(cols) == 0 {
		return dst
	}
	dst = append(dst, " ON CONFLICT ("...)
	dst = append(dst, target...)
	dst = append(dst, ") DO UPDATE SET "...)
	for i, c := range cols {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = d.AppendQuoted(dst, c)
		dst = append(dst, "=EXCLUDED."...)
		dst = d.AppendQuoted(dst, c)
	}
	return dst
}

// MySQLDialect renders ? placeholders and ON DUPLICATE KEY UPDATE.
type MySQLDialect struct{}

// AppendPlaceholder implements Dialect.
func (MySQLDialect) AppendPlaceholder(dst []byte, _ int) []byte { return append(dst, '?') }

// AppendQuoted implements Dialect.
func (MySQLDialect) AppendQuoted(dst []byte, id string) []byte {
	return appendQuoted(dst, id, '`')
}

// AppendUpsert implements Dialect. MySQL has no conflict target, so it is
// ignored.
func (d MySQLDialect) AppendUpsert(dst []byte, _ string, cols []string) []byte {
	if len(cols) == 0 {
		return dst
	}
	dst = append(dst, " ON DUPLICATE KEY UPDATE "...)
	for i, c := range cols {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = d.AppendQuoted(dst, c)
		dst = append(dst, "=VALUES("...)
		dst = d.AppendQuoted(dst, c)
		dst = append(dst, ')')
	}
	return dst
}

// appendQuoted appends ident wrapped in q, doubling any embedded q.
func appendQuoted(dst []byte, ident string, q byte) []byte {
	dst = append(dst, q)
	for i := 0; i < len(ident); i++ {
		if ident[i] == q {
			dst = append(dst, q)
		}
		dst = append(dst, ident[i])
	}
	return append(dst, q)
}

// SQLWriter bulk-inserts batches into a table.
//
// Columns come from the Arrow schema, so unlike v1 there is no pass to union
// and sort keys across payloads, and no missing-key nil fill: the schema is
// known before the first row is read.
type SQLWriter struct {
	NopFlush

	DB      *sql.DB
	Table   string
	Dialect Dialect

	// ConflictTarget is the Postgres conflict target (e.g. "id", or a
	// constraint expression). Ignored by MySQL. Empty means plain INSERT.
	ConflictTarget string

	// UpdateColumns are the columns overwritten on conflict. Empty means every
	// column, matching v1's behavior.
	UpdateColumns []string

	// MaxRows caps rows per INSERT statement. Zero means DefaultInsertRows.
	// Batches larger than this are split.
	MaxRows int

	stmts map[int]*sql.Stmt // prepared statements keyed by row count
	cols  []string
}

// DefaultInsertRows is the default number of rows per INSERT statement.
const DefaultInsertRows = 1000

// NewSQLWriter builds a writer for the given dialect.
func NewSQLWriter(db *sql.DB, table string, d Dialect) *SQLWriter {
	return &SQLWriter{DB: db, Table: table, Dialect: d}
}

// Process implements Processor.
func (w *SQLWriter) Process(ctx context.Context, b *Batch, emit Emit) error {
	if b.NumRows() == 0 {
		return nil
	}
	if w.cols == nil {
		w.cols = make([]string, b.NumCols())
		for i := range w.cols {
			w.cols[i] = b.Schema().Field(i).Name
		}
	}

	max := w.MaxRows
	if max <= 0 {
		max = DefaultInsertRows
	}

	// Resolve each column's value reader once per batch rather than per cell.
	readers := make([]valueReader, b.NumCols())
	for j := range readers {
		r, err := readerFor(b.Record().Column(j))
		if err != nil {
			return fmt.Errorf("column %q: %w", w.cols[j], err)
		}
		readers[j] = r
	}

	total := b.NumRows()
	for start := 0; start < total; start += max {
		n := max
		if r := total - start; r < n {
			n = r
		}
		if err := w.insert(ctx, readers, start, n); err != nil {
			return err
		}
	}
	return nil
}

// insert executes one INSERT covering n rows starting at start.
func (w *SQLWriter) insert(ctx context.Context, readers []valueReader, start, n int) error {
	stmt, err := w.stmtFor(ctx, n)
	if err != nil {
		return err
	}
	args := make([]any, 0, n*len(readers))
	for i := start; i < start+n; i++ {
		for _, r := range readers {
			args = append(args, r(i))
		}
	}
	if _, err := stmt.ExecContext(ctx, args...); err != nil {
		return fmt.Errorf("insert %d rows into %s: %w", n, w.Table, err)
	}
	return nil
}

// stmtFor returns a prepared statement for an n-row INSERT, preparing and
// caching on first use. Batches are usually uniform, so this typically prepares
// once for the full-size shape and once for the final partial batch. v1
// prepared a fresh statement for every batch.
func (w *SQLWriter) stmtFor(ctx context.Context, n int) (*sql.Stmt, error) {
	if s, ok := w.stmts[n]; ok {
		return s, nil
	}
	q := w.buildInsert(n)
	s, err := w.DB.PrepareContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("prepare insert: %w (%s)", err, q)
	}
	if w.stmts == nil {
		w.stmts = map[int]*sql.Stmt{}
	}
	w.stmts[n] = s
	return s, nil
}

// buildInsert renders an INSERT for n rows.
//
// v1 built this with repeated string concatenation inside nested loops, which
// is O(n^2) in the batch size. A single strings.Builder with one grow makes it
// linear.
func (w *SQLWriter) buildInsert(n int) string {
	// One allocation sized for the whole statement; ~8 bytes per placeholder
	// covers "$12345," comfortably.
	b := make([]byte, 0, 64+len(w.Table)+n*len(w.cols)*8)

	b = append(b, "INSERT INTO "...)
	b = append(b, w.Table...)
	b = append(b, " ("...)
	for i, c := range w.cols {
		if i > 0 {
			b = append(b, ',')
		}
		b = w.Dialect.AppendQuoted(b, c)
	}
	b = append(b, ") VALUES "...)

	p := 1
	for row := 0; row < n; row++ {
		if row > 0 {
			b = append(b, ',')
		}
		b = append(b, '(')
		for c := range w.cols {
			if c > 0 {
				b = append(b, ',')
			}
			b = w.Dialect.AppendPlaceholder(b, p)
			p++
		}
		b = append(b, ')')
	}

	upd := w.UpdateColumns
	if len(upd) == 0 {
		upd = w.cols
	}
	b = w.Dialect.AppendUpsert(b, w.ConflictTarget, upd)
	return string(b)
}

// Close releases cached prepared statements.
func (w *SQLWriter) Close() error {
	var first error
	for _, s := range w.stmts {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	w.stmts = nil
	return first
}

func (w *SQLWriter) String() string { return "SQLWriter(" + w.Table + ")" }

// valueReader returns the driver value for row i of one column.
type valueReader func(i int) any

// readerFor resolves an Arrow array to a value reader once, so the per-cell
// path is a direct typed index rather than an interface type switch.
func readerFor(arr arrow.Array) (valueReader, error) {
	switch a := arr.(type) {
	case *array.Int64:
		v := a.Int64Values()
		return func(i int) any {
			if a.IsNull(i) {
				return nil
			}
			return v[i]
		}, nil
	case *array.Uint64:
		v := a.Uint64Values()
		return func(i int) any {
			if a.IsNull(i) {
				return nil
			}
			return v[i]
		}, nil
	case *array.Float64:
		v := a.Float64Values()
		return func(i int) any {
			if a.IsNull(i) {
				return nil
			}
			return v[i]
		}, nil
	case *array.Boolean:
		return func(i int) any {
			if a.IsNull(i) {
				return nil
			}
			return a.Value(i)
		}, nil
	case *array.String:
		return func(i int) any {
			if a.IsNull(i) {
				return nil
			}
			return a.Value(i)
		}, nil
	}
	return nil, fmt.Errorf("unsupported type %s", arr.DataType())
}
