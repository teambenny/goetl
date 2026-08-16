//go:build duckdb_arrow

// Package duckdbx runs SQL transforms over Arrow batches using an embedded
// DuckDB.
//
// It is deliberately a separate package: DuckDB is C++ and reaches Go through
// cgo, so keeping it out of the core means the core stays pure Go and
// cross-compiles normally. Import this package only if you want SQL in the
// pipeline.
//
// Build with -tags duckdb_arrow; go-duckdb gates its Arrow interface behind
// that tag.
package duckdbx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/marcboeker/go-duckdb/v2"

	goetl "github.com/teambenny/goetl/v2"
)

// Transform executes a SQL query against each incoming batch.
//
// The batch is exposed to the query under View (default "batch") as a zero-copy
// Arrow view: DuckDB reads the Go-owned Arrow buffers directly through the
// Arrow C Data Interface, and results come back the same way. No rows are
// serialized in either direction.
type Transform struct {
	goetl.NopFlush

	// Query is the SQL to run. Reference the incoming batch by View.
	Query string

	// View is the table name the batch is registered under. Zero means "batch".
	View string

	db   *sql.DB
	conn *sql.Conn
	ar   *duckdb.Arrow
}

// Open creates an in-process DuckDB and prepares a Transform. The returned
// close func releases the connection and database.
func Open(ctx context.Context, query string) (*Transform, func() error, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, nil, fmt.Errorf("open duckdb: %w", err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("duckdb conn: %w", err)
	}

	var ar *duckdb.Arrow
	err = conn.Raw(func(dc any) error {
		c, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("not a driver.Conn: %T", dc)
		}
		var e error
		ar, e = duckdb.NewArrowFromConn(c)
		return e
	})
	if err != nil {
		conn.Close()
		db.Close()
		return nil, nil, fmt.Errorf("arrow interface: %w", err)
	}

	t := &Transform{Query: query, db: db, conn: conn, ar: ar}
	closeFn := func() error {
		if err := conn.Close(); err != nil {
			db.Close()
			return err
		}
		return db.Close()
	}
	return t, closeFn, nil
}

// Process implements goetl.Processor.
func (t *Transform) Process(ctx context.Context, b *goetl.Batch, emit goetl.Emit) error {
	view := t.View
	if view == "" {
		view = "batch"
	}

	// Export the batch to DuckDB as an Arrow stream. ExportRecordReader hands
	// over pointers to the existing buffers; nothing is copied.
	rec := b.Record()
	rdr, err := array.NewRecordReader(rec.Schema(), []arrow.Record{rec})
	if err != nil {
		return fmt.Errorf("record reader: %w", err)
	}
	defer rdr.Release()

	release, err := t.ar.RegisterView(rdr, view)
	if err != nil {
		return fmt.Errorf("register view %q: %w", view, err)
	}
	defer release()

	res, err := t.ar.QueryContext(ctx, t.Query)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer res.Release()

	for res.Next() {
		// The result record aliases stream-owned memory that is invalidated
		// when the stream advances or is released, so it must be copied into
		// Go-owned memory before it can travel down an async pipeline. See
		// copy.go for the full explanation.
		out := copyRecord(res.Record(), memory.DefaultAllocator)
		nb := goetl.NewBatch(out)
		err := emit(nb)
		nb.Release()
		if err != nil {
			return err
		}
	}
	if err := res.Err(); err != nil {
		return fmt.Errorf("read results: %w", err)
	}
	return nil
}

func (t *Transform) String() string { return "duckdbx.Transform" }
