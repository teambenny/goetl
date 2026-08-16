package goetl

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// DefaultBatchRows is how many rows accumulate before a batch is emitted.
// Vectorized execution wants batches large enough to amortize per-batch
// overhead; a few thousand rows is the usual sweet spot.
const DefaultBatchRows = 4096

// SQLSource runs a query against any database/sql driver and emits Arrow
// batches. It is pure Go and works with Postgres, MySQL, SQLite, DuckDB, or
// anything else exposing a driver.
//
// The column types are derived from the driver's ColumnTypes, so no schema
// declaration is required.
type SQLSource struct {
	DB    *sql.DB
	Query string
	Args  []any

	// BatchRows sets the emit granularity. Zero means DefaultBatchRows.
	BatchRows int

	// Alloc is the Arrow allocator. Zero means memory.DefaultAllocator.
	Alloc memory.Allocator
}

// NewSQLSource builds a source for a static query.
func NewSQLSource(db *sql.DB, query string, args ...any) *SQLSource {
	return &SQLSource{DB: db, Query: query, Args: args}
}

// Read implements Source.
func (s *SQLSource) Read(ctx context.Context, emit Emit) error {
	alloc := s.Alloc
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	size := s.BatchRows
	if size <= 0 {
		size = DefaultBatchRows
	}

	rows, err := s.DB.QueryContext(ctx, s.Query, s.Args...)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("column types: %w", err)
	}

	schema, err := schemaFromColumnTypes(colTypes)
	if err != nil {
		return err
	}

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	scan := make([]any, len(colTypes))
	holders := make([]any, len(colTypes))
	for i := range scan {
		scan[i] = &holders[i]
	}

	n := 0
	flush := func() error {
		if n == 0 {
			return nil
		}
		rec := bldr.NewRecord()
		b := NewBatch(rec)
		err := emit(b)
		b.Release()
		n = 0
		return err
	}

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := rows.Scan(scan...); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		for i, v := range holders {
			if err := appendValue(bldr.Field(i), v); err != nil {
				return fmt.Errorf("column %q: %w", schema.Field(i).Name, err)
			}
		}
		n++
		if n >= size {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate: %w", err)
	}
	return flush()
}

func (s *SQLSource) String() string { return "SQLSource" }

// schemaFromColumnTypes maps driver column types onto an Arrow schema.
func schemaFromColumnTypes(cts []*sql.ColumnType) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(cts))
	for i, ct := range cts {
		dt, err := arrowTypeFor(ct)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", ct.Name(), err)
		}
		nullable, ok := ct.Nullable()
		fields[i] = arrow.Field{Name: ct.Name(), Type: dt, Nullable: !ok || nullable}
	}
	return arrow.NewSchema(fields, nil), nil
}

// arrowTypeFor picks an Arrow type from the driver's declared scan type.
func arrowTypeFor(ct *sql.ColumnType) (arrow.DataType, error) {
	switch ct.ScanType().Kind().String() {
	case "int8", "int16", "int32", "int64", "int":
		return arrow.PrimitiveTypes.Int64, nil
	case "uint8", "uint16", "uint32", "uint64", "uint":
		return arrow.PrimitiveTypes.Uint64, nil
	case "float32", "float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "bool":
		return arrow.FixedWidthTypes.Boolean, nil
	case "string":
		return arrow.BinaryTypes.String, nil
	}
	// Fall back to the database's own type name for drivers with opaque
	// scan types (interface{} is common).
	switch ct.DatabaseTypeName() {
	case "BIGINT", "INTEGER", "INT", "SMALLINT", "TINYINT":
		return arrow.PrimitiveTypes.Int64, nil
	case "DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC":
		return arrow.PrimitiveTypes.Float64, nil
	case "BOOLEAN", "BOOL":
		return arrow.FixedWidthTypes.Boolean, nil
	case "VARCHAR", "TEXT", "CHAR", "STRING":
		return arrow.BinaryTypes.String, nil
	}
	// Anything else becomes a string; the driver's value is formatted.
	return arrow.BinaryTypes.String, nil
}

// appendValue writes one scanned value into the matching Arrow builder.
func appendValue(fb array.Builder, v any) error {
	if v == nil {
		fb.AppendNull()
		return nil
	}
	switch b := fb.(type) {
	case *array.Int64Builder:
		switch x := v.(type) {
		case int64:
			b.Append(x)
		case int32:
			b.Append(int64(x))
		case int:
			b.Append(int64(x))
		default:
			return fmt.Errorf("cannot append %T as int64", v)
		}
	case *array.Uint64Builder:
		switch x := v.(type) {
		case uint64:
			b.Append(x)
		case int64:
			b.Append(uint64(x))
		default:
			return fmt.Errorf("cannot append %T as uint64", v)
		}
	case *array.Float64Builder:
		switch x := v.(type) {
		case float64:
			b.Append(x)
		case float32:
			b.Append(float64(x))
		case int64:
			b.Append(float64(x))
		default:
			return fmt.Errorf("cannot append %T as float64", v)
		}
	case *array.BooleanBuilder:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("cannot append %T as bool", v)
		}
		b.Append(x)
	case *array.StringBuilder:
		switch x := v.(type) {
		case string:
			b.Append(x)
		case []byte:
			b.Append(string(x))
		default:
			b.Append(fmt.Sprintf("%v", x))
		}
	default:
		return fmt.Errorf("unsupported builder %T", fb)
	}
	return nil
}
