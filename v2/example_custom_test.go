package goetl_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goetl "github.com/teambenny/goetl/v2"
)

// These live in an external test package (goetl_test), so everything here is
// reachable through the exported API only. Custom processors are ordinary
// types satisfying goetl.Processor; nothing built in is privileged.
//
//	type Processor interface {
//	    Process(ctx context.Context, b *Batch, emit Emit) error
//	    Flush(ctx context.Context, emit Emit) error
//	}

// ---------- 1. stateful aggregation across batches ----------

// RegionTotals accumulates across every batch and emits one summary batch from
// Flush. State is just struct fields; Flush is guaranteed to run exactly once
// after the upstream stage closes.
type RegionTotals struct {
	totals map[string]float64
	order  []string
}

func (r *RegionTotals) Process(ctx context.Context, b *goetl.Batch, emit goetl.Emit) error {
	region, err := goetl.StringColumn(b, "region")
	if err != nil {
		return err
	}
	amount, err := goetl.Column[float64](b, "amount")
	if err != nil {
		return err
	}
	if r.totals == nil {
		r.totals = map[string]float64{}
	}
	for i := range amount {
		k := region.Value(i)
		if _, seen := r.totals[k]; !seen {
			r.order = append(r.order, k)
		}
		r.totals[k] += amount[i]
	}
	return nil
}

func (r *RegionTotals) Flush(ctx context.Context, emit goetl.Emit) error {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String},
		{Name: "total", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	bl := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bl.Release()
	for _, k := range r.order {
		bl.Field(0).(*array.StringBuilder).Append(k)
		bl.Field(1).(*array.Float64Builder).Append(r.totals[k])
	}
	out := goetl.NewBatch(bl.NewRecord())
	defer out.Release()
	return emit(out)
}

func ExampleProcessor_statefulAggregation() {
	var sb strings.Builder
	p := goetl.New(
		&goetl.GenSource{Rows: 8, PerBatch: 3}, // 3 batches: 3, 3, 2
		&RegionTotals{},
		goetl.NewCSVWriter(&sb),
	)
	if err := p.Run(context.Background()); err != nil {
		panic(err)
	}
	fmt.Print(sb.String())
	// Output:
	// region,total
	// west,5
	// east,7
	// north,9
	// south,11
}

// ---------- 2. filtering rows ----------

// Dropping rows means building a new batch, because Arrow arrays are fixed
// length. goetl.Filter does the column copying, so a filter is just the
// predicate. Written by hand this same processor was ~45 lines of type switch
// over every column type.
func ExampleNewFilter() {
	var sb strings.Builder
	// GenSource years cycle 2020..2025.
	recent := goetl.NewFilter("recent", func(b *goetl.Batch) ([]bool, error) {
		year, err := goetl.Column[int64](b, "year")
		if err != nil {
			return nil, err
		}
		keep := make([]bool, len(year))
		for i, y := range year {
			keep[i] = y >= 2024
		}
		return keep, nil
	})

	p := goetl.New(&goetl.GenSource{Rows: 6, PerBatch: 6}, recent, goetl.NewCSVWriter(&sb))
	if err := p.Run(context.Background()); err != nil {
		panic(err)
	}
	fmt.Print(sb.String())
	// Output:
	// id,region,year,amount
	// 4,west,2024,4.5
	// 5,east,2025,5.5
}

// ---------- 3. external enrichment (the Tier 3 case) ----------

// Enricher calls out to something the query engine cannot: an HTTP API, a
// model, a cache. This is the category that must be Go and cannot be SQL.
type Enricher struct {
	goetl.NopFlush
	lookup func(region string) string // stands in for a network call
}

func (e *Enricher) Process(ctx context.Context, b *goetl.Batch, emit goetl.Emit) error {
	region, err := goetl.StringColumn(b, "region")
	if err != nil {
		return err
	}
	n := b.NumRows()

	bl := array.NewStringBuilder(memory.DefaultAllocator)
	defer bl.Release()
	for i := 0; i < n; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		bl.Append(e.lookup(region.Value(i)))
	}
	arr := bl.NewArray()
	defer arr.Release()

	out, err := goetl.AppendColumn(b, arrow.Field{
		Name: "zone", Type: arrow.BinaryTypes.String,
	}, arr)
	if err != nil {
		return err
	}
	defer out.Release()
	return emit(out)
}

func ExampleProcessor_externalEnrichment() {
	var sb strings.Builder
	zones := map[string]string{"west": "us-w", "east": "us-e", "north": "ca", "south": "mx"}
	p := goetl.New(
		&goetl.GenSource{Rows: 2, PerBatch: 2},
		&Enricher{lookup: func(r string) string { return zones[r] }},
		goetl.NewCSVWriter(&sb),
	)
	if err := p.Run(context.Background()); err != nil {
		panic(err)
	}
	fmt.Print(sb.String())
	// Output:
	// id,region,year,amount,zone
	// 0,west,2020,0.5,us-w
	// 1,east,2021,1.5,us-e
}

// ---------- 4. a custom source ----------

// CountSource shows the Source side of the API: produce batches from anything.
type CountSource struct{ N int }

func (c *CountSource) Read(ctx context.Context, emit goetl.Emit) error {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	bl := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bl.Release()
	for i := 0; i < c.N; i++ {
		bl.Field(0).(*array.Int64Builder).Append(int64(i))
	}
	b := goetl.NewBatch(bl.NewRecord())
	defer b.Release()
	return emit(b)
}

func ExampleSource() {
	var sb strings.Builder
	if err := goetl.New(&CountSource{N: 3}, goetl.NewCSVWriter(&sb)).Run(context.Background()); err != nil {
		panic(err)
	}
	fmt.Print(sb.String())
	// Output:
	// n
	// 0
	// 1
	// 2
}
