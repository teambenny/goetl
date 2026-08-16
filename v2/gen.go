package goetl

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// GenSource emits synthetic batches with a fixed schema:
//
//	id int64, region string, year int64, amount float64
//
// It exists so benchmarks can measure runtime and transform cost without an I/O
// source in the way. Batches are built once and re-emitted, so the generator
// itself contributes almost nothing to the measurement.
type GenSource struct {
	Rows     int // total rows to emit
	PerBatch int // rows per batch; zero means DefaultBatchRows
	Alloc    memory.Allocator
}

// GenSchema is the schema produced by GenSource.
var GenSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "region", Type: arrow.BinaryTypes.String},
	{Name: "year", Type: arrow.PrimitiveTypes.Int64},
	{Name: "amount", Type: arrow.PrimitiveTypes.Float64},
}, nil)

var genRegions = []string{"west", "east", "north", "south"}

// Read implements Source.
func (g *GenSource) Read(ctx context.Context, emit Emit) error {
	alloc := g.Alloc
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	per := g.PerBatch
	if per <= 0 {
		per = DefaultBatchRows
	}

	for sent := 0; sent < g.Rows; sent += per {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := per
		if rem := g.Rows - sent; rem < n {
			n = rem
		}
		b := g.build(alloc, sent, n)
		err := emit(b)
		b.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

// build materializes one batch of n rows starting at the given offset.
func (g *GenSource) build(alloc memory.Allocator, offset, n int) *Batch {
	bldr := array.NewRecordBuilder(alloc, GenSchema)
	defer bldr.Release()

	ids := bldr.Field(0).(*array.Int64Builder)
	regions := bldr.Field(1).(*array.StringBuilder)
	years := bldr.Field(2).(*array.Int64Builder)
	amounts := bldr.Field(3).(*array.Float64Builder)

	ids.Reserve(n)
	regions.Reserve(n)
	years.Reserve(n)
	amounts.Reserve(n)

	for i := 0; i < n; i++ {
		k := offset + i
		ids.Append(int64(k))
		regions.Append(genRegions[k%len(genRegions)])
		years.Append(int64(2020 + k%6))
		amounts.Append(float64(k%1000) + 0.5)
	}
	return NewBatch(bldr.NewRecord())
}

func (g *GenSource) String() string { return "GenSource" }
