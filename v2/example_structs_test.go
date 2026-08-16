package goetl_test

import (
	"context"
	"fmt"
	"strings"

	goetl "github.com/teambenny/goetl/v2"
)

// Sale mirrors how a v1 user would model a row.
type Sale struct {
	ID     int64   `goetl:"id"`
	Region string  `goetl:"region"`
	Year   int64   `goetl:"year"`
	Amount float64 `goetl:"amount"`
}

// The friendly path: ordinary Go structs, closely mirroring v1's
// FuncTransformer. No column names at call sites, no per-access error handling,
// no Arrow builders. Filtering is just returning a shorter slice.
func ExampleNewStructTransform() {
	var sb strings.Builder

	markup := goetl.NewStructTransform("markup", func(rows []Sale) ([]Sale, error) {
		var out []Sale
		for _, r := range rows {
			if r.Year < 2024 {
				continue // filtering is just not appending
			}
			r.Amount *= 1.08
			out = append(out, r)
		}
		return out, nil
	})

	p := goetl.New(&goetl.GenSource{Rows: 6, PerBatch: 6}, markup, goetl.NewCSVWriter(&sb))
	if err := p.Run(context.Background()); err != nil {
		panic(err)
	}
	fmt.Print(sb.String())
	// Output:
	// id,region,year,amount
	// 4,west,2024,4.86
	// 5,east,2025,5.94
}
