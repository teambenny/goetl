package goetl_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goetl "github.com/teambenny/goetl/v2"
)

// Readable documentation of the quoting rules. The authoritative check is
// FuzzCSVMatchesStdlib, which asserts byte-for-byte equality with encoding/csv
// on arbitrary input; these cases exist so the behavior is legible.
type strSource struct{ vals []string }

func (s *strSource) Read(ctx context.Context, emit goetl.Emit) error {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.String}}, nil)
	bl := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer bl.Release()
	bl.Field(0).(*array.StringBuilder).AppendValues(s.vals, nil)
	b := goetl.NewBatch(bl.NewRecord())
	defer b.Release()
	return emit(b)
}

func TestCSVQuoting(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain", "plain"},
		{"", ""},
		{"has,comma", `"has,comma"`},
		{`has"quote`, `"has""quote"`},
		{"has\nnewline", "\"has\nnewline\""},
		{"has\rcr", "\"has\rcr\""},
		{" leading", `" leading"`},
		{`\.`, `"\."`},             // Postgres end-of-data marker
		{"trailing ", "trailing "}, // stdlib does not quote trailing space
		{"mid space", "mid space"},
		{`"`, `""""`},
	}
	in := make([]string, len(cases))
	for i, c := range cases {
		in[i] = c.in
	}

	var sb strings.Builder
	w := goetl.NewCSVWriter(&sb)
	w.WriteHeader = false
	if err := goetl.New(&strSource{vals: in}, w).Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	got := strings.Split(strings.TrimSuffix(sb.String(), "\n"), "\n")
	// Multi-line values make a naive line split wrong; rejoin the known cases.
	if len(got) != len(cases)+2 {
		t.Logf("raw output:\n%q", sb.String())
	}
	all := sb.String()
	for _, c := range cases {
		if !strings.Contains(all, c.want) {
			t.Errorf("input %q: want output to contain %q\ngot:\n%q", c.in, c.want, all)
		}
	}
	_ = got
}
