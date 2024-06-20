package etldata_test

import (
	"fmt"

	"github.com/will-beep-lamm/goetl/etldata"
)

type testStruct struct {
	A int
	B int
}

func ExampleNewJSON() {
	t := testStruct{A: 1, B: 2}

	d, _ := etldata.NewJSON(t)

	fmt.Println(string(d))
	// Output: {"A":1,"B":2}
}

func ExampleJSONFromHeaderAndRows() {
	header := []string{"A", "B", "C"}
	rows := [][]interface{}{
		[]interface{}{1, 2, 3},
		[]interface{}{4, 5, 6},
	}

	d, _ := etldata.JSONFromHeaderAndRows(header, rows)

	fmt.Println(fmt.Sprintf("%+v", string(d)))
	// Output: [{"A":1,"B":2,"C":3},{"A":4,"B":5,"C":6}]
}
