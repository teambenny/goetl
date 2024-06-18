package etlutil

import (
	"bufio"
	"bytes"
	"fmt"
	"sort"

	"github.com/will-beep-lamm/goetl/etldata"
)

// CSVString returns an empty string for nil values to make sure that the
// text "null" is not written to a file
func CSVString(v interface{}) string {
	switch v.(type) {
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

// CSVParameters allows you to define all of your csv writing preferences in a
// single struct for reuse in multiple processors
type CSVParameters struct {
	Writer        *CSVWriter
	WriteHeader   bool
	HeaderWritten bool
	Header        []string
	SendUpstream  bool
	QuoteEscape   string
	Comma         rune
}

// CSVProcess writes the contents to the file and optionally sends the written bytes
// upstream on outputChan
func CSVProcess(params *CSVParameters, d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	objects, err := d.Objects()
	KillPipelineIfErr(err, killChan)

	if params.Header == nil {
		for k := range objects[0] {
			params.Header = append(params.Header, k)
		}
		sort.Strings(params.Header)
	}

	rows := [][]string{}

	if params.WriteHeader && !params.HeaderWritten {
		headerRow := []string{}
		for _, k := range params.Header {
			headerRow = append(headerRow, CSVString(k))
		}
		rows = append(rows, headerRow)
		params.HeaderWritten = true
	}

	for _, object := range objects {
		row := []string{}
		for i := range params.Header {
			v := object[params.Header[i]]
			row = append(row, CSVString(v))
		}
		rows = append(rows, row)
	}

	if params.Comma != 0 {
		params.Writer.Comma = params.Comma
	}

	if params.SendUpstream {
		var b bytes.Buffer
		params.Writer.SetWriter(bufio.NewWriter(&b))

		err = params.Writer.WriteAll(rows)
		KillPipelineIfErr(err, killChan)

		outputChan <- etldata.JSON([]byte(b.String()))
	} else {
		err = params.Writer.WriteAll(rows)
		KillPipelineIfErr(err, killChan)
	}
}
