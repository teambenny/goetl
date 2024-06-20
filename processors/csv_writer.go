package processors

import (
	"io"

	"github.com/will-beep-lamm/goetl/etldata"
	"github.com/will-beep-lamm/goetl/etlutil"
)

// CSVWriter is handles converting etldata.JSON objects into CSV format,
// and writing them to the given io.Writer. The Data
// must be a valid JSON object or a slice of valid JSON objects.
// If you already have Data formatted as a CSV string you can
// use an IoWriter instead.
type CSVWriter struct {
	Parameters etlutil.CSVParameters
}

// NewCSVWriter returns a new CSVWriter wrapping the given io.Writer object
func NewCSVWriter(w io.Writer) *CSVWriter {
	writer := etlutil.NewCSVWriter()
	writer.SetWriter(w)

	return &CSVWriter{
		Parameters: etlutil.CSVParameters{
			Writer:        writer,
			WriteHeader:   true,
			HeaderWritten: false,
			SendUpstream:  false,
		},
	}
}

// ProcessData defers to etlutil.CSVProcess
func (w *CSVWriter) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	etlutil.CSVProcess(&w.Parameters, d, outputChan, killChan)
}

// Finish - see interface for documentation.
func (w *CSVWriter) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

func (w *CSVWriter) String() string {
	return "CSVWriter"
}
