package processors

import (
	"github.com/teambenny/goetl/etldata"
	"github.com/teambenny/goetl/etlutil"
)

// CSVTransformer converts etldata.Payload objects into a CSV string object
// and sends it on to the next stage. In use-cases where
// you simply want to write to a CSV file, use CSVWriter instead.
//
// CSVTransformer is for more complex use-cases where you need to
// generate CSV data and perhaps send it to multiple output stages.
type CSVTransformer struct {
	Parameters etlutil.CSVParameters
}

// NewCSVTransformer returns a new CSVTransformer wrapping the given io.Writer object
func NewCSVTransformer() *CSVTransformer {
	return &CSVTransformer{
		Parameters: etlutil.CSVParameters{
			Writer:        etlutil.NewCSVWriter(),
			WriteHeader:   true,
			HeaderWritten: false,
			SendUpstream:  true,
		},
	}
}

// ProcessData defers to etlutil.CSVProcess
func (w *CSVTransformer) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	etlutil.CSVProcess(&w.Parameters, d, outputChan, killChan)
}

// Finish - see interface for documentation.
func (w *CSVTransformer) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

func (w *CSVTransformer) String() string {
	return "CSVTransformer"
}
