package processors

import (
	"io"

	"github.com/will-beep-lamm/goetl/etldata"
)

// IoReaderWriter performs both the job of a IoReader and IoWriter.
// It will read data from the given io.Reader, write the resulting data to
// the given io.Writer, and (if the write was successful) send the data
// to the next stage of processing.
//
// IoReaderWriter is composed of both a IoReader and IoWriter, so it
// supports all of the same properties and usage options.
type IoReaderWriter struct {
	IoReader
	IoWriter
}

// NewIoReaderWriter returns a new IoReaderWriter wrapping the given io.Reader object
func NewIoReaderWriter(reader io.Reader, writer io.Writer) *IoReaderWriter {
	r := IoReaderWriter{}
	r.IoReader = *NewIoReader(reader)
	r.IoWriter = *NewIoWriter(writer)
	return &r
}

// ProcessData grabs data from IoReader.ForEachData, then sends it to IoWriter.ProcessData in addition
// to sending it upstream on the outputChan
func (r *IoReaderWriter) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	r.ForEachData(killChan, func(d etldata.Payload) {
		r.IoWriter.ProcessData(d, outputChan, killChan)
		outputChan <- d
	})
}

// Finish - see interface for documentation.
func (r *IoReaderWriter) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

func (r *IoReaderWriter) String() string {
	return "IoReaderWriter"
}
