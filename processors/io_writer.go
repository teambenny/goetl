package processors

import (
	"fmt"
	"io"

	"github.com/will-beep-lamm/goetl/etldata"
	"github.com/will-beep-lamm/goetl/etlutil"
	"github.com/will-beep-lamm/goetl/logger"
)

// IoWriter wraps any io.Writer object.
// It can be used to write data out to a File, os.Stdout, or
// any other task that can be supported via io.Writer.
type IoWriter struct {
	Writer     io.Writer
	AddNewline bool
}

// NewIoWriter returns a new IoWriter wrapping the given io.Writer object
func NewIoWriter(writer io.Writer) *IoWriter {
	return &IoWriter{Writer: writer, AddNewline: false}
}

// ProcessData writes the data
func (w *IoWriter) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	var bytesWritten int
	var err error
	if w.AddNewline {
		bytesWritten, err = fmt.Fprintln(w.Writer, string(d.Bytes()))
	} else {
		bytesWritten, err = w.Writer.Write(d.Bytes())
	}
	etlutil.KillPipelineIfErr(err, killChan)
	logger.Debug("IoWriter:", bytesWritten, "bytes written")
}

// Finish - see interface for documentation.
func (w *IoWriter) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

func (w *IoWriter) String() string {
	return "IoWriter"
}
