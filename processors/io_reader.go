package processors

import (
	"bufio"
	"compress/gzip"
	"io"

	"github.com/will-beep-lamm/goetl/etldata"
	"github.com/will-beep-lamm/goetl/etlutil"
)

// IoReader wraps an io.Reader and reads it.
type IoReader struct {
	Reader     io.Reader
	LineByLine bool // defaults to true
	BufferSize int
	Gzipped    bool
}

// NewIoReader returns a new IoReader wrapping the given io.Reader object.
func NewIoReader(reader io.Reader) *IoReader {
	return &IoReader{Reader: reader, LineByLine: true, BufferSize: 1024}
}

// ProcessData overwrites the reader if the content is Gzipped, then defers to ForEachData
func (r *IoReader) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	if r.Gzipped {
		gzReader, err := gzip.NewReader(r.Reader)
		etlutil.KillPipelineIfErr(err, killChan)
		r.Reader = gzReader
	}
	r.ForEachData(killChan, func(d etldata.Payload) {
		outputChan <- d
	})
}

// Finish - see interface for documentation.
func (r *IoReader) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

// ForEachData either reads by line or by buffered stream, sending the data
// back to the anonymous func that ultimately shoves it onto the outputChan
func (r *IoReader) ForEachData(killChan chan error, foo func(d etldata.Payload)) {
	if r.LineByLine {
		r.scanLines(killChan, foo)
	} else {
		r.bufferedRead(killChan, foo)
	}
}

func (r *IoReader) scanLines(killChan chan error, forEach func(d etldata.Payload)) {
	scanner := bufio.NewScanner(r.Reader)
	for scanner.Scan() {
		forEach(etldata.JSON(scanner.Text()))
	}
	err := scanner.Err()
	etlutil.KillPipelineIfErr(err, killChan)
}

func (r *IoReader) bufferedRead(killChan chan error, forEach func(d etldata.Payload)) {
	reader := bufio.NewReader(r.Reader)
	d := make([]byte, r.BufferSize)
	for {
		n, err := reader.Read(d)
		if err != nil && err != io.EOF {
			killChan <- err
		}
		if n == 0 {
			break
		}
		forEach(etldata.JSON(d))
		d = make([]byte, r.BufferSize)
	}
}

func (r *IoReader) String() string {
	return "IoReader"
}
