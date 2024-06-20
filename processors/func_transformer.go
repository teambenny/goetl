package processors

import "github.com/will-beep-lamm/goetl/etldata"

// FuncTransformer executes the given function on each data
// payload, sending the resuling data to the next stage.
//
// While FuncTransformer is useful for simple data transformation, more
// complicated tasks justify building a custom implementation of Processor.
type FuncTransformer struct {
	transform        func(d etldata.Payload) etldata.Payload
	Name             string // can be set for more useful log output
	ConcurrencyLevel int    // See ConcurrentProcessor
}

// NewFuncTransformer instantiates a new instance of func transformer
func NewFuncTransformer(transform func(d etldata.Payload) etldata.Payload) *FuncTransformer {
	return &FuncTransformer{transform: transform}
}

// ProcessData runs the supplied func and sends the returned value to outputChan
func (t *FuncTransformer) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	outputChan <- t.transform(d)
}

// Finish - see interface for documentation.
func (t *FuncTransformer) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

func (t *FuncTransformer) String() string {
	if t.Name != "" {
		return t.Name
	}
	return "FuncTransformer"
}

// Concurrency defers to ConcurrentProcessor
func (t *FuncTransformer) Concurrency() int {
	return t.ConcurrencyLevel
}
