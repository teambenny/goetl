package goetl

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/teambenny/goetl/etldata"
)

// Processor is the interface that should be implemented to perform data-related
// tasks within a Pipeline. Processors are responsible for receiving, processing,
// and then sending data on to the next stage of processing.
type Processor interface {
	// ProcessData will be called for each data sent from the previous stage.
	// ProcessData is called with a etldata.Payload instance, which is the data being received,
	// an outputChan, which is the channel to send data to, and a killChan,
	// which is a channel to send unexpected errors to (halting execution of the Pipeline).
	ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error)

	// Finish will be called after the previous stage has finished sending data,
	// and no more data will be received by this Processor. Often times
	// Finish can be an empty function implementation, but sometimes it is
	// necessary to perform final data processing.
	Finish(outputChan chan etldata.Payload, killChan chan error)
}

// DataProcessor is a type used internally to the Pipeline management
// code, and wraps a Processor instance. Processor is the main
// interface that should be implemented to perform work within the data
// pipeline, and this DataProcessor type simply embeds it and adds some
// helpful channel management and other attributes.
type DataProcessor struct {
	Processor
	executionStat
	concurrentProcessor
	chanBrancher
	chanMerger
	outputs    []Processor
	inputChan  chan etldata.Payload
	outputChan chan etldata.Payload
}

type chanBrancher struct {
	branchOutChans []chan etldata.Payload
}

func (dp *DataProcessor) branchOut() {
	go func() {
		for d := range dp.outputChan {
			for _, out := range dp.branchOutChans {
				// Make a copy to ensure concurrent stages
				// can alter data as needed.
				out <- d.Clone()
			}
			dp.recordDataSent(d.Bytes())
		}
		// Once all data is received, also close all the outputs
		for _, out := range dp.branchOutChans {
			close(out)
		}
	}()
}

type chanMerger struct {
	mergeInChans []chan etldata.Payload
	mergeWait    sync.WaitGroup
}

func (dp *DataProcessor) mergeIn() {
	// Start a merge goroutine for each input channel.
	mergeData := func(c chan etldata.Payload) {
		for d := range c {
			dp.inputChan <- d
		}
		dp.mergeWait.Done()
	}
	dp.mergeWait.Add(len(dp.mergeInChans))
	for _, in := range dp.mergeInChans {
		go mergeData(in)
	}

	go func() {
		dp.mergeWait.Wait()
		close(dp.inputChan)
	}()
}

// Do takes a Processor instance and returns the DataProcessor
// type that will wrap it for internal processing. The details
// of the DataProcessor wrapper type are abstracted away from the
// implementing end-user code. The "Do" function is named
// succinctly to provide a nicer syntax when creating a PipelineLayout.
// See the goetl package documentation for code examples of creating
// a new branching pipeline layout.
func Do(processor Processor) *DataProcessor {
	dp := DataProcessor{Processor: processor}
	dp.outputChan = make(chan etldata.Payload)
	dp.inputChan = make(chan etldata.Payload)

	if isConcurrent(processor) {
		dp.concurrency = processor.(ConcurrentProcessor).Concurrency()
		dp.workThrottle = make(chan workSignal, dp.concurrency)
		dp.workList = list.New()
		dp.doneChan = make(chan bool)
		dp.inputClosed = false
	}

	return &dp
}

// Outputs should be called to specify which Processor instances the current
// processor should send it's output to. See the goetl package
// documentation for code examples and diagrams.
func (dp *DataProcessor) Outputs(processors ...Processor) *DataProcessor {
	dp.outputs = processors
	return dp
}

// pass through String output to the Processor
func (dp *DataProcessor) String() string {
	return fmt.Sprintf("%v", dp.Processor)
}
