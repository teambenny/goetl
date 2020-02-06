package goetl

import (
	"container/list"
	"sync"

	"github.com/teambenny/goetl/etldata"
	"github.com/teambenny/goetl/logger"
)

// ConcurrentProcessor is a Processor that also defines
// a level of concurrency. For example, if Concurrency() returns 2,
// then the pipeline will allow the stage to execute up to 2 ProcessData()
// calls concurrently.
//
// Note that the order of data processing is maintained, meaning that
// when a Processor receives ProcessData calls d1, d2, ..., the resulting data
// payloads sent on the outputChan will be sent in the same order as received.
type ConcurrentProcessor interface {
	Processor
	Concurrency() int
}

// IsConcurrent returns true if the given Processor implements ConcurrentProcessor
func isConcurrent(p Processor) bool {
	_, ok := interface{}(p).(ConcurrentProcessor)
	return ok
}

// DataProcessor embeds concurrentProcessor
type concurrentProcessor struct {
	concurrency  int
	workThrottle chan workSignal
	workList     *list.List
	doneChan     chan bool
	inputClosed  bool
	sync.Mutex
}

type workSignal struct{}

type result struct {
	done       bool
	data       []etldata.Payload
	outputChan chan etldata.Payload
	open       bool
}

func (dp *DataProcessor) processData(d etldata.Payload, killChan chan error) chan bool {
	logger.Debug("DataProcessor: processData", dp, "with concurrency =", dp.concurrency)
	exit := make(chan bool, 1)
	// If no concurrency is needed, simply call stage.ProcessData and return...
	if dp.concurrency <= 1 {
		dp.recordExecution(func() {
			dp.ProcessData(d, dp.outputChan, killChan)
			exit <- true
		})
		return exit
	}
	// ... otherwise process the data in a concurrent queue/pool of goroutines
	logger.Debug("DataProcessor: processData", dp, "waiting for work")
	// wait for room in the queue
	dp.workThrottle <- workSignal{}
	logger.Debug("DataProcessor: processData", dp, "work obtained")
	rc := make(chan etldata.Payload)
	done := make(chan bool)
	// setup goroutine to handle result
	go func() {
		res := result{outputChan: dp.outputChan, data: []etldata.Payload{}, open: true}
		dp.Lock()
		dp.workList.PushBack(&res)
		dp.Unlock()
		logger.Debug("DataProcessor: processData", dp, "waiting to receive data on result chan")
		for {
			select {
			case d, open := <-rc:
				logger.Debug("DataProcessor: processData", dp, "received data on result chan")
				res.data = append(res.data, d)
				// outputChan will need to be closed if the rc chan was closed
				res.open = open
			case <-done:
				res.done = true
				logger.Debug("DataProcessor: processData", dp, "done, releasing work")
				<-dp.workThrottle
				dp.sendResults()
				exit <- true
				return
			}
		}
	}()
	// do normal data processing, passing in new result chan
	// instead of the original outputChan
	go dp.recordExecution(func() {
		dp.ProcessData(d, rc, killChan)
		done <- true
	})

	// wait on processing to complete
	return exit
}

// sendResults handles sending work that is completed, as well as
// guaranteeing a FIFO order of the resulting data sent over the
// original outputChan.
func (dp *DataProcessor) sendResults() {
	dp.Lock()
	logger.Debug("DataProcessor: sendResults checking for valid data to send")
	e := dp.workList.Front()
	for e != nil && e.Value.(*result).done {
		logger.Debug("dataHandler: sendResults sending data")
		res := dp.workList.Remove(e).(*result)
		for _, d := range res.data {
			res.outputChan <- d
		}
		if !res.open {
			logger.Debug("DataProcessor: sendResults closing outputChan")
			close(res.outputChan)
		}
		e = dp.workList.Front()
	}
	dp.Unlock()

	if dp.inputClosed && dp.workList.Len() == 0 {
		dp.doneChan <- true
	}
}
