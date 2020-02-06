package goetl

import (
	"time"
)

type executionStat struct {
	dataSentCounter     int
	dataReceivedCounter int
	executionsCounter   int
	totalExecutionTime  float64
	avgExecutionTime    float64
	totalBytesReceived  int
	avgBytesReceived    int
	totalBytesSent      int
	avgBytesSent        int
}

func (s *executionStat) recordExecution(foo func()) {
	s.executionsCounter++
	st := time.Now()
	foo()
	s.totalExecutionTime += time.Now().Sub(st).Seconds()
}

func (s *executionStat) recordDataSent(b []byte) {
	s.dataSentCounter++
	s.totalBytesSent += len(b)
}

func (s *executionStat) recordDataReceived(b []byte) {
	s.dataReceivedCounter++
	s.totalBytesReceived += len(b)
}

func (s *executionStat) calculate() {
	if s.executionsCounter > 0 {
		s.avgExecutionTime = (s.totalExecutionTime / float64(s.executionsCounter))
	}
	if s.dataReceivedCounter > 0 {
		s.avgBytesReceived = (s.totalBytesReceived / s.dataReceivedCounter)
	}
	if s.dataSentCounter > 0 {
		s.avgBytesSent = (s.totalBytesSent / s.dataSentCounter)
	}
}
