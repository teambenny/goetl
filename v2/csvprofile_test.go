package goetl_test

import (
	"bufio"
	"encoding/csv"
	"strconv"
	"testing"
)

// Where does CSV time actually go: the CSV framing, or number formatting?
// Each benchmark is per value, so the numbers are directly comparable.

var sinkStr string
var sinkBuf []byte

type nullw struct{}

func (nullw) Write(p []byte) (int, error) { return len(p), nil }

// Shortest-round-trip float formatting, allocating a string per value. This is
// what the current CSVWriter does.
func BenchmarkFormatFloatShortest(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sinkStr = strconv.FormatFloat(float64(i%1000)+0.5, 'g', -1, 64)
	}
}

// Same formatting, appended into a reused buffer instead of allocating.
func BenchmarkAppendFloatShortest(b *testing.B) {
	buf := make([]byte, 0, 64)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = strconv.AppendFloat(buf[:0], float64(i%1000)+0.5, 'g', -1, 64)
	}
	sinkBuf = buf
}

// Fixed precision instead of shortest-round-trip.
func BenchmarkAppendFloatFixed2(b *testing.B) {
	buf := make([]byte, 0, 64)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = strconv.AppendFloat(buf[:0], float64(i%1000)+0.5, 'f', 2, 64)
	}
	sinkBuf = buf
}

func BenchmarkAppendInt(b *testing.B) {
	buf := make([]byte, 0, 64)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = strconv.AppendInt(buf[:0], int64(i), 10)
	}
	sinkBuf = buf
}

// stdlib csv.Writer framing cost, with formatting hoisted out of the loop so
// only the CSV machinery is measured. Per row of 4 fields.
func BenchmarkStdlibCSVFraming(b *testing.B) {
	w := csv.NewWriter(bufio.NewWriterSize(nullw{}, 64<<10))
	row := []string{"0", "west", "2020", "0.5"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := w.Write(row); err != nil {
			b.Fatal(err)
		}
	}
	w.Flush()
}

// Hand-rolled framing: append fields and separators directly to a buffer.
func BenchmarkDirectFraming(b *testing.B) {
	bw := bufio.NewWriterSize(nullw{}, 64<<10)
	fields := [][]byte{[]byte("0"), []byte("west"), []byte("2020"), []byte("0.5")}
	buf := make([]byte, 0, 4<<10)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j, f := range fields {
			if j > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, f...)
		}
		buf = append(buf, '\n')
		if len(buf) > 3<<10 {
			bw.Write(buf)
			buf = buf[:0]
		}
	}
	bw.Write(buf)
	bw.Flush()
}
