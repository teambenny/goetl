package processors

import (
	"database/sql"

	"github.com/will-beep-lamm/goetl/etldata"
)

// SQLReaderMySQLWriter performs both the job of a SQLReader and MySQLWriter.
// This means it will run a SQL query, write the resulting data into a
// MySQL database, and (if the write was successful) send the queried data
// to the next stage of processing.
//
// SQLReaderMySQLWriter is composed of both a SQLReader and MySQLWriter, so it
// supports all of the same properties and usage options (such as static
// versus dynamic SQL querying).
type SQLReaderMySQLWriter struct {
	SQLReader
	MySQLWriter
	ConcurrencyLevel int // See ConcurrentProcessor
}

// NewSQLReaderMySQLWriter returns a new SQLReaderMySQLWriter ready for static querying.
func NewSQLReaderMySQLWriter(readConn *sql.DB, writeConn *sql.DB, readQuery, writeTable string) *SQLReaderMySQLWriter {
	s := SQLReaderMySQLWriter{}
	s.SQLReader = *NewSQLReader(readConn, readQuery)
	s.MySQLWriter = *NewMySQLWriter(writeConn, writeTable)
	return &s
}

// NewDynamicSQLReaderMySQLWriter returns a new SQLReaderMySQLWriter ready for dynamic querying.
func NewDynamicSQLReaderMySQLWriter(readConn *sql.DB, writeConn *sql.DB, sqlGenerator func(etldata.Payload) (string, error), writeTable string) *SQLReaderMySQLWriter {
	s := NewSQLReaderMySQLWriter(readConn, writeConn, "", writeTable)
	s.sqlGenerator = sqlGenerator
	return s
}

// ProcessData uses SQLReader methods for processing data - this works via composition
func (s *SQLReaderMySQLWriter) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	s.ForEachQueryData(d, killChan, func(d etldata.Payload) {
		s.MySQLWriter.ProcessData(d, outputChan, killChan)
		outputChan <- d
	})
}

// Finish - see interface for documentation.
func (s *SQLReaderMySQLWriter) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

func (s *SQLReaderMySQLWriter) String() string {
	return "SQLReaderMySQLWriter"
}

// Concurrency defers to ConcurrentProcessor
func (s *SQLReaderMySQLWriter) Concurrency() int {
	return s.ConcurrencyLevel
}
