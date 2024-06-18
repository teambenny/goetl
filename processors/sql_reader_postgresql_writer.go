package processors

import (
	"database/sql"

	"github.com/will-beep-lamm/goetl/etldata"
)

// SQLReaderPostgreSQLWriter performs both the job of a SQLReader and PostgreSQLWriter.
// This means it will run a SQL query, write the resulting data into a
// PostgreSQL database, and (if the write was successful) send the queried data
// to the next stage of processing.
//
// SQLReaderPostgreSQLWriter is composed of both a SQLReader and PostgreSQLWriter, so it
// supports all of the same properties and usage options (such as static
// versus dynamic SQL querying).
type SQLReaderPostgreSQLWriter struct {
	SQLReader
	PostgreSQLWriter
	ConcurrencyLevel int // See ConcurrentProcessor
}

// NewSQLReaderPostgreSQLWriter returns a new SQLReaderPostgreSQLWriter ready for static querying.
func NewSQLReaderPostgreSQLWriter(readConn *sql.DB, writeConn *sql.DB, readQuery, writeTable string) *SQLReaderPostgreSQLWriter {
	s := SQLReaderPostgreSQLWriter{}
	s.SQLReader = *NewSQLReader(readConn, readQuery)
	s.PostgreSQLWriter = *NewPostgreSQLWriter(writeConn, writeTable)
	return &s
}

// NewDynamicSQLReaderPostgreSQLWriter returns a new SQLReaderPostgreSQLWriter ready for dynamic querying.
func NewDynamicSQLReaderPostgreSQLWriter(readConn *sql.DB, writeConn *sql.DB, sqlGenerator func(etldata.Payload) (string, error), writeTable string) *SQLReaderPostgreSQLWriter {
	s := NewSQLReaderPostgreSQLWriter(readConn, writeConn, "", writeTable)
	s.sqlGenerator = sqlGenerator
	return s
}

// ProcessData uses SQLReader methods for processing data - this works via composition
func (s *SQLReaderPostgreSQLWriter) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	s.ForEachQueryData(d, killChan, func(d etldata.Payload) {
		s.PostgreSQLWriter.ProcessData(d, outputChan, killChan)
		outputChan <- d
	})
}

// Finish - see interface for documentation.
func (s *SQLReaderPostgreSQLWriter) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

func (s *SQLReaderPostgreSQLWriter) String() string {
	return "SQLReaderPostgreSQLWriter"
}

// Concurrency defers to ConcurrentProcessor
func (s *SQLReaderPostgreSQLWriter) Concurrency() int {
	return s.ConcurrencyLevel
}
