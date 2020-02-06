package processors

import (
	"database/sql"

	"github.com/teambenny/goetl/etldata"
	"github.com/teambenny/goetl/etlutil"
	"github.com/teambenny/goetl/logger"
)

// PostgreSQLWriter handles INSERTing etldata.JSON into a
// specified SQL table. If an error occurs while building
// or executing the INSERT, the error will be sent to the killChan.
//
// Note that the etldata.JSON must be a valid JSON object or a slice
// of valid objects, where the keys are column names and the
// the values are the SQL values to be inserted into those columns.
//
// For use-cases where a PostgreSQLWriter instance needs to write to
// multiple tables you can pass in SQLWriterData.
//
// Note that if `OnDupKeyUpdate` is true (the default), you *must*
// provide a value for `OnDupKeyIndex` (which is the PostgreSQL
// conflict target).
type PostgreSQLWriter struct {
	writeDB          *sql.DB
	TableName        string
	OnDupKeyUpdate   bool
	OnDupKeyIndex    string // The conflict target: see https://www.postgresql.org/docs/9.5/static/sql-insert.html
	OnDupKeyFields   []string
	ConcurrencyLevel int // See ConcurrentProcessor
	BatchSize        int
}

// NewPostgreSQLWriter returns a new PostgreSQLWriter
func NewPostgreSQLWriter(db *sql.DB, tableName string) *PostgreSQLWriter {
	return &PostgreSQLWriter{writeDB: db, TableName: tableName, OnDupKeyUpdate: true}
}

// ProcessData defers to etlutil.PostgreSQLInsertData
func (s *PostgreSQLWriter) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	// handle panics a bit more gracefully
	defer func() {
		if err := recover(); err != nil {
			etlutil.KillPipelineIfErr(err.(error), killChan)
		}
	}()

	// First check for SQLWriterData
	var wd SQLWriterData
	err := d.ParseSilent(&wd)
	logger.Info("PostgreSQLWriter: Writing data...")
	if err == nil && wd.TableName != "" && wd.InsertData != nil {
		logger.Debug("PostgreSQLWriter: SQLWriterData scenario")
		dd, err := etldata.NewJSON(wd.InsertData)
		etlutil.KillPipelineIfErr(err, killChan)
		err = etlutil.PostgreSQLInsertData(s.writeDB, dd, wd.TableName, s.OnDupKeyUpdate, s.OnDupKeyIndex, s.OnDupKeyFields, s.BatchSize)
		etlutil.KillPipelineIfErr(err, killChan)
	} else {
		logger.Debug("PostgreSQLWriter: normal data scenario")
		err = etlutil.PostgreSQLInsertData(s.writeDB, d, s.TableName, s.OnDupKeyUpdate, s.OnDupKeyIndex, s.OnDupKeyFields, s.BatchSize)
		etlutil.KillPipelineIfErr(err, killChan)
	}
	logger.Info("PostgreSQLWriter: Write complete")
}

// Finish - see interface for documentation.
func (s *PostgreSQLWriter) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

func (s *PostgreSQLWriter) String() string {
	return "PostgreSQLWriter"
}

// Concurrency defers to ConcurrentProcessor
func (s *PostgreSQLWriter) Concurrency() int {
	return s.ConcurrencyLevel
}
