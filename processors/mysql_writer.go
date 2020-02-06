package processors

import (
	"database/sql"

	"github.com/teambenny/goetl/etldata"
	"github.com/teambenny/goetl/etlutil"
	"github.com/teambenny/goetl/logger"
)

// MySQLWriter handles INSERTing etldata.JSON into a
// specified SQL table. If an error occurs while building
// or executing the INSERT, the error will be sent to the killChan.
//
// Note that the etldata.JSON must be a valid JSON object or a slice
// of valid objects, where the keys are column names and the
// the values are the SQL values to be inserted into those columns.
//
// For use-cases where a MySQLWriter instance needs to write to
// multiple tables you can pass in SQLWriterData.
type MySQLWriter struct {
	writeDB          *sql.DB
	TableName        string
	OnDupKeyUpdate   bool
	OnDupKeyFields   []string
	ConcurrencyLevel int // See ConcurrentProcessor
	BatchSize        int
}

// NewMySQLWriter returns a new MySQLWriter
func NewMySQLWriter(db *sql.DB, tableName string) *MySQLWriter {
	return &MySQLWriter{writeDB: db, TableName: tableName, OnDupKeyUpdate: true}
}

// ProcessData defers to etlutil.MySQLInsertData
func (s *MySQLWriter) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	// handle panics a bit more gracefully
	defer func() {
		if err := recover(); err != nil {
			etlutil.KillPipelineIfErr(err.(error), killChan)
		}
	}()

	// First check for SQLWriterData
	var wd SQLWriterData
	err := d.ParseSilent(&wd)
	logger.Info("MySQLWriter: Writing data...")
	if err == nil && wd.TableName != "" && wd.InsertData != nil {
		logger.Debug("MySQLWriter: SQLWriterData scenario")
		dd, err := etldata.NewJSON(wd.InsertData)
		etlutil.KillPipelineIfErr(err, killChan)
		err = etlutil.MySQLInsertData(s.writeDB, dd, wd.TableName, s.OnDupKeyUpdate, s.OnDupKeyFields, s.BatchSize)
		etlutil.KillPipelineIfErr(err, killChan)
	} else {
		logger.Debug("MySQLWriter: normal data scenario")
		err = etlutil.MySQLInsertData(s.writeDB, d, s.TableName, s.OnDupKeyUpdate, s.OnDupKeyFields, s.BatchSize)
		etlutil.KillPipelineIfErr(err, killChan)
	}
	logger.Info("MySQLWriter: Write complete")
}

// Finish - see interface for documentation.
func (s *MySQLWriter) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

func (s *MySQLWriter) String() string {
	return "MySQLWriter"
}

// Concurrency defers to ConcurrentProcessor
func (s *MySQLWriter) Concurrency() int {
	return s.ConcurrencyLevel
}
