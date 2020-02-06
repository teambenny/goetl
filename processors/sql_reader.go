package processors

import (
	"database/sql"
	"errors"

	"github.com/teambenny/goetl/etldata"
	"github.com/teambenny/goetl/etlutil"
	"github.com/teambenny/goetl/logger"
)

// SQLReader runs the given SQL and passes the resulting data
// to the next stage of processing.
//
// It can operate in 2 modes:
// 1) Static - runs the given SQL query and ignores any received data.
// 2) Dynamic - generates a SQL query for each data payload it receives.
//
// The dynamic SQL generation is implemented by passing in a "sqlGenerator"
// function to NewDynamicSQLReader. This allows you to write whatever code is
// needed to generate SQL based upon data flowing through the pipeline.
type SQLReader struct {
	readDB            *sql.DB
	query             string
	sqlGenerator      func(etldata.Payload) (string, error)
	BatchSize         int
	StructDestination interface{}
	ConcurrencyLevel  int // See ConcurrentProcessor
}

type dataErr struct {
	Error string
}

// NewSQLReader returns a new SQLReader operating in static mode.
func NewSQLReader(dbConn *sql.DB, sql string) *SQLReader {
	return &SQLReader{readDB: dbConn, query: sql, BatchSize: 1000}
}

// NewDynamicSQLReader returns a new SQLReader operating in dynamic mode.
func NewDynamicSQLReader(dbConn *sql.DB, sqlGenerator func(etldata.Payload) (string, error)) *SQLReader {
	return &SQLReader{readDB: dbConn, sqlGenerator: sqlGenerator, BatchSize: 1000}
}

// ProcessData - see interface for documentation.
func (s *SQLReader) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	s.ForEachQueryData(d, killChan, func(d etldata.Payload) {
		outputChan <- d
	})
}

// ForEachQueryData handles generating the SQL (in case of dynamic mode),
// running the query and retrieving the data in etldata.JSON format, and then
// passing the results back witih the function call to forEach.
func (s *SQLReader) ForEachQueryData(d etldata.Payload, killChan chan error, forEach func(d etldata.Payload)) {
	sql := ""
	var err error
	if s.query == "" && s.sqlGenerator != nil {
		sql, err = s.sqlGenerator(d)
		etlutil.KillPipelineIfErr(err, killChan)
	} else if s.query != "" {
		sql = s.query
	} else {
		killChan <- errors.New("SQLReader: must have either static query or sqlGenerator func")
	}

	logger.Debug("SQLReader: Running - ", sql)
	// See sql.go
	dataChan, err := etlutil.GetDataFromSQLQuery(s.readDB, sql, s.BatchSize, s.StructDestination)
	etlutil.KillPipelineIfErr(err, killChan)

	for d := range dataChan {
		// First check if an error was returned back from the SQL processing
		// helper, then if not call forEach with the received data.
		var derr dataErr
		if err := d.ParseSilent(&derr); err == nil {
			etlutil.KillPipelineIfErr(errors.New(derr.Error), killChan)
		} else {
			forEach(d)
		}
	}
}

// Finish - see interface for documentation.
func (s *SQLReader) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

func (s *SQLReader) String() string {
	return "SQLReader"
}

// Concurrency defers to ConcurrentProcessor
func (s *SQLReader) Concurrency() int {
	return s.ConcurrencyLevel
}
