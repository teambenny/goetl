package processors

import (
	bigquery "github.com/dailyburn/bigquery/client"
	"github.com/will-beep-lamm/goetl/etldata"
	"github.com/will-beep-lamm/goetl/etlutil"
	"github.com/will-beep-lamm/goetl/logger"
)

// BigQueryWriter is used to write data to Google's BigQuery. If the table you want to
// write to already exists, use NewBigQueryWriter, otherwise use NewBigQueryWriterForNewTable
// and the desired table structure will be created when the client is initiated.
type BigQueryWriter struct {
	client            *bigquery.Client
	config            *BigQueryConfig
	tableName         string
	fieldsForNewTable map[string]string
	ConcurrencyLevel  int // See ConcurrentProcessor
}

// NewBigQueryWriter instantiates a new instance of BigQueryWriter
func NewBigQueryWriter(config *BigQueryConfig, tableName string) *BigQueryWriter {
	w := BigQueryWriter{config: config, tableName: tableName}
	return &w
}

// NewBigQueryWriterForNewTable instantiates a new instance of BigQueryWriter and prepares
// to write results to a new table
func NewBigQueryWriterForNewTable(config *BigQueryConfig, tableName string, fields map[string]string) *BigQueryWriter {
	// This writer will attempt to write new table with the provided fields if it does not already exist.
	w := BigQueryWriter{config: config, tableName: tableName, fieldsForNewTable: fields}
	return &w
}

// ProcessData defers to WriterBatch
func (w *BigQueryWriter) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	queuedRows, err := d.Objects()
	etlutil.KillPipelineIfErr(err, killChan)

	logger.Info("BigQueryWriter: Writing -", len(queuedRows))
	err = w.WriteBatch(queuedRows)
	if err != nil {
		etlutil.KillPipelineIfErr(err, killChan)
	}
	logger.Info("BigQueryWriter: Write complete")
}

// WriteBatch inserts the supplied data into BigQuery
func (w *BigQueryWriter) WriteBatch(queuedRows []map[string]interface{}) (err error) {
	err = w.bqClient().InsertRows(w.config.ProjectID, w.config.DatasetID, w.tableName, queuedRows)
	return err
}

// Finish - see interface for documentation.
func (w *BigQueryWriter) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

func (w *BigQueryWriter) String() string {
	return "BigQueryWriter"
}

// Concurrency delegates to ConcurrentProcessor
func (w *BigQueryWriter) Concurrency() int {
	return w.ConcurrencyLevel
}

func (w *BigQueryWriter) bqClient() *bigquery.Client {
	if w.client == nil {
		w.client = bigquery.New(w.config.JSONPemPath)
		w.client.PrintDebug = true
		if w.fieldsForNewTable != nil {
			err := w.client.InsertNewTableIfDoesNotExist(w.config.ProjectID, w.config.DatasetID, w.tableName, w.fieldsForNewTable)
			if err != nil {
				// Only thrown if table existence could not be verified or if the table could not be created.
				panic(err)
			}
		}
	}
	return w.client
}
