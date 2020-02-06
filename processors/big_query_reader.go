package processors

import (
	"errors"

	bigquery "github.com/dailyburn/bigquery/client"
	"github.com/teambenny/goetl/etldata"
	"github.com/teambenny/goetl/etlutil"
	"github.com/teambenny/goetl/logger"
)

// BigQueryReader is used to query data from Google's BigQuery,
// and it behaves similarly to SQLReader. See SQLReader
// docs for explanation on static vs dynamic querying.
//
// Note: If your data set contains nested/repeated fields you will likely want to
// get results back "unflattened." By default BigQuery returns results in
// a flattened format, which duplicates rows for each repeated value. This can
// be annoying to deal with, so BigQueryReader provides a "UnflattenResults"
// flag that will handle querying in such a way to get back unflattened results.
// This involves using a temporary table setting and a couple of other special
// query settings - read the BigQuery docs related to flatten and repeated
// fields for more info.
//
type BigQueryReader struct {
	client           *bigquery.Client
	config           *BigQueryConfig
	query            string
	sqlGenerator     func(etldata.Payload) (string, error)
	PageSize         int    // defaults to 5000
	AggregateResults bool   // determines whether to send data as soon as available or to aggregate and send all query results, defaults to false
	UnflattenResults bool   // defaults to false
	TmpTableName     string // Used when UnflattenResults is true. default to "_goetl_tmp"
	ConcurrencyLevel int    // See ConcurrentProcessor
}

// BigQueryConfig is used when init'ing new BigQueryReader instances.
type BigQueryConfig struct {
	JSONPemPath string
	ProjectID   string
	DatasetID   string
}

// NewBigQueryReader returns an instance of a BigQueryExtractor ready to
// run a static query.
func NewBigQueryReader(config *BigQueryConfig, query string) *BigQueryReader {
	r := BigQueryReader{config: config}
	r.query = query
	r.PageSize = 5000 // default page size
	r.UnflattenResults = false
	r.TmpTableName = "_goetl_tmp"
	return &r
}

// NewDynamicBigQueryReader returns an instance of a BigQueryExtractor ready to
// run a dynamic query based on the sqlGenerator function.
func NewDynamicBigQueryReader(config *BigQueryConfig, sqlGenerator func(etldata.Payload) (string, error)) *BigQueryReader {
	r := NewBigQueryReader(config, "")
	r.sqlGenerator = sqlGenerator
	return r
}

// ProcessData defers to ForEachQueryData
func (r *BigQueryReader) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	r.ForEachQueryData(d, killChan, func(d etldata.Payload) {
		outputChan <- d
	})
}

// Finish - see interface for documentation.
func (r *BigQueryReader) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

// ForEachQueryData handles generating the SQL (in case of dynamic mode),
// running the query and retrieving the data in etldata.Payload format, and then
// passing the results back witih the function call to forEach.
func (r *BigQueryReader) ForEachQueryData(d etldata.Payload, killChan chan error, forEach func(d etldata.Payload)) {
	sql := ""
	var err error
	if r.query == "" && r.sqlGenerator != nil {
		sql, err = r.sqlGenerator(d)
		etlutil.KillPipelineIfErr(err, killChan)
	} else if r.query != "" {
		sql = r.query
	} else {
		killChan <- errors.New("BigQueryReader: must have either static query or sqlGenerator func")
	}

	logger.Debug("BigQueryReader: Running -", sql)

	bqDataChan := make(chan bigquery.Data)
	go r.bqClient().AsyncQuery(r.PageSize, r.config.DatasetID, r.config.ProjectID, sql, bqDataChan)
	aggregatedData := bigquery.Data{}

	for bqd := range bqDataChan {
		etlutil.KillPipelineIfErr(bqd.Err, killChan)
		logger.Info("BigQueryReader: received bqData: len(rows) =", len(bqd.Rows))
		// logger.Debug("   %+v", bqd)

		if bqd.Rows != nil && bqd.Headers != nil && len(bqd.Rows) > 0 {
			if r.AggregateResults {
				logger.Debug("BigQueryReader: aggregating results")
				aggregatedData.Headers = bqd.Headers
				aggregatedData.Rows = append(aggregatedData.Rows, bqd.Rows...)
			} else {
				// Send data as soon as we get it back
				logger.Debug("BigQueryReader: sending data without aggregation")
				d, err := etldata.JSONFromHeaderAndRows(bqd.Headers, bqd.Rows)
				etlutil.KillPipelineIfErr(err, killChan)
				forEach(d) // pass back out via the forEach func
			}
		}
	}
	if r.AggregateResults {
		logger.Info("BigQueryReader: sending aggregated results: len(rows) =", len(aggregatedData.Rows))
		d, err := etldata.JSONFromHeaderAndRows(aggregatedData.Headers, aggregatedData.Rows)
		etlutil.KillPipelineIfErr(err, killChan)
		forEach(d) // pass back out via the forEach func
	}
}

func (r *BigQueryReader) String() string {
	return "BigQueryReader"
}

// Concurrency defers to ConcurrentProcessor
func (r *BigQueryReader) Concurrency() int {
	return r.ConcurrencyLevel
}

func (r *BigQueryReader) bqClient() *bigquery.Client {
	if r.client == nil {
		if r.UnflattenResults {
			tmpTable := r.TmpTableName
			r.client = bigquery.New(r.config.JSONPemPath, bigquery.AllowLargeResults(true, tmpTable, false))
		} else {
			r.client = bigquery.New(r.config.JSONPemPath)
		}
		r.client.PrintDebug = false
	}
	return r.client
}
