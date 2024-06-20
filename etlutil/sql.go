package etlutil

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/kisielk/sqlstruct"
	"github.com/will-beep-lamm/goetl/etldata"
)

// GetDataFromSQLQuery is a util function that, given a properly intialized sql.DB
// and a valid SQL query, will handle executing the query and getting back etldata.JSON
// objects. This function is asynch, and etldata.JSON should be received on the return
// data channel. If there was a problem setting up the query, then an error will also be
// returned immediately. It is also possible for errors to occur during execution as data
// is retrieved from the query. If this happens, the object returned will be a JSON
// object in the form of {"Error": "description"}.
func GetDataFromSQLQuery(db *sql.DB, query string, batchSize int, structDest interface{}) (chan etldata.Payload, error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	dataChan := make(chan etldata.Payload)

	if structDest != nil {
		go scanRowsUsingStruct(rows, columns, structDest, batchSize, dataChan)
	} else {
		go scanDataGeneric(rows, columns, batchSize, dataChan)
	}

	return dataChan, nil
}

func scanRowsUsingStruct(rows *sql.Rows, columns []string, structDest interface{}, batchSize int, dataChan chan etldata.Payload) {
	defer rows.Close()

	tableData := []map[string]interface{}{}

	for rows.Next() {
		err := sqlstruct.Scan(structDest, rows)
		if err != nil {
			sendErr(err, dataChan)
		}

		d, err := etldata.NewJSON(structDest)
		if err != nil {
			sendErr(err, dataChan)
		}

		entry := make(map[string]interface{})
		err = d.Parse(&entry)
		if err != nil {
			sendErr(err, dataChan)
		}

		tableData = append(tableData, entry)

		if batchSize > 0 && len(tableData) >= batchSize {
			sendTableData(tableData, dataChan)
			tableData = []map[string]interface{}{}
		}
	}
	if rows.Err() != nil {
		sendErr(rows.Err(), dataChan)
	}

	// Flush remaining tableData
	if len(tableData) > 0 {
		sendTableData(tableData, dataChan)
	}

	close(dataChan) // signal completion to caller
}

func scanDataGeneric(rows *sql.Rows, columns []string, batchSize int, dataChan chan etldata.Payload) {
	defer rows.Close()

	tableData := []map[string]interface{}{}
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := 0; i < len(columns); i++ {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			sendErr(err, dataChan)
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			// logger.Debug("Value Type for", col, " -> ", reflect.TypeOf(val))
			switch vv := val.(type) {
			case []byte:
				v = string(vv)
			default:
				v = vv
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)

		if batchSize > 0 && len(tableData) >= batchSize {
			sendTableData(tableData, dataChan)
			tableData = []map[string]interface{}{}
		}
	}
	if rows.Err() != nil {
		sendErr(rows.Err(), dataChan)
	}

	// Flush remaining tableData
	if len(tableData) > 0 {
		sendTableData(tableData, dataChan)
	}

	close(dataChan) // signal completion to caller
}

// http://play.golang.org/p/2wHfO6YS3_
func determineBytesValue(b []byte) (interface{}, error) {
	d := etldata.JSON(b)
	var v interface{}
	err := d.ParseSilent(&v)
	if err != nil {
		// need to quote strings for JSON to parse correctly
		if !strings.Contains(string(b), `"`) {
			b = []byte(fmt.Sprintf(`"%v"`, string(b)))
			return determineBytesValue(b)
		}
	}
	switch vv := v.(type) {
	case []byte:
		return string(vv), err
	default:
		return v, err
	}
}

func sendTableData(tableData []map[string]interface{}, dataChan chan etldata.Payload) {
	d, err := etldata.NewJSON(tableData)
	if err != nil {
		sendErr(err, dataChan)
	} else {
		dataChan <- d
	}
}

func sendErr(err error, dataChan chan etldata.Payload) {
	dataChan <- etldata.JSON([]byte(`{"Error":"` + err.Error() + `"}`))
}

// ExecuteSQLQuery allows you to execute arbitrary SQL statements
func ExecuteSQLQuery(db *sql.DB, query string) error {
	_, err := db.Exec(query)
	return err
}

// ExecuteSQLQueryTx allows you to execute arbitrary SQL statements
// within a transaction.
func ExecuteSQLQueryTx(tx *sql.Tx, query string) error {
	_, err := tx.Exec(query)
	return err
}

func sortedColumns(objects []map[string]interface{}) []string {
	// Since we don't know if all objects have the same keys, we need to
	// iterate over all the objects to gather all possible keys/columns
	// to use in the INSERT statement.
	colsMap := make(map[string]struct{})
	for _, o := range objects {
		for col := range o {
			colsMap[col] = struct{}{}
		}
	}

	cols := []string{}
	for col := range colsMap {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	return cols
}
