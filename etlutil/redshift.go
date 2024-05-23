package etlutil

import (
	"database/sql"
	"fmt"
	"strings"
)

// CreateTempTable generates a unique table name and creates schema based
// on the target table.
func CreateTempTable(tx *sql.Tx, likeTable string) (string, error) {
	if tx == nil || likeTable == "" {
		return "", nil
	}
	id, _ := UUID()

	tmpTable := fmt.Sprintf("%v_%v",
		strings.Replace(likeTable, ".", "_", -1),
		strings.Replace(fmt.Sprintf("%v", id), "-", "_", -1),
	)

	q := fmt.Sprintf("CREATE TEMPORARY TABLE %v (LIKE %v INCLUDING DEFAULTS)", tmpTable, likeTable)
	_, err := tx.Exec(q)
	return tmpTable, err
}

// VacuumTable vacuums a specific table.
func VacuumTable(db *sql.DB, table string) error {
	if db == nil || table == "" {
		return nil
	}
	q := fmt.Sprintf("VACUUM %v", table)
	_, err := db.Exec(q)
	return err
}

// VacuumAll vacuums all tables.
func VacuumAll(db *sql.DB) error {
	if db == nil {
		return nil
	}
	_, err := db.Exec("VACUUM")
	return err
}

// Dedupe writes all unique values into a temp table then runs TruncateMerge.
func Dedupe(tx *sql.Tx, targetTable string) error {
	if tx == nil || targetTable == "" {
		return nil
	}
	tempTable, err := CreateTempTable(tx, targetTable)
	if err != nil {
		return err
	}

	insertUnique := fmt.Sprintf(`
			INSERT INTO %v
			SELECT DISTINCT * FROM %v
	`, tempTable, targetTable)
	_, err = tx.Exec(insertUnique)

	if err != nil {
		return err
	}

	return TruncateMerge(tx, targetTable, tempTable)
}

// DeltaMerge deletes any records in the targetTable that are in the tempTable bound by the conditional.
// It then inserts all records in the tempTable into the targetTable.
//
// This should be used when you are inserting a subset of records into the tempTable (instead of running
// a complete snapshot). You would then join based on the primary key, so that all records written to the
// tempTable will only appear once in the targetTable once the job is complete.
//
// This is effectively a workaround for the lack of primary key constraints in Redshift.
func DeltaMerge(tx *sql.Tx, targetTable, tempTable, conditional string) error {
	if tx == nil || targetTable == "" || tempTable == "" || conditional == "" {
		return nil
	}
	deleteQuery := fmt.Sprintf(`
			DELETE FROM %v
			USING %v
			WHERE %v
	`, targetTable, tempTable, conditional)

	if _, err := tx.Exec(deleteQuery); err != nil {
		return err
	}

	insertQuery := fmt.Sprintf("INSERT INTO %v SELECT DISTINCT * FROM %v", targetTable, tempTable)
	if _, err := tx.Exec(insertQuery); err != nil {
		return err
	}

	return nil
}

// TruncateMerge clears out the targetTable and then writes all records from the tempTable into
// targetTable. This method is used when a full snapshot of the table is written in its entirety
// into tempTable.
func TruncateMerge(tx *sql.Tx, targetTable, tempTable string) error {
	if tx == nil || targetTable == "" || tempTable == "" {
		return nil
	}

	id, err := UUID()
	if err != nil {
		return err
	}

	holdingTable := strings.ReplaceAll(fmt.Sprintf("%v_%v", targetTable, id), "-", "_")
	q := fmt.Sprintf("CREATE TABLE %v (LIKE %v INCLUDING DEFAULTS)", holdingTable, targetTable)
	err = ExecuteSQLQueryTx(tx, q)
	if err != nil {
		return err
	}

	insertQuery := fmt.Sprintf("INSERT INTO %v SELECT DISTINCT * FROM %v", holdingTable, tempTable)
	err = ExecuteSQLQueryTx(tx, insertQuery)
	if err != nil {
		return err
	}

	err = ExecuteSQLQueryTx(tx, fmt.Sprintf("DROP TABLE %v", targetTable))
	if err != nil {
		return err
	}

	targetTableNames := strings.Split(targetTable, ".")
	targetTableName := targetTableNames[len(targetTableNames)-1]
	return ExecuteSQLQueryTx(tx, fmt.Sprintf("ALTER TABLE %v RENAME TO %v", holdingTable, targetTableName))
}

// PurgeMerge clears out the targetTable based on the conditional, and then writes
// all records from the tempTable into targetTable. This method is used when a full
// snapshot of a specific applicationID table is written in into tempTable.
func PurgeMerge(tx *sql.Tx, targetTable, tempTable, conditional string) error {
	if tx == nil || targetTable == "" || tempTable == "" {
		return nil
	}

	purgeQuery := fmt.Sprintf("DELETE FROM %v WHERE %v", targetTable, conditional)
	if _, err := tx.Exec(purgeQuery); err != nil {
		return err
	}

	insertQuery := fmt.Sprintf("INSERT INTO %v SELECT DISTINCT * FROM %v", targetTable, tempTable)
	if _, err := tx.Exec(insertQuery); err != nil {
		return err
	}

	return nil
}
