package goetl

// Test-only hooks, so SQL generation can be asserted exactly without a database
// and without exporting internals from the public API.

// BuildInsertForTest renders the INSERT statement for n rows.
func BuildInsertForTest(w *SQLWriter, n int) string { return w.buildInsert(n) }

// SetColumnsForTest sets the column list that is normally derived from the
// first batch's Arrow schema.
func SetColumnsForTest(w *SQLWriter, cols []string) { w.cols = cols }
