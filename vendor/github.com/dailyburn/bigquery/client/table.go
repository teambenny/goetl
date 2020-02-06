package client

import bigquery "google.golang.org/api/bigquery/v2"

// InsertNewTable creates a new empty table for the given project and dataset with the field name/types defined in the fields map
func (c *Client) InsertNewTable(projectID, datasetID, tableName string, fields map[string]string) error {
	// If the table already exists, an error will be raised here.
	service, err := c.connect()
	if err != nil {
		return err
	}

	// build the table schema
	schema := &bigquery.TableSchema{}
	for k, v := range fields {
		schema.Fields = append(schema.Fields, &bigquery.TableFieldSchema{Name: k, Type: v})
	}

	// build the table to insert
	table := &bigquery.Table{}
	table.Schema = schema

	tr := &bigquery.TableReference{}
	tr.DatasetId = datasetID
	tr.ProjectId = projectID
	tr.TableId = tableName

	table.TableReference = tr

	_, err = service.Tables.Insert(projectID, datasetID, table).Do()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) InsertNewTableIfDoesNotExist(projectID, datasetID, tableID string, fields map[string]string) error {
	// This will not return an error if the table already exists
	exists, err := c.tableDoesExist(projectID, datasetID, tableID)
	if err != nil {
		return err
	}
	if !exists {
		return c.InsertNewTable(projectID, datasetID, tableID, fields)
	}
	return nil
}

// PatchTableSchema sends a patch request to bigquery to modify the table with only the fields provided
func (c *Client) PatchTableSchema(projectID, datasetID, tableID string, fields map[string]string) error {
	service, err := c.connect()
	if err != nil {
		return err
	}

	// build the table schema
	schema := &bigquery.TableSchema{}
	for k, v := range fields {
		schema.Fields = append(schema.Fields, &bigquery.TableFieldSchema{Name: k, Type: v})
	}

	table := &bigquery.Table{}
	table.Schema = schema

	tr := &bigquery.TableReference{}
	tr.DatasetId = datasetID
	tr.ProjectId = projectID
	tr.TableId = tableID

	table.TableReference = tr

	_, err = service.Tables.Patch(projectID, datasetID, tableID, table).Do()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) tableDoesExist(projectID, datasetID, tableID string) (bool, error) {
	// return err only if connection fails
	service, err := c.connect()
	if err != nil {
		return false, err
	}

	_, err = service.Tables.Get(projectID, datasetID, tableID).Do()
	if err != nil {
		return false, nil
	}
	return true, nil
}
