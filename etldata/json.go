// Package data holds custom types and functions for passing JSON
// between stages.
package etldata

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/will-beep-lamm/goetl/logger"
)

// JSON is the data type that is passed along all data channels.
// Under the covers, JSON is simply a []byte containing JSON data.
type JSON []byte

// NewJSON is a simple wrapper for json.Marshal.
func NewJSON(v interface{}) (JSON, error) {
	d, err := json.Marshal(v)
	if err != nil {
		logger.Debug(fmt.Sprintf("data: failure to marshal JSON %+v - error is \"%v\"", v, err.Error()))
		logger.Debug(fmt.Sprintf("	Failed val: %+v", v))
	}
	return d, err
}

// Parse implements Payload interface. It is a simple wrapper for json.Unmarshal.
func (d JSON) Parse(v interface{}) error {
	err := json.Unmarshal(d, v)
	if err != nil {
		logger.Debug(fmt.Sprintf("data: failure to unmarshal JSON into %+v - error is \"%v\"", v, err.Error()))
		logger.Debug(fmt.Sprintf("	Failed Data: %+v", string(d)))
	}
	return err
}

// ParseSilent implements Payload interface.
func (d JSON) ParseSilent(v interface{}) error {
	return json.Unmarshal(d, v)
}

// Objects implements Payload interface.
func (d JSON) Objects() ([]map[string]interface{}, error) {
	var objects []map[string]interface{}

	// return if we have null instead of object(s).
	if bytes.Equal(d, []byte("null")) {
		logger.Debug("ObjectsFromJSON: received null. Expected object or objects. Skipping.")
		return objects, nil
	}

	var v interface{}
	err := d.Parse(&v)
	if err != nil {
		return nil, err
	}

	// check if we have a single object or a slice of objects
	switch vv := v.(type) {
	case []interface{}:
		for _, o := range vv {
			objects = append(objects, o.(map[string]interface{}))
		}
	case map[string]interface{}:
		objects = []map[string]interface{}{vv}
	case []map[string]interface{}:
		objects = vv
	default:
		err = fmt.Errorf("JSON.Objects: unsupported data type: %T", vv)
		return nil, err
	}

	return objects, nil
}

// Bytes implements Payload interface.
func (d JSON) Bytes() []byte {
	return d
}

// Clone implements Payload interface.
func (d JSON) Clone() Payload {
	dc := make(JSON, len(d.Bytes()))
	copy(dc, d.Bytes())
	return dc
}

// JSONFromHeaderAndRows takes the given header and rows of values, and
// turns it into a JSON array of objects.
func JSONFromHeaderAndRows(header []string, rows [][]interface{}) (JSON, error) {
	var b bytes.Buffer
	b.Write([]byte("["))
	for i, row := range rows {
		if i > 0 {
			b.Write([]byte(","))
		}
		b.Write([]byte("{"))
		for j, v := range row {
			if j > 0 {
				b.Write([]byte(","))
			}
			d, err := NewJSON(v)
			if err != nil {
				return nil, err
			}
			headerStr := "null"
			if len(header) > 0 && len(header) > j {
				headerStr = header[j]
			}
			b.Write([]byte(`"` + headerStr + `":` + string(d)))
		}
		b.Write([]byte("}"))
	}
	b.Write([]byte("]"))

	return JSON(b.Bytes()), nil
}
