package etlutil

import "github.com/will-beep-lamm/goetl/etldata"

// TypeableColumnName is exposed as a constant to prevent fat fingering.
const (
	TypeableColumnName = "goetl_data_type"
)

// Typeable looks at the type attribute.
type Typeable struct {
	Type string `json:"goetl_data_type"`
}

// Typecheck returns the value of the Typeable.Type.
func Typecheck(d etldata.Payload) (key string, err error) {
	var typeables []Typeable
	err = d.Parse(&typeables)
	key = typeables[0].Type
	return
}
