package etlutil

import "github.com/teambenny/goetl/etldata"

// Typeable looks at the type attribute.
type Typeable struct {
	Type string `json:"type"`
}

// Typecheck returns the value of the Typeable.Type.
func Typecheck(d etldata.Payload) (key string, err error) {
	var typeables []Typeable
	err = d.Parse(&typeables)
	key = typeables[0].Type
	return
}
