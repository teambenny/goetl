package etlutil

import (
	"fmt"

	uuid "github.com/satori/go.uuid"
)

// UUID returns a new UUID
func UUID() (id string, err error) {
	var uid uuid.UUID
	uid = uuid.NewV4()

	id = uid.String()
	if id == "" {
		err = fmt.Errorf("Unable to generate id")
	}
	return
}
