package etlutil

import (
	uuid "github.com/satori/go.uuid"
)

// UUID returns a new UUID
func UUID() (id string, err error) {
	var uid uuid.UUID
	uid, err = uuid.NewV4()
	if err != nil {
		return
	}
	id = uid.String()
	return
}
