package etlutil

import (
	"fmt"

	"github.com/google/uuid"
)

// UUID returns a new UUID
func UUID() (id string, err error) {
	var u uuid.UUID
	u, err = uuid.NewV7()
	if err != nil {
		return
	}

	id = u.String()
	if id == "" {
		err = fmt.Errorf("unable to generate id")
	}
	return
}
