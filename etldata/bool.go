package etldata

// Bool supports both 1/0 and true/false
type Bool bool

func (b *Bool) UnmarshalJSON(d []byte) (err error) {
	switch string(d) {
	case "1", `"1"`, "true", `"true"`:
		*b = Bool(true)
	case "0", `"0"`, "false", `"false"`:
		*b = Bool(false)
	}
	return
}

func (b Bool) MarshalJSON() ([]byte, error) {
	if b {
		return []byte(`true`), nil
	}
	return []byte(`false`), nil
}
