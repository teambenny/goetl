package etldata

// Payload is how data flows through pipelines.
type Payload interface {
	// Parse transforms the bytes into a struct. It should log
	// output when unmarshaling fails.
	Parse(v interface{}) error

	// ParseSilent should not log output when unmarshaling fails.
	// It can be used in cases where failure is expected.
	ParseSilent(v interface{}) error

	// Objects is a helper for parsing into a slice of
	// generic maps/objects. The use-case is when a stage is expecting
	// to receive either an object or an array of objects, and
	// we want to deal with it in a generic fashion.
	Objects() ([]map[string]interface{}, error)

	// Bytes returns the byte representation of the underlying payload.
	Bytes() []byte

	// Clone returns a new instance of the Payload to send to
	// multiple processors (to prevent race conditions)
	Clone() Payload
}
