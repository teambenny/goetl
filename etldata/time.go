package etldata

import (
	"fmt"
	"time"
)

const (
	sqlLayout1 = "2006-01-02 15:04:05"
	sqlLayout2 = "2006-01-02T15:04:05Z"
)

type SQLTime struct {
	time.Time
}

func (t *SQLTime) UnmarshalJSON(d []byte) (err error) {
	t.Time, err = t.unmarshalJSON(d, sqlLayout1)
	if err != nil {
		t.Time, err = t.unmarshalJSON(d, sqlLayout2)
	}
	return
}

func (t *SQLTime) unmarshalJSON(d []byte, layout string) (time.Time, error) {
	layout = fmt.Sprintf(`"%v"`, layout)
	return time.Parse(layout, string(d))
}

func (t SQLTime) MarshalJSON() ([]byte, error) {
	if t.Time.IsZero() {
		return []byte(""), nil
	}

	val := fmt.Sprintf(`"%v"`, t.Format(sqlLayout1))
	return []byte(val), nil
}

func (t SQLTime) Format(layout string) string {
	return t.Time.Format(layout)
}

func (t SQLTime) String() string {
	return t.Format(sqlLayout1)
}

func (t *SQLTime) In(l *time.Location) *SQLTime {
	if t == nil || l == nil {
		return nil
	}
	return &SQLTime{Time: t.Time.In(l)}
}

func (t *SQLTime) UTCTime() *SQLTime {
	if t == nil {
		return nil
	}
	return &SQLTime{Time: t.UTC()}
}
