package etlutil

import (
	"fmt"
	"time"
)

// Timer is a basic mechanism for measuring execution time.
type Timer struct {
	startTime time.Time
	endTime   time.Time
}

// StartTimer returns a new Timer that's already "started".
func StartTimer() (t *Timer) {
	return &Timer{startTime: time.Now()}
}

// Stop sets the end time for the Timer and returns itself.
func (t *Timer) Stop() *Timer {
	t.endTime = time.Now()
	return t
}

// Stopped returns true if Stop() has been called on the timer.
func (t *Timer) Stopped() bool {
	zeroTime := time.Time{}
	return zeroTime != t.endTime
}

// Duration returns either the total executino duration (if Timer stopped)
// or the duration until time.Now() if timer is still running.
func (t *Timer) Duration() time.Duration {
	if t.Stopped() {
		return t.endTime.Sub(t.startTime)
	}
	return time.Now().Sub(t.startTime)
}

func (t *Timer) String() string {
	if t.Stopped() {
		return fmt.Sprintf("Ran in %v secs", t.Duration().Seconds())
	}
	return fmt.Sprintf("Running for %v secs", t.Duration().Seconds())
}
