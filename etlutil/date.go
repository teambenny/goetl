package etlutil

import (
	"fmt"
	"math"
	"time"
)

const (
	DateLayout  = "2006-01-02"
	TimeLayout  = "15:04:05"
	monthLayout = "2006-01"
)

// LastMonth the monthLayout of last month.
func LastMonth() string {
	return MonthsAgo(1)
}

// MonthsAgo returns the monthLayout of X months ago.
func MonthsAgo(ago int) string {
	ago = ago * -1
	if ago > 0 {
		ago = 0
	}

	now := time.Now()
	monthString := fmt.Sprintf("%v-%02d-01", now.Year(), int(now.Month()))
	beginningOfMonth, _ := time.Parse(DateLayout, monthString)
	return beginningOfMonth.AddDate(0, ago, 0).Format(monthLayout)
}

// MonthsAgoFromYesterday defers to MonthsAgo from yesterday's date.
func MonthsAgoFromYesterday(ago int) string {
	ago = ago * -1
	if ago > 0 {
		ago = 0
	}

	yesterday := time.Now().AddDate(0, 0, -1)
	monthString := fmt.Sprintf("%v-%02d-01", yesterday.Year(), int(yesterday.Month()))
	beginningOfMonth, _ := time.Parse(DateLayout, monthString)
	return beginningOfMonth.AddDate(0, ago, 0).Format(monthLayout)
}

// MonthToDate starts from yesterday and returns the monthLayout of yesterday,
// the DateLayout of the first of yesterday's month, and the DateLayout of
// yesterday.
func MonthToDate() (month, startDate, endDate string) {
	yesterday := time.Now().AddDate(0, 0, -1)
	month = yesterday.Format(monthLayout)
	startDate = fmt.Sprintf("%v-01", month)
	endDate = yesterday.Format(DateLayout)
	return
}

// QuarterToDate looks at yesterday and returns yesterday's
// quarterly start date (in DateLayout) and yesterday (in DateLayout.
func QuarterToDate() (startDate, endDate string) {
	yesterday := time.Now().AddDate(0, 0, -1)
	qtr := (yesterday.Month()-1)/3*3 + 1
	startDate = fmt.Sprintf("%v-%02d-01", yesterday.Year(), qtr)
	endDate = yesterday.Format(DateLayout)
	return
}

// MonthToTime takes a month (in monthLayout) and returns the
// time object of its first day.
func MonthToTime(month string) time.Time {
	startTime, err := time.Parse(DateLayout, fmt.Sprintf("%v-01", month))
	if err != nil {
		panic(err.Error())
	}

	return startTime
}

// MonthDateRange takes a month (in monthLayout) and returns the
// first and last day of that month (in DateLayout).
func MonthDateRange(month string) (startDate, endDate string) {
	startTime := MonthToTime(month)
	endTime := startTime.AddDate(0, 1, 0).AddDate(0, 0, -1)

	startDate = startTime.Format(DateLayout)
	endDate = endTime.Format(DateLayout)

	return
}

// DaysInMonth takes a month (in monthLayout) and returns how many days
// are in that month.
func DaysInMonth(month string) int {
	startTime := MonthToTime(month)
	endTime := startTime.AddDate(0, 1, 0).AddDate(0, 0, -1)
	return endTime.Day()
}

// BeginningOfDay returns the time (in any location) of
// the start of that location's day.
func BeginningOfDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}

// DaysBetween calculates how many days pass between two time objects.
func DaysBetween(start, finish time.Time) int {
	diff := BeginningOfDay(finish).Sub(BeginningOfDay(start))
	return int(math.Round(diff.Hours() / 24.0))
}

// FirstDayOfWeek takes a time object and returns the first day
// of the week of that time object (scoped to firstDayOfWeek).
func FirstDayOfWeek(day time.Time, firstDayOfWeek time.Weekday) time.Time {
	offset := int(day.Weekday() - firstDayOfWeek)
	if offset < 0 { // Sunday
		offset += 7
	}
	return day.AddDate(0, 0, -1*offset)
}

// BeginningOfTodayInUTC looks at the beginning of the day based on the
// provided location and then converts it back to UTC.
func BeginningOfTodayInUTC(loc *time.Location) time.Time {
	return BeginningOfDay(time.Now().In(loc)).UTC()
}
