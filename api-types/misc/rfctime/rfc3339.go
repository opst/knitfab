package rfctime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// Format string for date-time in RFC3339, disallowing Z as time-offset.
//
// Use it to stringify time.Time forcing timezone offset not to use "Z".
const RFC3339DateTimeFormat string = "2006-01-02T15:04:05.999-07:00"

// Format string for date-time in RFC3339, allowing Z as time-offset.
//
// Use it to parse RFC3339 date-time expression.
const RFC3339DateTimeFormatZ string = time.RFC3339Nano

// The following format is used to parse the abbreviated form of RFC3339 date-time.
const (
	RFC3339DateNano       = "2006-01-02T15:04:05.999999999"
	RFC3339DateNanoSpace  = "2006-01-02 15:04:05.999999999"
	RFC3339DateNanoZSpace = "2006-01-02 15:04:05.999999999Z07:00"

	RFC3339DateSec       = "2006-01-02T15:04:05"
	RFC3339DateSecZ      = "2006-01-02T15:04:05Z07:00"
	RFC3339DateSecSpace  = "2006-01-02 15:04:05"
	RFC3339DateSecZSpace = "2006-01-02 15:04:05Z07:00"

	RFC3339DateMin       = "2006-01-02T15:04"
	RFC3339DateMinZ      = "2006-01-02T15:04Z07:00"
	RFC3339DateMinSpace  = "2006-01-02 15:04"
	RFC3339DateMinZSpace = "2006-01-02 15:04Z07:00"

	RFC3339DateHour       = "2006-01-02T15"
	RFC3339DateHourZ      = "2006-01-02T15Z07:00"
	RFC3339DateHourSpace  = "2006-01-02 15"
	RFC3339DateHourZSpace = "2006-01-02 15Z07:00"

	RFC3339DateOnly  = "2006-01-02"
	RFC3339DateOnlyZ = "2006-01-02Z07:00"
)

// date-time in https://www.ietf.org/rfc/rfc3339.txt .
// this is known as a subset of ISO8601 extended format.
//
// This type is useful to interchange timestamps via network/file.
type RFC3339 time.Time

func (rfctime RFC3339) Time() time.Time {
	return time.Time(rfctime)
}

func (rfctime RFC3339) Equal(other RFC3339) bool {
	return rfctime.Time().Equal(other.Time())
}

// return true if this and other `.Time()` are equal.
// If both this and other are nil, also return true.
//
// otherwise, return false.
func (rfctime RFC3339) Equiv(other interface{ Time() time.Time }) bool {
	return other == nil || rfctime.Time().Equal(other.Time())
}

// get string expression.
//
// It formatted by RFC3339DateTimeFormat.
//
// When you need other format, use
func (t RFC3339) String() string {
	return time.Time(t).Format(RFC3339DateTimeFormat)
}

// Parse string to ISO8601 time.
//
// It trancates resolution to milli second.
func ParseRFC3339DateTime(s string) (RFC3339, error) {
	t, err := time.Parse(RFC3339DateTimeFormatZ, s)
	if err != nil {
		return *new(RFC3339), err
	}
	return RFC3339(t), nil
}

// When you need to parse string with the abbreviated forms of RFC3339 date-time, use this function.
func ParseLooseRFC3339(s string) (RFC3339, error) {
	formats := []string{
		RFC3339DateTimeFormatZ, RFC3339DateNanoZSpace,
		RFC3339DateSecZ, RFC3339DateSecZSpace,
		RFC3339DateMinZ, RFC3339DateMinZSpace,
		RFC3339DateHourZ, RFC3339DateHourZSpace,
		RFC3339DateOnlyZ,
	}

	for _, format := range formats {
		t, err := time.Parse(format, s)
		if err == nil {
			return RFC3339(t), nil
		}
	}

	// get local timezone
	location, err := time.LoadLocation("Local")
	if err != nil {
		return RFC3339{}, err
	}

	formatsWithoutTimeZone := []string{
		RFC3339DateNano, RFC3339DateNanoSpace,
		RFC3339DateSec, RFC3339DateSecSpace,
		RFC3339DateMin, RFC3339DateMinSpace,
		RFC3339DateHour, RFC3339DateHourSpace,
		RFC3339DateOnly,
	}

	for _, format := range formatsWithoutTimeZone {
		t, err := time.ParseInLocation(format, s, location)
		if err == nil {
			return RFC3339(t), nil
		}
	}

	return RFC3339{}, fmt.Errorf("failed to parse %s", s)
}

// implement encoding/json.Marshaller
func (t RFC3339) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, t)), nil
}

// implement encoding/json.Unmarshaller
func (t *RFC3339) UnmarshalJSON(b []byte) error {
	if bytes.Equal(b, []byte("null")) {
		return nil
	}

	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	ret, err := ParseRFC3339DateTime(s)
	if err != nil {
		return err
	}

	*t = RFC3339(ret)

	return nil
}
