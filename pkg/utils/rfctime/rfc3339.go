package rfctime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
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

// date-time in https://www.ietf.org/rfc/rfc3339.txt .
// this is known as a subset of ISO8601 extended format.
//
// This type is useful to interchange timestamps via network/file.
type RFC3339 time.Time

func (rfctime RFC3339) Time() time.Time {
	return time.Time(rfctime)
}

func (rfctime *RFC3339) Equal(other *RFC3339) bool {
	if (rfctime == nil) != (other == nil) {
		return false
	}
	return rfctime == nil || rfctime.Time().Equal(other.Time())
}

// return true if this and other `.Time()` are equal.
// If both this and other are nil, also return true.
//
// otherwise, return false.
func (rfctime *RFC3339) Equiv(other interface{ Time() time.Time }) bool {
	thisIsNil := rfctime == nil

	thatIsNil := func() (isnil bool) {
		// careing typed-nil

		panicking := true
		defer func() {
			recover() // ignore panic from reflect
			if panicking {
				isnil = false
			}
		}()
		isnil = reflect.ValueOf(other).IsNil() // careing typed-nil
		panicking = false
		return
	}()

	if thisIsNil != thatIsNil {
		return false
	}

	return rfctime == nil || rfctime.Time().Equal(other.Time())
}

// get string expression.
//
// It formatted by RFC3339DateTimeFormat.
//
// When you need other format, use
func (t RFC3339) String() string {
	return time.Time(t).Format(RFC3339DateTimeFormat)
}

// When you need to get string with local timezone, use
func (t RFC3339) StringWithLocalTimeZone() (string, error) {
	location, err := time.LoadLocation("Local")
	if err != nil {
		return "", err
	}
	return time.Time(t).In(location).Format(RFC3339DateTimeFormat), nil
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

// when you need to parse multiple formats, use
func ParseMultipleFormats(s string, formats ...string) (RFC3339, string, error) {
	var err error
	var t time.Time
	for _, format := range formats {
		t, err = time.Parse(format, s)
		if err == nil {
			return RFC3339(t), format, nil
		}
	}
	return *new(RFC3339), "", fmt.Errorf("failed to parse %s", s)
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
