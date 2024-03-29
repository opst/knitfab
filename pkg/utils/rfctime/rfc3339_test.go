package rfctime_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestRFC3339(t *testing.T) {
	t.Run("it should fail to parse when passed wrong format", func(t *testing.T) {
		s := "2021/10/22 12:34:56 +07:00"
		_, err := rfctime.ParseRFC3339DateTime(s)

		if err == nil {
			t.Error("no error unexpectedly")
		}
	})

	t.Run("it should parse when passed rfc3396 date-time format", func(t *testing.T) {
		s := "2021-10-22T12:34:56.987654321+07:00"
		testee := try.To(rfctime.ParseRFC3339DateTime(s)).OrFatal(t)

		expected := time.Date(
			2021, 10, 22, 12, 34, 56, 987654321,
			time.FixedZone("+07:00", int((7*time.Hour).Seconds())),
		)

		if !testee.Time().Equal(expected) {
			t.Errorf("unmatch: as time: (actual, expected) = (%+v, %+v)", testee, expected)
		}

		if !testee.Equiv(rfctime.RFC3339(expected)) {
			t.Errorf("unmatch: as RFC3339: (actual, expected) = (%+v, %+v)", testee, expected)
		}

	})

	t.Run("it can be marshalled into json", func(t *testing.T) {
		s := "2021-10-22T12:34:56+07:00"
		testee := try.To(rfctime.ParseRFC3339DateTime(s)).OrFatal(t)

		actual := try.To(json.Marshal(testee)).OrFatal(t)
		expected := fmt.Sprintf(`"%s"`, s) // String in json

		if string(actual) != expected {
			t.Errorf("unmatch: json marshall: (actual, expected) = (%s, %s)", actual, expected)
		}
	})

	t.Run("it can be unmarshalled from json", func(t *testing.T) {
		s := "2021-10-22T12:34:56+07:00"
		jsonExpression := fmt.Sprintf(`"%s"`, s)

		var actual rfctime.RFC3339
		if err := json.Unmarshal([]byte(jsonExpression), &actual); err != nil {
			t.Fatal(err)
		}

		expected := try.To(rfctime.ParseRFC3339DateTime(s)).OrFatal(t)

		if !actual.Time().Equal(expected.Time()) {
			t.Errorf("unmatch: json unmarshall: (actual, expected) = (%s, %s)", actual, expected)
		}
	})

	t.Run("it do nothing when json.Unmarshall is passed null", func(t *testing.T) {
		t.Run("start from zero value", func(t *testing.T) {
			expected := new(rfctime.RFC3339)
			actual := new(rfctime.RFC3339)
			if err := json.Unmarshal([]byte("null"), actual); err != nil {
				t.Fatal(err)
			}

			if !actual.Equal(expected) {
				t.Errorf("updated by unmarshalling null, unexpectedly: %s", actual)
			}
		})

		t.Run("start from non-zero value", func(t *testing.T) {
			expected := rfctime.RFC3339(time.Date(
				2022, 10, 11, 12, 13, 14, 987654321,
				time.FixedZone("01:00", int((1*time.Hour).Seconds())),
			))
			actual := rfctime.RFC3339(time.Date(
				2022, 10, 11, 12, 13, 14, 987654321,
				time.FixedZone("01:00", int((1*time.Hour).Seconds())),
			))
			if err := json.Unmarshal([]byte("null"), &actual); err != nil {
				t.Fatal(err)
			}

			if !actual.Equal(&expected) {
				t.Errorf("updated by unmarshalling null, unexpectedly: %s", actual)
			}
		})
	})
}
