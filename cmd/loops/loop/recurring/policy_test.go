package recurring_test

import (
	"testing"
	"time"

	"github.com/opst/knitfab/cmd/loops/loop/recurring"
)

func TestParsePolicy(t *testing.T) {
	for name, testcase := range map[string]struct {
		when        string
		then        recurring.Policy
		expectError bool
	}{
		"forever means forever": {
			when: "forever",
			then: recurring.Forever(0),
		},
		"forever:3s means forever with cooldown 3 seconds": {
			when: "forever:3s",
			then: recurring.Forever(3 * time.Second),
		},
		"forever:someday can not be parsed (someday is not time.Duration)": {
			when:        "forever:someday",
			expectError: true,
		},
		"backlog means backlog": {
			when: "backlog",
			then: recurring.Backlog(),
		},
		"backlog:param can not be parsed (it should not take any parameters)": {
			when:        "backlog:param",
			expectError: true,
		},
		"empty string can not be parsed (it is not policy)": {
			when:        "",
			expectError: true,
		},
		"known policy can not be parsed (it is not policy)": {
			when:        "???????unknown??????",
			expectError: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			when, expected := testcase.when, testcase.then
			actual, err := recurring.ParsePolicy(when)

			if testcase.expectError {
				if err == nil {
					t.Fatal("expected error does not occured")
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if actual != expected {
				t.Errorf("unmatch: (actual, expected) = (%v, %v)", actual, expected)
			}
		})
	}

}
