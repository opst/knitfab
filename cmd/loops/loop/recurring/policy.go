package recurring

import (
	"fmt"
	"strings"
	"time"

	"github.com/opst/knitfab/cmd/loops/loop"
)

func ParsePolicy(s string) (Policy, error) {
	typ, param, ok := strings.Cut(s, ":")
	switch typ {
	case "forever":
		if !ok || param == "" {
			return Forever(0), nil
		}

		period, err := time.ParseDuration(param)
		if err != nil {
			return nil, fmt.Errorf(`failed to parse: %s as "forever:COOLDOWN": %w`, s, err)
		}
		return Forever(period), nil
	case "backlog":
		if ok {
			return nil, fmt.Errorf("backlog policy does not take paramters: %s", s)
		}
		return Backlog(), nil
	}
	return nil, fmt.Errorf("unknown policy name: %s (should be one of -- forever|backlog)", typ)
}

// Policy for lopp task behavior.
// How the policy behaves depends on the implementation of Next() method.
type Policy interface {
	Next(updated bool, err error) loop.Next
	String() string
}

// Restart immediately while there are things to do.
// Otherwise, restart after interval.
func Forever(intervalWaitingBacklog time.Duration) Policy {
	return forever(intervalWaitingBacklog)
}

type forever time.Duration

func (f forever) String() string {
	return fmt.Sprintf("forever:%s", time.Duration(f).String())
}

func (f forever) Next(updated bool, err error) loop.Next {
	if updated {
		return loop.Continue(0)
	}
	return loop.Continue(time.Duration(f))
}

// Restart immediately while there are things to do.
// Otherwise, Break(nil).
func Backlog() Policy {
	return backlog
}

type backlogPolicy struct {
	name string
}

func (b backlogPolicy) String() string {
	return b.name
}

func (b backlogPolicy) Next(updated bool, err error) loop.Next {
	if updated {
		return loop.Continue(0)
	}
	return loop.Break(nil)
}

var backlog = backlogPolicy{} // singleton

// add a provisory clause: In case of error, Break with that error.
func UntilError(p Policy) Policy {
	return untilError{base: p}
}

type untilError struct {
	base Policy
}

func (u untilError) String() string {
	return fmt.Sprintf("%s (until error)", u.base.String())
}

func (u untilError) Next(updated bool, err error) loop.Next {
	if err != nil {
		return loop.Break(err)
	}
	return u.base.Next(updated, err)
}
