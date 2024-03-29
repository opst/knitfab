package matcher

import (
	"fmt"
	"time"
)

type after struct {
	from time.Time
}

func (a after) Match(t time.Time) bool {
	return t.After(a.from)
}

func (a after) String() string {
	return fmt.Sprintf("after %s", a.from)
}

func After(t time.Time) Matcher[time.Time] {
	return after{from: t}
}

type between struct {
	from time.Time
	to   time.Time
}

func (b between) Match(t time.Time) bool {
	return !t.Before(b.from) && !t.After(b.to)
}

func (b between) String() string {
	return fmt.Sprintf("between %s and %s", b.from, b.to)
}

func Between(from, to time.Time) Matcher[time.Time] {
	return between{from: from, to: to}
}
