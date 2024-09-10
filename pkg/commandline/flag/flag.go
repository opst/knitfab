package flag

import (
	"fmt"
	"strings"
	"time"

	apitags "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/rfctime"
)

type Argslice []string

func (s *Argslice) String() string {
	return fmt.Sprintf("%v", *s)
}

func (s *Argslice) Set(v string) error {
	*s = append(*s, v)
	return nil
}

type Parseable interface {
	Parse(string) error
	String()
}

type Tags []apitags.Tag

func (t *Tags) String() string {
	if t == nil || len(*t) == 0 {
		return ""
	}

	return strings.Join(utils.Map(*t, apitags.Tag.String), " ")
}

func (t *Tags) Set(v string) error {
	var tag apitags.Tag
	if err := tag.Parse(v); err != nil {
		return err
	}
	*t = append(*t, tag)
	return nil
}

type LooseRFC3339 time.Time

func (t *LooseRFC3339) String() string {
	if t == nil {
		return ""
	}
	return time.Time(*t).Format(rfctime.RFC3339DateTimeFormatZ)
}

func (t *LooseRFC3339) Set(v string) error {
	parsedTime, err := rfctime.ParseLooseRFC3339(v)
	if err != nil {
		return err
	}
	*t = LooseRFC3339(parsedTime)
	return nil
}

func (t *LooseRFC3339) Time() *time.Time {
	if t == nil {
		return nil
	}
	return (*time.Time)(t)
}

type OptionalLooseRFC3339 struct {
	v     time.Time
	isSet bool
}

func (t *OptionalLooseRFC3339) String() string {
	if t == nil || !t.isSet {
		return ""
	}
	return t.v.String()
}

func (t *OptionalLooseRFC3339) Set(v string) error {
	got, err := rfctime.ParseLooseRFC3339(v)
	if err != nil {
		return err
	}
	t.v = got.Time()
	t.isSet = true
	return nil
}

func (t *OptionalLooseRFC3339) Time() *time.Time {
	if t == nil || !t.isSet {
		return nil
	}
	return &t.v
}

type OptionalDuration struct {
	d     time.Duration
	isSet bool
}

func (t *OptionalDuration) String() string {
	if t == nil || !t.isSet {
		return ""
	}
	return t.d.String()
}

func (t *OptionalDuration) Set(v string) error {
	d, err := time.ParseDuration(v)
	if err != nil {
		return err
	}
	t.d = d
	t.isSet = true
	return nil
}

func (t *OptionalDuration) Duration() *time.Duration {
	if t == nil || !t.isSet {
		return nil
	}
	return &t.d
}
