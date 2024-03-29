package flag

import (
	"fmt"
	"strings"

	apitags "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/utils"
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
