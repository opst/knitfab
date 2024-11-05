package matcher

import (
	"fmt"
	"strings"
)

type prefix string

func Prefix(s string) Matcher[string]       { return prefix(s) }
func (p prefix) Match(s string) bool        { return strings.HasPrefix(s, string(p)) }
func (p prefix) String() string             { return string(p) }
func (p prefix) Format(s fmt.State, _ rune) { fmt.Fprint(s, p.String()) }
