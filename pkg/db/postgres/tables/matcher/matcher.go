package matcher

import (
	"fmt"

	"github.com/opst/knitfab/pkg/cmp"
)

type Matcher[T any] interface {
	Match(T) bool
	String() string
}

type anyMatcher[T any] struct{}

func Any[T any]() Matcher[T]                       { return anyMatcher[T]{} }
func (a anyMatcher[T]) Match(T) bool               { return true }
func (a anyMatcher[T]) String() string             { return "(match any)" }
func (a anyMatcher[T]) Format(s fmt.State, _ rune) { fmt.Fprint(s, a.String()) }

type equal[T interface{ Equal(T) bool }] struct{ v T }

func Equal[T interface{ Equal(T) bool }](v T) Matcher[T] { return equal[T]{v: v} }
func (e equal[T]) Match(t T) bool                        { return t.Equal(e.v) }
func (e equal[T]) String() string                        { return fmt.Sprintf("%+v", e.v) }
func (e equal[T]) Format(s fmt.State, _ rune)            { fmt.Fprint(s, e.String()) }

type eqeq[T comparable] struct{ v T }

func EqEq[T comparable](v T) Matcher[T]      { return eqeq[T]{v: v} }
func (e eqeq[T]) Match(t T) bool             { return t == e.v }
func (e eqeq[T]) String() string             { return fmt.Sprintf("%+v", e.v) }
func (e eqeq[T]) Format(s fmt.State, _ rune) { fmt.Fprint(s, e.String()) }

type sliceContentsEq[T comparable] struct{ values []T }

func SliceContentsEq[T comparable](a []T) Matcher[[]T]  { return sliceContentsEq[T]{values: a} }
func (c sliceContentsEq[T]) Match(t []T) bool           { return cmp.SliceContentEq(c.values, t) }
func (c sliceContentsEq[T]) String() string             { return fmt.Sprintf("%+v", c.values) }
func (c sliceContentsEq[T]) Format(s fmt.State, _ rune) { fmt.Fprint(s, c.String()) }
