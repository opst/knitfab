package cmp_test

import (
	"testing"

	"github.com/opst/knitfab/pkg/cmp"
)

type p struct {
	p1 string
	p2 int
}

func (a p) Equal(other cmp.Eq) bool {
	b, ok := other.(p)
	if !ok {
		return false
	}
	return a.p1 == b.p1 && a.p2 == b.p2
}

func TestEq(t *testing.T) {
	t.Run("they are equals when same type & same value", func(t *testing.T) {
		a := p{
			p1: "t",
			p2: 23,
		}
		b := p{
			p1: "t",
			p2: 23,
		}
		if !cmp.Equal(a, b) {
			t.Error("a != b, unexpectedly.")
		}
		if !cmp.Equal(b, a) {
			t.Error("a != b, unexpectedly.")
		}
	})

	t.Run("they are equals when same type & same value", func(t *testing.T) {
		a := p{
			p1: "s",
			p2: 23,
		}
		b := p{
			p1: "t",
			p2: 23,
		}
		c := p{
			p1: "s",
			p2: 22,
		}
		if cmp.Equal(a, b) {
			t.Error("a == b, unexpectedly.")
		}
		if cmp.Equal(a, c) {
			t.Error("a == c, unexpectedly.")
		}
		if cmp.Equal(b, c) {
			t.Error("c == b, unexpectedly.")
		}
	})
}
