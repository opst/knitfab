package cmp_test

import (
	"testing"

	"github.com/opst/knitfab-api-types/internal/utils/cmp"
)

type Int int

func (t Int) Equal(other Int) bool {
	return t == other
}

func TestSliceEqualUnordered(t *testing.T) {

	type When struct {
		A []Int
		B []Int
	}
	type Then struct {
		Want bool
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			got := cmp.SliceEqualUnordered(when.A, when.B)
			if got != then.Want {
				t.Errorf("got %v, want %v", got, then.Want)
			}
		}
	}

	t.Run("when A and B are empty", theory(
		When{A: []Int{}, B: []Int{}},
		Then{Want: true},
	))
	t.Run("when A and B are the same", theory(
		When{A: []Int{Int(1), Int(2), Int(3)}, B: []Int{Int(1), Int(2), Int(3)}},
		Then{Want: true},
	))
	t.Run("when A and B are the same but in different order", theory(
		When{A: []Int{Int(1), Int(2), Int(3)}, B: []Int{Int(3), Int(2), Int(1)}},
		Then{Want: true},
	))

	t.Run("when A and B are different", theory(
		When{A: []Int{Int(1), Int(2), Int(3)}, B: []Int{Int(1), Int(2), Int(4)}},
		Then{Want: false},
	))
	t.Run("when A and B have different length (B is shorter)", theory(
		When{A: []Int{Int(1), Int(2), Int(3)}, B: []Int{Int(1), Int(2)}},
		Then{Want: false},
	))

	t.Run("when A and B have different length (A is shorter)", theory(
		When{A: []Int{Int(1), Int(2)}, B: []Int{Int(1), Int(2), Int(3)}},
		Then{Want: false},
	))
}

func TestMapEqual(t *testing.T) {
	type When struct {
		A map[string]Int
		B map[string]Int
	}

	type Then struct {
		Want bool
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			got := cmp.MapEqual(when.A, when.B)
			if got != then.Want {
				t.Errorf("got %v, want %v", got, then.Want)
			}
		}
	}

	t.Run("when A and B are empty", theory(
		When{A: map[string]Int{}, B: map[string]Int{}},
		Then{Want: true},
	))

	t.Run("when A and B are the same", theory(
		When{
			A: map[string]Int{"a": Int(1), "b": Int(2)},
			B: map[string]Int{"a": Int(1), "b": Int(2)},
		},
		Then{Want: true},
	))

	t.Run("when A and B are same in keys, different in values", theory(
		When{
			A: map[string]Int{"a": Int(1), "b": Int(2)},
			B: map[string]Int{"a": Int(1), "b": Int(3)},
		},
		Then{Want: false},
	))

	t.Run("when A and B are different in keys", theory(
		When{
			A: map[string]Int{"a": Int(1), "b": Int(2)},
			B: map[string]Int{"a": Int(1), "c": Int(2)},
		},
		Then{Want: false},
	))

	t.Run("when A and B are different in length (A is longer)", theory(
		When{
			A: map[string]Int{"a": Int(1), "b": Int(2)},
			B: map[string]Int{"a": Int(1)},
		},
		Then{Want: false},
	))

	t.Run("when A and B are different in length (B is longer)", theory(
		When{
			A: map[string]Int{"a": Int(1)},
			B: map[string]Int{"a": Int(1), "b": Int(2)},
		},
		Then{Want: false},
	))
}
