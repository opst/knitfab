package args

import (
	"fmt"
	"strconv"
)

// Depth is a type that represents the depth of a search.
type Depth struct {
	n        uint
	infinity bool
}

func (d Depth) String() string {
	if d.infinity {
		return "all"
	}
	return fmt.Sprintf("%v", d.n)
}

// Value returns the value of the depth.
//
// Regardless of whether the depth is infinity or not, this function will return the value of the depth.
func (d Depth) Value() uint {
	return d.n
}

// IsInfinity returns whether the depth is infinity.
func (d Depth) IsInfinity() bool {
	return d.infinity
}

// NewDepth creates a new (finite) Depth with the given value.
func NewDepth(value uint) Depth {
	return Depth{n: value, infinity: false}
}

// NewInfinityDepth creates a new infinity Depth.
func NewInfinityDepth() Depth {
	return Depth{n: 0, infinity: true}
}

func (d Depth) Equal(other Depth) bool {
	if d.infinity != other.infinity {
		return false
	}
	if d.infinity {
		return true
	}

	return d.n == other.n
}

// Set sets the value of the depth.
//
// Compliant with the flag.Value interface.
func (d *Depth) Set(s string) error {
	if s == "all" {
		d.infinity = true
		return nil
	}

	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fmt.Errorf(`the value should be non-negative integer or "all": %v`, s)
	}
	d.n = (uint)(v)
	return nil
}
