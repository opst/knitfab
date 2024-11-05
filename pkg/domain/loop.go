package domain

import (
	"errors"
	"fmt"
)

type LoopType string

const (
	Projection        LoopType = "projection"
	Initialize        LoopType = "initialize"
	RunManagement     LoopType = "run_management"
	Finishing         LoopType = "finishing"
	GarbageCollection LoopType = "garbage_collection"
	Housekeeping      LoopType = "housekeeping"
)

// NOTE: we define them here, because...
//
// 1. "we have loops, they are like this" is a part of the model of knit.
//
// 2. When we make loops scalable, we will use database to throttle loops.
//

func (lt LoopType) String() string {
	return string(lt)
}

func (lt LoopType) IsKnown() bool {
	switch lt {
	case Projection, Initialize, RunManagement, Finishing, GarbageCollection, Housekeeping:
		return true
	default:
		return false
	}
}

func AsLoopType(s string) (LoopType, error) {
	l := LoopType(s)
	if l.IsKnown() {
		return l, nil
	}
	return l, fmt.Errorf(`%w: "%s"`, ErrUnknwonLoopType, s)
}

var ErrUnknwonLoopType = errors.New("unknown loop type")
