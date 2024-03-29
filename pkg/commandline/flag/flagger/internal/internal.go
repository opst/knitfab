package internal_test

import (
	"flag"
	"time"
)

type V struct {
	Value string
}

var _ flag.Value = &V{}

func (v *V) Set(val string) error {
	v.Value = val
	return nil
}

func (v *V) String() string {
	return v.Value
}

type Flags struct {
	BoolFlag     bool          `flag:"bool,help=help bool flag,short=b"`
	IntFlag      int           `flag:"int,help=help int flag,short=i"`
	Int64Flag    int64         `flag:"int64,help=help int64 flag,short=l"`
	UintFlag     uint          `flag:"uint,help=help uint flag,short=u"`
	Uint64Flag   uint64        `flag:"uint64,help=help uint64 flag,short=U"`
	Float64Flag  float64       `flag:"float64,help=help float64 flag,short=f"`
	StrFlag      string        `flag:"string,help=help str flag,short=s"`
	DurationFlag time.Duration `flag:"duration,help=help duration flag,short=d"`
	ValueFlag    *V            `flag:"value,help=help value flag,short=v"`
}

type FlagsAllDefaulted struct {
	BoolFlag     bool          `flag:""`
	IntFlag      int           `flag:""`
	Int64Flag    int64         `flag:""`
	UintFlag     uint          `flag:""`
	Uint64Flag   uint64        `flag:""`
	Float64Flag  float64       `flag:""`
	StrFlag      string        `flag:""`
	DurationFlag time.Duration `flag:""`
	ValueFlag    *V            `flag:""`
}
