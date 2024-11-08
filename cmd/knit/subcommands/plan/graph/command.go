package graph

import "github.com/opst/knitfab/pkg/utils/args"

type Flag struct {
	Upstream   bool        `flag:"upstream" alias:"u" help:"Trace the upstream of the specified Plan."`
	Downstream bool        `flag:"downstream" alias:"d" help:"Trace the downstream of the specified Plan."`
	Numbers    *args.Depth `flag:"numbers" alias:"n" help:"Trace up to the specified depth. Trace to the upstream-most/downstream-most if 'all' is specified.,metavar=number of depth"`
}
