package graph

import (
	"context"
	"fmt"
	"log"

	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/knitgraph"
	"github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	"github.com/opst/knitfab/pkg/utils/args"
	"github.com/opst/knitfab/pkg/utils/nils"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/youta-t/flarc"
)

type Flag struct {
	Upstream   *bool       `flag:"upstream" alias:"u" help:"Trace the upstream of the specified Plan."`
	Downstream *bool       `flag:"downstream" alias:"d" help:"Trace the downstream of the specified Plan."`
	Numbers    *args.Depth `flag:"numbers" alias:"n" help:"Trace up to the specified depth. Trace to the upstream-most/downstream-most if 'all' is specified.,metavar=number of depth"`
}

const ARG_PLANID = "PLAN_ID"

func New() (flarc.Command, error) {
	return flarc.NewCommand(
		"Output a Plan Graph, overview of Plan pipeline, in a dot format.",
		Flag{
			Upstream:   nil,
			Downstream: nil,
			Numbers:    pointer.Ref(args.NewDepth(3)),
		},
		flarc.Args{
			{
				Name: ARG_PLANID, Required: true,
				Help: "The ID of the starting Plan to traverse.",
			},
		},
		common.NewTask(Task(MakeGraph)),
		flarc.WithDescription(`
{{ .Command }} outputs a "Plan Graph" in a dot format.

The Plan Graph explains the overview of the specified Plan and its upstream/downstream Plans with the specified depth.

"Plan A is upstream of Plan B" or "Plan B is downstream of Plan A" mean that an output of a Run of Plan A can be an input of Plan B.

The graph is output in a dot format, which can be converted to an image using the dot command.

    {{ .Command }} PLAN_ID | dot -Tpng -o graph.png

By default, the graph includes both upstream and downstream Plans upto 3 Plans away from specified plans.

To restrict the depth of the graph, use the --numbers flag.

	{{ .Command }} PLAN_ID --numbers 2 | dot -Tpng -o graph.png

Or, to traverse unlimitedly, use 'all'.

	{{ .Command }} PLAN_ID --numbers all | dot -Tpng -o graph.png

To traverse only the upstream or downstream Plans, use the --upstream or --downstream flag.

	{{ .Command }} PLAN_ID --upstream | dot -Tpng -o graph.png
	{{ .Command }} PLAN_ID --downstream | dot -Tpng -o graph.png
`),
	)
}

func Task(makeGraph MakeGraphFunc) common.Task[Flag] {
	return func(
		ctx context.Context,
		logger *log.Logger,
		knitEnv env.KnitEnv,
		client rest.KnitClient,
		cl flarc.Commandline[Flag],
		params []any,
	) error {
		planId := cl.Args()[ARG_PLANID][0]
		numbers := *cl.Flags().Numbers
		if numbers.IsZero() {
			return fmt.Errorf("%w: --numbers must be a positive integer or 'all'", flarc.ErrUsage)
		}

		dir := Direction{}
		{
			u := cl.Flags().Upstream
			d := cl.Flags().Downstream

			if u == nil && d == nil {
				dir.Upstream = true
				dir.Downstream = true
			} else {
				dir.Upstream = nils.Default(u, false)
				dir.Downstream = nils.Default(d, false)
			}
		}

		graph, err := makeGraph(
			ctx, client, knitgraph.NewDirectedGraph(), planId, dir, numbers,
		)

		if err != nil {
			return err
		}

		return graph.GenerateDot(cl.Stdout())
	}
}

type Direction struct {
	Upstream   bool
	Downstream bool
}

type MakeGraphFunc func(
	ctx context.Context,
	client rest.KnitClient,
	graph *knitgraph.DirectedGraph,
	planId string,
	dir Direction,
	maxDepth args.Depth,
) (*knitgraph.DirectedGraph, error)

func MakeGraph(
	ctx context.Context,
	client rest.KnitClient,
	graph *knitgraph.DirectedGraph,
	planId string,
	dir Direction,
	maxDepth args.Depth,
) (*knitgraph.DirectedGraph, error) {

	p, err := client.GetPlans(ctx, planId)
	if err != nil {
		return nil, err
	}
	graph.AddPlanNode(p, knitgraph.Emphasize())

	if dir.Upstream {
		if err := traverse(
			ctx, client, graph, p, maxDepth,
			func(p plans.Detail) []string {
				ret := []string{}
				for _, in := range p.Inputs {
					for _, upstream := range in.Upstreams {
						ret = append(ret, upstream.Plan.PlanId)
					}
				}
				return ret
			},
		); err != nil {
			return nil, err
		}
	}

	if dir.Downstream {
		if err := traverse(
			ctx, client, graph, p, maxDepth,
			func(p plans.Detail) []string {
				ret := []string{}
				for _, out := range p.Outputs {
					for _, downstream := range out.Downstreams {
						ret = append(ret, downstream.Plan.PlanId)
					}
				}
				if log := p.Log; log != nil {
					for _, downstream := range log.Downstreams {
						ret = append(ret, downstream.Plan.PlanId)
					}
				}
				return ret
			},
		); err != nil {
			return nil, err
		}
	}

	return graph, nil
}

func traverse(
	ctx context.Context,
	client rest.KnitClient,
	graph *knitgraph.DirectedGraph,
	start plans.Detail,
	depth args.Depth,
	next func(plans.Detail) []string,
) error {

	leaves := []plans.Detail{start}

	for 0 < len(leaves) {
		if depth.IsZero() {
			return nil
		}
		newLeaves := []plans.Detail{}

		for _, leaf := range leaves {
			for _, planId := range next(leaf) {
				if _, ok := graph.PlanNodes.Get(planId); ok {
					continue
				}

				p, err := client.GetPlans(ctx, planId)
				if err != nil {
					return err
				}
				graph.AddPlanNode(p)
				newLeaves = append(newLeaves, p)
			}
		}

		leaves = newLeaves
		depth = depth.Add(-1)
	}

	return nil
}
