package graph_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/opst/knitfab-api-types/plans"
	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/knitgraph"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	krstmock "github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	"github.com/opst/knitfab/cmd/knit/subcommands/plan/graph"
	"github.com/opst/knitfab/pkg/utils/args"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/slices"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/youta-t/flarc"
)

func TestTask(t *testing.T) {

	type When struct {
		Flag graph.Flag
		Args map[string][]string
		Err  error
	}

	type Then struct {
		PlanId        string
		Dir           graph.Direction
		MaxDepth      args.Depth
		taskIsInvoked bool
		Err           error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			logger := logger.Null()
			profile := &kprof.KnitProfile{
				ApiRoot: "http://api.knit.invalid/api",
			}
			client := try.To(krst.NewClient(profile)).OrFatal(t)
			fakeCommandline := commandline.MockCommandline[graph.Flag]{
				Fullname_: "knit plan graph",
				Args_:     when.Args,
				Flags_:    when.Flag,
				Stdout_:   io.Discard,
				Stderr_:   io.Discard,
			}

			taskIsInvoked := false
			testee := graph.Task(func(
				ctx context.Context,
				client krst.KnitClient,
				graph *knitgraph.DirectedGraph,
				planId string,
				dir graph.Direction,
				maxDepth args.Depth,
			) (*knitgraph.DirectedGraph, error) {
				taskIsInvoked = true

				if graph == nil {
					t.Errorf("graph is nil")
				}

				if planId != then.PlanId {
					t.Errorf("wrong planId: %s", planId)
				}

				if dir != then.Dir {
					t.Errorf("wrong direction: %v", dir)
				}

				if !maxDepth.Equal(then.MaxDepth) {
					t.Errorf("wrong maxDepth: %v", maxDepth)
				}

				return graph, when.Err
			})

			got := testee(
				ctx,
				logger,
				*env.New(),
				client,
				fakeCommandline,
				[]interface{}{},
			)

			if then.taskIsInvoked != taskIsInvoked {
				t.Errorf("wrong taskIsInvoked: %v", taskIsInvoked)
			}

			if !errors.Is(got, then.Err) {
				t.Errorf("wrong status: (actual, expected) != (%v, %v)", got, then.Err)
			}
		}
	}

	t.Run("-u and -d is not passed, both are activated", theory(
		When{
			Flag: graph.Flag{
				Numbers:    pointer.Ref(args.NewDepth(42)),
				Upstream:   nil,
				Downstream: nil,
			},
			Args: map[string][]string{
				graph.ARG_PLANID: {"test-Id"},
			},
			Err: nil,
		},
		Then{
			PlanId: "test-Id",
			Dir: graph.Direction{
				Upstream:   true,
				Downstream: true,
			},
			MaxDepth:      args.NewDepth(42),
			taskIsInvoked: true,
			Err:           nil,
		},
	))

	t.Run("-u is passed, -d is not passed, only -u is activated", theory(
		When{
			Flag: graph.Flag{
				Numbers:    pointer.Ref(args.NewDepth(42)),
				Upstream:   pointer.Ref(true),
				Downstream: nil,
			},
			Args: map[string][]string{
				graph.ARG_PLANID: {"test-Id"},
			},
			Err: nil,
		},
		Then{
			PlanId: "test-Id",
			Dir: graph.Direction{
				Upstream:   true,
				Downstream: false,
			},
			MaxDepth:      args.NewDepth(42),
			taskIsInvoked: true,
			Err:           nil,
		},
	))

	t.Run("-d is passed, -u is not passed, only -d is activated", theory(
		When{
			Flag: graph.Flag{
				Numbers:    pointer.Ref(args.NewDepth(42)),
				Upstream:   nil,
				Downstream: pointer.Ref(true),
			},
			Args: map[string][]string{
				graph.ARG_PLANID: {"test-Id"},
			},
			Err: nil,
		},
		Then{
			PlanId: "test-Id",
			Dir: graph.Direction{
				Upstream:   false,
				Downstream: true,
			},
			MaxDepth:      args.NewDepth(42),
			taskIsInvoked: true,
			Err:           nil,
		},
	))

	t.Run("-u and -d is passed, both are activated", theory(
		When{
			Flag: graph.Flag{
				Numbers:    pointer.Ref(args.NewDepth(42)),
				Upstream:   pointer.Ref(true),
				Downstream: pointer.Ref(true),
			},
			Args: map[string][]string{
				graph.ARG_PLANID: {"test-Id"},
			},
			Err: nil,
		},
		Then{
			PlanId: "test-Id",
			Dir: graph.Direction{
				Upstream:   true,
				Downstream: true,
			},
			MaxDepth:      args.NewDepth(42),
			taskIsInvoked: true,
			Err:           nil,
		},
	))

	t.Run("when --number is zero, it returns an ErrUsage", theory(
		When{
			Flag: graph.Flag{
				Numbers:    pointer.Ref(args.NewDepth(0)),
				Upstream:   nil,
				Downstream: nil,
			},
			Args: map[string][]string{
				graph.ARG_PLANID: {"test-Id"},
			},
			Err: nil,
		},
		Then{
			taskIsInvoked: false,
			Err:           flarc.ErrUsage,
		},
	))

	fakeError := errors.New("fake error")
	t.Run("when, error is caused, it returns the error", theory(
		When{
			Flag: graph.Flag{
				Numbers:    pointer.Ref(args.NewDepth(42)),
				Upstream:   nil,
				Downstream: nil,
			},
			Args: map[string][]string{
				graph.ARG_PLANID: {"test-Id"},
			},
			Err: fakeError,
		},
		Then{
			PlanId: "test-Id",
			Dir: graph.Direction{
				Upstream:   true,
				Downstream: true,
			},
			MaxDepth:      args.NewDepth(42),
			taskIsInvoked: true,
			Err:           fakeError,
		},
	))

}

func TestMakeGraph(t *testing.T) {

	type When struct {
		StartingPlanId string
		Dir            graph.Direction
		MaxDepth       args.Depth

		Plans    map[string]plans.Detail
		ErrPlans map[string]error
	}

	type Then struct {
		FoundPlanIds []string
		Err          error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			client := krstmock.New(t)
			client.Impl.GetPlans = func(
				ctx context.Context,
				planId string,
			) (plans.Detail, error) {
				if err, ok := when.ErrPlans[planId]; ok {
					return plans.Detail{}, err
				}
				if plan, ok := when.Plans[planId]; ok {
					return plan, nil
				}

				return plans.Detail{}, errors.New("not found")
			}

			g := knitgraph.NewDirectedGraph()
			got, err := graph.MakeGraph(
				ctx, client, g, when.StartingPlanId, when.Dir, when.MaxDepth,
			)

			if !errors.Is(err, then.Err) {
				t.Errorf("wrong status: (actual, expected) != (%v, %v)", err, then.Err)
			}

			if got != nil {
				foundPlanIds := slices.Map(
					got.PlanNodes.Values(),
					func(n knitgraph.PlanNode) string { return n.PlanId },
				)

				if !cmp.SliceEq(foundPlanIds, then.FoundPlanIds) {
					t.Errorf("wrong foundPlanIds: %v", foundPlanIds)
				}
			}
		}
	}

	//
	// (upstream side)
	//
	//                                                      (no upstream here) .-[/in/1]-
	//                                                                                   \
	// p_a --[/out/1]-.-[/in/1]--> p_b --[/out/1]-.-[/in/1]--> p_c --[/out/1]-.-[/in/2] --> p_d --[(log)]-.-[/in/1]--> p_start
	//                \                                                                                   /         |
	//                 -----------------------------------------------------.-[/in/1]--> p_e --[/out/1]-.-          |
	//                                                                      |                                       /
	//                                                                     /           (no upstream here) .-[/in/2]-
	//                     (no upstream here) .--[/in/1]--> p_f --[/out/1]-
	//
	//
	// (downstream side)
	//
	// p_start --[/out/1]-.-[/in/1]--> p_1 --[/out/1]-.-[/in/1]--> p_2 --[/out/1]-.-[/in/1]--> p_3 --[/out/1]-.-[/in/1]--> p_4
	//         |           \                                                                                 /
	//         |            -[/in/1]--> p_5 --[(log)]--------------------------------------------------------
	//         |                            |        \
	//         |                            |         -. -[/in/1]--> p_6
	//         |                             \
	//         |                               -[/out/1]-.-[/in/1]--> p_7
	//         |\
	//         | -[/out/2]-.(no downstream here)
	//          \
	//           -[(log)]-.-[/in/1]--> p_8 --[(log)]-. (no downstream here)
	//
	// (explanatory notes)
	//
	// p_1 --[output path of p_1]-.-[input path of p_2]--> p_2
	// ^^^
	//  |
	// planId
	//
	fakePlans := map[string]plans.Detail{
		"p_start": {
			Summary: plans.Summary{
				PlanId: "p_start",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams: []plans.Upstream{
						{
							Plan: plans.Summary{PlanId: "p_d"},
							Log:  &plans.LogPoint{},
						},
						{
							Plan:       plans.Summary{PlanId: "p_e"},
							Mountpoint: &plans.Mountpoint{Path: "/out/1"},
						},
					},
				},
				{
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
					Upstreams:  []plans.Upstream{},
				},
			},
			Outputs: []plans.Output{
				{
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
					Downstreams: []plans.Downstream{
						{
							Plan:       plans.Summary{PlanId: "p_1"},
							Mountpoint: plans.Mountpoint{Path: "/in/1"},
						},
						{
							Plan:       plans.Summary{PlanId: "p_5"},
							Mountpoint: plans.Mountpoint{Path: "/in/1"},
						},
					},
				},
				{
					Mountpoint:  plans.Mountpoint{Path: "/out/2"},
					Downstreams: []plans.Downstream{},
				},
			},
			Log: &plans.Log{
				Downstreams: []plans.Downstream{
					{
						Plan:       plans.Summary{PlanId: "p_8"},
						Mountpoint: plans.Mountpoint{Path: "/in/1"},
					},
				},
			},
		},
		"p_a": {
			Summary: plans.Summary{
				PlanId: "p_a",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams:  []plans.Upstream{},
				},
			},
			Outputs: []plans.Output{
				{
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
					Downstreams: []plans.Downstream{
						{
							Plan:       plans.Summary{PlanId: "p_b"},
							Mountpoint: plans.Mountpoint{Path: "/in/1"},
						},
						{
							Plan:       plans.Summary{PlanId: "p_e"},
							Mountpoint: plans.Mountpoint{Path: "/in/1"},
						},
					},
				},
			},
		},
		"p_b": {
			Summary: plans.Summary{
				PlanId: "p_b",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams: []plans.Upstream{
						{
							Plan:       plans.Summary{PlanId: "p_a"},
							Mountpoint: &plans.Mountpoint{Path: "/out/1"},
						},
					},
				},
			},
			Outputs: []plans.Output{
				{
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
					Downstreams: []plans.Downstream{
						{
							Plan:       plans.Summary{PlanId: "p_c"},
							Mountpoint: plans.Mountpoint{Path: "/in/1"},
						},
					},
				},
			},
		},
		"p_c": {
			Summary: plans.Summary{
				PlanId: "p_c",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams: []plans.Upstream{
						{
							Plan:       plans.Summary{PlanId: "p_b"},
							Mountpoint: &plans.Mountpoint{Path: "/out/1"},
						},
					},
				},
			},
			Outputs: []plans.Output{
				{
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
					Downstreams: []plans.Downstream{
						{
							Plan:       plans.Summary{PlanId: "p_d"},
							Mountpoint: plans.Mountpoint{Path: "/in/2"},
						},
					},
				},
			},
		},
		"p_d": {
			Summary: plans.Summary{
				PlanId: "p_d",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams:  []plans.Upstream{},
				},
				{
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
					Upstreams: []plans.Upstream{
						{
							Plan:       plans.Summary{PlanId: "p_c"},
							Mountpoint: &plans.Mountpoint{Path: "/out/1"},
						},
					},
				},
			},
			Outputs: []plans.Output{},
			Log: &plans.Log{
				Downstreams: []plans.Downstream{
					{
						Plan:       plans.Summary{PlanId: "p_start"},
						Mountpoint: plans.Mountpoint{Path: "/in/1"},
					},
				},
			},
		},
		"p_e": {
			Summary: plans.Summary{
				PlanId: "p_e",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams: []plans.Upstream{
						{
							Plan:       plans.Summary{PlanId: "p_a"},
							Mountpoint: &plans.Mountpoint{Path: "/out/1"},
						},
						{
							Plan:       plans.Summary{PlanId: "p_f"},
							Mountpoint: &plans.Mountpoint{Path: "/out/1"},
						},
					},
				},
			},
			Outputs: []plans.Output{
				{
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
					Downstreams: []plans.Downstream{
						{
							Plan:       plans.Summary{PlanId: "p_start"},
							Mountpoint: plans.Mountpoint{Path: "/in/1"},
						},
					},
				},
			},
		},
		"p_f": {
			Summary: plans.Summary{
				PlanId: "p_f",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams:  []plans.Upstream{},
				},
			},
			Outputs: []plans.Output{
				{
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
					Downstreams: []plans.Downstream{
						{
							Plan:       plans.Summary{PlanId: "p_e"},
							Mountpoint: plans.Mountpoint{Path: "/in/1"},
						},
					},
				},
			},
		},
		"p_1": {
			Summary: plans.Summary{
				PlanId: "p_1",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams: []plans.Upstream{
						{
							Plan:       plans.Summary{PlanId: "p_start"},
							Mountpoint: &plans.Mountpoint{Path: "/out/1"},
						},
					},
				},
			},
			Outputs: []plans.Output{
				{
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
					Downstreams: []plans.Downstream{
						{
							Plan:       plans.Summary{PlanId: "p_2"},
							Mountpoint: plans.Mountpoint{Path: "/in/1"},
						},
					},
				},
			},
		},
		"p_2": {
			Summary: plans.Summary{
				PlanId: "p_2",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams: []plans.Upstream{
						{
							Plan:       plans.Summary{PlanId: "p_1"},
							Mountpoint: &plans.Mountpoint{Path: "/out/1"},
						},
					},
				},
			},
			Outputs: []plans.Output{
				{
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
					Downstreams: []plans.Downstream{
						{
							Plan:       plans.Summary{PlanId: "p_3"},
							Mountpoint: plans.Mountpoint{Path: "/in/1"},
						},
					},
				},
			},
		},
		"p_3": {
			Summary: plans.Summary{
				PlanId: "p_3",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams: []plans.Upstream{
						{
							Plan:       plans.Summary{PlanId: "p_2"},
							Mountpoint: &plans.Mountpoint{Path: "/out/1"},
						},
					},
				},
			},
			Outputs: []plans.Output{
				{
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
					Downstreams: []plans.Downstream{
						{
							Plan:       plans.Summary{PlanId: "p_4"},
							Mountpoint: plans.Mountpoint{Path: "/in/1"},
						},
					},
				},
			},
		},
		"p_4": {
			Summary: plans.Summary{
				PlanId: "p_4",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams: []plans.Upstream{
						{
							Plan:       plans.Summary{PlanId: "p_3"},
							Mountpoint: &plans.Mountpoint{Path: "/out/1"},
						},
						{
							Plan: plans.Summary{PlanId: "p_5"},
							Log:  &plans.LogPoint{},
						},
					},
				},
			},
			Outputs: []plans.Output{},
		},
		"p_5": {
			Summary: plans.Summary{
				PlanId: "p_5",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams: []plans.Upstream{
						{
							Plan:       plans.Summary{PlanId: "p_start"},
							Mountpoint: &plans.Mountpoint{Path: "/out/1"},
						},
					},
				},
			},
			Outputs: []plans.Output{
				{
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
					Downstreams: []plans.Downstream{
						{
							Plan:       plans.Summary{PlanId: "p_7"},
							Mountpoint: plans.Mountpoint{Path: "/in/1"},
						},
					},
				},
			},
			Log: &plans.Log{
				Downstreams: []plans.Downstream{
					{
						Plan:       plans.Summary{PlanId: "p_4"},
						Mountpoint: plans.Mountpoint{Path: "/in/1"},
					},
					{
						Plan:       plans.Summary{PlanId: "p_6"},
						Mountpoint: plans.Mountpoint{Path: "/in/1"},
					},
				},
			},
		},
		"p_6": {
			Summary: plans.Summary{
				PlanId: "p_6",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams: []plans.Upstream{
						{
							Plan:       plans.Summary{PlanId: "p_5"},
							Mountpoint: &plans.Mountpoint{Path: "/out/1"},
						},
					},
				},
			},
			Outputs: []plans.Output{},
		},
		"p_7": {
			Summary: plans.Summary{
				PlanId: "p_7",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams: []plans.Upstream{
						{
							Plan:       plans.Summary{PlanId: "p_5"},
							Mountpoint: &plans.Mountpoint{Path: "/out/1"},
						},
					},
				},
			},
			Outputs: []plans.Output{},
		},
		"p_8": {
			Summary: plans.Summary{
				PlanId: "p_8",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
					Upstreams: []plans.Upstream{
						{
							Plan: plans.Summary{PlanId: "p_start"},
							Log:  &plans.LogPoint{},
						},
					},
				},
			},
			Outputs: []plans.Output{},
			Log: &plans.Log{
				Downstreams: []plans.Downstream{},
			},
		},
	}

	t.Run("traverse graph to upstream only upto depth 2", theory(
		When{
			StartingPlanId: "p_start",
			Dir: graph.Direction{
				Upstream:   true,
				Downstream: false,
			},
			MaxDepth: args.NewDepth(2),

			Plans: fakePlans,
		},
		Then{
			FoundPlanIds: []string{
				"p_start",
				"p_d",
				"p_e",
				"p_c",
				"p_a",
				"p_f",
			},
			Err: nil,
		},
	))

	t.Run("traverse graph to downstream only upto depth 2", theory(
		When{
			StartingPlanId: "p_start",
			Dir: graph.Direction{
				Upstream:   false,
				Downstream: true,
			},
			MaxDepth: args.NewDepth(2),

			Plans: fakePlans,
		},
		Then{
			FoundPlanIds: []string{
				"p_start",
				"p_1",
				"p_5",
				"p_8",
				"p_2",
				"p_7",
				"p_4",
				"p_6",
			},
			Err: nil,
		},
	))

	t.Run("traverse graph to up- and downstream upto depth 2", theory(
		When{
			StartingPlanId: "p_start",
			Dir: graph.Direction{
				Upstream:   true,
				Downstream: true,
			},
			MaxDepth: args.NewDepth(2),

			Plans: fakePlans,
		},
		Then{
			FoundPlanIds: []string{
				"p_start",
				// upstream
				"p_d",
				"p_e",
				"p_c",
				"p_a",
				"p_f",
				// downstream
				"p_1",
				"p_5",
				"p_8",
				"p_2",
				"p_7",
				"p_4",
				"p_6",
			},
			Err: nil,
		},
	))

	t.Run("traverse graph to up- and downstream unlimitedly", theory(
		When{
			StartingPlanId: "p_start",
			Dir: graph.Direction{
				Upstream:   true,
				Downstream: true,
			},
			MaxDepth: args.NewInfinityDepth(),

			Plans: fakePlans,
		},
		Then{
			FoundPlanIds: []string{
				"p_start",
				// upstream
				"p_d",
				"p_e",
				"p_c",
				"p_a",
				"p_f",
				"p_b",
				// downstream
				"p_1",
				"p_5",
				"p_8",
				"p_2",
				"p_7",
				"p_4",
				"p_6",
				"p_3",
			},
			Err: nil,
		},
	))

	fakeErr := errors.New("fake error")
	t.Run("upstream: when error is caused in client, it returns the error", theory(
		When{
			StartingPlanId: "p_start",
			Dir: graph.Direction{
				Upstream:   true,
				Downstream: false,
			},
			MaxDepth: args.NewDepth(2),

			Plans:    fakePlans,
			ErrPlans: map[string]error{"p_a": fakeErr},
		},
		Then{
			FoundPlanIds: nil,
			Err:          fakeErr,
		},
	))

	t.Run("downstream: when error is caused in client, it returns the error", theory(
		When{
			StartingPlanId: "p_start",
			Dir: graph.Direction{
				Upstream:   false,
				Downstream: true,
			},
			MaxDepth: args.NewDepth(2),

			Plans:    fakePlans,
			ErrPlans: map[string]error{"p_1": fakeErr},
		},
		Then{
			FoundPlanIds: nil,
			Err:          fakeErr,
		},
	))

}
