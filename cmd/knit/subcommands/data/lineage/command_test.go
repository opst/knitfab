package lineage_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	"github.com/opst/knitfab/cmd/knit/knitgraph"
	"github.com/youta-t/flarc"

	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/data/lineage"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	"github.com/opst/knitfab/pkg/utils/args"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestLineageCommand(t *testing.T) {
	mockGraphFromUpstream := knitgraph.NewDirectedGraph()
	mockGraphFromDownstream := knitgraph.NewDirectedGraph()

	type when struct {
		knitId string
		flag   lineage.Flag
		err    error
	}

	type then struct {
		err error
	}

	theory := func(when when, then then) func(*testing.T) {
		return func(t *testing.T) {
			profile := &kprof.KnitProfile{ApiRoot: "http://api.knit.invalid"}
			client := try.To(krst.NewClient(profile)).OrFatal(t)

			funcForUpstream := func(
				ctx context.Context,
				client krst.KnitClient,
				graph *knitgraph.DirectedGraph,
				knitId string,
				depth args.Depth,
			) (*knitgraph.DirectedGraph, error) {
				if !when.flag.Upstream && when.flag.Downstream {
					t.Errorf("should not call task for traceupstream")
				}
				return mockGraphFromUpstream, when.err
			}
			funcForDownstream := func(
				ctx context.Context,
				client krst.KnitClient,
				graph *knitgraph.DirectedGraph,
				knitId string,
				depth args.Depth,
			) (*knitgraph.DirectedGraph, error) {
				if when.flag.Upstream && !when.flag.Downstream {
					t.Errorf("should not call task for tracedownstream")
				}
				if graph == mockGraphFromUpstream {
					if !when.flag.Upstream && when.flag.Downstream {
						t.Errorf("should also call task for tracdownstream immediately")
					}
				}
				return mockGraphFromDownstream, when.err
			}

			testee := lineage.Task(
				lineage.Traverser{ForUpstream: funcForUpstream, ForDownstream: funcForDownstream},
			)

			stdout := new(strings.Builder)
			stderr := new(strings.Builder)

			ctx := context.Background()

			//test start
			actual := testee(
				ctx, logger.Null(),
				*env.New(),
				client,
				commandline.MockCommandline[lineage.Flag]{
					Stdout_: stdout,
					Stderr_: stderr,
					Flags_:  when.flag,
					Args_: map[string][]string{
						lineage.ARG_KNITID: {when.knitId},
					},
				},
				[]any{},
			)

			if !errors.Is(actual, then.err) {
				t.Errorf(
					"wrong status: (actual, expected) != (%d, %d)",
					actual, then.err,
				)
			}
		}
	}

	t.Run("when only upstream flag is specifed, it should call only task for traceupstream.", theory(
		when{
			knitId: "test-Id",
			flag: lineage.Flag{
				Upstream:   true,
				Downstream: false,
				Numbers:    pointer.Ref(args.NewDepth(3)),
			},
			err: nil,
		},
		then{
			err: nil,
		},
	))

	t.Run("when only downstream flag is specifed, it should call only task for tracedownstream.", theory(
		when{
			knitId: "test-Id",
			flag: lineage.Flag{
				Upstream:   false,
				Downstream: true,
				Numbers:    pointer.Ref(args.NewDepth(3)),
			},
			err: nil,
		},
		then{
			err: nil,
		},
	))

	t.Run("when upstream and downstream flag are specifed, it should call both tasks", theory(
		when{
			knitId: "test-Id",
			flag: lineage.Flag{
				Upstream:   true,
				Downstream: true,
				Numbers:    pointer.Ref(args.NewDepth(3)),
			},
			err: nil,
		},
		then{
			err: nil,
		},
	))

	t.Run("when upstream and downstream flag are not specifed, it should call both tasks", theory(
		when{
			knitId: "test-Id",
			flag: lineage.Flag{
				Upstream:   false,
				Downstream: false,
				Numbers:    pointer.Ref(args.NewDepth(3)),
			},
			err: nil,
		},
		then{
			err: nil,
		},
	))

	t.Run("when number of depth flag is natural number it shoule return Exitsucess", theory(
		when{
			knitId: "test-Id",
			flag: lineage.Flag{
				Upstream:   false,
				Downstream: false,
				Numbers:    pointer.Ref(args.NewDepth(1)),
			},
			err: nil,
		},
		then{
			err: nil,
		},
	))

	t.Run("when number of depth flag is `all` it shoule return Exitsucess", theory(
		when{
			knitId: "test-Id",
			flag: lineage.Flag{
				Upstream:   false,
				Downstream: false,
				Numbers:    pointer.Ref(args.NewInfinityDepth()),
			},
			err: nil,
		},
		then{
			err: nil,
		},
	))

	t.Run("when number of depth flag is 0 or under, it shoule return ErrUsage", theory(
		when{
			knitId: "test-Id",
			flag: lineage.Flag{
				Upstream:   false,
				Downstream: false,
				Numbers:    pointer.Ref(args.NewDepth(0)),
			},
			err: nil,
		},
		then{
			err: flarc.ErrUsage,
		},
	))

	err := errors.New("fake error")
	t.Run("when task returns error, it should return with ExitFailure", theory(
		when{
			knitId: "test-Id",
			flag: lineage.Flag{
				Upstream:   false,
				Downstream: false,
				Numbers:    pointer.Ref(args.NewDepth(3)),
			},
			err: err,
		},
		then{
			err: err,
		},
	))
}

func TestTraceDownStream(t *testing.T) {
	type When struct {
		RootKnitId      string
		Depth           args.Depth
		ArgGraph        *knitgraph.DirectedGraph
		FindDataReturns [][]data.Detail
		GetRunReturns   []runs.Detail
	}

	type Then struct {
		Graph *knitgraph.DirectedGraph
		Err   error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {

			mock := mock.New(t)

			// Store arguments and return values for each call
			nthData := 0
			mock.Impl.FindData = func(
				ctx context.Context, tags []tags.Tag, since *time.Time, duration *time.Duration,
			) ([]data.Detail, error) {
				ret := when.FindDataReturns[nthData]
				nthData += 1
				return ret, nil
			}

			nthRun := 0
			mock.Impl.GetRun = func(
				ctx context.Context, runId string,
			) (runs.Detail, error) {
				ret := when.GetRunReturns[nthRun]
				nthRun += 1
				return ret, nil
			}

			ctx := context.Background()
			graph, actual := lineage.TraceDownStream(ctx, mock, when.ArgGraph, when.RootKnitId, when.Depth)
			if !errors.Is(actual, then.Err) {
				t.Errorf(
					"wrong status: (actual, expected) != (%d, %d)",
					actual, then.Err,
				)
			}

			if !cmp.MapEqWith(
				graph.DataNodes.ToMap(),
				then.Graph.DataNodes.ToMap(),
				func(a, b knitgraph.DataNode) bool { return a.Equal(&b) },
			) {
				t.Errorf(
					"DataNodes is not equal (actual,expected): %v,%v",
					graph.DataNodes, then.Graph.DataNodes,
				)
			}
			if !cmp.MapEqWith(
				graph.RunNodes.ToMap(),
				then.Graph.RunNodes.ToMap(),
				func(a, b knitgraph.RunNode) bool { return a.Summary.Equal(b.Summary) },
			) {
				t.Errorf(
					"RunNodes is not equal (actual,expected): %v,%v",
					graph.RunNodes, then.Graph.RunNodes,
				)
			}
			if !cmp.SliceContentEq(graph.Edges, then.Graph.Edges) {
				t.Errorf(
					"Edges is not equal (actual,expected): %v,%v",
					graph.Edges, then.Graph.Edges,
				)
			}
			if !cmp.SliceContentEq(graph.RootNodes, then.Graph.RootNodes) {
				t.Errorf(
					"RootNodes is not equal (actual,expected): %v,%v",
					graph.RootNodes, then.Graph.RootNodes,
				)
			}
		}
	}

	{
		// [test case of data lineage]
		// data1 --[/in/1]--> run1 --[/out/1]--> data2
		//nodes in the order of appearance to the graph.
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run0"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run1"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Image:  &plans.Image{Repository: "repo1", Tag: "tag1"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
		}

		t.Run("Confirm that all the above nodes can be traced in graph depth:1", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewDepth(1),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}},
				GetRunReturns:   []runs.Detail{run1},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithRun(run1),
				),
				Err: nil,
			},
		))

		t.Run("Confirm that the tracing result remains the same when graph depth is no limit", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewInfinityDepth(),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}},
				GetRunReturns:   []runs.Detail{run1},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithRun(run1),
				),
				Err: nil,
			},
		))
	}

	{
		// [test case of data lineage]
		// data1 --[/in/1]--> run1 --[/out/1]--> data2 --[/in/2]--> run2 --[/out/2]--> data3
		//                                                               \
		//                                                                -[/out/3]--> data4
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run0"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run1"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Image:  &plans.Image{Repository: "repo1", Tag: "tag1"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
		}
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2",
				Plan: plans.Summary{
					PlanId: "plan2",
					Image:  &plans.Image{Repository: "repo2", Tag: "tag2"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/out/2"},
				},
				{
					KnitId:     "data4",
					Mountpoint: plans.Mountpoint{Path: "/out/3"},
				},
			},
		}
		data3 := data.Detail{
			KnitId: "data3",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/2"},
			},
		}
		data4 := data.Detail{
			KnitId: "data4",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/3"},
			},
		}
		t.Run("Confirm that all nodes can be traced in graph depth:2", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewDepth(2),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []runs.Detail{run1, run2},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithData(data4),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run2),
				),
				Err: nil,
			},
		))

		t.Run("Confirm that it traces up to data2 in graph depth:1", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewDepth(1),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}},
				GetRunReturns:   []runs.Detail{run1},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithRun(run1),
				),
				Err: nil,
			},
		),
		)
	}

	{
		// [test case of data lineage]
		// data1(arg) --[/in/1]--> run1 --[/out/1]--> data3
		//                      /
		//      data2 --[/in/2]-
		//            \
		//             -[/in/2]--> run2 (do not searched, because it is not downstream of data1)
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "runxx"},
				Mountpoint: &plans.Mountpoint{Path: "/out/x1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run1"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Image:  &plans.Image{Repository: "repo1", Tag: "tag1"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "runxx"},
				Mountpoint: &plans.Mountpoint{Path: "/out/x2"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run1"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
		}
		data3 := data.Detail{
			KnitId: "data3",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{},
		}

		t.Run("Confirm that Nodes except run2 can be traced in graph depth: no limit", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewInfinityDepth(),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}, {data3}},
				GetRunReturns:   []runs.Detail{run1},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithRun(run1),
				),
				Err: nil,
			},
		))
	}

	{
		// [test case of data lineage]
		// data1(arg) --[/in/1]--> run1 --[/out/1]--> data2 --[/in/3]--> run3 --[/out/3]--> data4
		//            \                                               /
		//             -[/in/2]--> run2 --[/out/2]--> data3 --[/in/4]-
		data1 := data.Detail{
			KnitId:   "data1",
			Upstream: data.CreatedFrom{},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run1"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Image:  &plans.Image{Repository: "repo1", Tag: "tag1"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
			},
		}
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2",
				Plan: plans.Summary{
					PlanId: "plan2",
					Image:  &plans.Image{Repository: "repo2", Tag: "tag2"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/out/2"},
				},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run3"},
					Mountpoint: plans.Mountpoint{Path: "/in/3"},
				},
			},
		}
		data3 := data.Detail{
			KnitId: "data3",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/2"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run3"},
					Mountpoint: plans.Mountpoint{Path: "/in/4"},
				},
			},
		}
		run3 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run3",
				Plan: plans.Summary{
					PlanId: "plan3",
					Image:  &plans.Image{Repository: "repo3", Tag: "tag3"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/in/3"},
				},
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/in/4"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data4",
					Mountpoint: plans.Mountpoint{Path: "/out/3"},
				},
			},
		}
		data4 := data.Detail{
			KnitId: "data4",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run3"},
				Mountpoint: &plans.Mountpoint{Path: "/out/3"},
			},
		}

		t.Run("Confirm that all nodes can be traced", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewInfinityDepth(),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []runs.Detail{run1, run2, run3},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithData(data4),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run2),
					knitgraph.WithRun(run3),
				),
				Err: nil,
			},
		))
	}

	{
		// [test case of data lineage]
		// data1(arg) --[/in/1]---------> run1 --[/out/1]--> data3 --[/in/3]--> run2 --[/out/2]--> data4
		//                            /                                      /
		//        ------------[/in/2]-                                      |
		//       /                                                         /
		// data2 --------------------------------------------------[/in/4]-
		//       \
		//        ------------[/in/2]--> run3 -- (do not searched, because it is not downstream of data1)
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "runxx"},
				Mountpoint: &plans.Mountpoint{Path: "/out/x1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run1"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/3"},
				},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Image:  &plans.Image{Repository: "repo1", Tag: "tag1"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "runxx"},
				Mountpoint: &plans.Mountpoint{Path: "/out/x2"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run1"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/4"},
				},
				{
					Run:        runs.Summary{RunId: "run3"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
		}
		data3 := data.Detail{
			KnitId: "data3",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/3"},
				},
			},
		}
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2",
				Plan: plans.Summary{
					PlanId: "plan2",
					Image:  &plans.Image{Repository: "repo2", Tag: "tag2"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/in/3"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data4",
					Mountpoint: plans.Mountpoint{Path: "/out/2"},
				},
			},
		}
		data4 := data.Detail{
			KnitId: "data4",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/2"},
			},
		}

		t.Run("Confirm that it traces only downstreams and direct inputs in downstream runs", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewInfinityDepth(),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []runs.Detail{run1, run2},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithData(data4),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run2),
				),
				Err: nil,
			},
		))
	}

	{
		// [test case of data lineage]
		//                                                                                    -[/in/5]--> run4 --[/out/4]--> data5
		//                                                                                   /
		// data1 --[/in/1]--> run1 --[/out/1]--> data2 --[/in/3]--> run2 --[/out/2]--> data3 --[/in/4]--> run3 --[/out/3]--> data4
		//       \                                                                                     /
		//        -[/in/2]-----------------------------------------------------------------------------
		//Appearing nodes in graph depth:1
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "runxx"},
				Mountpoint: &plans.Mountpoint{Path: "/out/x1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run1"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Image:  &plans.Image{Repository: "repo1", Tag: "tag1"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/3"},
				},
			},
		}
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2",
				Plan: plans.Summary{
					PlanId: "plan2",
					Image:  &plans.Image{Repository: "repo2", Tag: "tag2"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/in/3"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/out/2"},
				},
			},
		}
		data3 := data.Detail{
			KnitId: "data3",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/2"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run3"},
					Mountpoint: plans.Mountpoint{Path: "/in/4"},
				},
				{
					Run:        runs.Summary{RunId: "run4"},
					Mountpoint: plans.Mountpoint{Path: "/in/5"},
				},
			},
		}
		run3 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run3",
				Plan: plans.Summary{
					PlanId: "plan3",
					Image:  &plans.Image{Repository: "repo3", Tag: "tag3"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/in/4"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data4",
					Mountpoint: plans.Mountpoint{Path: "/out/3"},
				},
			},
		}
		data4 := data.Detail{
			KnitId: "data4",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run3"},
				Mountpoint: &plans.Mountpoint{Path: "/out/3"},
			},
			Downstreams: []data.AssignedTo{},
		}
		run4 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run4",
				Plan: plans.Summary{
					PlanId: "plan4",
					Image:  &plans.Image{Repository: "repo4", Tag: "tag4"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/in/5"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data5",
					Mountpoint: plans.Mountpoint{Path: "/out/4"},
				},
			},
		}
		data5 := data.Detail{
			KnitId: "data5",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run4"},
				Mountpoint: &plans.Mountpoint{Path: "/out/4"},
			},
			Downstreams: []data.AssignedTo{},
		}
		t.Run("Confirm that nodes except run2,run4 and data5 can be traced in graph depth:1", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewDepth(1),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []runs.Detail{run1, run3},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithData(data4),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run3),
				),
				Err: nil,
			},
		))

		t.Run("Confirm that nodes except run4 and data5 can be traced in graph depth:2", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewDepth(2),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []runs.Detail{run1, run3, run2},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithData(data4),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run3),
					knitgraph.WithRun(run2),
				),
				Err: nil,
			},
		))

		t.Run("Confirm that all nodes can be traced in graph depth:3", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewDepth(3),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}, {data3}, {data4}, {data5}},
				GetRunReturns:   []runs.Detail{run1, run3, run2, run4},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithData(data4),
					knitgraph.WithData(data5),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run3),
					knitgraph.WithRun(run2),
					knitgraph.WithRun(run4),
				),
				Err: nil,
			},
		))
	}

	{
		// [test case of data lineage]
		// data1(arg) --[/in/1]--> run1 --[/out/1]--> data2 --[/in/2]--> run2 (no outputs)
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "runxx"},
				Mountpoint: &plans.Mountpoint{Path: "/out/x1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run1"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Image:  &plans.Image{Repository: "repo1", Tag: "tag1"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
		}
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2",
				Plan: plans.Summary{
					PlanId: "plan2",
					Image:  &plans.Image{Repository: "repo2", Tag: "tag2"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
			Outputs: []runs.Assignment{},
		}
		t.Run("confirm that all nodes can be traced even when the run does not have an output", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewInfinityDepth(),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}},
				GetRunReturns:   []runs.Detail{run1, run2},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run2),
				),
				Err: nil,
			},
		))
	}
	{
		// [test case of data lineage]
		//
		// data3 --[/in/1]--> run1 --[/out/1]--> data1(arg) --[/in/2]--> run2 --[/out/3]--> data4
		//                         \                                  /
		//                          -[/out/2]--> data2 -------[/in/3]-
		//

		//Appearing nodes by tracing upstream from data1
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Image:  &plans.Image{Repository: "repo1", Tag: "tag1"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/out/2"},
				},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/out/2"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/3"},
				},
			},
		}
		data3 := data.Detail{
			KnitId: "data3",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "runxx"},
				Mountpoint: &plans.Mountpoint{Path: "/out/x3"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run1"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
		}
		//Appearing nodes by tracing downstream from data1
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2",
				Plan: plans.Summary{
					PlanId: "plan2",
					Image:  &plans.Image{Repository: "repo2", Tag: "tag2"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/in/3"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data4",
					Mountpoint: plans.Mountpoint{Path: "/out/3"},
				},
			},
		}
		data4 := data.Detail{
			KnitId: "data4",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/3"},
			},
			Downstreams: []data.AssignedTo{},
		}

		t.Run("Confirm that all nodes can be obtained by tracing both upstream and downstream.", theory(
			When{
				RootKnitId: "data1",
				Depth:      args.NewInfinityDepth(),
				ArgGraph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithRun(run1),
				),
				FindDataReturns: [][]data.Detail{{data4}},
				GetRunReturns:   []runs.Detail{run2},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithData(data4),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run2),
				),
				Err: nil,
			},
		))
	}

	// [test case of data lineage]
	//
	// data1 --[/in/1]--> run1 --[/out/1]--> data2
	//                         \
	//                          -[(log)]--> data3(log) --[/in/2]--> run2 --[/out/2]--> data4

	{
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "runxx"},
				Mountpoint: &plans.Mountpoint{Path: "/out/x1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run1"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Image:  &plans.Image{Repository: "repo1", Tag: "tag1"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
			},
			Log: &runs.LogSummary{
				KnitId:   "data3",
				LogPoint: plans.LogPoint{},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{},
		}
		data3 := data.Detail{
			KnitId: "data3",
			Upstream: data.CreatedFrom{
				Run: runs.Summary{RunId: "run1"},
				Log: &plans.LogPoint{},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
		}
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2",
				Plan: plans.Summary{
					PlanId: "plan2",
					Image:  &plans.Image{Repository: "repo2", Tag: "tag2"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data4",
					Mountpoint: plans.Mountpoint{Path: "/out/2"},
				},
			},
		}
		data4 := data.Detail{
			KnitId: "data4",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/2"},
			},
			Downstreams: []data.AssignedTo{},
		}

		t.Run("Confirm that all nodes containg log can be obtained", theory(
			When{
				RootKnitId: "data1",
				Depth:      args.NewInfinityDepth(),
				ArgGraph:   knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{
					{data1}, {data2}, {data3}, {data4},
				},
				GetRunReturns: []runs.Detail{run1, run2},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithData(data4),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run2),
				),
				Err: nil,
			},
		))
	}

	t.Run("When FindData returns Empty array it returns ErrNotFoundData", func(t *testing.T) {
		mock := mock.New(t)
		mock.Impl.FindData = func(ctx context.Context, tags []tags.Tag, since *time.Time, duration *time.Duration) ([]data.Detail, error) {
			return []data.Detail{}, nil
		}
		mock.Impl.GetRun = func(ctx context.Context, runId string) (runs.Detail, error) {
			return runs.Detail{}, nil
		}

		ctx := context.Background()
		graph := knitgraph.NewDirectedGraph()
		knitId := "knitId-test"
		_, actual := lineage.TraceDownStream(ctx, mock, graph, knitId, args.NewDepth(1))
		if !errors.Is(actual, lineage.ErrNotFoundData) {
			t.Errorf("wrong status: (actual, expected) != (%v, %v)", actual, lineage.ErrNotFoundData)
		}
	})

	expectedError := errors.New("fake error")
	t.Run("When FindData fails, it returns the error that contains that error ", func(t *testing.T) {
		mock := mock.New(t)
		mock.Impl.FindData = func(ctx context.Context, tags []tags.Tag, since *time.Time, duration *time.Duration) ([]data.Detail, error) {
			return []data.Detail{}, expectedError
		}
		mock.Impl.GetRun = func(ctx context.Context, runId string) (runs.Detail, error) {
			return runs.Detail{}, nil
		}

		ctx := context.Background()
		graph := knitgraph.NewDirectedGraph()
		knitId := "knitId-test"
		_, actual := lineage.TraceDownStream(ctx, mock, graph, knitId, args.NewDepth(1))
		if !errors.Is(actual, expectedError) {
			t.Errorf("wrong status: (actual, expected) != (%v, %v)", actual, expectedError)
		}
	})

	t.Run("When GetRun fails, it returns the error that contains that error", func(t *testing.T) {
		mock := mock.New(t)
		knitId := "knitId-test"
		mock.Impl.FindData = func(ctx context.Context, tags []tags.Tag, since *time.Time, duration *time.Duration) ([]data.Detail, error) {
			return []data.Detail{
				{
					KnitId: knitId,
					Upstream: data.CreatedFrom{
						Run:        runs.Summary{RunId: "run1"},
						Mountpoint: &plans.Mountpoint{Path: "/out/1"},
					},
					Downstreams: []data.AssignedTo{
						{
							Run:        runs.Summary{RunId: "run2"},
							Mountpoint: plans.Mountpoint{Path: "/in/1"},
						},
					},
				},
			}, nil
		}
		mock.Impl.GetRun = func(ctx context.Context, runId string) (runs.Detail, error) {
			return runs.Detail{}, expectedError
		}
		ctx := context.Background()
		graph := knitgraph.NewDirectedGraph()
		_, actual := lineage.TraceDownStream(ctx, mock, graph, knitId, args.NewDepth(1))
		if !errors.Is(actual, expectedError) {
			t.Errorf("wrong status: (actual, expected) != (%s, %d)", actual, expectedError)
		}
	})
}

func TestTraceUpStream(t *testing.T) {
	type When struct {
		RootKnitId      string
		Depth           args.Depth
		FindDataReturns [][]data.Detail
		GetRunReturns   []runs.Detail
	}

	type Then struct {
		Graph *knitgraph.DirectedGraph
		Err   error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {

			mock := mock.New(t)

			// Store arguments and return values for each call
			nthData := 0
			mock.Impl.FindData = func(
				ctx context.Context, tags []tags.Tag, since *time.Time, duration *time.Duration) (
				[]data.Detail, error) {
				ret := when.FindDataReturns[nthData]
				nthData += 1
				return ret, nil
			}

			nthRun := 0
			mock.Impl.GetRun = func(
				ctx context.Context, runId string) (
				runs.Detail, error) {
				ret := when.GetRunReturns[nthRun]
				nthRun += 1
				return ret, nil
			}

			ctx := context.Background()
			graph := knitgraph.NewDirectedGraph()
			graph, actual := lineage.TraceUpStream(ctx, mock, graph, when.RootKnitId, when.Depth)
			if !errors.Is(actual, then.Err) {
				t.Errorf(
					"wrong status: (actual, expected) != (%d, %d)",
					actual, then.Err,
				)
			}

			if !cmp.MapEqWith(
				graph.DataNodes.ToMap(),
				then.Graph.DataNodes.ToMap(),
				func(a, b knitgraph.DataNode) bool { return a.Equal(&b) },
			) {
				t.Errorf(
					"DataNodes is not equal (actual,expected): %v,%v",
					graph.DataNodes, then.Graph.DataNodes,
				)
			}
			if !cmp.MapEqWith(
				graph.RunNodes.ToMap(),
				then.Graph.RunNodes.ToMap(),
				func(a, b knitgraph.RunNode) bool { return a.Summary.Equal(b.Summary) },
			) {
				t.Errorf(
					"RunNodes is not equal (actual,expected): %v,%v",
					graph.RunNodes, then.Graph.RunNodes,
				)
			}
			if !cmp.SliceContentEq(graph.Edges, then.Graph.Edges) {
				t.Errorf(
					"EdgesFromData is not equal (actual,expected): %v,%v",
					graph.Edges, then.Graph.Edges,
				)
			}
			if !cmp.SliceContentEq(
				graph.RootNodes, then.Graph.RootNodes,
			) {
				t.Errorf(
					"RootNodes is not equal (actual,expected): %v,%v",
					graph.RootNodes, then.Graph.RootNodes,
				)
			}
		}
	}

	{
		// [test case of data lineage]
		// root --> run1 --[/upload]--> data1(arg)

		//nodes are in the order of appearance to the graph.
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/upload"},
			},
			Downstreams: []data.AssignedTo{},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Name:   "knit#uploaded",
				},
			},
			Inputs: []runs.Assignment{},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/upload"},
				},
			},
		}

		t.Run("Confirm that all the above nodes can be traced in graph depth:1", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewDepth(1),
				FindDataReturns: [][]data.Detail{{data1}},
				GetRunReturns:   []runs.Detail{run1},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithRun(run1),
				),
				Err: nil,
			},
		))
		t.Run("Confirm that all the above nodes can be traced in graph depth: no limit", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewInfinityDepth(),
				FindDataReturns: [][]data.Detail{{data1}},
				GetRunReturns:   []runs.Detail{run1},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, knitgraph.Emphasize()),
					knitgraph.WithRun(run1),
				),
				Err: nil,
			},
		))
	}

	{
		// [test case of data lineage]
		// root -->  run1 --[/upload]--> data1 --[/in/1]--> run2 --[/out/1]--> data2(arg)
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{},
		}
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2",
				Plan: plans.Summary{
					PlanId: "plan2",
					Name:   "knit#uploaded",
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
			},
		}
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/upload"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Name:   "knit#uploaded",
				},
			},
			Inputs: []runs.Assignment{},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/upload"},
				},
			},
		}
		t.Run("Confirm that all nodes can be traced in graph depth:2", theory(
			When{
				RootKnitId:      "data2",
				Depth:           args.NewDepth(2),
				FindDataReturns: [][]data.Detail{{data2}, {data1}},
				GetRunReturns:   []runs.Detail{run2, run1},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1),
					knitgraph.WithData(data2, knitgraph.Emphasize()),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run2),
				),
				Err: nil,
			},
		))
		t.Run("Confirm that all nodes can be traced up to data1 in graph depth:1", theory(
			When{
				RootKnitId:      "data2",
				Depth:           args.NewDepth(1),
				FindDataReturns: [][]data.Detail{{data2}, {data1}},
				GetRunReturns:   []runs.Detail{run2},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1),
					knitgraph.WithData(data2, knitgraph.Emphasize()),
					knitgraph.WithRun(run2),
				),
				Err: nil,
			},
		))
	}

	{
		// [test case of data lineage]
		// root --> run1 --[/upload]--> data1 --[/in/1]--> run2 --[/out/1]--> data2
		//                                    \                 \
		//                                     |                 -> [/out/3] --> data3
		//                                      \
		//                                       --[/in/2]--> run3 (is not searched since it is not upstream of data2)
		data3 := data.Detail{
			KnitId: "data3",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/3"},
			},
			Downstreams: []data.AssignedTo{},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{},
		}
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2",
				Plan: plans.Summary{
					PlanId: "plan2",
					Image:  &plans.Image{Repository: "repo2", Tag: "tag2"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/out/3"},
				},
			},
		}
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/upload"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
				{
					Run:        runs.Summary{RunId: "run3"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Name:   "knit#uploaded",
				},
			},
			Inputs: []runs.Assignment{},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/upload"},
				},
			},
		}
		t.Run("Confirm it traces only upstreams and direct outputs in upstream runs", theory(
			When{
				RootKnitId:      "data3",
				Depth:           args.NewDepth(2),
				FindDataReturns: [][]data.Detail{{data3}, {data2}, {data1}},
				GetRunReturns:   []runs.Detail{run2, run1},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3, knitgraph.Emphasize()),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run2),
				),
				Err: nil,
			},
		))
	}

	{
		// [test case of data lineage]
		// root --> run1 --[/upload]--> data1 --[/in/1]--> run2 --[/out/1]--> data2 --[/in/2]--> run3 --[/out/3]--> data4(arg)
		//                                                      \                             /
		//                                                       -[/out/2]--> data3 --[/in/3]-
		data4 := data.Detail{
			KnitId: "data4",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run3"},
				Mountpoint: &plans.Mountpoint{Path: "/out/3"},
			},
			Downstreams: []data.AssignedTo{},
		}
		run3 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run3",
				Plan: plans.Summary{
					PlanId: "plan3",
					Image:  &plans.Image{Repository: "repo3", Tag: "tag3"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/in/3"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data4",
					Mountpoint: plans.Mountpoint{Path: "/out/3"},
				},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run3"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
		}
		data3 := data.Detail{
			KnitId: "data3",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/2"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run3"},
					Mountpoint: plans.Mountpoint{Path: "/in/3"},
				},
			},
		}
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2",
				Plan: plans.Summary{
					PlanId: "plan2",
					Image:  &plans.Image{Repository: "repo2", Tag: "tag2"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/out/2"},
				},
			},
		}
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/upload"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Name:   "knit#uploaded",
				},
			},
			Inputs: []runs.Assignment{},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/upload"},
				},
			},
		}

		t.Run("Confirm that all nodes can be traced", theory(
			When{
				RootKnitId:      "data4",
				Depth:           args.NewInfinityDepth(),
				FindDataReturns: [][]data.Detail{{data4}, {data2}, {data3}, {data1}},
				GetRunReturns:   []runs.Detail{run3, run2, run1},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithData(data4, knitgraph.Emphasize()),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run2),
					knitgraph.WithRun(run3),
				),
				Err: nil,
			},
		))
	}

	{
		// [test case of data lineage]
		// root --> run1 --[/upload]--> data1 --[/in/1]--> run2 --[/out/1]--> data2 --[/in/4]--> run3 --[/out/2]--> data3 --[/in/4]--> run4 --[/out/3]--> data4(arg)
		//                                    |\                                              /                                     /
		//                                    | -[/in/2]--------------------------------------                                     |
		//                                     \                                                                                  /
		//                                      -[/in/3]--------------------------------------------------------------------------
		data4 := data.Detail{
			KnitId: "data4",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run4"},
				Mountpoint: &plans.Mountpoint{Path: "/out/3"},
			},
			Downstreams: []data.AssignedTo{},
		}
		run4 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run4",
				Plan: plans.Summary{
					PlanId: "plan4",
					Image:  &plans.Image{Repository: "repo4", Tag: "tag4"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/3"},
				},
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/in/4"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data4",
					Mountpoint: plans.Mountpoint{Path: "/out/3"},
				},
			},
		}
		data3 := data.Detail{
			KnitId: "data3",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run3"},
				Mountpoint: &plans.Mountpoint{Path: "/out/2"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run4"},
					Mountpoint: plans.Mountpoint{Path: "/in/4"},
				},
			},
		}
		run3 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run3",
				Plan: plans.Summary{
					PlanId: "plan3",
					Image:  &plans.Image{Repository: "repo3", Tag: "tag3"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/in/4"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/out/2"},
				},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run3"},
					Mountpoint: plans.Mountpoint{Path: "/in/4"},
				},
			},
		}
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2",
				Plan: plans.Summary{
					PlanId: "plan2",
					Image:  &plans.Image{Repository: "repo2", Tag: "tag2"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data2",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
			},
		}
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/upload"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
				{
					Run:        runs.Summary{RunId: "run3"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
				{
					Run:        runs.Summary{RunId: "run4"},
					Mountpoint: plans.Mountpoint{Path: "/in/3"},
				},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Name:   "knit#uploaded",
				},
			},
			Inputs: []runs.Assignment{},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/upload"},
				},
			},
		}
		t.Run("Confirm that run4, data3 and data1 can be traced in graph depth;1 ", theory(
			When{
				RootKnitId:      "data4",
				Depth:           args.NewDepth(1),
				FindDataReturns: [][]data.Detail{{data4}, {data1}, {data3}},
				GetRunReturns:   []runs.Detail{run4},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1),
					knitgraph.WithData(data3),
					knitgraph.WithData(data4, knitgraph.Emphasize()),
					knitgraph.WithRun(run4),
				),
				Err: nil,
			},
		))
		t.Run("Confirm that run3, data2, run1 and root can be traced in graph depth;2 ", theory(
			When{
				RootKnitId:      "data4",
				Depth:           args.NewDepth(2),
				FindDataReturns: [][]data.Detail{{data4}, {data1}, {data3}, {data2}},
				GetRunReturns:   []runs.Detail{run4, run1, run3},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithData(data4, knitgraph.Emphasize()),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run3),
					knitgraph.WithRun(run4),
				),
				Err: nil,
			},
		))

		t.Run("Confirm that all nodes root can be traced in graph depth;3 ", theory(
			When{
				RootKnitId:      "data4",
				Depth:           args.NewDepth(3),
				FindDataReturns: [][]data.Detail{{data4}, {data1}, {data3}, {data2}},
				GetRunReturns:   []runs.Detail{run4, run1, run3, run2},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithData(data4, knitgraph.Emphasize()),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run3),
					knitgraph.WithRun(run2),
					knitgraph.WithRun(run4),
				),
				Err: nil,
			},
		))
	}

	{
		// [test case of data lineage]
		//
		// data1 --[/in/1]--> run1 --[/out/1] --> data3 --> [/in/2] --> run2 --[/out/2]--> data4
		//                         \                                         \
		//                           -[(log)]--> data2(log)                   -[(log)]--> data5(log)(arg)
		//
		data5 := data.Detail{
			KnitId: "data5",
			Upstream: data.CreatedFrom{
				Run: runs.Summary{RunId: "run2"},
				Log: &plans.LogPoint{},
			},
			Downstreams: []data.AssignedTo{},
		}
		data4 := data.Detail{
			KnitId: "data4",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run2"},
				Mountpoint: &plans.Mountpoint{Path: "/out/2"},
			},
			Downstreams: []data.AssignedTo{},
		}
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2",
				Plan: plans.Summary{
					PlanId: "plan2",
					Image:  &plans.Image{Repository: "repo2", Tag: "tag2"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data4",
					Mountpoint: plans.Mountpoint{Path: "/out/2"},
				},
			},
			Log: &runs.LogSummary{KnitId: "data5"},
		}
		data3 := data.Detail{
			KnitId: "data3",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "run1"},
				Mountpoint: &plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run2"},
					Mountpoint: plans.Mountpoint{Path: "/in/2"},
				},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Upstream: data.CreatedFrom{
				Run: runs.Summary{RunId: "run1"},
				Log: &plans.LogPoint{},
			},
			Downstreams: []data.AssignedTo{},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1",
				Plan: plans.Summary{
					PlanId: "plan1",
					Image:  &plans.Image{Repository: "repo1", Tag: "tag1"},
				},
			},
			Inputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data3",
					Mountpoint: plans.Mountpoint{Path: "/out/1"},
				},
			},
			Log: &runs.LogSummary{KnitId: "data2"},
		}
		data1 := data.Detail{
			KnitId: "data1",
			Upstream: data.CreatedFrom{
				Run:        runs.Summary{RunId: "runxx"},
				Mountpoint: &plans.Mountpoint{Path: "/in/x1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run:        runs.Summary{RunId: "run1"},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
		}

		t.Run("Confirm that all nodes containing log can be obtained.", theory(
			When{
				RootKnitId:      "data5",
				Depth:           args.NewDepth(2),
				FindDataReturns: [][]data.Detail{{data5}, {data4}, {data3}, {data2}, {data1}},
				GetRunReturns:   []runs.Detail{run2, run1},
			},
			Then{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1),
					knitgraph.WithData(data2),
					knitgraph.WithData(data3),
					knitgraph.WithData(data4),
					knitgraph.WithData(data5, knitgraph.Emphasize()),
					knitgraph.WithRun(run1),
					knitgraph.WithRun(run2),
				),
				Err: nil,
			},
		))
	}

	t.Run("When FindData returns Empty array it returns ErrNotFoundData", func(t *testing.T) {
		mock := mock.New(t)
		mock.Impl.FindData = func(ctx context.Context, tags []tags.Tag, since *time.Time, duration *time.Duration) ([]data.Detail, error) {
			return []data.Detail{}, nil
		}
		mock.Impl.GetRun = func(ctx context.Context, runId string) (runs.Detail, error) {
			return runs.Detail{}, nil
		}

		ctx := context.Background()
		graph := knitgraph.NewDirectedGraph()
		knitId := "knitId-test"
		_, actual := lineage.TraceUpStream(ctx, mock, graph, knitId, args.NewDepth(1))
		if !errors.Is(actual, lineage.ErrNotFoundData) {
			t.Errorf("wrong status: (actual, expected) != (%v, %v)", actual, lineage.ErrNotFoundData)
		}
	})

	expectedError := errors.New("fake error")
	t.Run("When FindData fails, it returns the error that contains that error ", func(t *testing.T) {
		mock := mock.New(t)
		mock.Impl.FindData = func(ctx context.Context, tags []tags.Tag, since *time.Time, duration *time.Duration) ([]data.Detail, error) {
			return []data.Detail{}, expectedError
		}
		mock.Impl.GetRun = func(ctx context.Context, runId string) (runs.Detail, error) {
			return runs.Detail{}, nil
		}

		ctx := context.Background()
		graph := knitgraph.NewDirectedGraph()
		knitId := "knitId-test"
		_, actual := lineage.TraceUpStream(ctx, mock, graph, knitId, args.NewDepth(1))
		if !errors.Is(actual, expectedError) {
			t.Errorf("wrong status: (actual, expected) != (%v, %v)", actual, expectedError)
		}
	})

	t.Run("When GetRun fails, it returns the error that contains that error", func(t *testing.T) {
		mock := mock.New(t)
		mock.Impl.GetRun = func(ctx context.Context, runId string) (runs.Detail, error) {
			return runs.Detail{}, expectedError
		}
		knitId := "knitId-test"
		mock.Impl.FindData = func(ctx context.Context, tags []tags.Tag, since *time.Time, duration *time.Duration) ([]data.Detail, error) {
			return []data.Detail{
				{
					KnitId: knitId,
					Upstream: data.CreatedFrom{
						Run:        runs.Summary{RunId: "run0"},
						Mountpoint: &plans.Mountpoint{Path: "/upload"},
					},
					Downstreams: []data.AssignedTo{
						{
							Run:        runs.Summary{RunId: "run2"},
							Mountpoint: plans.Mountpoint{Path: "/in/1"},
						},
					},
				},
			}, nil
		}
		ctx := context.Background()
		graph := knitgraph.NewDirectedGraph()
		graph, actual := lineage.TraceUpStream(ctx, mock, graph, knitId, args.NewDepth(1))
		if !errors.Is(actual, expectedError) {
			t.Errorf("wrong status: (actual, expected) != (%s, %d)", graph.DataNodes, expectedError)
		}
	})
}
