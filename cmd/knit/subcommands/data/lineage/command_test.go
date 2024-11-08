package lineage_test

import (
	"context"
	"errors"
	"sort"
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
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/args"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/maps"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/opst/knitfab/pkg/utils/tuple"
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
		Graph knitgraph.DirectedGraph
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
			if !cmp.MapEqWith(
				graph.EdgesFromData,
				then.Graph.EdgesFromData,
				func(a []knitgraph.Edge, b []knitgraph.Edge) bool {
					return cmp.SliceContentEq(a, b)
				},
			) {
				t.Errorf(
					"EdgesFromData is not equal (actual,expected): %v,%v",
					graph.EdgesFromData, then.Graph.EdgesFromData,
				)
			}
			if !cmp.MapEqWith(
				graph.EdgesFromRun,
				then.Graph.EdgesFromRun,
				func(a []knitgraph.Edge, b []knitgraph.Edge) bool {
					return cmp.SliceContentEq(a, b)
				},
			) {
				t.Errorf(
					"EdgesFromRun is not equal (actual,expected): %v,%v",
					graph.EdgesFromRun, then.Graph.EdgesFromRun,
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
	// [test case of data lineage]
	// data1 --> [in/1] -->  run1 --> [out/1] --> data2
	{
		//nodes in the order of appearance to the graph.
		data1 := dummyData("data1", "run0", "run1")
		run1 := dummyRun("run1", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1"})
		data2 := dummyData("data2", "run1")

		t.Run("Confirm that all the above nodes can be traced in graph depth:1", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewDepth(1),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}},
				GetRunReturns:   []runs.Detail{run1},
			},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{"data1": {{ToId: "run1", Label: "in/1"}}},
					EdgesFromRun:  map[string][]knitgraph.Edge{"run1": {{ToId: "data2", Label: "out/1"}}},
					RootNodes:     []string{},
				},
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
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{"data1": {{ToId: "run1", Label: "in/1"}}},
					EdgesFromRun:  map[string][]knitgraph.Edge{"run1": {{ToId: "data2", Label: "out/1"}}},
					RootNodes:     []string{},
				},
				Err: nil,
			},
		))
	}
	// [test case of data lineage]
	// data1 --> [in/1] -->  run1 --> [out/1] --> data2 --> [in/2] --> run2 --> [out/2] --> data3
	//                                                                      |-> [out/3] --> data4
	{
		data1 := dummyData("data1", "run0", "run1")
		run1 := dummyRun("run1", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1"})
		data2 := dummyData("data2", "run1", "run2")
		run2 := dummyRun("run2", map[string]string{"data2": "in/2"}, map[string]string{"data3": "out/2", "data4": "out/3"})
		data3 := dummyData("data3", "run2")
		data4 := dummyData("data4", "run2")
		t.Run("Confirm that all nodes can be traced in graph depth:2", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewDepth(2),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []runs.Detail{run1, run2},
			},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
						tuple.PairOf("data4", toDataNode(data4)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run1", Label: "in/1"}},
						"data2": {{ToId: "run2", Label: "in/2"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run1": {{ToId: "data2", Label: "out/1"}},
						"run2": {{ToId: "data3", Label: "out/2"}, {ToId: "data4", Label: "out/3"}},
					},
					RootNodes: []string{},
				},
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
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{"data1": {{ToId: "run1", Label: "in/1"}}},
					EdgesFromRun:  map[string][]knitgraph.Edge{"run1": {{ToId: "data2", Label: "out/1"}}},
					RootNodes:     []string{},
				},
				Err: nil,
			},
		),
		)
	}
	// [test case of data lineage]
	// data1(arg) --> [in/1] --> run1 --> [out/1] --> data3
	//      data2 --> [in/2] -|
	//                       --> run2 --> ......
	{
		data1 := dummyData("data1", "runxx", "run1")
		run1 := dummyRun("run1", map[string]string{"data1": "in/1", "data2": "in/2"}, map[string]string{"data3": "out/1"})
		data2 := dummyData("data2", "runxxx", "run1", "run2")
		data3 := dummyData("data3", "run1")

		t.Run("Confirm that Nodes except run2 can be traced in graph depth: no limit", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewInfinityDepth(),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}, {data3}},
				GetRunReturns:   []runs.Detail{run1}},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{"data1": {{ToId: "run1", Label: "in/1"}}, "data2": {{ToId: "run1", Label: "in/2"}}},
					EdgesFromRun:  map[string][]knitgraph.Edge{"run1": {{ToId: "data3", Label: "out/1"}}},
					RootNodes:     []string{},
				},
				Err: nil,
			},
		))
	}
	// [test case of data lineage]
	// data1(arg) --> [in/1] --> run1 --> [out/1] --> data2 --> [in/3] --> run3 --> [out/3] --> data4
	//            |-> [in/2] --> run2 --> [out/2] --> data3 --> [in/4] -|
	{
		data1 := dummyData("data1", "runxx", "run1", "run2")
		run1 := dummyRun("run1", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1"})
		data2 := dummyData("data2", "run1", "run3")
		run2 := dummyRun("run2", map[string]string{"data1": "in/2"}, map[string]string{"data3": "out/2"})
		data3 := dummyData("data3", "run2", "run3")
		run3 := dummyRun("run3", map[string]string{"data2": "in/3", "data3": "in/4"}, map[string]string{"data4": "out/3"})
		data4 := dummyData("data4", "run3")

		t.Run("Confirm that all nodes can be traced", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewInfinityDepth(),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []runs.Detail{run1, run2, run3},
			},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
						tuple.PairOf("data4", toDataNode(data4)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
						tuple.PairOf("run3", knitgraph.RunNode{Summary: run3.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run1", Label: "in/1"}, {ToId: "run2", Label: "in/2"}},
						"data2": {{ToId: "run3", Label: "in/3"}}, "data3": {{ToId: "run3", Label: "in/4"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run1": {{ToId: "data2", Label: "out/1"}}, "run2": {{ToId: "data3", Label: "out/2"}}, "run3": {{ToId: "data4", Label: "out/3"}},
					},
					RootNodes: []string{},
				},
				Err: nil,
			},
		))
	}
	// [test case of data lineage]
	// data1(arg) --> [in/1] --> run1 --> [out/1] --> data3 --> [in/3] --> run2 --> [out/2] --> data4
	//      data2 --> [in/2] -|-------------------------------> [in/4] -|
	//                        |
	//                       -->  run3 --> ......
	{
		data1 := dummyData("data1", "runxx", "run1")
		run1 := dummyRun("run1", map[string]string{"data1": "in/1", "data2": "in/2"}, map[string]string{"data3": "out/1"})
		data2 := dummyData("data2", "runxxx", "run1", "run2", "run3")
		data3 := dummyData("data3", "run1", "run2")
		run2 := dummyRun("run2", map[string]string{"data2": "in/3", "data3": "in/4"}, map[string]string{"data4": "out/2"})
		data4 := dummyData("data4", "run2")

		t.Run("Confirm that it traces only downstreams and direct inputs in downstream runs", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewInfinityDepth(),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []runs.Detail{run1, run2},
			},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
						tuple.PairOf("data4", toDataNode(data4)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run1", Label: "in/1"}}, "data2": {{ToId: "run1", Label: "in/2"}, {ToId: "run2", Label: "in/3"}},
						"data3": {{ToId: "run2", Label: "in/4"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run1": {{ToId: "data3", Label: "out/1"}}, "run2": {{ToId: "data4", Label: "out/2"}},
					},
					RootNodes: []string{},
				},
				Err: nil,
			},
		))
	}
	// [test case of data lineage]
	//                                                                                           |-> [in/5] --> run4 --> [out/4] --> data5
	// data1 --> [in/1] --> run1 --> [out/1] --> data2 --> [in/3] --> run2 --> [out/2] --> data3 --> [in/4] --> run3 --> [out/3] --> data4
	//       |-> [in/2]--------------------------------------------------------------------------------------|
	{
		//Appearing nodes in graph depth:1
		data1 := dummyData("data1", "runxx", "run1", "run3")
		run1 := dummyRun("run1", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1"})
		data2 := dummyData("data2", "run1", "run2")
		run3 := dummyRun("run3", map[string]string{"data3": "in/4", "data1": "in/2"}, map[string]string{"data4": "out/3"})
		data4 := dummyData("data4", "run3")
		//Appearing nodes in graph depth:2
		run2 := dummyRun("run2", map[string]string{"data2": "in/3"}, map[string]string{"data3": "out/2"})
		data3 := dummyData("data3", "run2", "run3", "run4")
		//Appearing nodes in graph depth:3
		run4 := dummyRun("run4", map[string]string{"data3": "in/5"}, map[string]string{"data5": "out/4"})
		data5 := dummyData("data5", "run4")
		t.Run("Confirm that nodes except run2,run4 and data5 can be traced in graph depth:1", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewDepth(1),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []runs.Detail{run1, run3},
			},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
						tuple.PairOf("data4", toDataNode(data4)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run3", knitgraph.RunNode{Summary: run3.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run1", Label: "in/1"}, {ToId: "run3", Label: "in/2"}}, "data3": {{ToId: "run3", Label: "in/4"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{"run1": {{ToId: "data2", Label: "out/1"}}, "run3": {{ToId: "data4", Label: "out/3"}}},
					RootNodes:    []string{},
				},
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
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
						tuple.PairOf("data4", toDataNode(data4)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run3", knitgraph.RunNode{Summary: run3.Summary}),
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run1", Label: "in/1"}, {ToId: "run3", Label: "in/2"}},
						"data2": {{ToId: "run2", Label: "in/3"}}, "data3": {{ToId: "run3", Label: "in/4"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run1": {{ToId: "data2", Label: "out/1"}}, "run2": {{ToId: "data3", Label: "out/2"}}, "run3": {{ToId: "data4", Label: "out/3"}},
					},
					RootNodes: []string{},
				},
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
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
						tuple.PairOf("data4", toDataNode(data4)),
						tuple.PairOf("data5", toDataNode(data5)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run3", knitgraph.RunNode{Summary: run3.Summary}),
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
						tuple.PairOf("run4", knitgraph.RunNode{Summary: run4.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run1", Label: "in/1"}, {ToId: "run3", Label: "in/2"}},
						"data2": {{ToId: "run2", Label: "in/3"}},
						"data3": {{ToId: "run3", Label: "in/4"}, {ToId: "run4", Label: "in/5"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run1": {{ToId: "data2", Label: "out/1"}}, "run2": {{ToId: "data3", Label: "out/2"}},
						"run3": {{ToId: "data4", Label: "out/3"}}, "run4": {{ToId: "data5", Label: "out/4"}},
					},
					RootNodes: []string{},
				},
				Err: nil,
			},
		))
	}
	// [test case of data lineage]
	// data1(arg) --> [in/1] --> run1 --> [out/1] --> data2 -->[in/2] run2
	{
		data1 := dummyData("data1", "runxx", "run1")
		data2 := dummyData("data2", "run1", "run2")
		run1 := dummyRun("run1", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1"})
		run2 := dummyRun("run2", map[string]string{"data2": "in/2"}, map[string]string{})
		t.Run("confirm that all nodes can be traced even when the run does not have an output", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewInfinityDepth(),
				ArgGraph:        knitgraph.NewDirectedGraph(),
				FindDataReturns: [][]data.Detail{{data1}, {data2}},
				GetRunReturns:   []runs.Detail{run1, run2},
			},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{"data1": {{ToId: "run1", Label: "in/1"}}, "data2": {{ToId: "run2", Label: "in/2"}}},
					EdgesFromRun:  map[string][]knitgraph.Edge{"run1": {{ToId: "data2", Label: "out/1"}}},
					RootNodes:     []string{},
				},
				Err: nil,
			},
		))
	}
	// [test case of data lineage]
	//
	// data3 --> [in/1] --> run1 --> [out/1] --> data1(arg) --> [in/2] --> run2 --> [out/3] --> data4
	//                           |-> [out/2] --> data2  ------> [in/3] -|
	//
	{
		//Appearing nodes by tracing upstream from data1
		data1 := dummyData("data1", "run1", "run2")
		run1 := dummyRun("run1", map[string]string{"data3": "in/1"}, map[string]string{"data1": "out/1", "data2": "out/2"})
		data2 := dummyData("data2", "run1", "run2", "run3")
		data3 := dummyData("data3", "runxx", "run1")
		//Appearing nodes by tracing downstream from data1
		run2 := dummyRun("run2", map[string]string{"data1": "in/2", "data2": "in/3"}, map[string]string{"data4": "out/3"})
		data4 := dummyData("data4", "run2")

		t.Run("Confirm that all nodes can be obtained by tracing both upstream and downstream.", theory(
			When{
				RootKnitId: "data1",
				Depth:      args.NewInfinityDepth(),
				ArgGraph: &knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data3": {{ToId: "run1", Label: "in/1"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run1": {{ToId: "data1", Label: "out/1"}, {ToId: "data2", Label: "out/2"}},
					},
					RootNodes: []string{},
				},
				FindDataReturns: [][]data.Detail{{data4}},
				GetRunReturns:   []runs.Detail{run2},
			},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
						tuple.PairOf("data4", toDataNode(data4)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run2", Label: "in/2"}}, "data2": {{ToId: "run2", Label: "in/3"}},
						"data3": {{ToId: "run1", Label: "in/1"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run1": {{ToId: "data1", Label: "out/1"}, {ToId: "data2", Label: "out/2"}},
						"run2": {{ToId: "data4", Label: "out/3"}},
					},
					RootNodes: []string{},
				},
				Err: nil,
			},
		))
	}

	// [test case of data lineage]
	//
	// data1 --> [in/1] --> run1 --> [out/1] --> data2
	//                           |-> [(log)] --> data3(log) --> [in/2] --> run2 --> [out/2] --> data4

	{
		data1 := dummyData("data1", "runxx", "run1")
		run1 := dummyRunWithLog("run1", "data3", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1"})
		data2 := dummyData("data2", "run1")
		data3 := dummyLogData("data3", "run1", "run2")
		run2 := dummyRun("run2", map[string]string{"data3": "in/2"}, map[string]string{"data4": "out/2"})
		data4 := dummyData("data4", "run2")

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
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
						tuple.PairOf("data4", toDataNode(data4)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run1", Label: "in/1"}}, "data3": {{ToId: "run2", Label: "in/2"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run1": {{ToId: "data2", Label: "out/1"}, {ToId: "data3", Label: "(log)"}},
						"run2": {{ToId: "data4", Label: "out/2"}},
					},
					RootNodes: []string{},
				},
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
				dummyData(knitId, "run1", "run2"),
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
		Graph knitgraph.DirectedGraph
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
			if !cmp.MapEqWith(
				graph.EdgesFromData,
				then.Graph.EdgesFromData,
				func(a []knitgraph.Edge, b []knitgraph.Edge) bool {
					return cmp.SliceContentEq(a, b)
				},
			) {
				t.Errorf(
					"EdgesFromData is not equal (actual,expected): %v,%v",
					graph.EdgesFromData, then.Graph.EdgesFromData,
				)
			}
			if !cmp.MapEqWith(
				graph.EdgesFromRun,
				then.Graph.EdgesFromRun,
				func(a []knitgraph.Edge, b []knitgraph.Edge) bool {
					return cmp.SliceContentEq(a, b)
				},
			) {
				t.Errorf(
					"EdgesFromRun is not equal (actual,expected): %v,%v",
					graph.EdgesFromRun, then.Graph.EdgesFromRun,
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
	// [test case of data lineage]
	// root -->  run1 --> [upload] --> data1(arg)
	{
		//nodes in  in the order of appearance to the graph.
		data1 := dummyData("data1", "run1")
		run1 := dummyRun("run1", map[string]string{}, map[string]string{"data1": "upload"})
		t.Run("Confirm that all the above nodes can be traced in graph depth:1", theory(
			When{
				RootKnitId:      "data1",
				Depth:           args.NewDepth(1),
				FindDataReturns: [][]data.Detail{{data1}},
				GetRunReturns:   []runs.Detail{run1},
			},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{},
					EdgesFromRun:  map[string][]knitgraph.Edge{"run1": {{ToId: "data1", Label: "upload"}}},
					RootNodes:     []string{"run1"},
				},
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
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{},
					EdgesFromRun:  map[string][]knitgraph.Edge{"run1": {{ToId: "data1", Label: "upload"}}},
					RootNodes:     []string{"run1"},
				},
				Err: nil,
			},
		))
	}
	// [test case of data lineage]
	// root -->  run1 --> [upload] --> data1 --> [in/1] --> run2 --> [out/1] --> data2(arg)
	{
		data2 := dummyData("data2", "run2")
		run2 := dummyRun("run2", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1"})
		data1 := dummyData("data1", "run1", "run2")
		run1 := dummyRun("run1", map[string]string{}, map[string]string{"data1": "upload"})
		t.Run("Confirm that all nodes can be traced in graph depth:2", theory(
			When{
				RootKnitId:      "data2",
				Depth:           args.NewDepth(2),
				FindDataReturns: [][]data.Detail{{data2}, {data1}},
				GetRunReturns:   []runs.Detail{run2, run1},
			},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{"data1": {{ToId: "run2", Label: "in/1"}}},
					EdgesFromRun:  map[string][]knitgraph.Edge{"run1": {{ToId: "data1", Label: "upload"}}, "run2": {{ToId: "data2", Label: "out/1"}}},
					RootNodes:     []string{"run1"},
				},
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
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{"data1": {{ToId: "run2", Label: "in/1"}}},
					EdgesFromRun:  map[string][]knitgraph.Edge{"run2": {{ToId: "data2", Label: "out/1"}}},
					RootNodes:     []string{},
				},
				Err: nil,
			},
		))
	}
	// [test case of data lineage]
	// root --> run1 --> [upload] --> data1 --> [in/1] --> run2 --> [out/1] --> data2
	//                                       |                  |-> [out/2] --> data3(arg)
	//                                       |-> run3 ....
	{
		data3 := dummyData("data3", "run2")
		run2 := dummyRun("run2", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1", "data3": "out/2"})
		data2 := dummyData("data2", "run2")
		data1 := dummyData("data1", "run1", "run2", "run3")
		run1 := dummyRun("run1", map[string]string{}, map[string]string{"data1": "upload"})
		t.Run("Confirm it traces only upstreams and direct outputs in upstream runs", theory(
			When{
				RootKnitId:      "data3",
				Depth:           args.NewDepth(2),
				FindDataReturns: [][]data.Detail{{data3}, {data2}, {data1}},
				GetRunReturns:   []runs.Detail{run2, run1},
			},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{"data1": {{ToId: "run2", Label: "in/1"}}},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run1": {{ToId: "data1", Label: "upload"}}, "run2": {{ToId: "data2", Label: "out/1"}, {ToId: "data3", Label: "out/2"}},
					},
					RootNodes: []string{"run1"},
				},
				Err: nil,
			},
		))
	}
	// [test case of data lineage]
	// root --> run1 --> [upload] --> data1 --> [in/1] --> run2 --> [out/1] --> data2 --> [in/2] --> run3 --> [out/3] --> data4(arg)
	//                                                          |-> [out/2] --> data3 --> [in/3] -|
	{
		data4 := dummyData("data4", "run3")
		run3 := dummyRun("run3", map[string]string{"data3": "in/3", "data2": "in/2"}, map[string]string{"data4": "out/3"})
		data2 := dummyData("data2", "run2", "run3")
		data3 := dummyData("data3", "run2", "run3")
		run2 := dummyRun("run2", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1", "data3": "out/2"})
		data1 := dummyData("data1", "run1")
		run1 := dummyRun("run1", map[string]string{}, map[string]string{"data1": "upload"})

		t.Run("Confirm that all nodes can be traced", theory(
			When{
				RootKnitId:      "data4",
				Depth:           args.NewInfinityDepth(),
				FindDataReturns: [][]data.Detail{{data4}, {data2}, {data3}, {data1}},
				GetRunReturns:   []runs.Detail{run3, run2, run1},
			},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
						tuple.PairOf("data4", toDataNode(data4)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
						tuple.PairOf("run3", knitgraph.RunNode{Summary: run3.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run2", Label: "in/1"}}, "data2": {{ToId: "run3", Label: "in/2"}}, "data3": {{ToId: "run3", Label: "in/3"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run1": {{ToId: "data1", Label: "upload"}}, "run2": {{ToId: "data2", Label: "out/1"}, {ToId: "data3", Label: "out/2"}},
						"run3": {{ToId: "data4", Label: "out/3"}},
					},
					RootNodes: []string{"run1"},
				},
				Err: nil,
			},
		))
	}
	// [test case of data lineage]
	// root --> run1 --> [upload] --> data1 --> [in/1] --> run2 --> [out/1] --> data2 --> [in/4] --> run3 --> [out/2] --> data3 ---> [in/4] --> run4 --> [out/3] --> data4(arg)
	//                                      |-> [in/2] -------------------------------------------|                                          |
	//                                      |-> [in/3]---------------------------------------------------------------------------------------|
	{
		//Appearing nodes in graph depth;1
		data4 := dummyData("data4", "run4")
		run4 := dummyRun("run4", map[string]string{"data3": "in/4", "data1": "in/3"}, map[string]string{"data4": "out/3"})
		data1 := dummyData("data1", "run1", "run2", "run3", "run4")
		data3 := dummyData("data3", "run3", "run4")
		//Appearing nodes in graph depth;2
		run1 := dummyRun("run1", map[string]string{}, map[string]string{"data1": "upload"})
		run3 := dummyRun("run3", map[string]string{"data2": "in/4", "data1": "in/2"}, map[string]string{"data3": "out/2"})
		data2 := dummyData("data2", "run2", "run3")
		//Appearing nodes in graph depth;3
		run2 := dummyRun("run2", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1"})
		t.Run("Confirm that run4, data3 and data1 can be traced in graph depth;1 ", theory(
			When{
				RootKnitId:      "data4",
				Depth:           args.NewDepth(1),
				FindDataReturns: [][]data.Detail{{data4}, {data1}, {data3}},
				GetRunReturns:   []runs.Detail{run4},
			},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data3", toDataNode(data3)),
						tuple.PairOf("data4", toDataNode(data4)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run4", knitgraph.RunNode{Summary: run4.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run4", Label: "in/3"}}, "data3": {{ToId: "run4", Label: "in/4"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run4": {{ToId: "data4", Label: "out/3"}},
					},
					RootNodes: []string{},
				},
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
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
						tuple.PairOf("data4", toDataNode(data4)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run3", knitgraph.RunNode{Summary: run3.Summary}),
						tuple.PairOf("run4", knitgraph.RunNode{Summary: run4.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run4", Label: "in/3"}, {ToId: "run3", Label: "in/2"}},
						"data2": {{ToId: "run3", Label: "in/4"}}, "data3": {{ToId: "run4", Label: "in/4"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run4": {{ToId: "data4", Label: "out/3"}}, "run3": {{ToId: "data3", Label: "out/2"}}, "run1": {{ToId: "data1", Label: "upload"}},
					},
					RootNodes: []string{"run1"},
				},
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
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
						tuple.PairOf("data4", toDataNode(data4)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
						tuple.PairOf("run3", knitgraph.RunNode{Summary: run3.Summary}),
						tuple.PairOf("run4", knitgraph.RunNode{Summary: run4.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run4", Label: "in/3"}, {ToId: "run3", Label: "in/2"}, {ToId: "run2", Label: "in/1"}},
						"data2": {{ToId: "run3", Label: "in/4"}}, "data3": {{ToId: "run4", Label: "in/4"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run4": {{ToId: "data4", Label: "out/3"}}, "run3": {{ToId: "data3", Label: "out/2"}}, "run1": {{ToId: "data1", Label: "upload"}},
						"run2": {{ToId: "data2", Label: "out/1"}},
					},
					RootNodes: []string{"run1"},
				},
				Err: nil,
			},
		))
	}

	// [test case of data lineage]
	//
	// data1 --> [in/1] --> run1 --> [out/1] --> data3 --> [in/2] --> run2 --> [out/2] --> data4
	//                           |-> [(log)] --> data2(log)                |-> [(log)] --> data5(log)(arg)
	//
	{
		//Appearing nodes in graph depth;1
		data5 := dummyLogData("data5", "run2")
		run2 := dummyRunWithLog("run2", "data5", map[string]string{"data3": "in/2"}, map[string]string{"data4": "out/2"})
		data4 := dummyData("data4", "run2")
		data3 := dummyData("data3", "run1", "run2")
		//Appearing nodes in graph depth;2
		run1 := dummyRunWithLog("run1", "data2", map[string]string{"data1": "in/1"}, map[string]string{"data3": "out/1"})
		data2 := dummyLogData("data2", "run1")
		data1 := dummyData("data1", "runxx", "run1")

		t.Run("Confirm that all nodes containing log can be obtained.", theory(
			When{
				RootKnitId:      "data5",
				Depth:           args.NewDepth(2),
				FindDataReturns: [][]data.Detail{{data5}, {data4}, {data3}, {data2}, {data1}},
				GetRunReturns:   []runs.Detail{run2, run1},
			},
			Then{
				Graph: knitgraph.DirectedGraph{
					DataNodes: maps.NewOrderedMap(
						tuple.PairOf("data1", toDataNode(data1)),
						tuple.PairOf("data2", toDataNode(data2)),
						tuple.PairOf("data3", toDataNode(data3)),
						tuple.PairOf("data4", toDataNode(data4)),
						tuple.PairOf("data2", toDataNode(data5)),
					),
					RunNodes: maps.NewOrderedMap(
						tuple.PairOf("run1", knitgraph.RunNode{Summary: run1.Summary}),
						tuple.PairOf("run2", knitgraph.RunNode{Summary: run2.Summary}),
					),
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run1", Label: "in/1"}}, "data3": {{ToId: "run2", Label: "in/2"}},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run1": {{ToId: "data3", Label: "out/1"}, {ToId: "data2", Label: "(log)"}},
						"run2": {{ToId: "data4", Label: "out/2"}, {ToId: "data5", Label: "(log)"}},
					},
					RootNodes: []string{},
				},
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
				dummyData(knitId, "run0", "run2"),
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

func toDataNode(data data.Detail) knitgraph.DataNode {
	return knitgraph.DataNode{
		KnitId:    data.KnitId,
		Tags:      data.Tags,
		FromRunId: data.Upstream.Run.RunId,
		ToRunIds: func() []string {
			ids := []string{}
			for _, downstream := range data.Downstreams {
				ids = append(ids, downstream.Run.RunId)
			}
			return ids
		}(),
	}
}

func dummyCreatedFrom(runId string) data.CreatedFrom {
	return data.CreatedFrom{
		Run: runs.Summary{
			RunId:  runId,
			Status: "done",
			Plan: plans.Summary{
				PlanId: "plan-3",
				Image:  &plans.Image{Repository: "knit.image.repo.invalid/trainer", Tag: "v1"},
			},
		},
		Mountpoint: &plans.Mountpoint{Path: "/out"},
	}
}

func dummyLogFrom(runId string) data.CreatedFrom {
	return data.CreatedFrom{
		Run: runs.Summary{
			RunId:  runId,
			Status: "done",
			Plan: plans.Summary{
				PlanId: "plan-3",
				Image:  &plans.Image{Repository: "knit.image.repo.invalid/trainer", Tag: "v1"},
			},
		},
		Log: &plans.LogPoint{},
	}
}

func dummyAssignedTo(runId string) data.AssignedTo {
	return data.AssignedTo{
		Run: runs.Summary{
			RunId:  runId,
			Status: "done",
			Plan: plans.Summary{
				PlanId: "plan-3",
				Image:  &plans.Image{Repository: "knit.image.repo.invalid/trainer", Tag: "v1"},
			},
		},
		Mountpoint: plans.Mountpoint{Path: "/out"},
	}
}

func dummySliceAssignedTo(runIds ...string) []data.AssignedTo {
	slice := []data.AssignedTo{}
	for _, runId := range runIds {
		element := dummyAssignedTo(runId)
		slice = append(slice, element)
	}
	return slice
}

func dummyData(knitId string, fromRunId string, toRunIds ...string) data.Detail {
	return data.Detail{
		KnitId: knitId,
		Tags: []tags.Tag{
			{Key: "foo", Value: "bar"},
			{Key: "fizz", Value: "bazz"},
			{Key: domain.KeyKnitId, Value: knitId},
			{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T12:34:56+00:00"},
		},
		Upstream:    dummyCreatedFrom(fromRunId),
		Downstreams: dummySliceAssignedTo(toRunIds...),
		Nomination:  []data.NominatedBy{},
	}
}

func dummyDataForFailed(knitId string, fromRunId string, toRunIds ...string) data.Detail {
	return data.Detail{
		KnitId: knitId,
		Tags: []tags.Tag{
			{Key: "foo", Value: "bar"},
			{Key: "fizz", Value: "bazz"},
			{Key: domain.KeyKnitId, Value: knitId},
			{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T12:34:56+00:00"},
			{Key: domain.KeyKnitTransient, Value: domain.ValueKnitTransientFailed},
		},
		Upstream:    dummyCreatedFrom(fromRunId),
		Downstreams: dummySliceAssignedTo(toRunIds...),
		Nomination:  []data.NominatedBy{},
	}
}

func dummyRun(runId string, inputs map[string]string, outputs map[string]string) runs.Detail {
	return runs.Detail{
		Summary: runs.Summary{
			RunId:  runId,
			Status: "done",
			Plan: plans.Summary{
				PlanId: "test-Id",
				Image: &plans.Image{
					Repository: "test-image",
					Tag:        "test-version",
				},
				Name: "test-Name",
			},
		},
		Inputs:  dummySliceAssignment(inputs),
		Outputs: dummySliceAssignment(outputs),
		Log:     nil,
	}
}

func dummyRunWithLog(runId string, knitId string, inputs map[string]string, outputs map[string]string) runs.Detail {
	return runs.Detail{
		Summary: runs.Summary{
			RunId:  runId,
			Status: "done",
			Plan: plans.Summary{
				PlanId: "test-Id",
				Image: &plans.Image{
					Repository: "test-image",
					Tag:        "test-version",
				},
				Name: "test-Name",
			},
		},
		Inputs:  dummySliceAssignment(inputs),
		Outputs: dummySliceAssignment(outputs),
		Log: &runs.LogSummary{
			LogPoint: plans.LogPoint{
				Tags: []tags.Tag{
					{Key: "type", Value: "log"},
					{Key: "format", Value: "jsonl"},
				},
			},
			KnitId: knitId,
		},
	}
}

func dummyFailedRunWithLog(runId string, knitId string, inputs map[string]string, outputs map[string]string) runs.Detail {
	return runs.Detail{
		Summary: runs.Summary{
			RunId:  runId,
			Status: "failed",
			Plan: plans.Summary{
				PlanId: "test-Id",
				Image: &plans.Image{
					Repository: "test-image",
					Tag:        "test-version",
				},
				Name: "test-Name",
			},
		},
		Inputs:  dummySliceAssignment(inputs),
		Outputs: dummySliceAssignment(outputs),
		Log: &runs.LogSummary{
			LogPoint: plans.LogPoint{
				Tags: []tags.Tag{
					{Key: "type", Value: "log"},
					{Key: "format", Value: "jsonl"},
				},
			},
			KnitId: knitId,
		},
	}
}

func dummyLogData(knitId string, fromRunId string, toRunIds ...string) data.Detail {
	return data.Detail{
		KnitId: knitId,
		Tags: []tags.Tag{
			{Key: "type", Value: "log"},
			{Key: "format", Value: "jsonl"},
		},
		Upstream:    dummyLogFrom(fromRunId),
		Downstreams: dummySliceAssignedTo(toRunIds...),
		Nomination:  []data.NominatedBy{},
	}
}

func dummyAssignment(knitId string, mountPath string) runs.Assignment {
	return runs.Assignment{
		Mountpoint: plans.Mountpoint{
			Path: mountPath,
			Tags: []tags.Tag{
				{Key: "type", Value: "training data"},
				{Key: "format", Value: "mask"},
			},
		},
		KnitId: knitId,
	}
}

func dummySliceAssignment(knitIdToMoutPath map[string]string) []runs.Assignment {
	slice := []runs.Assignment{}
	keys := make([]string, 0, len(knitIdToMoutPath))
	for k := range knitIdToMoutPath {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		element := dummyAssignment(k, knitIdToMoutPath[k])
		slice = append(slice, element)

	}
	return slice
}
