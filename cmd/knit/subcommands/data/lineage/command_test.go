package lineage_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	"github.com/youta-t/flarc"

	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/data/lineage"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	apidata "github.com/opst/knitfab/pkg/api/types/data"
	apiplan "github.com/opst/knitfab/pkg/api/types/plans"
	apirun "github.com/opst/knitfab/pkg/api/types/runs"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestLineageCommand(t *testing.T) {
	mockGraphFromUpstream := lineage.NewDirectedGraph()
	mockGraphFromDownstream := lineage.NewDirectedGraph()

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
				graph *lineage.DirectedGraph,
				knitId string,
				depth int,
			) (*lineage.DirectedGraph, error) {
				if when.flag.Numbers == "all" && depth != -1 {
					t.Errorf("should call task for traceupstream with depth -1")
				}
				if !when.flag.Upstream && when.flag.Downstream {
					t.Errorf("should not call task for traceupstream")
				}
				return mockGraphFromUpstream, when.err
			}
			funcForDownstream := func(
				ctx context.Context,
				client krst.KnitClient,
				graph *lineage.DirectedGraph,
				knitId string,
				depth int,
			) (*lineage.DirectedGraph, error) {
				if when.flag.Numbers == "all" && depth != -1 {
					t.Errorf("should call task for tracedownstream with depth -1")
				}
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
				Numbers:    "3",
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
				Numbers:    "3",
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
				Numbers:    "3",
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
				Numbers:    "3",
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
				Numbers:    "1",
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
				Numbers:    "all",
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
				Numbers:    "0",
			},
			err: nil,
		},
		then{
			err: flarc.ErrUsage,
		},
	))

	t.Run("when number of depth flag is letter except `all`, it shoule return ErrUsage", theory(
		when{
			knitId: "test-Id",
			flag: lineage.Flag{
				Upstream:   false,
				Downstream: false,
				Numbers:    "abc",
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
				Numbers:    "3",
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
		Depth           int
		ArgGraph        *lineage.DirectedGraph
		FindDataReturns [][]apidata.Detail
		GetRunReturns   []apirun.Detail
	}

	type Then struct {
		Graph lineage.DirectedGraph
		Err   error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {

			mock := mock.New(t)

			// Store arguments and return values for each call
			nthData := 0
			mock.Impl.FindData = func(
				ctx context.Context, tags []apitag.Tag, since *time.Time, duration *time.Duration,
			) ([]apidata.Detail, error) {
				ret := when.FindDataReturns[nthData]
				nthData += 1
				return ret, nil
			}

			nthRun := 0
			mock.Impl.GetRun = func(
				ctx context.Context, runId string,
			) (apirun.Detail, error) {
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
				graph.DataNodes,
				then.Graph.DataNodes,
				func(a, b lineage.DataNode) bool { return a.Equal(&b) },
			) {
				t.Errorf(
					"DataNodes is not equal (actual,expected): %v,%v",
					graph.DataNodes, then.Graph.DataNodes,
				)
			}
			if !cmp.MapEqWith(
				graph.RunNodes,
				then.Graph.RunNodes,
				func(a, b lineage.RunNode) bool { return a.Equal(&b.Summary) },
			) {
				t.Errorf(
					"RunNodes is not equal (actual,expected): %v,%v",
					graph.RunNodes, then.Graph.RunNodes,
				)
			}
			if !cmp.MapEqWith(
				graph.EdgesFromData,
				then.Graph.EdgesFromData,
				func(a []lineage.Edge, b []lineage.Edge) bool {
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
				func(a []lineage.Edge, b []lineage.Edge) bool {
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
				Depth:           1,
				ArgGraph:        lineage.NewDirectedGraph(),
				FindDataReturns: [][]apidata.Detail{{data1}, {data2}},
				GetRunReturns:   []apirun.Detail{run1},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes:     map[string]lineage.DataNode{"data1": toDataNode(data1), "data2": toDataNode(data2)},
					RunNodes:      map[string]lineage.RunNode{"run1": {run1.Summary}},
					EdgesFromData: map[string][]lineage.Edge{"data1": {{ToId: "run1", Label: "in/1"}}},
					EdgesFromRun:  map[string][]lineage.Edge{"run1": {{ToId: "data2", Label: "out/1"}}},
					RootNodes:     []string{},
				},
				Err: nil,
			},
		))

		t.Run("Confirm that the tracing result remains the same when graph depth is no limit", theory(
			When{
				RootKnitId:      "data1",
				Depth:           -1,
				ArgGraph:        lineage.NewDirectedGraph(),
				FindDataReturns: [][]apidata.Detail{{data1}, {data2}},
				GetRunReturns:   []apirun.Detail{run1},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes:     map[string]lineage.DataNode{"data1": toDataNode(data1), "data2": toDataNode(data2)},
					RunNodes:      map[string]lineage.RunNode{"run1": {run1.Summary}},
					EdgesFromData: map[string][]lineage.Edge{"data1": {{ToId: "run1", Label: "in/1"}}},
					EdgesFromRun:  map[string][]lineage.Edge{"run1": {{ToId: "data2", Label: "out/1"}}},
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
				Depth:           2,
				ArgGraph:        lineage.NewDirectedGraph(),
				FindDataReturns: [][]apidata.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []apirun.Detail{run1, run2},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2), "data3": toDataNode(data3),
						"data4": toDataNode(data4),
					},
					RunNodes: map[string]lineage.RunNode{
						"run1": {run1.Summary}, "run2": {run2.Summary},
					},
					EdgesFromData: map[string][]lineage.Edge{"data1": {{"run1", "in/1"}}, "data2": {{"run2", "in/2"}}},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{"data2", "out/1"}},
						"run2": {{"data3", "out/2"}, {"data4", "out/3"}},
					},
					RootNodes: []string{},
				},
				Err: nil,
			},
		))

		t.Run("Confirm that it traces up to data2 in graph depth:1", theory(
			When{
				RootKnitId:      "data1",
				Depth:           1,
				ArgGraph:        lineage.NewDirectedGraph(),
				FindDataReturns: [][]apidata.Detail{{data1}, {data2}},
				GetRunReturns:   []apirun.Detail{run1},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes:     map[string]lineage.DataNode{"data1": toDataNode(data1), "data2": toDataNode(data2)},
					RunNodes:      map[string]lineage.RunNode{"run1": {run1.Summary}},
					EdgesFromData: map[string][]lineage.Edge{"data1": {{ToId: "run1", Label: "in/1"}}},
					EdgesFromRun:  map[string][]lineage.Edge{"run1": {{ToId: "data2", Label: "out/1"}}},
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
				Depth:           -1,
				ArgGraph:        lineage.NewDirectedGraph(),
				FindDataReturns: [][]apidata.Detail{{data1}, {data2}, {data3}},
				GetRunReturns:   []apirun.Detail{run1}},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2), "data3": toDataNode(data3),
					},
					RunNodes:      map[string]lineage.RunNode{"run1": {run1.Summary}},
					EdgesFromData: map[string][]lineage.Edge{"data1": {{"run1", "in/1"}}, "data2": {{"run1", "in/2"}}},
					EdgesFromRun:  map[string][]lineage.Edge{"run1": {{"data3", "out/1"}}},
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
				Depth:           -1,
				ArgGraph:        lineage.NewDirectedGraph(),
				FindDataReturns: [][]apidata.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []apirun.Detail{run1, run2, run3},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2),
						"data3": toDataNode(data3), "data4": toDataNode(data4),
					},
					RunNodes: map[string]lineage.RunNode{
						"run1": {run1.Summary}, "run2": {run2.Summary}, "run3": {run3.Summary},
					},
					EdgesFromData: map[string][]lineage.Edge{
						"data1": {{"run1", "in/1"}, {"run2", "in/2"}},
						"data2": {{"run3", "in/3"}}, "data3": {{"run3", "in/4"}},
					},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{"data2", "out/1"}}, "run2": {{"data3", "out/2"}}, "run3": {{"data4", "out/3"}},
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
				Depth:           -1,
				ArgGraph:        lineage.NewDirectedGraph(),
				FindDataReturns: [][]apidata.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []apirun.Detail{run1, run2},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2),
						"data3": toDataNode(data3), "data4": toDataNode(data4),
					},
					RunNodes: map[string]lineage.RunNode{"run1": {run1.Summary}, "run2": {run2.Summary}},
					EdgesFromData: map[string][]lineage.Edge{
						"data1": {{"run1", "in/1"}}, "data2": {{"run1", "in/2"}, {"run2", "in/3"}},
						"data3": {{"run2", "in/4"}},
					},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{"data3", "out/1"}}, "run2": {{"data4", "out/2"}},
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
				Depth:           1,
				ArgGraph:        lineage.NewDirectedGraph(),
				FindDataReturns: [][]apidata.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []apirun.Detail{run1, run3},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2),
						"data3": toDataNode(data3), "data4": toDataNode(data4),
					},
					RunNodes: map[string]lineage.RunNode{"run1": {run1.Summary}, "run3": {run3.Summary}},
					EdgesFromData: map[string][]lineage.Edge{
						"data1": {{"run1", "in/1"}, {"run3", "in/2"}}, "data3": {{"run3", "in/4"}},
					},
					EdgesFromRun: map[string][]lineage.Edge{"run1": {{"data2", "out/1"}}, "run3": {{"data4", "out/3"}}},
					RootNodes:    []string{},
				},
				Err: nil,
			},
		))

		t.Run("Confirm that nodes except run4 and data5 can be traced in graph depth:2", theory(
			When{
				RootKnitId:      "data1",
				Depth:           2,
				ArgGraph:        lineage.NewDirectedGraph(),
				FindDataReturns: [][]apidata.Detail{{data1}, {data2}, {data3}, {data4}},
				GetRunReturns:   []apirun.Detail{run1, run3, run2},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2),
						"data3": toDataNode(data3), "data4": toDataNode(data4),
					},
					RunNodes: map[string]lineage.RunNode{
						"run1": {run1.Summary}, "run3": {run3.Summary}, "run2": {run2.Summary},
					},
					EdgesFromData: map[string][]lineage.Edge{
						"data1": {{"run1", "in/1"}, {"run3", "in/2"}},
						"data2": {{"run2", "in/3"}}, "data3": {{"run3", "in/4"}},
					},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{"data2", "out/1"}}, "run2": {{"data3", "out/2"}}, "run3": {{"data4", "out/3"}},
					},
					RootNodes: []string{},
				},
				Err: nil,
			},
		))

		t.Run("Confirm that all nodes can be traced in graph depth:3", theory(
			When{
				RootKnitId:      "data1",
				Depth:           3,
				ArgGraph:        lineage.NewDirectedGraph(),
				FindDataReturns: [][]apidata.Detail{{data1}, {data2}, {data3}, {data4}, {data5}},
				GetRunReturns:   []apirun.Detail{run1, run3, run2, run4},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2), "data3": toDataNode(data3),
						"data4": toDataNode(data4), "data5": toDataNode(data5),
					},
					RunNodes: map[string]lineage.RunNode{
						"run1": {run1.Summary}, "run3": {run3.Summary},
						"run2": {run2.Summary}, "run4": {run4.Summary},
					},
					EdgesFromData: map[string][]lineage.Edge{
						"data1": {{"run1", "in/1"}, {"run3", "in/2"}},
						"data2": {{"run2", "in/3"}},
						"data3": {{"run3", "in/4"}, {"run4", "in/5"}},
					},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{"data2", "out/1"}}, "run2": {{"data3", "out/2"}},
						"run3": {{"data4", "out/3"}}, "run4": {{"data5", "out/4"}},
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
				Depth:           -1,
				ArgGraph:        lineage.NewDirectedGraph(),
				FindDataReturns: [][]apidata.Detail{{data1}, {data2}},
				GetRunReturns:   []apirun.Detail{run1, run2},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes:     map[string]lineage.DataNode{"data1": toDataNode(data1), "data2": toDataNode(data2)},
					RunNodes:      map[string]lineage.RunNode{"run1": {run1.Summary}, "run2": {run2.Summary}},
					EdgesFromData: map[string][]lineage.Edge{"data1": {{"run1", "in/1"}}, "data2": {{"run2", "in/2"}}},
					EdgesFromRun:  map[string][]lineage.Edge{"run1": {{"data2", "out/1"}}},
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
				Depth:      -1,
				ArgGraph: &lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{"data1": toDataNode(data1), "data2": toDataNode(data2), "data3": toDataNode(data3)},
					RunNodes:  map[string]lineage.RunNode{"run1": {run1.Summary}},
					EdgesFromData: map[string][]lineage.Edge{
						"data3": {{"run1", "in/1"}},
					},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{"data1", "out/1"}, {"data2", "out/2"}},
					},
					RootNodes: []string{},
				},
				FindDataReturns: [][]apidata.Detail{{data4}},
				GetRunReturns:   []apirun.Detail{run2},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2),
						"data3": toDataNode(data3), "data4": toDataNode(data4),
					},
					RunNodes: map[string]lineage.RunNode{"run1": {run1.Summary}, "run2": {run2.Summary}},
					EdgesFromData: map[string][]lineage.Edge{
						"data1": {{"run2", "in/2"}}, "data2": {{"run2", "in/3"}},
						"data3": {{"run1", "in/1"}},
					},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{"data1", "out/1"}, {"data2", "out/2"}},
						"run2": {{"data4", "out/3"}},
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
				Depth:      -1,
				ArgGraph:   lineage.NewDirectedGraph(),
				FindDataReturns: [][]apidata.Detail{
					{data1}, {data2}, {data3}, {data4},
				},
				GetRunReturns: []apirun.Detail{run1, run2},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2),
						"data3": toDataNode(data3), "data4": toDataNode(data4),
					},
					RunNodes: map[string]lineage.RunNode{"run1": {run1.Summary}, "run2": {run2.Summary}},
					EdgesFromData: map[string][]lineage.Edge{
						"data1": {{"run1", "in/1"}}, "data3": {{"run2", "in/2"}},
					},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{"data2", "out/1"}, {"data3", "(log)"}},
						"run2": {{"data4", "out/2"}},
					},
					RootNodes: []string{},
				},
				Err: nil,
			},
		))
	}

	t.Run("When FindData returns Empty array it returns ErrNotFoundData", func(t *testing.T) {
		mock := mock.New(t)
		mock.Impl.FindData = func(ctx context.Context, tags []apitag.Tag, since *time.Time, duration *time.Duration) ([]apidata.Detail, error) {
			return []apidata.Detail{}, nil
		}
		mock.Impl.GetRun = func(ctx context.Context, runId string) (apirun.Detail, error) {
			return apirun.Detail{}, nil
		}

		ctx := context.Background()
		graph := lineage.NewDirectedGraph()
		knitId := "knitId-test"
		_, actual := lineage.TraceDownStream(ctx, mock, graph, knitId, 1)
		if !errors.Is(actual, lineage.ErrNotFoundData) {
			t.Errorf("wrong status: (actual, expected) != (%v, %v)", actual, lineage.ErrNotFoundData)
		}
	})

	expectedError := errors.New("fake error")
	t.Run("When FindData fails, it returns the error that contains that error ", func(t *testing.T) {
		mock := mock.New(t)
		mock.Impl.FindData = func(ctx context.Context, tags []apitag.Tag, since *time.Time, duration *time.Duration) ([]apidata.Detail, error) {
			return []apidata.Detail{}, expectedError
		}
		mock.Impl.GetRun = func(ctx context.Context, runId string) (apirun.Detail, error) {
			return apirun.Detail{}, nil
		}

		ctx := context.Background()
		graph := lineage.NewDirectedGraph()
		knitId := "knitId-test"
		_, actual := lineage.TraceDownStream(ctx, mock, graph, knitId, 1)
		if !errors.Is(actual, expectedError) {
			t.Errorf("wrong status: (actual, expected) != (%v, %v)", actual, expectedError)
		}
	})

	t.Run("When GetRun fails, it returns the error that contains that error", func(t *testing.T) {
		mock := mock.New(t)
		knitId := "knitId-test"
		mock.Impl.FindData = func(ctx context.Context, tags []apitag.Tag, since *time.Time, duration *time.Duration) ([]apidata.Detail, error) {
			return []apidata.Detail{
				dummyData(knitId, "run1", "run2"),
			}, nil
		}
		mock.Impl.GetRun = func(ctx context.Context, runId string) (apirun.Detail, error) {
			return apirun.Detail{}, expectedError
		}
		ctx := context.Background()
		graph := lineage.NewDirectedGraph()
		_, actual := lineage.TraceDownStream(ctx, mock, graph, knitId, 1)
		if !errors.Is(actual, expectedError) {
			t.Errorf("wrong status: (actual, expected) != (%s, %d)", actual, expectedError)
		}
	})
}

func TestTraceUpStream(t *testing.T) {
	type When struct {
		RootKnitId      string
		Depth           int
		FindDataReturns [][]apidata.Detail
		GetRunReturns   []apirun.Detail
	}

	type Then struct {
		Graph lineage.DirectedGraph
		Err   error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {

			mock := mock.New(t)

			// Store arguments and return values for each call
			nthData := 0
			mock.Impl.FindData = func(
				ctx context.Context, tags []apitag.Tag, since *time.Time, duration *time.Duration) (
				[]apidata.Detail, error) {
				ret := when.FindDataReturns[nthData]
				nthData += 1
				return ret, nil
			}

			nthRun := 0
			mock.Impl.GetRun = func(
				ctx context.Context, runId string) (
				apirun.Detail, error) {
				ret := when.GetRunReturns[nthRun]
				nthRun += 1
				return ret, nil
			}

			ctx := context.Background()
			graph := lineage.NewDirectedGraph()
			graph, actual := lineage.TraceUpStream(ctx, mock, graph, when.RootKnitId, when.Depth)
			if !errors.Is(actual, then.Err) {
				t.Errorf(
					"wrong status: (actual, expected) != (%d, %d)",
					actual, then.Err,
				)
			}

			if !cmp.MapEqWith(
				graph.DataNodes,
				then.Graph.DataNodes,
				func(a, b lineage.DataNode) bool { return a.Equal(&b) },
			) {
				t.Errorf(
					"DataNodes is not equal (actual,expected): %v,%v",
					graph.DataNodes, then.Graph.DataNodes,
				)
			}
			if !cmp.MapEqWith(
				graph.RunNodes,
				then.Graph.RunNodes,
				func(a, b lineage.RunNode) bool { return a.Equal(&b.Summary) },
			) {
				t.Errorf(
					"RunNodes is not equal (actual,expected): %v,%v",
					graph.RunNodes, then.Graph.RunNodes,
				)
			}
			if !cmp.MapEqWith(
				graph.EdgesFromData,
				then.Graph.EdgesFromData,
				func(a []lineage.Edge, b []lineage.Edge) bool {
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
				func(a []lineage.Edge, b []lineage.Edge) bool {
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
				Depth:           1,
				FindDataReturns: [][]apidata.Detail{{data1}},
				GetRunReturns:   []apirun.Detail{run1},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes:     map[string]lineage.DataNode{"data1": toDataNode(data1)},
					RunNodes:      map[string]lineage.RunNode{"run1": {run1.Summary}},
					EdgesFromData: map[string][]lineage.Edge{},
					EdgesFromRun:  map[string][]lineage.Edge{"run1": {{ToId: "data1", Label: "upload"}}},
					RootNodes:     []string{"run1"},
				},
				Err: nil,
			},
		))
		t.Run("Confirm that all the above nodes can be traced in graph depth: no limit", theory(
			When{
				RootKnitId:      "data1",
				Depth:           -1,
				FindDataReturns: [][]apidata.Detail{{data1}},
				GetRunReturns:   []apirun.Detail{run1},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes:     map[string]lineage.DataNode{"data1": toDataNode(data1)},
					RunNodes:      map[string]lineage.RunNode{"run1": {run1.Summary}},
					EdgesFromData: map[string][]lineage.Edge{},
					EdgesFromRun:  map[string][]lineage.Edge{"run1": {{ToId: "data1", Label: "upload"}}},
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
				Depth:           2,
				FindDataReturns: [][]apidata.Detail{{data2}, {data1}},
				GetRunReturns:   []apirun.Detail{run2, run1},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2),
					},
					RunNodes: map[string]lineage.RunNode{
						"run1": {run1.Summary}, "run2": {run2.Summary},
					},
					EdgesFromData: map[string][]lineage.Edge{"data1": {{"run2", "in/1"}}},
					EdgesFromRun:  map[string][]lineage.Edge{"run1": {{"data1", "upload"}}, "run2": {{"data2", "out/1"}}},
					RootNodes:     []string{"run1"},
				},
				Err: nil,
			},
		))
		t.Run("Confirm that all nodes can be traced up to data1 in graph depth:1", theory(
			When{
				RootKnitId:      "data2",
				Depth:           1,
				FindDataReturns: [][]apidata.Detail{{data2}, {data1}},
				GetRunReturns:   []apirun.Detail{run2},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2),
					},
					RunNodes: map[string]lineage.RunNode{
						"run2": {run2.Summary},
					},
					EdgesFromData: map[string][]lineage.Edge{"data1": {{"run2", "in/1"}}},
					EdgesFromRun:  map[string][]lineage.Edge{"run2": {{"data2", "out/1"}}},
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
				Depth:           2,
				FindDataReturns: [][]apidata.Detail{{data3}, {data2}, {data1}},
				GetRunReturns:   []apirun.Detail{run2, run1},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2), "data3": toDataNode(data3),
					},
					RunNodes: map[string]lineage.RunNode{
						"run1": {run1.Summary}, "run2": {run2.Summary},
					},
					EdgesFromData: map[string][]lineage.Edge{"data1": {{"run2", "in/1"}}},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{"data1", "upload"}}, "run2": {{"data2", "out/1"}, {"data3", "out/2"}},
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
				Depth:           -1,
				FindDataReturns: [][]apidata.Detail{{data4}, {data2}, {data3}, {data1}},
				GetRunReturns:   []apirun.Detail{run3, run2, run1},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2), "data3": toDataNode(data3),
						"data4": toDataNode(data4),
					},
					RunNodes: map[string]lineage.RunNode{
						"run1": {run1.Summary}, "run2": {run2.Summary}, "run3": {run3.Summary},
					},
					EdgesFromData: map[string][]lineage.Edge{
						"data1": {{"run2", "in/1"}}, "data2": {{"run3", "in/2"}}, "data3": {{"run3", "in/3"}},
					},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{"data1", "upload"}}, "run2": {{"data2", "out/1"}, {"data3", "out/2"}},
						"run3": {{"data4", "out/3"}},
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
				Depth:           1,
				FindDataReturns: [][]apidata.Detail{{data4}, {data1}, {data3}},
				GetRunReturns:   []apirun.Detail{run4},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data3": toDataNode(data3), "data4": toDataNode(data4),
					},
					RunNodes: map[string]lineage.RunNode{
						"run4": {run4.Summary},
					},
					EdgesFromData: map[string][]lineage.Edge{
						"data1": {{"run4", "in/3"}}, "data3": {{"run4", "in/4"}},
					},
					EdgesFromRun: map[string][]lineage.Edge{
						"run4": {{"data4", "out/3"}},
					},
					RootNodes: []string{},
				},
				Err: nil,
			},
		))
		t.Run("Confirm that run3, data2, run1 and root can be traced in graph depth;2 ", theory(
			When{
				RootKnitId:      "data4",
				Depth:           2,
				FindDataReturns: [][]apidata.Detail{{data4}, {data1}, {data3}, {data2}},
				GetRunReturns:   []apirun.Detail{run4, run1, run3},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2),
						"data3": toDataNode(data3), "data4": toDataNode(data4),
					},
					RunNodes: map[string]lineage.RunNode{
						"run1": {run1.Summary}, "run3": {run3.Summary}, "run4": {run4.Summary},
					},
					EdgesFromData: map[string][]lineage.Edge{
						"data1": {{"run4", "in/3"}, {"run3", "in/2"}},
						"data2": {{"run3", "in/4"}}, "data3": {{"run4", "in/4"}},
					},
					EdgesFromRun: map[string][]lineage.Edge{
						"run4": {{"data4", "out/3"}}, "run3": {{"data3", "out/2"}}, "run1": {{"data1", "upload"}},
					},
					RootNodes: []string{"run1"},
				},
				Err: nil,
			},
		))

		t.Run("Confirm that all nodes root can be traced in graph depth;3 ", theory(
			When{
				RootKnitId:      "data4",
				Depth:           3,
				FindDataReturns: [][]apidata.Detail{{data4}, {data1}, {data3}, {data2}},
				GetRunReturns:   []apirun.Detail{run4, run1, run3, run2},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2),
						"data3": toDataNode(data3), "data4": toDataNode(data4),
					},
					RunNodes: map[string]lineage.RunNode{
						"run1": {run1.Summary}, "run2": {run2.Summary},
						"run3": {run3.Summary}, "run4": {run4.Summary},
					},
					EdgesFromData: map[string][]lineage.Edge{
						"data1": {{"run4", "in/3"}, {"run3", "in/2"}, {"run2", "in/1"}},
						"data2": {{"run3", "in/4"}}, "data3": {{"run4", "in/4"}},
					},
					EdgesFromRun: map[string][]lineage.Edge{
						"run4": {{"data4", "out/3"}}, "run3": {{"data3", "out/2"}}, "run1": {{"data1", "upload"}},
						"run2": {{"data2", "out/1"}},
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
				Depth:           2,
				FindDataReturns: [][]apidata.Detail{{data5}, {data4}, {data3}, {data2}, {data1}},
				GetRunReturns:   []apirun.Detail{run2, run1},
			},
			Then{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2),
						"data3": toDataNode(data3), "data4": toDataNode(data4), "data5": toDataNode(data5),
					},
					RunNodes: map[string]lineage.RunNode{
						"run1": {run1.Summary}, "run2": {run2.Summary},
					},
					EdgesFromData: map[string][]lineage.Edge{
						"data1": {{"run1", "in/1"}}, "data3": {{"run2", "in/2"}},
					},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{"data3", "out/1"}, {"data2", "(log)"}},
						"run2": {{"data4", "out/2"}, {"data5", "(log)"}},
					},
					RootNodes: []string{},
				},
				Err: nil,
			},
		))
	}

	t.Run("When FindData returns Empty array it returns ErrNotFoundData", func(t *testing.T) {
		mock := mock.New(t)
		mock.Impl.FindData = func(ctx context.Context, tags []apitag.Tag, since *time.Time, duration *time.Duration) ([]apidata.Detail, error) {
			return []apidata.Detail{}, nil
		}
		mock.Impl.GetRun = func(ctx context.Context, runId string) (apirun.Detail, error) {
			return apirun.Detail{}, nil
		}

		ctx := context.Background()
		graph := lineage.NewDirectedGraph()
		knitId := "knitId-test"
		_, actual := lineage.TraceUpStream(ctx, mock, graph, knitId, 1)
		if !errors.Is(actual, lineage.ErrNotFoundData) {
			t.Errorf("wrong status: (actual, expected) != (%v, %v)", actual, lineage.ErrNotFoundData)
		}
	})

	expectedError := errors.New("fake error")
	t.Run("When FindData fails, it returns the error that contains that error ", func(t *testing.T) {
		mock := mock.New(t)
		mock.Impl.FindData = func(ctx context.Context, tags []apitag.Tag, since *time.Time, duration *time.Duration) ([]apidata.Detail, error) {
			return []apidata.Detail{}, expectedError
		}
		mock.Impl.GetRun = func(ctx context.Context, runId string) (apirun.Detail, error) {
			return apirun.Detail{}, nil
		}

		ctx := context.Background()
		graph := lineage.NewDirectedGraph()
		knitId := "knitId-test"
		_, actual := lineage.TraceUpStream(ctx, mock, graph, knitId, 1)
		if !errors.Is(actual, expectedError) {
			t.Errorf("wrong status: (actual, expected) != (%v, %v)", actual, expectedError)
		}
	})

	t.Run("When GetRun fails, it returns the error that contains that error", func(t *testing.T) {
		mock := mock.New(t)
		mock.Impl.GetRun = func(ctx context.Context, runId string) (apirun.Detail, error) {
			return apirun.Detail{}, expectedError
		}
		knitId := "knitId-test"
		mock.Impl.FindData = func(ctx context.Context, tags []apitag.Tag, since *time.Time, duration *time.Duration) ([]apidata.Detail, error) {
			return []apidata.Detail{
				dummyData(knitId, "run0", "run2"),
			}, nil
		}
		ctx := context.Background()
		graph := lineage.NewDirectedGraph()
		graph, actual := lineage.TraceUpStream(ctx, mock, graph, knitId, 1)
		if !errors.Is(actual, expectedError) {
			t.Errorf("wrong status: (actual, expected) != (%s, %d)", graph.DataNodes, expectedError)
		}
	})
}

func toDataNode(data apidata.Detail) lineage.DataNode {
	return lineage.DataNode{
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

func dummyAssignedTo(runId string) apidata.AssignedTo {
	return apidata.AssignedTo{
		Run: apirun.Summary{
			RunId:  runId,
			Status: "done",
			Plan: apiplan.Summary{
				PlanId: "plan-3",
				Image:  &apiplan.Image{Repository: "knit.image.repo.invalid/trainer", Tag: "v1"},
			},
		},
		Mountpoint: apiplan.Mountpoint{Path: "/out"},
	}
}

func dummySliceAssignedTo(runIds ...string) []apidata.AssignedTo {
	slice := []apidata.AssignedTo{}
	for _, runId := range runIds {
		element := dummyAssignedTo(runId)
		slice = append(slice, element)
	}
	return slice
}

func dummyData(knitId string, fromRunId string, toRunIds ...string) apidata.Detail {
	return apidata.Detail{
		KnitId: knitId,
		Tags: []apitag.Tag{
			{Key: "foo", Value: "bar"},
			{Key: "fizz", Value: "bazz"},
			{Key: kdb.KeyKnitId, Value: knitId},
			{Key: kdb.KeyKnitTimestamp, Value: "2024-04-01T12:34:56+00:00"},
		},
		Upstream:    dummyAssignedTo(fromRunId),
		Downstreams: dummySliceAssignedTo(toRunIds...),
		Nomination:  []apidata.NominatedBy{},
	}
}

func dummyDataForFailed(knitId string, fromRunId string, toRunIds ...string) apidata.Detail {
	return apidata.Detail{
		KnitId: knitId,
		Tags: []apitag.Tag{
			{Key: "foo", Value: "bar"},
			{Key: "fizz", Value: "bazz"},
			{Key: kdb.KeyKnitId, Value: knitId},
			{Key: kdb.KeyKnitTimestamp, Value: "2024-04-01T12:34:56+00:00"},
			{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientFailed},
		},
		Upstream:    dummyAssignedTo(fromRunId),
		Downstreams: dummySliceAssignedTo(toRunIds...),
		Nomination:  []apidata.NominatedBy{},
	}
}

func dummyRun(runId string, inputs map[string]string, outputs map[string]string) apirun.Detail {
	return apirun.Detail{
		Summary: apirun.Summary{
			RunId:  runId,
			Status: "done",
			Plan: apiplan.Summary{
				PlanId: "test-Id",
				Image: &apiplan.Image{
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

func dummyRunWithLog(runId string, knitId string, inputs map[string]string, outputs map[string]string) apirun.Detail {
	return apirun.Detail{
		Summary: apirun.Summary{
			RunId:  runId,
			Status: "done",
			Plan: apiplan.Summary{
				PlanId: "test-Id",
				Image: &apiplan.Image{
					Repository: "test-image",
					Tag:        "test-version",
				},
				Name: "test-Name",
			},
		},
		Inputs:  dummySliceAssignment(inputs),
		Outputs: dummySliceAssignment(outputs),
		Log: &apirun.LogSummary{
			LogPoint: apiplan.LogPoint{
				Tags: []apitag.Tag{
					{Key: "type", Value: "log"},
					{Key: "format", Value: "jsonl"},
				},
			},
			KnitId: knitId,
		},
	}
}

func dummyFailedRunWithLog(runId string, knitId string, inputs map[string]string, outputs map[string]string) apirun.Detail {
	return apirun.Detail{
		Summary: apirun.Summary{
			RunId:  runId,
			Status: "failed",
			Plan: apiplan.Summary{
				PlanId: "test-Id",
				Image: &apiplan.Image{
					Repository: "test-image",
					Tag:        "test-version",
				},
				Name: "test-Name",
			},
		},
		Inputs:  dummySliceAssignment(inputs),
		Outputs: dummySliceAssignment(outputs),
		Log: &apirun.LogSummary{
			LogPoint: apiplan.LogPoint{
				Tags: []apitag.Tag{
					{Key: "type", Value: "log"},
					{Key: "format", Value: "jsonl"},
				},
			},
			KnitId: knitId,
		},
	}
}

func dummyLogData(knitId string, fromRunId string, toRunIds ...string) apidata.Detail {
	return apidata.Detail{
		KnitId: knitId,
		Tags: []apitag.Tag{
			{Key: "type", Value: "log"},
			{Key: "format", Value: "jsonl"},
		},
		Upstream:    dummyAssignedTo(fromRunId),
		Downstreams: dummySliceAssignedTo(toRunIds...),
		Nomination:  []apidata.NominatedBy{},
	}
}

func dummyAssignment(knitId string, mountPath string) apirun.Assignment {
	return apirun.Assignment{
		Mountpoint: apiplan.Mountpoint{
			Path: mountPath,
			Tags: []apitag.Tag{
				{Key: "type", Value: "training data"},
				{Key: "format", Value: "mask"},
			},
		},
		KnitId: knitId,
	}
}

func dummySliceAssignment(knitIdToMoutPath map[string]string) []apirun.Assignment {
	slice := []apirun.Assignment{}
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
func TestGenerateDot(t *testing.T) {
	type When struct {
		Graph     lineage.DirectedGraph
		ArgKnitId string
	}
	type Then struct {
		RequiredContent string
	}
	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			w := new(strings.Builder)

			if err := when.Graph.GenerateDot(w, when.ArgKnitId); err != nil {
				t.Fatal(err)
			}

			if w.String() != then.RequiredContent {
				t.Errorf("fail \nactual:\n%s \n=========\nexpect:\n%s", w.String(), then.RequiredContent)
			}
		}
	}
	{
		// [test case of data lineage]
		// data1 (When there is no downstream for data to be tracked)
		data1 := dummyData("data1", "run0")
		t.Run(" Confirm that when only nodes exist in the graph, they can be output as dot format.", theory(
			When{
				Graph: lineage.DirectedGraph{
					DataNodes:     map[string]lineage.DataNode{"data1": toDataNode(data1)},
					RunNodes:      map[string]lineage.RunNode{},
					EdgesFromData: map[string][]lineage.Edge{},
					EdgesFromRun:  map[string][]lineage.Edge{},
					RootNodes:     []string{},
					KeysDataNode:  []string{"data1"},
					KeysRunNode:   []string{},
				},
				ArgKnitId: "data1",
			},
			Then{
				// The specification is to represent the node name as "r" + runId for run,
				// and "d" + knitId for data.
				RequiredContent: `digraph G {
	node [shape=record fontsize=10]
	edge [fontsize=10]

	"ddata1"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#d4ecc6">knit#id: data1</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];


}`,
			},
		))
	}
	{
		// [test case of data lineage]
		// data1 --> [in/1] -->  run1 --> [out/1] --> data2
		data1 := dummyData("data1", "run0", "run1")
		data2 := dummyData("data2", "run1")
		run1 := dummyRun("run1", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1"})
		t.Run(" Confirm that when only nodes, edges, exist in the graph, they can be output as dot format.", theory(
			When{
				Graph: lineage.DirectedGraph{
					DataNodes:     map[string]lineage.DataNode{"data1": toDataNode(data1), "data2": toDataNode(data2)},
					RunNodes:      map[string]lineage.RunNode{"run1": {run1.Summary}},
					EdgesFromData: map[string][]lineage.Edge{"data1": {{"run1", "in/1"}}, "data2": {{"run1", "out/1"}}},
					EdgesFromRun:  map[string][]lineage.Edge{"run1": {{"data1", "in/1"}, {"data2", "out/1"}}},
					RootNodes:     []string{},
					KeysDataNode:  []string{"data1", "data2"},
					KeysRunNode:   []string{"run1"},
				},
				ArgKnitId: "data1",
			},
			Then{
				RequiredContent: `digraph G {
	node [shape=record fontsize=10]
	edge [fontsize=10]

	"ddata1"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#d4ecc6">knit#id: data1</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"ddata2"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#FFFFFF">knit#id: data2</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"rrun1"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD><FONT COLOR="#007700"><B>done</B></FONT></TD><TD>id: run1</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: 0001-01-01T09:18:59+09:18</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = test-image:test-version</TD></TR>
			</TABLE>
		>
	];

	"ddata1" -> "rrun1" [label="in/1"];
	"ddata2" -> "rrun1" [label="out/1"];
	"rrun1" -> "ddata1" [label="in/1"];
	"rrun1" -> "ddata2" [label="out/1"];

}`,
			},
		))
	}
	{
		// [test case of data lineage]
		// root -->  run1 --> [upload] --> data1 --> [in/1] --> run2 --> [out/1] --> data2
		run1 := dummyRun("run1", map[string]string{}, map[string]string{"data1": "upload"})
		data1 := dummyData("data1", "run1")
		run2 := dummyRun("run2", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1"})
		data2 := dummyData("data2", "run2")
		t.Run("Confirm that when nodes, edges, and roots exist in the graph, they can be output as dot format.", theory(
			When{
				Graph: lineage.DirectedGraph{
					DataNodes:     map[string]lineage.DataNode{"data1": toDataNode(data1), "data2": toDataNode(data2)},
					RunNodes:      map[string]lineage.RunNode{"run1": {run1.Summary}, "run2": {run2.Summary}},
					EdgesFromData: map[string][]lineage.Edge{"data1": {{"run2", "in/1"}}, "data2": {}},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{ToId: "data1", Label: "upload"}}, "run2": {{ToId: "data2", Label: "out/1"}}},
					KeysDataNode: []string{"data1", "data2"},
					KeysRunNode:  []string{"run1", "run2"},
					RootNodes:    []string{"run1"},
				},
				ArgKnitId: "data2",
			},
			Then{
				RequiredContent: `digraph G {
	node [shape=record fontsize=10]
	edge [fontsize=10]

	"ddata1"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#FFFFFF">knit#id: data1</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"ddata2"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#d4ecc6">knit#id: data2</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"rrun1"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD><FONT COLOR="#007700"><B>done</B></FONT></TD><TD>id: run1</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: 0001-01-01T09:18:59+09:18</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = test-image:test-version</TD></TR>
			</TABLE>
		>
	];
	"rrun2"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD><FONT COLOR="#007700"><B>done</B></FONT></TD><TD>id: run2</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: 0001-01-01T09:18:59+09:18</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = test-image:test-version</TD></TR>
			</TABLE>
		>
	];
	"root#0"[shape=Mdiamond];

	"ddata1" -> "rrun2" [label="in/1"];
	"rrun1" -> "ddata1" [label="upload"];
	"rrun2" -> "ddata2" [label="out/1"];
	"root#0" -> "rrun1";

}`,
			},
		))
	}
	{
		// [test case of data lineage]
		// root -->  run1 --> [upload] --> data1 --> [in/1] --> run2 (failed) --> [out/1] --> data2
		//                                                                                |-> log
		run1 := dummyRun("run1", map[string]string{}, map[string]string{"data1": "upload"})
		data1 := dummyData("data1", "run1")
		log := dummyDataForFailed("log", "run2")
		run2 := dummyFailedRunWithLog("run2", "log", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1"})
		data2 := dummyDataForFailed("data2", "run2")
		t.Run("When there are failed run and its output, they can be output as dot format.", theory(
			When{
				Graph: lineage.DirectedGraph{
					DataNodes:     map[string]lineage.DataNode{"data1": toDataNode(data1), "data2": toDataNode(data2), "log": toDataNode(log)},
					RunNodes:      map[string]lineage.RunNode{"run1": {run1.Summary}, "run2": {run2.Summary}},
					EdgesFromData: map[string][]lineage.Edge{"data1": {{"run2", "in/1"}}, "data2": {}, "log": {}},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{ToId: "data1", Label: "upload"}}, "run2": {{ToId: "data2", Label: "out/1"}, {ToId: "log", Label: "(log)"}}},
					KeysDataNode: []string{"data1", "data2", "log"},
					KeysRunNode:  []string{"run1", "run2"},
					RootNodes:    []string{"run1"},
				},
				ArgKnitId: "data1",
			},
			Then{
				RequiredContent: `digraph G {
	node [shape=record fontsize=10]
	edge [fontsize=10]

	"ddata1"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#d4ecc6">knit#id: data1</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"ddata2"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#FFFFFF">knit#id: data2</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00 | knit#transient:failed</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"dlog"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#FFFFFF">knit#id: log</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00 | knit#transient:failed</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"rrun1"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD><FONT COLOR="#007700"><B>done</B></FONT></TD><TD>id: run1</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: 0001-01-01T09:18:59+09:18</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = test-image:test-version</TD></TR>
			</TABLE>
		>
	];
	"rrun2"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD><FONT COLOR="red"><B>failed</B></FONT></TD><TD>id: run2</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: 0001-01-01T09:18:59+09:18</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = test-image:test-version</TD></TR>
			</TABLE>
		>
	];
	"root#0"[shape=Mdiamond];

	"ddata1" -> "rrun2" [label="in/1"];
	"rrun1" -> "ddata1" [label="upload"];
	"rrun2" -> "ddata2" [label="out/1"];
	"rrun2" -> "dlog" [label="(log)"];
	"root#0" -> "rrun1";

}`,
			},
		))
	}
	{
		// [test case of data lineage]
		//         	                                  data4 --> [in/3] -|
		// data1 --> [in/1] -->  run1 --> [out/1] --> data2 --> [in/2] --> run2 --> [out/3] --> data5
		//					          |-> [out/2] --> data3 --> [in/4] --> run3 --> [out/4] --> data6
		data1 := dummyData("data1", "run0", "run1")
		run1 := dummyRun("run1", map[string]string{"data1": "in/1"}, map[string]string{"data2": "out/1", "data3": "out/2"})
		data2 := dummyData("data2", "run1", "run2")
		data3 := dummyData("data3", "run1", "run3")
		run2 := dummyRun("run2", map[string]string{"data2": "in/2", "data4": "in/3"}, map[string]string{"data5": "out/3"})
		data4 := dummyData("data4", "runxx", "run2")
		data5 := dummyData("data5", "run2")
		run3 := dummyRun("run3", map[string]string{"data3": "in/4"}, map[string]string{"data6": "out/4"})
		data6 := dummyData("data6", "run3")

		t.Run("Confirm that when the graph configuration is complex, they can be output as dot format.", theory(
			When{
				Graph: lineage.DirectedGraph{
					DataNodes: map[string]lineage.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2), "data3": toDataNode(data3),
						"data4": toDataNode(data4), "data5": toDataNode(data5), "data6": toDataNode(data6),
					},
					RunNodes: map[string]lineage.RunNode{
						"run1": {run1.Summary}, "run2": {run2.Summary}, "run3": {run3.Summary},
					},
					EdgesFromData: map[string][]lineage.Edge{
						"data1": {{"run1", "in/1"}}, "data2": {{"run2", "in/2"}}, "data3": {{"run3", "in/4"}},
						"data4": {{"run2", "in/3"}}, "data5": {}, "data6": {},
					},
					EdgesFromRun: map[string][]lineage.Edge{
						"run1": {{"data2", "out/1"}, {"data3", "out/2"}},
						"run2": {{"data5", "out/3"}}, "run3": {{"data6", "out/4"}},
					},
					KeysDataNode: []string{"data1", "data2", "data3", "data4", "data5", "data6"},
					KeysRunNode:  []string{"run1", "run2", "run3"},
					RootNodes:    []string{},
				},
				ArgKnitId: "data1",
			},
			Then{
				RequiredContent: `digraph G {
	node [shape=record fontsize=10]
	edge [fontsize=10]

	"ddata1"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#d4ecc6">knit#id: data1</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"ddata2"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#FFFFFF">knit#id: data2</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"ddata3"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#FFFFFF">knit#id: data3</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"ddata4"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#FFFFFF">knit#id: data4</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"ddata5"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#FFFFFF">knit#id: data5</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"ddata6"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#FFFFFF">knit#id: data6</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">2024-04-01T21:34:56+09:00</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"rrun1"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD><FONT COLOR="#007700"><B>done</B></FONT></TD><TD>id: run1</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: 0001-01-01T09:18:59+09:18</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = test-image:test-version</TD></TR>
			</TABLE>
		>
	];
	"rrun2"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD><FONT COLOR="#007700"><B>done</B></FONT></TD><TD>id: run2</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: 0001-01-01T09:18:59+09:18</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = test-image:test-version</TD></TR>
			</TABLE>
		>
	];
	"rrun3"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD><FONT COLOR="#007700"><B>done</B></FONT></TD><TD>id: run3</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: 0001-01-01T09:18:59+09:18</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = test-image:test-version</TD></TR>
			</TABLE>
		>
	];

	"ddata1" -> "rrun1" [label="in/1"];
	"ddata2" -> "rrun2" [label="in/2"];
	"ddata3" -> "rrun3" [label="in/4"];
	"ddata4" -> "rrun2" [label="in/3"];
	"rrun1" -> "ddata2" [label="out/1"];
	"rrun1" -> "ddata3" [label="out/2"];
	"rrun2" -> "ddata5" [label="out/3"];
	"rrun3" -> "ddata6" [label="out/4"];

}`,
			},
		))
	}
}
