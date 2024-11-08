package knitgraph_test

import (
	"sort"
	"strings"
	"testing"

	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/cmd/knit/knitgraph"
	"github.com/opst/knitfab/pkg/domain"
)

func TestGenerateDot(t *testing.T) {
	type When struct {
		Graph     knitgraph.DirectedGraph
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
				Graph: knitgraph.DirectedGraph{
					DataNodes:     map[string]knitgraph.DataNode{"data1": toDataNode(data1)},
					RunNodes:      map[string]knitgraph.RunNode{},
					EdgesFromData: map[string][]knitgraph.Edge{},
					EdgesFromRun:  map[string][]knitgraph.Edge{},
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
				Graph: knitgraph.DirectedGraph{
					DataNodes:     map[string]knitgraph.DataNode{"data1": toDataNode(data1), "data2": toDataNode(data2)},
					RunNodes:      map[string]knitgraph.RunNode{"run1": {Summary: run1.Summary}},
					EdgesFromData: map[string][]knitgraph.Edge{"data1": {{ToId: "run1", Label: "in/1"}}, "data2": {{ToId: "run1", Label: "out/1"}}},
					EdgesFromRun:  map[string][]knitgraph.Edge{"run1": {{ToId: "data1", Label: "in/1"}, {ToId: "data2", Label: "out/1"}}},
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
				Graph: knitgraph.DirectedGraph{
					DataNodes:     map[string]knitgraph.DataNode{"data1": toDataNode(data1), "data2": toDataNode(data2)},
					RunNodes:      map[string]knitgraph.RunNode{"run1": {Summary: run1.Summary}, "run2": {Summary: run2.Summary}},
					EdgesFromData: map[string][]knitgraph.Edge{"data1": {{ToId: "run2", Label: "in/1"}}, "data2": {}},
					EdgesFromRun: map[string][]knitgraph.Edge{
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
				Graph: knitgraph.DirectedGraph{
					DataNodes:     map[string]knitgraph.DataNode{"data1": toDataNode(data1), "data2": toDataNode(data2), "log": toDataNode(log)},
					RunNodes:      map[string]knitgraph.RunNode{"run1": {Summary: run1.Summary}, "run2": {Summary: run2.Summary}},
					EdgesFromData: map[string][]knitgraph.Edge{"data1": {{ToId: "run2", Label: "in/1"}}, "data2": {}, "log": {}},
					EdgesFromRun: map[string][]knitgraph.Edge{
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
				Graph: knitgraph.DirectedGraph{
					DataNodes: map[string]knitgraph.DataNode{
						"data1": toDataNode(data1), "data2": toDataNode(data2), "data3": toDataNode(data3),
						"data4": toDataNode(data4), "data5": toDataNode(data5), "data6": toDataNode(data6),
					},
					RunNodes: map[string]knitgraph.RunNode{
						"run1": {Summary: run1.Summary}, "run2": {Summary: run2.Summary}, "run3": {Summary: run3.Summary},
					},
					EdgesFromData: map[string][]knitgraph.Edge{
						"data1": {{ToId: "run1", Label: "in/1"}}, "data2": {{ToId: "run2", Label: "in/2"}}, "data3": {{ToId: "run3", Label: "in/4"}},
						"data4": {{ToId: "run2", Label: "in/3"}}, "data5": {}, "data6": {},
					},
					EdgesFromRun: map[string][]knitgraph.Edge{
						"run1": {{ToId: "data2", Label: "out/1"}, {ToId: "data3", Label: "out/2"}},
						"run2": {{ToId: "data5", Label: "out/3"}}, "run3": {{ToId: "data6", Label: "out/4"}},
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
		Upstream:    dummyAssignedTo(fromRunId),
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
		Upstream:    dummyAssignedTo(fromRunId),
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
		Upstream:    dummyAssignedTo(fromRunId),
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
