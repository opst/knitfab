package knitgraph_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/cmd/knit/knitgraph"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestGenerateDot(t *testing.T) {
	type When struct {
		Graph     *knitgraph.DirectedGraph
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
		// data1 --[in/1]--> run1 --[out/1]--> data2
		data1 := data.Detail{
			KnitId: "data1",
			Tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "fizz", Value: "bazz"},
				{Key: domain.KeyKnitId, Value: "data1"},
				{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T12:34:55+00:00"},
			},
			Upstream: data.AssignedTo{
				Run: runs.Summary{
					RunId: "run0", Status: "done",
					Plan: plans.Summary{PlanId: "upload", Name: "knit#uploaded"},
				},
				Mountpoint: plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run: runs.Summary{
						RunId: "run1", Status: "done",
						Plan: plans.Summary{
							PlanId: "plan-3",
							Image: &plans.Image{
								Repository: "knit.image.repo.invalid/trainer",
								Tag:        "v1",
							},
						},
					},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "fizz", Value: "bazz"},
				{Key: domain.KeyKnitId, Value: "data2"},
				{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T12:34:56+00:00"},
			},
			Upstream: data.AssignedTo{
				Run: runs.Summary{
					RunId: "run1", Status: "done",
					Plan: plans.Summary{
						PlanId: "plan-3",
						Image:  &plans.Image{Repository: "knit.image.repo.invalid/trainer", Tag: "v1"},
					},
				},
				Mountpoint: plans.Mountpoint{Path: "/out/1"},
			},
		}
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1", Status: "done",
				Plan: plans.Summary{
					PlanId: "plan-3",
					Image:  &plans.Image{Repository: "knit.image.repo.invalid/trainer", Tag: "v1"},
				},
				UpdatedAt: try.To(
					rfctime.ParseRFC3339DateTime("2024-04-01T12:34:56+00:00"),
				).OrFatal(t),
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
		t.Run("When graph have nodes and edges, then they should be output as dot format.", theory(
			When{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, data2),
					knitgraph.WithRun(run1),
				),
				ArgKnitId: "data1",
			},
			Then{
				RequiredContent: fmt.Sprintf(
					`digraph G {
	node [shape=record fontsize=10]
	edge [fontsize=10]

	"ddata1"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#d4ecc6">knit#id: data1</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s</FONT></TD></TR>
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
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s</FONT></TD></TR>
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
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: %s</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = knit.image.repo.invalid/trainer:v1</TD></TR>
			</TABLE>
		>
	];

	"ddata1" -> "rrun1" [label="/in/1"];
	"rrun1" -> "ddata2" [label="/out/1"];

}`,
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T12:34:55+00:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T12:34:56+00:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T12:34:56+00:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
				),
			},
		))
	}
	{
		// [test case of data lineage]
		// root -->  run1 --[/upload]--> data1 --[/in/1]--> run2 --[/out/1]--> data2
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1", Status: "done",
				Plan: plans.Summary{PlanId: "upload", Name: "knit#uploaded"},
				UpdatedAt: try.To(
					rfctime.ParseRFC3339DateTime("2024-04-01T12:34:56+00:00"),
				).OrFatal(t),
			},
			Inputs: []runs.Assignment{},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/upload"},
				},
			},
		}
		data1 := data.Detail{
			KnitId: "data1",
			Tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "fizz", Value: "bazz"},
				{Key: domain.KeyKnitId, Value: "data1"},
				{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T21:34:56+09:00"},
			},
			Upstream: data.AssignedTo{
				Run: runs.Summary{
					RunId: "run1", Status: "done",
					Plan: plans.Summary{
						PlanId: "upload",
						Name:   "knit#uploaded",
					},
				},
				Mountpoint: plans.Mountpoint{Path: "/upload"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run: runs.Summary{
						RunId: "run2", Status: "done",
						Plan: plans.Summary{
							PlanId: "plan-3",
							Image: &plans.Image{
								Repository: "knit.image.repo.invalid/trainer",
								Tag:        "v1",
							},
						},
					},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
		}
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2", Status: "done",
				Plan: plans.Summary{
					PlanId: "plan-3",
					Image: &plans.Image{
						Repository: "knit.image.repo.invalid/trainer",
						Tag:        "v2",
					},
				},
				UpdatedAt: try.To(
					rfctime.ParseRFC3339DateTime("2024-04-01T12:34:56+00:00"),
				).OrFatal(t),
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
			Tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "fizz", Value: "bazz"},
				{Key: domain.KeyKnitId, Value: "data2"},
				{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T21:34:56+09:00"},
			},
			Upstream: data.AssignedTo{
				Run: runs.Summary{
					RunId: "run2", Status: "done",
					Plan: plans.Summary{
						PlanId: "plan-3",
						Image: &plans.Image{
							Repository: "knit.image.repo.invalid/trainer",
							Tag:        "v2",
						},
					},
				},
				Mountpoint: plans.Mountpoint{Path: "/out/1"},
			},
		}
		t.Run("Confirm that when nodes, edges, and roots exist in the graph, they can be output as dot format.", theory(
			When{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data2),
					knitgraph.WithRun(run2),
					knitgraph.WithData(data1),
					knitgraph.WithRun(run1),
				),
				ArgKnitId: "data2",
			},
			Then{
				RequiredContent: fmt.Sprintf(`digraph G {
	node [shape=record fontsize=10]
	edge [fontsize=10]

	"ddata2"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#d4ecc6">knit#id: data2</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"ddata1"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#FFFFFF">knit#id: data1</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s</FONT></TD></TR>
				<TR><TD COLSPAN="2"><B>foo</B>:bar<BR/><B>fizz</B>:bazz</TD></TR>
			</TABLE>
		>
	];
	"rrun2"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD><FONT COLOR="#007700"><B>done</B></FONT></TD><TD>id: run2</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: %s</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = knit.image.repo.invalid/trainer:v2</TD></TR>
			</TABLE>
		>
	];
	"rrun1"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD><FONT COLOR="#007700"><B>done</B></FONT></TD><TD>id: run1</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: %s</FONT></TD></TR>
				<TR><TD COLSPAN="3">knit#uploaded</TD></TR>
			</TABLE>
		>
	];
	"root#0"[shape=Mdiamond];

	"rrun2" -> "ddata2" [label="/out/1"];
	"ddata1" -> "rrun2" [label="/in/1"];
	"rrun1" -> "ddata1" [label="/upload"];
	"root#0" -> "rrun1";

}`,
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T12:34:56+00:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T12:34:56+00:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
				),
			},
		))
	}
	{
		// [test case of data lineage]
		// root -->  run1 --[/upload]--> data1 --[/in/1]--> run2 (failed) --[/out/1]--> data2
		//                                                                          `-> log
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1", Status: "done",
				Plan: plans.Summary{PlanId: "upload", Name: "knit#uploaded"},
				UpdatedAt: try.To(
					rfctime.ParseRFC3339DateTime("2024-04-01T12:34:56+00:00"),
				).OrFatal(t),
			},
			Inputs: []runs.Assignment{},
			Outputs: []runs.Assignment{
				{
					KnitId:     "data1",
					Mountpoint: plans.Mountpoint{Path: "/upload"},
				},
			},
		}
		data1 := data.Detail{
			KnitId: "data1",
			Tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "fizz", Value: "bazz"},
				{Key: domain.KeyKnitId, Value: "data1"},
				{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T12:34:56+00:00"},
			},
			Upstream: data.AssignedTo{
				Run:        run1.Summary,
				Mountpoint: plans.Mountpoint{Path: "/upload"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run: runs.Summary{
						RunId: "run2", Status: "failed",
						Plan: plans.Summary{
							PlanId: "plan-3",
							Image: &plans.Image{
								Repository: "knit.image.repo.invalid/trainer",
								Tag:        "v1",
							},
						},
					},
					Mountpoint: plans.Mountpoint{Path: "/in/1"},
				},
			},
		}
		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2", Status: "failed",
				Plan: plans.Summary{
					PlanId: "plan-3",
					Image: &plans.Image{
						Repository: "knit.image.repo.invalid/trainer",
						Tag:        "v1",
					},
				},
				UpdatedAt: try.To(
					rfctime.ParseRFC3339DateTime("2024-04-01T12:34:56+00:00"),
				).OrFatal(t),
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
			Log: &runs.LogSummary{KnitId: "log"},
		}

		data2 := data.Detail{
			KnitId: "data2",
			Tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "fizz", Value: "bazz"},
				{Key: domain.KeyKnitId, Value: "data2"},
				{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T12:34:56+00:00"},
				{Key: domain.KeyKnitTransient, Value: "failed"},
			},
			Upstream: data.AssignedTo{
				Run:        run2.Summary,
				Mountpoint: plans.Mountpoint{Path: "/out/1"},
			},
		}
		log := data.Detail{
			KnitId: "log",
			Tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "fizz", Value: "bazz"},
				{Key: domain.KeyKnitId, Value: "log"},
				{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T12:34:56+00:00"},
				{Key: domain.KeyKnitTransient, Value: "failed"},
			},
			Upstream: data.AssignedTo{
				Run:        run2.Summary,
				Mountpoint: plans.Mountpoint{Path: "/log"},
			},
		}

		t.Run("When there are failed run and its output, they can be output as dot format.", theory(
			When{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1, data2, log),
					knitgraph.WithRun(run1, run2),
				),
				ArgKnitId: "data1",
			},
			Then{
				RequiredContent: fmt.Sprintf(
					`digraph G {
	node [shape=record fontsize=10]
	edge [fontsize=10]

	"ddata1"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#d4ecc6">knit#id: data1</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s</FONT></TD></TR>
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
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s | knit#transient:failed</FONT></TD></TR>
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
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s | knit#transient:failed</FONT></TD></TR>
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
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: %s</FONT></TD></TR>
				<TR><TD COLSPAN="3">knit#uploaded</TD></TR>
			</TABLE>
		>
	];
	"rrun2"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD><FONT COLOR="red"><B>failed</B></FONT></TD><TD>id: run2</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: %s</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = knit.image.repo.invalid/trainer:v1</TD></TR>
			</TABLE>
		>
	];
	"root#0"[shape=Mdiamond];

	"rrun1" -> "ddata1" [label="/upload"];
	"root#0" -> "rrun1";
	"ddata1" -> "rrun2" [label="/in/1"];
	"rrun2" -> "ddata2" [label="/out/1"];
	"rrun2" -> "dlog" [label="(log)"];

}`,
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T12:34:56+00:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T12:34:56+00:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T12:34:56+00:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
				),
			},
		))
	}
	{
		// [test case of comprex data lineage]
		//         	                           data4 --[in/3]-,
		// data1 --[in/1]--> run1 --[out/1]--> data2 --[in/2]--> run2 --[out/3]--> data5
		//                        `-[out/2]--> data3 --[in/4]--> run3 --[out/4]--> data6
		run1 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run1", Status: "done",
				Plan: plans.Summary{
					PlanId: "plan-1",
					Image: &plans.Image{
						Repository: "knit.image.repo.invalid/trainer",
						Tag:        "v1",
					},
				},
				UpdatedAt: try.To(
					rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00"),
				).OrFatal(t),
			},
			Inputs: []runs.Assignment{
				{KnitId: "data1", Mountpoint: plans.Mountpoint{Path: "/in/1"}},
			},
			Outputs: []runs.Assignment{
				{KnitId: "data2", Mountpoint: plans.Mountpoint{Path: "/out/1"}},
				{KnitId: "data3", Mountpoint: plans.Mountpoint{Path: "/out/2"}},
			},
		}

		run2 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run2", Status: "done",
				Plan: plans.Summary{
					PlanId: "plan-2",
					Image: &plans.Image{
						Repository: "knit.image.repo.invalid/trainer",
						Tag:        "v2",
					},
				},
				UpdatedAt: try.To(
					rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00"),
				).OrFatal(t),
			},
			Inputs: []runs.Assignment{
				{KnitId: "data2", Mountpoint: plans.Mountpoint{Path: "/in/2"}},
				{KnitId: "data4", Mountpoint: plans.Mountpoint{Path: "/in/3"}},
			},
			Outputs: []runs.Assignment{
				{KnitId: "data5", Mountpoint: plans.Mountpoint{Path: "/out/3"}},
			},
		}

		run3 := runs.Detail{
			Summary: runs.Summary{
				RunId: "run3", Status: "done",
				Plan: plans.Summary{
					PlanId: "plan-3",
					Image: &plans.Image{
						Repository: "knit.image.repo.invalid/trainer",
						Tag:        "v3",
					},
				},
				UpdatedAt: try.To(
					rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00"),
				).OrFatal(t),
			},
			Inputs: []runs.Assignment{
				{KnitId: "data3", Mountpoint: plans.Mountpoint{Path: "/in/4"}},
			},
			Outputs: []runs.Assignment{
				{KnitId: "data6", Mountpoint: plans.Mountpoint{Path: "/out/4"}},
			},
		}

		data1 := data.Detail{
			KnitId: "data1",
			Tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "fizz", Value: "bazz"},
				{Key: domain.KeyKnitId, Value: "data1"},
				{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T21:34:56+09:00"},
			},
			Upstream: data.AssignedTo{
				Run: runs.Summary{
					RunId: "run0", Status: "done",
					Plan: plans.Summary{PlanId: "upload", Name: "knit#uploaded"},
				},
				Mountpoint: plans.Mountpoint{Path: "/out/1"},
			},
			Downstreams: []data.AssignedTo{
				{
					Run: run1.Summary,
					Mountpoint: plans.Mountpoint{
						Path: "/in/1",
					},
				},
			},
		}
		data2 := data.Detail{
			KnitId: "data2",
			Tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "fizz", Value: "bazz"},
				{Key: domain.KeyKnitId, Value: "data2"},
				{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T21:34:56+09:00"},
			},
			Upstream: data.AssignedTo{
				Run: run1.Summary,
				Mountpoint: plans.Mountpoint{
					Path: "/out/1",
				},
			},
			Downstreams: []data.AssignedTo{
				{
					Run: run2.Summary,
					Mountpoint: plans.Mountpoint{
						Path: "/in/2",
					},
				},
			},
		}

		data3 := data.Detail{
			KnitId: "data3",
			Tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "fizz", Value: "bazz"},
				{Key: domain.KeyKnitId, Value: "data3"},
				{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T21:34:56+09:00"},
			},
			Upstream: data.AssignedTo{
				Run: run1.Summary,
				Mountpoint: plans.Mountpoint{
					Path: "/out/2",
				},
			},
			Downstreams: []data.AssignedTo{
				{
					Run: run3.Summary,
					Mountpoint: plans.Mountpoint{
						Path: "/in/4",
					},
				},
			},
		}

		data4 := data.Detail{
			KnitId: "data4",
			Tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "fizz", Value: "bazz"},
				{Key: domain.KeyKnitId, Value: "data4"},
				{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T21:34:56+09:00"},
			},
			Upstream: data.AssignedTo{
				Run: runs.Summary{
					RunId: "runxx", Status: "done",
					Plan: plans.Summary{PlanId: "plan-xx", Name: "knit#xx"},
				},
				Mountpoint: plans.Mountpoint{
					Path: "/out/xx",
				},
			},
			Downstreams: []data.AssignedTo{
				{
					Run: run2.Summary,
					Mountpoint: plans.Mountpoint{
						Path: "/in/3",
					},
				},
			},
		}
		data5 := data.Detail{
			KnitId: "data5",
			Tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "fizz", Value: "bazz"},
				{Key: domain.KeyKnitId, Value: "data5"},
				{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T21:34:56+09:00"},
			},
			Upstream: data.AssignedTo{
				Run: run2.Summary,
				Mountpoint: plans.Mountpoint{
					Path: "/out/3",
				},
			},
		}

		data6 := data.Detail{
			KnitId: "data6",
			Tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "fizz", Value: "bazz"},
				{Key: domain.KeyKnitId, Value: "data6"},
				{Key: domain.KeyKnitTimestamp, Value: "2024-04-01T21:34:56+09:00"},
			},
			Upstream: data.AssignedTo{
				Run: run3.Summary,
				Mountpoint: plans.Mountpoint{
					Path: "/out/4",
				},
			},
		}

		t.Run("Confirm that when the graph configuration is complex, they can be output as dot format.", theory(
			When{
				Graph: knitgraph.NewDirectedGraph(
					knitgraph.WithData(data1),
					knitgraph.WithRun(run1),
					knitgraph.WithData(data2, data3),
					knitgraph.WithRun(run2, run3),
					knitgraph.WithData(data4),
					knitgraph.WithData(data5, data6),
				),
				ArgKnitId: "data1",
			},
			Then{
				RequiredContent: fmt.Sprintf(
					`digraph G {
	node [shape=record fontsize=10]
	edge [fontsize=10]

	"ddata1"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="#d4ecc6">knit#id: data1</TD></TR>
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s</FONT></TD></TR>
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
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s</FONT></TD></TR>
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
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s</FONT></TD></TR>
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
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s</FONT></TD></TR>
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
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s</FONT></TD></TR>
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
				<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s</FONT></TD></TR>
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
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: %s</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = knit.image.repo.invalid/trainer:v1</TD></TR>
			</TABLE>
		>
	];
	"rrun2"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD><FONT COLOR="#007700"><B>done</B></FONT></TD><TD>id: run2</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: %s</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = knit.image.repo.invalid/trainer:v2</TD></TR>
			</TABLE>
		>
	];
	"rrun3"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD><FONT COLOR="#007700"><B>done</B></FONT></TD><TD>id: run3</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: %s</FONT></TD></TR>
				<TR><TD COLSPAN="3">image = knit.image.repo.invalid/trainer:v3</TD></TR>
			</TABLE>
		>
	];

	"ddata1" -> "rrun1" [label="/in/1"];
	"rrun1" -> "ddata2" [label="/out/1"];
	"rrun1" -> "ddata3" [label="/out/2"];
	"ddata2" -> "rrun2" [label="/in/2"];
	"ddata3" -> "rrun3" [label="/in/4"];
	"ddata4" -> "rrun2" [label="/in/3"];
	"rrun2" -> "ddata5" [label="/out/3"];
	"rrun3" -> "ddata6" [label="/out/4"];

}`,
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
					try.To(rfctime.ParseRFC3339DateTime("2024-04-01T21:34:56+09:00")).OrFatal(t).
						Time().Local().Format(rfctime.RFC3339DateTimeFormat),
				),
			},
		))
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
