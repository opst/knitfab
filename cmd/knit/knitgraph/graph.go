package knitgraph

import (
	"fmt"
	"html"
	"io"
	"strings"

	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/maps"
)

type DirectedGraph struct {
	DataNodes     maps.Map[string, DataNode]
	RunNodes      maps.Map[string, RunNode]
	RootNodes     []string          //to runId
	EdgesFromRun  map[string][]Edge //key:from runId, value:to knitId
	EdgesFromData map[string][]Edge //key:from knitId, value:to runId
}

func NewDirectedGraph() *DirectedGraph {
	return &DirectedGraph{
		DataNodes:     maps.NewOrderedMap[string, DataNode](),
		RunNodes:      maps.NewOrderedMap[string, RunNode](),
		RootNodes:     []string{},
		EdgesFromRun:  map[string][]Edge{},
		EdgesFromData: map[string][]Edge{},
	}
}

type DataNode struct {
	KnitId string
	Tags   []tags.Tag
	// FromRunId is coreresponding runId in Upstream of apidata.Detail.
	// ToRunIds is coreresponding runIds in Downstreams of apidata.Detail.
	// The reason for holding these two types of runIds in this node is related to the data tracking algorithm.
	// According to the algorithm's specifications, when data is first identified,
	// there may be runIds contained in that data that are not yet determined to be added to the graph.
	// The timing for confirming the addition is when the algorithm identifies that data again.
	FromRunId string
	ToRunIds  []string
}

func (d *DataNode) Equal(o *DataNode) bool {
	return d.KnitId == o.KnitId &&
		cmp.SliceContentEqWith(d.Tags, o.Tags, tags.Tag.Equal)
}

func (d *DataNode) ToDot(w io.Writer, isArgKnitId bool) error {
	knitId := d.KnitId

	systemtag := []string{}
	userTags := []string{}
	for _, tag := range d.Tags {
		switch tag.Key {
		case domain.KeyKnitTimestamp:
			tsp, err := rfctime.ParseRFC3339DateTime(tag.Value)
			if err != nil {
				return err
			}
			systemtag = append(
				systemtag,
				html.EscapeString(tsp.Time().Local().Format(rfctime.RFC3339DateTimeFormat)),
			)
			continue
		case domain.KeyKnitTransient:
			systemtag = append(systemtag, html.EscapeString(tag.String()))
			continue
		case tags.KeyKnitId:
			continue
		}

		userTags = append(
			userTags,
			fmt.Sprintf(`<B>%s</B>:%s`, html.EscapeString(tag.Key), html.EscapeString(tag.Value)),
		)
	}
	subheader := ""
	if len(systemtag) != 0 {
		subheader = fmt.Sprintf(
			`<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s</FONT></TD></TR>`,
			strings.Join(systemtag, " | "),
		)
	}

	//The background color of the data node that is the argument gets highlighted from the others.
	bgColor := map[bool]string{true: "#d4ecc6", false: "#FFFFFF"}[isArgKnitId]
	_, err := fmt.Fprintf(
		w,
		`	"d%s"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="%s">knit#id: %s</TD></TR>
				%s
				<TR><TD COLSPAN="2">%s</TD></TR>
			</TABLE>
		>
	];
`,
		knitId,
		bgColor,
		html.EscapeString(knitId),
		subheader,
		strings.Join(userTags, "<BR/>"),
		// "d" is a prefix used to denote a data node in dot format.
	)
	return err

}

type RunNode struct {
	runs.Summary
}

func (r *RunNode) ToDot(w io.Writer) error {
	title := ""
	if r.Plan.Image != nil {
		title = "image = " + r.Plan.Image.String()
	} else if r.Plan.Name != "" {
		title = r.Plan.Name
	}

	status := html.EscapeString(r.Status)
	switch domain.KnitRunStatus(r.Status) {
	case domain.Deactivated:
		status = fmt.Sprintf(`<FONT COLOR="gray"><B>%s</B></FONT>`, status)
	case domain.Completing, domain.Done:
		status = fmt.Sprintf(`<FONT COLOR="#007700"><B>%s</B></FONT>`, status)
	case domain.Aborting, domain.Failed:
		status = fmt.Sprintf(`<FONT COLOR="red"><B>%s</B></FONT>`, status)
	}

	_, err := fmt.Fprintf(
		w,
		`	"r%s"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD>%s</TD><TD>id: %s</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: %s</FONT></TD></TR>
				<TR><TD COLSPAN="3">%s</TD></TR>
			</TABLE>
		>
	];
`,
		r.RunId,
		status,
		html.EscapeString(r.RunId),
		html.EscapeString(r.UpdatedAt.Time().Local().Format(rfctime.RFC3339DateTimeFormat)),
		html.EscapeString(title),
		// "r" is a prefix used to denote a run node in dot format.
	)
	return err
}

type Edge struct {
	ToId  string
	Label string //mountpath
}

func (g *DirectedGraph) AddDataNode(data data.Detail) {
	g.DataNodes.Set(data.KnitId, DataNode{
		KnitId:    data.KnitId,
		Tags:      data.Tags,
		FromRunId: data.Upstream.Run.RunId,
		ToRunIds: func() []string {
			runIds := []string{}
			for _, ds := range data.Downstreams {
				runIds = append(runIds, ds.Run.RunId)
			}
			return runIds
		}(),
	})
}

func (g *DirectedGraph) AddRunNode(run runs.Detail) {
	g.RunNodes.Set(run.RunId, RunNode{
		Summary: run.Summary,
	})
}

// This method assumes that the run nodes of the edge to be added are included in the graph.
func (g *DirectedGraph) AddRootNode(runId string) {
	g.RootNodes = append(g.RootNodes, runId)
}

// This method assumes that the nodes of the edge to be added are included in the graph.
func (g *DirectedGraph) AddEdgeFromRun(runId string, knitId string, label string) {
	g.EdgesFromRun[runId] = append(g.EdgesFromRun[runId], Edge{ToId: knitId, Label: label})
}

// This method assumes that the nodes of the edge to be added are included in the graph.
func (g *DirectedGraph) AddEdgeFromData(knitId string, runId string, label string) {
	g.EdgesFromData[knitId] = append(g.EdgesFromData[knitId], Edge{ToId: runId, Label: label})
}

func (g *DirectedGraph) GenerateDotFromNodes(w io.Writer, argKnitId string) error {
	for knitId, data := range g.DataNodes.Iter() {
		if err := data.ToDot(w, argKnitId == knitId); err != nil {
			return err
		}
	}
	for _, run := range g.RunNodes.Iter() {
		if err := run.ToDot(w); err != nil {
			return err
		}
	}
	for i := range g.RootNodes {
		_, err := fmt.Fprintf(
			w,
			`	"root#%d"[shape=Mdiamond];
`,
			i)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g DirectedGraph) GenerateDotFromEdges(w io.Writer) error {
	err := writeEdgesToDot(w, g.DataNodes.Keys(), g.EdgesFromData, "d", "r")
	if err != nil {
		return err
	}

	err = writeEdgesToDot(w, g.RunNodes.Keys(), g.EdgesFromRun, "r", "d")
	if err != nil {
		return err
	}

	for i, runId := range g.RootNodes {
		_, err := fmt.Fprintf(
			w,
			`	"root#%d" -> "r%s";
`,
			i, runId,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeEdgesToDot(
	w io.Writer,
	keys []string,
	edgesMap map[string][]Edge,
	fromPrefix, toPrefix string,
) error {
	for _, id := range keys {
		if edges, ok := edgesMap[id]; ok {
			for _, edge := range edges {
				toId := edge.ToId
				_, err := fmt.Fprintf(
					w,
					`	"%s%s" -> "%s%s" [label="%s"];
`,
					fromPrefix, id,
					toPrefix, toId,
					edge.Label,
				)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (g *DirectedGraph) GenerateDot(w io.Writer, argKnitId string) error {
	_, err := w.Write([]byte(`digraph G {
	node [shape=record fontsize=10]
	edge [fontsize=10]

`))
	if err != nil {
		return err
	}

	err = g.GenerateDotFromNodes(w, argKnitId)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte("\n"))
	if err != nil {
		return err
	}

	err = g.GenerateDotFromEdges(w)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte("\n}"))
	if err != nil {
		return err
	}
	return nil
}

func ErrFindDataWithKnitId(knitId string, err error) error {
	return fmt.Errorf("%w: during searching data %s", err, knitId)
}

func ErrGetRunWithRunId(runId string, err error) error {
	return fmt.Errorf("%w: during searching run %s", err, runId)
}

func knitIdTag(knitId string) tags.Tag {
	return tags.Tag{
		Key:   domain.KeyKnitId,
		Value: knitId,
	}
}
