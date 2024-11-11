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

type NodeId string

type DirectedGraph struct {
	DataNodes maps.Map[string, DataNode]
	RunNodes  maps.Map[string, RunNode]
	RootNodes []RootNode

	// Edges
	Edges []Edge

	thunkData map[string][]func(DataNode) // key:knitId
	thunkRun  map[string][]func(RunNode)  // key:runId
}

type Option func(*DirectedGraph)

func WithData(d ...data.Detail) Option {
	return func(g *DirectedGraph) {
		for _, _d := range d {
			g.AddDataNode(_d)
		}
	}
}

func WithRun(r ...runs.Detail) Option {
	return func(g *DirectedGraph) {
		for _, _r := range r {
			g.AddRunNode(_r)
		}
	}
}

func NewDirectedGraph(opt ...Option) *DirectedGraph {
	g := &DirectedGraph{
		RootNodes: []RootNode{},
		DataNodes: maps.NewOrderedMap[string, DataNode](),
		RunNodes:  maps.NewOrderedMap[string, RunNode](),
		Edges:     []Edge{},

		thunkData: map[string][]func(DataNode){},
		thunkRun:  map[string][]func(RunNode){},
	}

	for _, o := range opt {
		o(g)
	}

	return g
}

type RootNode struct {
	NodeId NodeId
}

func (r *RootNode) Equal(o *RootNode) bool {
	return r.NodeId == o.NodeId
}

func (r *RootNode) ToDot(w io.Writer) error {
	_, err := fmt.Fprintf(
		w,
		`	"%s"[shape=Mdiamond];
`,
		r.NodeId,
	)
	return err
}

type DataNode struct {
	NodeId NodeId
	data.Detail
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
		`	"%s"[
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
		d.NodeId,
		bgColor,
		html.EscapeString(knitId),
		subheader,
		strings.Join(userTags, "<BR/>"),
		// "d" is a prefix used to denote a data node in dot format.
	)
	return err

}

type RunNode struct {
	NodeId NodeId
	runs.Detail
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
		`	"%s"[
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
		r.NodeId,
		status,
		html.EscapeString(r.RunId),
		html.EscapeString(r.UpdatedAt.Time().Local().Format(rfctime.RFC3339DateTimeFormat)),
		html.EscapeString(title),
		// "r" is a prefix used to denote a run node in dot format.
	)
	return err
}

type Edge struct {
	FromId NodeId
	ToId   NodeId
	Label  string
}

func (e Edge) ToDot(w io.Writer) error {
	label := ""
	if e.Label != "" {
		label = fmt.Sprintf(` [label="%s"]`, e.Label)
	}

	_, err := fmt.Fprintf(
		w,
		`	"%s" -> "%s"%s;
`,
		e.FromId, e.ToId, label,
	)
	return err
}

func (g *DirectedGraph) AddDataNode(data data.Detail) NodeId {
	nodeId := NodeId("d" + data.KnitId)
	newNode := DataNode{NodeId: nodeId, Detail: data}
	g.DataNodes.Set(data.KnitId, newNode)

	for _, thunk := range g.thunkData[data.KnitId] {
		thunk(newNode)
	}
	delete(g.thunkData, data.KnitId)

	{
		upstreamId := data.Upstream.Run.RunId
		if _, ok := g.RunNodes.Get(upstreamId); !ok {
			thunks, ok := g.thunkRun[upstreamId]
			if !ok {
				thunks = []func(RunNode){}
				g.thunkRun[upstreamId] = thunks
			}
			g.thunkRun[upstreamId] = append(thunks, func(run RunNode) {
				if log := run.Log; log != nil && log.KnitId == data.KnitId {
					g.addEdge(run.NodeId, newNode.NodeId, "(log)")
					return
				}
				g.addEdge(run.NodeId, newNode.NodeId, data.Upstream.Path)
			})
		}
	}

	for _, ds := range data.Downstreams {
		downstreamId := ds.Run.RunId
		if _, ok := g.RunNodes.Get(downstreamId); ok {
			continue
		}
		thunks, ok := g.thunkRun[downstreamId]
		if !ok {
			thunks = []func(RunNode){}
			g.thunkRun[downstreamId] = thunks
		}
		g.thunkRun[downstreamId] = append(thunks, func(run RunNode) {
			g.addEdge(newNode.NodeId, run.NodeId, ds.Path)
		})
	}

	return newNode.NodeId
}

func (g *DirectedGraph) AddRunNode(run runs.Detail) NodeId {
	nodeId := NodeId("r" + run.RunId)
	newNode := RunNode{NodeId: nodeId, Detail: run}
	g.RunNodes.Set(run.RunId, newNode)

	for _, thunk := range g.thunkRun[run.RunId] {
		thunk(newNode)
	}
	delete(g.thunkRun, run.RunId)

	for _, in := range run.Inputs {
		if _, ok := g.DataNodes.Get(in.KnitId); ok {
			continue
		}
		thunks, ok := g.thunkData[in.KnitId]
		if !ok {
			thunks = []func(DataNode){}
			g.thunkData[in.KnitId] = thunks
		}
		g.thunkData[in.KnitId] = append(thunks, func(data DataNode) {
			g.addEdge(data.NodeId, newNode.NodeId, in.Path)
		})
	}

	for _, out := range run.Outputs {
		if _, ok := g.DataNodes.Get(out.KnitId); !ok {
			thunks, ok := g.thunkData[out.KnitId]
			if !ok {
				thunks = []func(DataNode){}
				g.thunkData[out.KnitId] = thunks
			}
			g.thunkData[out.KnitId] = append(thunks, func(data DataNode) {
				g.addEdge(newNode.NodeId, data.NodeId, out.Path)
			})
		}
	}

	if log := run.Log; log != nil {
		if _, ok := g.DataNodes.Get(log.KnitId); !ok {
			thunks, ok := g.thunkData[log.KnitId]
			if !ok {
				thunks = []func(DataNode){}
				g.thunkData[log.KnitId] = thunks
			}
			g.thunkData[log.KnitId] = append(thunks, func(data DataNode) {
				g.addEdge(data.NodeId, newNode.NodeId, "(log)")
			})
		}
	}

	if inputLen := len(run.Inputs); inputLen == 0 {
		rootId := g.addRootNode()
		g.addEdge(rootId, newNode.NodeId, "")
	}

	return newNode.NodeId
}

// This method assumes that the run nodes of the edge to be added are included in the graph.
func (g *DirectedGraph) addRootNode() NodeId {
	newRootId := NodeId(fmt.Sprintf("root#%d", len(g.RootNodes)))
	g.RootNodes = append(g.RootNodes, RootNode{NodeId: newRootId})
	return newRootId
}

func (g *DirectedGraph) addEdge(fromId, toId NodeId, label string) {
	g.Edges = append(g.Edges, Edge{FromId: fromId, ToId: toId, Label: label})
}

func (g *DirectedGraph) GenerateDotFromNodes(w io.Writer, argKnitId string) error {
	for _, data := range g.DataNodes.Iter() {
		if err := data.ToDot(w, argKnitId == data.KnitId); err != nil {
			return err
		}
	}
	for _, run := range g.RunNodes.Iter() {
		if err := run.ToDot(w); err != nil {
			return err
		}
	}
	for _, r := range g.RootNodes {
		if err := r.ToDot(w); err != nil {
			return err
		}
	}
	return nil
}

func (g DirectedGraph) GenerateDotFromEdges(w io.Writer) error {
	for _, e := range g.Edges {
		if err := e.ToDot(w); err != nil {
			return err
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
