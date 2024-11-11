package knitgraph

import (
	"fmt"
	"io"

	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/pkg/domain"
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
