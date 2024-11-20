package knitgraph

import (
	"fmt"
	"io"

	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/maps"
)

type NodeId string

type MountpointAddress struct {
	PlanId string
	Path   string
	Log    bool
}

type DirectedGraph struct {
	PlanNodes maps.Map[string, PlanNode]
	DataNodes maps.Map[string, DataNode]
	RunNodes  maps.Map[string, RunNode]
	RootNodes []RootNode

	// Edges
	Edges []Edge

	thunkPlan    map[string][]func(PlanNode) // key:planId
	thunkInput   map[MountpointAddress][]func(InputNode)
	thunkOutput  map[MountpointAddress][]func(OutputNode)
	thunkPlanLog map[string][]func(PlanLogNode) // key:planId

	thunkData map[string][]func(DataNode) // key:knitId

	thunkRun map[string][]func(RunNode) // key:runId
}

type Option func(*DirectedGraph)

func WithData(d data.Detail, style ...StyleOption) Option {
	return func(g *DirectedGraph) {
		g.AddDataNode(d, style...)
	}
}

func WithRun(r runs.Detail, style ...StyleOption) Option {
	return func(g *DirectedGraph) {
		g.AddRunNode(r, style...)
	}
}

func WithPlan(p plans.Detail, style ...StyleOption) Option {
	return func(g *DirectedGraph) {
		g.AddPlanNode(p, style...)
	}
}

type Style struct {
	Emphasize bool
}

type StyleOption func(*Style)

func Emphasize() StyleOption {
	return func(s *Style) {
		s.Emphasize = true
	}
}

func NewDirectedGraph(opt ...Option) *DirectedGraph {
	g := &DirectedGraph{
		RootNodes: []RootNode{},
		DataNodes: maps.NewOrderedMap[string, DataNode](),
		RunNodes:  maps.NewOrderedMap[string, RunNode](),
		PlanNodes: maps.NewOrderedMap[string, PlanNode](),
		Edges:     []Edge{},

		thunkData:    map[string][]func(DataNode){},
		thunkRun:     map[string][]func(RunNode){},
		thunkPlan:    map[string][]func(PlanNode){},
		thunkInput:   map[MountpointAddress][]func(InputNode){},
		thunkOutput:  map[MountpointAddress][]func(OutputNode){},
		thunkPlanLog: map[string][]func(PlanLogNode){},
	}

	for _, o := range opt {
		o(g)
	}

	return g
}

func (g *DirectedGraph) AddDataNode(data data.Detail, styles ...StyleOption) NodeId {
	style := Style{}
	for _, s := range styles {
		s(&style)
	}

	nodeId := NodeId("d" + data.KnitId)
	newNode := DataNode{NodeId: nodeId, Detail: data, Emphasize: style.Emphasize}
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
			if log := data.Upstream.Log; log != nil {
				g.thunkRun[upstreamId] = append(thunks, func(run RunNode) {
					g.addEdge(run.NodeId, newNode.NodeId, "(log)")
				})
			}

			if mp := data.Upstream.Mountpoint; mp != nil {
				g.thunkRun[upstreamId] = append(thunks, func(run RunNode) {
					g.addEdge(run.NodeId, newNode.NodeId, mp.Path)
				})
			}
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
			g.addEdge(newNode.NodeId, run.NodeId, ds.Mountpoint.Path)
		})
	}

	return newNode.NodeId
}

func (g *DirectedGraph) AddPlanNode(plan plans.Detail, styles ...StyleOption) NodeId {
	style := Style{}
	for _, s := range styles {
		s(&style)
	}

	nodeId := NodeId("p" + plan.PlanId)
	newNode := PlanNode{
		NodeId:      nodeId,
		Detail:      plan,
		InputNodes:  maps.NewOrderedMap[string, InputNode](),
		OutputNodes: maps.NewOrderedMap[string, OutputNode](),
		Emphasize:   style.Emphasize,
	}

	{
		// build PlanNode
		for _, in := range plan.Inputs {
			newNode.InputNodes.Set(in.Path, InputNode{
				NodeId: NodeId(fmt.Sprintf("p%s@%s", plan.PlanId, in.Path)),
				Input:  in,
			})
		}

		for _, out := range plan.Outputs {
			newNode.OutputNodes.Set(out.Path, OutputNode{
				NodeId: NodeId(fmt.Sprintf("p%s@%s", plan.PlanId, out.Path)),
				Output: out,
			})
		}

		if log := plan.Log; log != nil {
			newNode.LogNode = &PlanLogNode{
				NodeId: NodeId(fmt.Sprintf("p%s@log", plan.PlanId)),
				Log:    *log,
			}
		}
	}

	g.PlanNodes.Set(plan.PlanId, newNode)
	{
		// invoke thunks for each registered nodes (Plan, Input, Output, Log)
		for _, thunk := range g.thunkPlan[plan.PlanId] {
			thunk(newNode)
		}
		delete(g.thunkPlan, plan.PlanId)

		for _, in := range newNode.InputNodes.Iter() {
			mpaddr := MountpointAddress{PlanId: plan.PlanId, Path: in.Input.Path}
			for _, thunk := range g.thunkInput[mpaddr] {
				thunk(in)
			}
			delete(g.thunkInput, mpaddr)
		}

		for _, out := range newNode.OutputNodes.Iter() {
			mpaddr := MountpointAddress{PlanId: plan.PlanId, Path: out.Output.Path}
			for _, thunk := range g.thunkOutput[mpaddr] {
				thunk(out)
			}
			delete(g.thunkOutput, mpaddr)
		}

		if log := newNode.LogNode; log != nil {
			for _, thunk := range g.thunkPlanLog[plan.PlanId] {
				thunk(*log)
			}
			delete(g.thunkPlanLog, plan.PlanId)
		}
	}

	{
		// register thunks for each upstreams and downstreams

		for _, in := range newNode.InputNodes.Iter() {
			for _, ups := range in.Upstreams {
				if log := ups.Log; log != nil {
					if uPlan, ok := g.PlanNodes.Get(ups.Plan.PlanId); ok {
						if uLog := uPlan.LogNode; uLog != nil {
							continue
						}
					}

					thunks, ok := g.thunkPlanLog[ups.Plan.PlanId]
					if !ok {
						thunks = []func(PlanLogNode){}
					}

					g.thunkPlanLog[ups.Plan.PlanId] = append(thunks, func(log PlanLogNode) {
						g.addEdge(log.NodeId, in.NodeId, "")
					})
				} else if mp := ups.Mountpoint; mp != nil {
					mpaddr := MountpointAddress{PlanId: ups.Plan.PlanId, Path: mp.Path}

					if uPlan, ok := g.PlanNodes.Get(ups.Plan.PlanId); ok {
						if _, ok := uPlan.OutputNodes.Get(mpaddr.Path); ok {
							continue
						}
					}

					thunks, ok := g.thunkOutput[mpaddr]
					if !ok {
						thunks = []func(OutputNode){}
					}

					g.thunkOutput[mpaddr] = append(thunks, func(out OutputNode) {
						g.addEdge(out.NodeId, in.NodeId, "")
					})
				}
			}
		}

		for _, out := range newNode.OutputNodes.Iter() {
			for _, ds := range out.Downstreams {
				mpaddr := MountpointAddress{PlanId: ds.Plan.PlanId, Path: ds.Mountpoint.Path}

				if dPlan, ok := g.PlanNodes.Get(ds.Plan.PlanId); ok {
					if _, ok := dPlan.InputNodes.Get(mpaddr.Path); ok {
						continue
					}
				}

				thunks, ok := g.thunkInput[mpaddr]
				if !ok {
					thunks = []func(InputNode){}
				}
				g.thunkInput[mpaddr] = append(thunks, func(in InputNode) {
					g.addEdge(out.NodeId, in.NodeId, "")
				})
			}
		}

		if logNode := newNode.LogNode; logNode != nil {
			for _, ds := range logNode.Downstreams {
				mpaddr := MountpointAddress{PlanId: ds.Plan.PlanId, Path: ds.Mountpoint.Path}
				if _, ok := g.PlanNodes.Get(ds.Plan.PlanId); ok {
					continue
				}
				thunks, ok := g.thunkInput[mpaddr]
				if !ok {
					thunks = []func(InputNode){}
				}
				g.thunkInput[mpaddr] = append(thunks, func(in InputNode) {
					g.addEdge(logNode.NodeId, in.NodeId, "")
				})
			}
		}
	}

	return newNode.NodeId
}

func (g *DirectedGraph) AddRunNode(run runs.Detail, styles ...StyleOption) NodeId {
	style := Style{}
	for _, s := range styles {
		s(&style)
	}

	nodeId := NodeId("r" + run.RunId)
	newNode := RunNode{NodeId: nodeId, Detail: run, Emphasize: style.Emphasize}
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
		if _, ok := g.DataNodes.Get(out.KnitId); ok {
			continue
		}
		thunks, ok := g.thunkData[out.KnitId]
		if !ok {
			thunks = []func(DataNode){}
			g.thunkData[out.KnitId] = thunks
		}
		g.thunkData[out.KnitId] = append(thunks, func(data DataNode) {
			g.addEdge(newNode.NodeId, data.NodeId, out.Path)
		})
	}

	if log := run.Log; log != nil {
		if _, ok := g.DataNodes.Get(log.KnitId); !ok {
			thunks, ok := g.thunkData[log.KnitId]
			if !ok {
				thunks = []func(DataNode){}
				g.thunkData[log.KnitId] = thunks
			}
			g.thunkData[log.KnitId] = append(thunks, func(data DataNode) {
				g.addEdge(newNode.NodeId, data.NodeId, "(log)")
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

func (g *DirectedGraph) GenerateDotFromNodes(w io.Writer) error {
	for _, data := range g.DataNodes.Iter() {
		if err := data.ToDot(w); err != nil {
			return err
		}
	}
	for _, run := range g.RunNodes.Iter() {
		if err := run.ToDot(w); err != nil {
			return err
		}
	}
	for _, plan := range g.PlanNodes.Iter() {
		if err := plan.ToDot(w); err != nil {
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

func (g *DirectedGraph) GenerateDot(w io.Writer) error {
	_, err := w.Write([]byte(`digraph G {
	node [shape=record fontsize=10]
	edge [fontsize=10]

`))
	if err != nil {
		return err
	}

	err = g.GenerateDotFromNodes(w)
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
