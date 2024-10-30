package lineage

import (
	"context"
	"fmt"
	"html"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/youta-t/flarc"

	krst "github.com/opst/knitfab/cmd/knit/rest"
)

type Flag struct {
	Upstream   bool   `flag:"upstream" alias:"u" help:"Trace the upstream of the specified Data."`
	Downstream bool   `flag:"downstream" alias:"d" help:"Trace the downstream of the specified Data."`
	Numbers    string `flag:"numbers" alias:"n" help:"Trace up to the specified depth. Trace to the upstream-most/downstream-most if 'all' is specified.,metavar=number of depth"`
}

type Option struct {
	Traverser Traverser
}

type Traverser struct {
	ForUpstream   Runner
	ForDownstream Runner
}

type Runner func(
	ctx context.Context,
	client krst.KnitClient,
	graph *DirectedGraph,
	knitId string,
	depth int,
) (*DirectedGraph, error)

var _ Runner = TraceUpStream
var _ Runner = TraceDownStream

func WithTraverser(trav Traverser) func(*Option) *Option {
	return func(opt *Option) *Option {
		opt.Traverser = trav
		return opt
	}
}

const ARG_KNITID = "KNIT_ID"

func New(
	options ...func(*Option) *Option,
) (flarc.Command, error) {
	opt := &Option{
		Traverser: Traverser{
			ForUpstream:   TraceUpStream,
			ForDownstream: TraceDownStream,
		},
	}
	for _, o := range options {
		opt = o(opt)
	}

	return flarc.NewCommand(
		"Output the result of tracing the Data Lineage as dot format.",
		Flag{
			Upstream:   false,
			Downstream: false,
			Numbers:    "3",
		},
		flarc.Args{
			{
				Name: ARG_KNITID, Required: true,
				Help: "Specify the Knit Id of Data you want to trace.",
			},
		},
		common.NewTask(Task(opt.Traverser)),
		flarc.WithDescription(
			`
This command traces the Data Lineage of the specified Data
and outputs the result in dot format (graphviz).
You can specify the depth of the trace as a natural number,
and choose whether to trace upstream, downstream, or both.

Example
-------

- Trace the upstream:

	{{ .Command }} -u KNIT_ID

	(If the -n flag is not set, trace up to 3 depth.)

- Trace the downstream:

	{{ .Command }} -d KNIT_ID

- Trace both upstream and downstream:

	{{ .Command }} -u -d KNIT_ID
	{{ .Command }} KNIT_ID

	(both above are equivalent)

- Trace up to the specified depth:

	{{ .Command }} -n number KNIT_ID

- Trace the upstream to the upstream-most (root):

	{{ .Command }} -u -n all KNIT_ID

- Trace the downstream to the downstream-most:

	{{ .Command }} -d -n all KNIT_ID

- Generate the traced result as a dot file:
	{{ .Command }} KNIT_ID > graph.dot
`,
		),
	)
}

func Task(option Traverser) common.Task[Flag] {
	return func(
		ctx context.Context,
		logger *log.Logger,
		knitEnv kenv.KnitEnv,
		client krst.KnitClient,
		cl flarc.Commandline[Flag],
		params []any,
	) error {
		flags := cl.Flags()
		numbers := flags.Numbers
		var depth int
		var err error

		if numbers == "all" {
			depth = -1
		} else {
			depth, err = strconv.Atoi(numbers)
			if err != nil {
				return fmt.Errorf("%w: invalid number %s", flarc.ErrUsage, numbers)
			}
			if depth <= 0 {
				return fmt.Errorf("%w: depth should be natural number", flarc.ErrUsage)
			}
		}

		shoudUpstream := flags.Upstream
		shoudDownstream := flags.Downstream
		knitId := cl.Args()[ARG_KNITID][0]
		graph := NewDirectedGraph()

		// If both flags are not set, upstream and downstream will be traced.
		if !shoudUpstream && !shoudDownstream {
			shoudDownstream = true
			shoudUpstream = true
		}

		if shoudUpstream {
			graph, err = option.ForUpstream(ctx, client, graph, knitId, depth)
			if err != nil {
				return err
			}
		}
		if shoudDownstream {
			graph, err = option.ForDownstream(ctx, client, graph, knitId, depth)
			if err != nil {
				return err
			}
		}

		if err := graph.GenerateDot(cl.Stdout(), knitId); err != nil {
			return fmt.Errorf("fail to output dot format: %w", err)
		}
		logger.Println("success to output dot format")
		return nil
	}
}

type DirectedGraph struct {
	DataNodes     map[string]DataNode
	RunNodes      map[string]RunNode
	RootNodes     []string          //to runId
	EdgesFromRun  map[string][]Edge //key:from runId, value:to knitId
	EdgesFromData map[string][]Edge //key:from knitId, value:to runId
	//The order of these array becomes the order of description when outputting to the dot format.
	KeysDataNode []string
	KeysRunNode  []string
}

func NewDirectedGraph() *DirectedGraph {
	return &DirectedGraph{
		DataNodes:     map[string]DataNode{},
		RunNodes:      map[string]RunNode{},
		RootNodes:     []string{},
		EdgesFromRun:  map[string][]Edge{},
		EdgesFromData: map[string][]Edge{},
		KeysDataNode:  make([]string, 0),
		KeysRunNode:   make([]string, 0),
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
		case kdb.KeyKnitTimestamp:
			tsp, err := rfctime.ParseRFC3339DateTime(tag.Value)
			if err != nil {
				return err
			}
			systemtag = append(
				systemtag,
				html.EscapeString(tsp.Time().Local().Format(rfctime.RFC3339DateTimeFormat)),
			)
			continue
		case kdb.KeyKnitTransient:
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
	switch kdb.KnitRunStatus(r.Status) {
	case kdb.Deactivated:
		status = fmt.Sprintf(`<FONT COLOR="gray"><B>%s</B></FONT>`, status)
	case kdb.Completing, kdb.Done:
		status = fmt.Sprintf(`<FONT COLOR="#007700"><B>%s</B></FONT>`, status)
	case kdb.Aborting, kdb.Failed:
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
	g.DataNodes[data.KnitId] = DataNode{
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
	}
	g.KeysDataNode = append(g.KeysDataNode, data.KnitId)
}

func (g *DirectedGraph) AddRunNode(run runs.Detail) {
	g.RunNodes[run.RunId] = RunNode{
		Summary: run.Summary,
	}
	g.KeysRunNode = append(g.KeysRunNode, run.RunId)
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
	for _, knitId := range g.KeysDataNode {
		if data, ok := g.DataNodes[knitId]; ok {
			if err := data.ToDot(w, argKnitId == knitId); err != nil {
				return err
			}
		}
	}
	for _, runId := range g.KeysRunNode {
		if run, ok := g.RunNodes[runId]; ok {
			if err := run.ToDot(w); err != nil {
				return err
			}
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
	err := writeEdgesToDot(w, g.KeysDataNode, g.EdgesFromData, "d", "r")
	if err != nil {
		return err
	}

	err = writeEdgesToDot(w, g.KeysRunNode, g.EdgesFromRun, "r", "d")
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
	fromPrefix, toPrefix string) error {
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

var ErrNotFoundData = fmt.Errorf("data not found")

func errNotFoundData(knitId string) error {
	return fmt.Errorf("%w: %s", ErrNotFoundData, knitId)
}

func AddEdgeFromRunToLog(
	ctx context.Context,
	client krst.KnitClient,
	graph *DirectedGraph,
	run runs.Detail,
) (string, error) {

	log := run.Log

	if log == nil {
		return "", nil
	}

	if _, ok := graph.DataNodes[log.KnitId]; ok {
		graph.AddEdgeFromRun(run.RunId, log.KnitId, "(log)")
		return log.KnitId, nil
	} else {
		//If the data node does not exist in the graph, add that data node before adding the edge.
		logData, err := getData(ctx, client, log.KnitId)
		if err != nil {
			return "", err
		}
		graph.AddDataNode(logData)
		graph.AddEdgeFromRun(run.RunId, logData.KnitId, "(log)")
		return log.KnitId, nil
	}
}

// Trace the downstream data lineage.
func TraceDownStream(
	ctx context.Context,
	client krst.KnitClient,
	graph *DirectedGraph,
	rootKnitId string,
	maxDepth int,
) (*DirectedGraph, error) {

	startKnitIds := []string{rootKnitId}
	depth := 0

	for (maxDepth == -1 || depth < maxDepth) && 0 < len(startKnitIds) {
		var err error
		graph, startKnitIds, err = TraceDownstreamOneStep(ctx, client, graph, startKnitIds)
		if err != nil {
			return graph, err
		}
		depth++
	}
	return graph, nil
}

// Trace the downstream data lineage for just one depth.
func TraceDownstreamOneStep(
	ctx context.Context,
	client krst.KnitClient,
	graph *DirectedGraph,
	knitIds []string,
) (*DirectedGraph, []string, error) {

	TotalnextKnitIds := []string{}

	for _, knitId := range knitIds {
		var err error
		var nextKnitIds []string
		graph, nextKnitIds, err = TraceDownstreamForSingleNode(ctx, client, graph, knitId)
		if err != nil {
			return graph, []string{}, err
		}
		TotalnextKnitIds = append(TotalnextKnitIds, nextKnitIds...)
	}
	return graph, TotalnextKnitIds, nil
}

// Trace the downstream data lineage for just one depth for a single data.
func TraceDownstreamForSingleNode(
	ctx context.Context,
	client krst.KnitClient,
	graph *DirectedGraph,
	knitId string,
) (*DirectedGraph, []string, error) {

	//1. If the argument's data does not exist in the graph, add that data　to the graph.
	if _, ok := graph.DataNodes[knitId]; !ok {
		data, err := getData(ctx, client, knitId)
		if err != nil {
			return graph, []string{}, err
		}
		graph.AddDataNode(data)
	}

	nextKnitIds := []string{}
	//2-1. trace the run node where that data is input.
	for _, toRunId := range graph.DataNodes[knitId].ToRunIds {
		if _, ok := graph.RunNodes[toRunId]; ok {
			continue
		}
		//If the run does not exist in the graph, add it to the graph.
		run, err := client.GetRun(ctx, toRunId)
		if err != nil {
			return graph, []string{}, ErrGetRunWithRunId(toRunId, err)
		}
		graph.AddRunNode(run)

		//2-2. Add the edges from the data that serve as the input to that run.
		for _, in := range run.Inputs {
			if _, ok := graph.DataNodes[in.KnitId]; ok {
				graph.AddEdgeFromData(in.KnitId, run.RunId, in.Mountpoint.Path)
				continue
			}
			//If the data node does not exist in the graph, add that data node before adding the edge.
			otherData, err := getData(ctx, client, in.KnitId)
			if err != nil {
				return graph, []string{}, err
			}
			graph.AddDataNode(otherData)
			graph.AddEdgeFromData(otherData.KnitId, run.RunId, in.Mountpoint.Path)
		}
		//2-3. Add the edges from that run to the data that serve as the output.
		for _, out := range run.Outputs {
			if _, ok := graph.DataNodes[out.KnitId]; ok {
				graph.AddEdgeFromRun(run.RunId, out.KnitId, out.Mountpoint.Path)

				//Hold the data that will be the next argument.
				nextKnitIds = append(nextKnitIds, out.KnitId)
				continue
			}
			//If the data node does not exist in the graph, add that data node before adding the edge.
			outputData, err := getData(ctx, client, out.KnitId)
			if err != nil {
				return graph, []string{}, err
			}
			graph.AddDataNode(outputData)
			graph.AddEdgeFromRun(run.RunId, outputData.KnitId, out.Mountpoint.Path)

			//Hold the data that will be the next argument.
			nextKnitIds = append(nextKnitIds, outputData.KnitId)
		}

		//2-4. Add the edges from the run to the data that serves as its log.
		logKnitId, err := AddEdgeFromRunToLog(ctx, client, graph, run)
		if err != nil {
			return graph, []string{}, err
		}
		if logKnitId != "" {
			nextKnitIds = append(nextKnitIds, logKnitId)
		}
	}
	return graph, nextKnitIds, nil
}

// Trace the upstream data lineage.
func TraceUpStream(
	ctx context.Context,
	client krst.KnitClient,
	graph *DirectedGraph,
	rootKnitId string,
	maxDepth int,
) (*DirectedGraph, error) {

	startKnitIds := []string{rootKnitId}
	depth := 0

	for (maxDepth == -1 || depth < maxDepth) && 0 < len(startKnitIds) {
		var err error
		graph, startKnitIds, err = TraceUpstreamOneStep(ctx, client, graph, startKnitIds)
		if err != nil {
			return graph, err
		}
		depth++
	}
	return graph, nil
}

// Trace the upstream data lineage for just one depth.
func TraceUpstreamOneStep(
	ctx context.Context,
	client krst.KnitClient,
	graph *DirectedGraph,
	knitIds []string,
) (*DirectedGraph, []string, error) {

	TotalnextKnitIds := []string{}

	for _, knitId := range knitIds {
		var err error
		var nextKnitIds []string
		graph, nextKnitIds, err = TraceUpstreamForSingleNode(ctx, client, graph, knitId)
		if err != nil {
			return graph, []string{}, err
		}
		TotalnextKnitIds = append(TotalnextKnitIds, nextKnitIds...)
	}
	return graph, TotalnextKnitIds, nil
}

// Trace the upstream data lineage for just one depth for a single data.
func TraceUpstreamForSingleNode(
	ctx context.Context,
	client krst.KnitClient,
	graph *DirectedGraph,
	knitId string,
) (*DirectedGraph, []string, error) {

	///1. If the argument's data does not exist in the graph, add that data　to the graph.
	if _, ok := graph.DataNodes[knitId]; !ok {
		data, err := getData(ctx, client, knitId)
		if err != nil {
			return graph, []string{}, err
		}
		graph.AddDataNode(data)
	}

	nextKnitIds := []string{}
	//2-1. trace the run node where that data is output.
	fromRunId := graph.DataNodes[knitId].FromRunId
	if _, ok := graph.RunNodes[fromRunId]; ok {
	} else {
		//If the run does not exist in the graph, add it to the graph.
		run, err := client.GetRun(ctx, fromRunId)
		if err != nil {
			return graph, []string{}, ErrGetRunWithRunId(fromRunId, err)
		}
		graph.AddRunNode(run)

		//2-2. Add the edges from that run to the data that serve as the output.
		for _, out := range run.Outputs {
			if _, ok := graph.DataNodes[out.KnitId]; ok {
				graph.AddEdgeFromRun(run.RunId, out.KnitId, out.Mountpoint.Path)
				continue
			}
			//If the data node does not exist in the graph, add that data node before adding the edge.
			otherData, err := getData(ctx, client, out.KnitId)
			if err != nil {
				return graph, []string{}, err
			}
			graph.AddDataNode(otherData)
			graph.AddEdgeFromRun(run.RunId, otherData.KnitId, out.Mountpoint.Path)
		}

		//2-3. Add the edges from the run to the data that serves as its log.
		_, err = AddEdgeFromRunToLog(ctx, client, graph, run)
		if err != nil {
			return graph, []string{}, err
		}

		//2-4. Add the edges from the data that serve as the output to that run.
		if len(run.Inputs) == 0 {
			//Since a run without an Input is a PeudoRun, add the root node for that run to the graph.
			graph.AddRootNode(run.RunId)
		} else {
			for _, in := range run.Inputs {
				if _, ok := graph.DataNodes[in.KnitId]; ok {
					graph.AddEdgeFromData(in.KnitId, run.RunId, in.Mountpoint.Path)
					//Hold the data that will be the next argument.
					nextKnitIds = append(nextKnitIds, in.KnitId)
					continue
				}
				inputData, err := getData(ctx, client, in.KnitId)
				if err != nil {
					return graph, []string{}, err
				}
				graph.AddDataNode(inputData)
				graph.AddEdgeFromData(inputData.KnitId, run.RunId, in.Mountpoint.Path)

				//Hold the data that will be the next argument.
				nextKnitIds = append(nextKnitIds, inputData.KnitId)
			}
		}
	}
	return graph, nextKnitIds, nil
}

func getData(ctx context.Context, client krst.KnitClient, knitId string) (data.Detail, error) {
	datas, err := client.FindData(ctx, []tags.Tag{knitIdTag(knitId)}, nil, nil)
	if err != nil {
		return data.Detail{}, ErrFindDataWithKnitId(knitId, err)
	}
	if len(datas) == 0 {
		return data.Detail{}, errNotFoundData(knitId)
	}
	return datas[0], nil
}

func knitIdTag(knitId string) tags.Tag {
	return tags.Tag{
		Key:   kdb.KeyKnitId,
		Value: knitId,
	}
}
