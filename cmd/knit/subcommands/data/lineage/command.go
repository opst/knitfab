package lineage

import (
	"context"
	"fmt"
	"log"

	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/knitgraph"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/args"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/slices"
	"github.com/youta-t/flarc"

	krst "github.com/opst/knitfab/cmd/knit/rest"
)

type Flag struct {
	Upstream   bool        `flag:"upstream" alias:"u" help:"Trace the upstream of the specified Data."`
	Downstream bool        `flag:"downstream" alias:"d" help:"Trace the downstream of the specified Data."`
	Numbers    *args.Depth `flag:"numbers" alias:"n" help:"Trace up to the specified depth. Trace to the upstream-most/downstream-most if 'all' is specified.,metavar=number of depth"`
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
	graph *knitgraph.DirectedGraph,
	knitId string,
	depth args.Depth,
) (*knitgraph.DirectedGraph, error)

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
			Numbers:    pointer.Ref(args.NewDepth(3)),
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
		if numbers.IsZero() {
			return fmt.Errorf("%w: --numbers must be a positive integer or 'all'", flarc.ErrUsage)
		}
		shoudUpstream := flags.Upstream
		shoudDownstream := flags.Downstream
		knitId := cl.Args()[ARG_KNITID][0]
		graph := knitgraph.NewDirectedGraph()

		// If both flags are not set, upstream and downstream will be traced.
		if !shoudUpstream && !shoudDownstream {
			shoudDownstream = true
			shoudUpstream = true
		}

		if shoudUpstream {
			_graph, err := option.ForUpstream(ctx, client, graph, knitId, *numbers)
			if err != nil {
				return err
			}
			graph = _graph
		}
		if shoudDownstream {
			_graph, err := option.ForDownstream(ctx, client, graph, knitId, *numbers)
			if err != nil {
				return err
			}
			graph = _graph
		}

		if err := graph.GenerateDot(cl.Stdout()); err != nil {
			return fmt.Errorf("fail to output dot format: %w", err)
		}
		logger.Println("success to output dot format")
		return nil
	}
}

var ErrNotFoundData = fmt.Errorf("data not found")

func errNotFoundData(knitId string) error {
	return fmt.Errorf("%w: %s", ErrNotFoundData, knitId)
}

func AddEdgeFromRunToLog(
	ctx context.Context,
	client krst.KnitClient,
	graph *knitgraph.DirectedGraph,
	run runs.Detail,
) (string, error) {

	log := run.Log

	if log == nil {
		return "", nil
	}

	if _, ok := graph.DataNodes.Get(log.KnitId); ok {
		return log.KnitId, nil
	} else {
		//If the data node does not exist in the graph, add that data node before adding the edge.
		logData, err := getData(ctx, client, log.KnitId)
		if err != nil {
			return "", err
		}
		graph.AddDataNode(logData)
		return log.KnitId, nil
	}
}

// Trace the downstream data lineage.
func TraceDownStream(
	ctx context.Context,
	client krst.KnitClient,
	graph *knitgraph.DirectedGraph,
	rootKnitId string,
	maxDepth args.Depth,
) (*knitgraph.DirectedGraph, error) {

	startKnitIds := []string{rootKnitId}
	depth := uint(0)

	for (maxDepth.IsInfinity() || depth < maxDepth.Value()) && 0 < len(startKnitIds) {
		var err error
		graph, startKnitIds, err = TraceDownstreamOneStep(ctx, client, graph, startKnitIds, depth == 0)
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
	graph *knitgraph.DirectedGraph,
	knitIds []string,
	emphasize bool,
) (*knitgraph.DirectedGraph, []string, error) {

	TotalnextKnitIds := []string{}

	for _, knitId := range knitIds {
		var err error
		var nextKnitIds []string
		graph, nextKnitIds, err = TraceDownstreamForSingleNode(ctx, client, graph, knitId, emphasize)
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
	graph *knitgraph.DirectedGraph,
	knitId string,
	emphasize bool,
) (*knitgraph.DirectedGraph, []string, error) {

	//1. If the argument's d does not exist in the graph, add that Data to the graph.
	d, ok := graph.DataNodes.Get(knitId)
	if !ok {
		_data, err := getData(ctx, client, knitId)
		if err != nil {
			return graph, []string{}, err
		}
		styles := []knitgraph.StyleOption{}
		if emphasize {
			styles = append(styles, knitgraph.Emphasize())
		}
		graph.AddDataNode(_data, styles...)
		d, _ = graph.DataNodes.Get(knitId)
	}

	nextKnitIds := []string{}
	//2-1. trace the run node where that data is input.
	for _, toRunId := range slices.Map(
		d.Downstreams,
		func(a data.AssignedTo) string { return a.Run.RunId },
	) {
		if _, ok := graph.RunNodes.Get(toRunId); ok {
			continue
		}
		//If the run does not exist in the graph, add it to the graph.
		run, err := client.GetRun(ctx, toRunId)
		if err != nil {
			return graph, []string{}, knitgraph.ErrGetRunWithRunId(toRunId, err)
		}
		graph.AddRunNode(run)

		//2-2. Add the edges from the data that serve as the input to that run.
		for _, in := range run.Inputs {
			//If the data node does not exist in the graph, add that data node before adding the edge.
			if _, ok := graph.DataNodes.Get(in.KnitId); ok {
				continue
			}
			otherData, err := getData(ctx, client, in.KnitId)
			if err != nil {
				return graph, []string{}, err
			}
			graph.AddDataNode(otherData)
		}
		//2-3. Add the edges from that run to the data that serve as the output.
		for _, out := range run.Outputs {
			if _, ok := graph.DataNodes.Get(out.KnitId); ok {

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
	graph *knitgraph.DirectedGraph,
	rootKnitId string,
	maxDepth args.Depth,
) (*knitgraph.DirectedGraph, error) {

	startKnitIds := []string{rootKnitId}
	depth := uint(0)

	for (maxDepth.IsInfinity() || depth < maxDepth.Value()) && 0 < len(startKnitIds) {
		var err error
		graph, startKnitIds, err = TraceUpstreamOneStep(ctx, client, graph, startKnitIds, depth == 0)
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
	graph *knitgraph.DirectedGraph,
	knitIds []string,
	entry bool,
) (*knitgraph.DirectedGraph, []string, error) {

	TotalnextKnitIds := []string{}

	for _, knitId := range knitIds {
		var err error
		var nextKnitIds []string
		graph, nextKnitIds, err = TraceUpstreamForSingleNode(ctx, client, graph, knitId, entry)
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
	graph *knitgraph.DirectedGraph,
	knitId string,
	entry bool,
) (*knitgraph.DirectedGraph, []string, error) {

	///1. If the argument's d does not exist in the graph, add that d　to the graph.
	d, ok := graph.DataNodes.Get(knitId)
	if !ok {
		_data, err := getData(ctx, client, knitId)
		if err != nil {
			return graph, []string{}, err
		}
		styles := []knitgraph.StyleOption{}
		if entry {
			styles = append(styles, knitgraph.Emphasize())
		}
		graph.AddDataNode(_data, styles...)
		d, _ = graph.DataNodes.Get(knitId)
	}

	nextKnitIds := []string{}
	//2-1. trace the run node where that data is output.
	fromRunId := d.Upstream.Run.RunId
	if _, ok := graph.RunNodes.Get(fromRunId); !ok {
		//If the run does not exist in the graph, add it to the graph.
		run, err := client.GetRun(ctx, fromRunId)
		if err != nil {
			return graph, []string{}, knitgraph.ErrGetRunWithRunId(fromRunId, err)
		}
		graph.AddRunNode(run)

		//2-2. Add the edges from that run to the data that serve as the output.
		for _, out := range run.Outputs {
			if _, ok := graph.DataNodes.Get(out.KnitId); ok {
				continue
			}
			//If the data node does not exist in the graph, add that data node before adding the edge.
			otherData, err := getData(ctx, client, out.KnitId)
			if err != nil {
				return graph, []string{}, err
			}
			graph.AddDataNode(otherData)
		}

		//2-3. Add the edges from the run to the data that serves as its log.
		_, err = AddEdgeFromRunToLog(ctx, client, graph, run)
		if err != nil {
			return graph, []string{}, err
		}

		//2-4. Add the edges from the data that serve as the output to that run.
		for _, in := range run.Inputs {
			if _, ok := graph.DataNodes.Get(in.KnitId); ok {
				//Hold the data that will be the next argument.
				nextKnitIds = append(nextKnitIds, in.KnitId)
				continue
			}
			inputData, err := getData(ctx, client, in.KnitId)
			if err != nil {
				return graph, []string{}, err
			}
			graph.AddDataNode(inputData)

			//Hold the data that will be the next argument.
			nextKnitIds = append(nextKnitIds, inputData.KnitId)
		}
	}
	return graph, nextKnitIds, nil
}

func getData(ctx context.Context, client krst.KnitClient, knitId string) (data.Detail, error) {
	datas, err := client.FindData(ctx, []tags.Tag{knitIdTag(knitId)}, nil, nil)
	if err != nil {
		return data.Detail{}, knitgraph.ErrFindDataWithKnitId(knitId, err)
	}
	if len(datas) == 0 {
		return data.Detail{}, errNotFoundData(knitId)
	}
	return datas[0], nil
}

func knitIdTag(knitId string) tags.Tag {
	return tags.Tag{
		Key:   domain.KeyKnitId,
		Value: knitId,
	}
}
