package template

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"strings"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	apiplans "github.com/opst/knitfab/pkg/api/types/plans"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"github.com/opst/knitfab/pkg/images/analyzer"
	"github.com/opst/knitfab/pkg/utils"
	y "github.com/opst/knitfab/pkg/utils/yamler"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/resource"
)

type Command struct {
	taskFromScratch func(context.Context, *log.Logger, string, env.KnitEnv) (apiplans.PlanSpec, error)
	taskFromImage   func(context.Context, *log.Logger, namedReader, string, env.KnitEnv) (apiplans.PlanSpec, error)
}

func WithTask(
	fromScratch func(context.Context, *log.Logger, string, env.KnitEnv) (apiplans.PlanSpec, error),
	fromImage func(context.Context, *log.Logger, namedReader, string, env.KnitEnv) (apiplans.PlanSpec, error),
) func(*Command) *Command {
	return func(cmd *Command) *Command {
		cmd.taskFromScratch = fromScratch
		cmd.taskFromImage = fromImage
		return cmd
	}
}

func New(options ...func(*Command) *Command) kcmd.KnitCommand[Flag] {
	return utils.ApplyAll(
		&Command{
			taskFromScratch: FromScratch(),
			taskFromImage:   FromImage(analyzer.Analyze),
		},
		options...,
	)
}

func (cmd *Command) Name() string {
	return "template"
}

func (cmd *Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "Generate a new Plan definition from a container image.",
		Example: `
Generate a Plan file from "docker save".

	docker save image:tag | {{ .Command }} > plan.yaml

Generate a Plan file from a container image file.

	docker save image:tag > image.tar
	{{ .Command }} -i image.tar > plan.yaml

You may need to specify image:tag explicitly when the image has multiple tags, like below:

	{{ .Command }} -i image-with-multiple-tag.tar image:tag > plan.yaml
`,
	}
}

type Flag struct {
	Scratch bool   `flag:",help=Generate a Plan file without reading any image."`
	Input   string `flag:",short=i,metavar=path/to/image.tar,help=Tar file containing image (for example: output of 'docker save') to be used for the Plan."`
}

const (
	ARG_IMAGE_TAG = "image:tag"
)

func (*Command) Usage() usage.Usage[Flag] {
	return usage.New(
		Flag{Input: "-"},
		usage.Args{
			{
				Name: ARG_IMAGE_TAG, Required: false,
				Help: fmt.Sprintf(`
Specify the image tag to use for the Plan.
This is optional when the image has just one tag.

If --scratch is given, %s is prohibited.`,
					ARG_IMAGE_TAG,
				),
			},
		},
	)
}

func (cmd *Command) Execute(
	ctx context.Context,
	l *log.Logger,
	e env.KnitEnv,
	_ krst.KnitClient,
	flags usage.FlagSet[Flag],
) error {

	var plan apiplans.PlanSpec
	if flags.Flags.Scratch {
		image := "image:version"
		if l := len(flags.Args[ARG_IMAGE_TAG]); 0 < l {
			return fmt.Errorf(
				"%w: image:tag and --scratch are exclusive", kcmd.ErrUsage,
			)
		}

		spec, err := cmd.taskFromScratch(ctx, l, image, e)
		if err != nil {
			return fmt.Errorf("can not generate Plan file: %w", err)
		}
		plan = spec
	} else {
		imageTag := ""
		if 0 < len(flags.Args[ARG_IMAGE_TAG]) {
			imageTag = flags.Args[ARG_IMAGE_TAG][0]
		}

		source := os.Stdin
		if flags.Flags.Input != "-" {
			f, err := os.Open(flags.Flags.Input)
			if err != nil {
				return fmt.Errorf(
					"cannot open input file: %s: %w", flags.Flags.Input, err,
				)
			}
			defer f.Close()
			source = f
		}

		spec, err := cmd.taskFromImage(ctx, l, source, imageTag, e)
		if err != nil {
			return fmt.Errorf("failed to generate Plan file: %w", err)
		}
		plan = spec
	}

	active := true
	if plan.Active != nil {
		active = *plan.Active
	}

	res := map[string]string{}
	for k, v := range plan.Resources {
		res[k] = v.String()
	}

	yplan := planSpecWithDocument{
		Image:    image(plan.Image),
		Inputs:   utils.Map(plan.Inputs, func(i apiplans.Mountpoint) mountpoint { return mountpoint(i) }),
		Outputs:  utils.Map(plan.Outputs, func(i apiplans.Mountpoint) mountpoint { return mountpoint(i) }),
		Log:      (*logpoint)(plan.Log),
		Resource: res,
		Active:   active,
	}

	os.Stdout.WriteString("\n")
	enc := yaml.NewEncoder(os.Stdout)
	defer enc.Close()
	enc.SetIndent(2)
	if err := enc.Encode(yplan); err != nil {
		return fmt.Errorf("cannot write Plan file: %w", err)
	}
	os.Stdout.WriteString("\n")
	l.Println("# Plan file is generated. modify it as you like.")
	return nil

}

func FromScratch() func(context.Context, *log.Logger, string, env.KnitEnv) (apiplans.PlanSpec, error) {
	return func(
		ctx context.Context,
		l *log.Logger,
		tag string,
		env env.KnitEnv,
	) (apiplans.PlanSpec, error) {
		image := new(apiplans.Image)
		if err := image.Parse(tag); err != nil {
			return apiplans.PlanSpec{}, err
		}
		tags := env.Tags()
		if len(tags) == 0 {
			tags = []apitag.Tag{{Key: "example", Value: "tag"}}
		}

		ress := apiplans.Resources{}
		for k, v := range env.Resource {
			q, err := resource.ParseQuantity(v)
			if err != nil {
				return apiplans.PlanSpec{}, fmt.Errorf("invalid resource value: %w", err)
			}
			ress[k] = q
		}
		if _, ok := ress["cpu"]; !ok {
			ress["cpu"] = resource.MustParse("1")
		}
		if _, ok := ress["memory"]; !ok {
			ress["memory"] = resource.MustParse("1Gi")
		}

		result := apiplans.PlanSpec{
			Image: *image,
			Inputs: []apiplans.Mountpoint{
				{Path: "/in", Tags: tags},
			},
			Outputs: []apiplans.Mountpoint{
				{Path: "/out", Tags: tags},
			},
			Resources: ress,
			Log: &apiplans.LogPoint{Tags: append([]apitag.Tag{
				{Key: "type", Value: "log"}}, tags...,
			)},
		}

		return result, nil
	}
}

func FromImage(
	analyze func(io.Reader, ...analyzer.Option) (*analyzer.TaggedConfig, error),
) func(context.Context, *log.Logger, namedReader, string, env.KnitEnv) (apiplans.PlanSpec, error) {
	return func(
		ctx context.Context,
		l *log.Logger,
		source namedReader,
		tag string,
		env env.KnitEnv,
	) (apiplans.PlanSpec, error) {
		options := []analyzer.Option{}
		if tag != "" {
			options = append(options, analyzer.WithTag(tag))
		}

		l.Printf(`...analyzing image from "%s"`, source.Name())
		cfg, err := analyze(source, options...)
		if err != nil {
			return apiplans.PlanSpec{}, err
		}

		inputs := map[string]struct{}{}
		outputs := map[string]struct{}{}

		wd := cfg.Config.WorkingDir
		if wd == "" {
			wd = "/"
		}

		for _, e := range cfg.Config.Entrypoint {
			p := e
			if !path.IsAbs(p) {
				p = path.Join(wd, p)
			}
			p = path.Clean(p)
			if _INPUT.MatchString(e) {
				inputs[p] = struct{}{}
			} else if _OUTPUT.MatchString(e) {
				outputs[p] = struct{}{}
			}
		}

		for _, c := range cfg.Config.Cmd {
			p := c
			if !path.IsAbs(p) {
				p = path.Join(wd, p)
			}
			p = path.Clean(p)
			if _INPUT.MatchString(c) {
				inputs[p] = struct{}{}
			} else if _OUTPUT.MatchString(c) {
				outputs[p] = struct{}{}
			}
		}

		for v := range cfg.Config.Volumes {
			p := v
			if !path.IsAbs(p) {
				p = path.Join(wd, p)
			}
			p = path.Clean(p)
			if _INPUT.MatchString(v) {
				inputs[p] = struct{}{}
			} else if _OUTPUT.MatchString(v) {
				outputs[p] = struct{}{}
			}
		}

		ress := apiplans.Resources{}
		for k, v := range env.Resource {
			q, err := resource.ParseQuantity(v)
			if err != nil {
				return apiplans.PlanSpec{}, fmt.Errorf("invalid resource value: %w", err)
			}
			ress[k] = q
		}
		if _, ok := ress["cpu"]; !ok {
			ress["cpu"] = resource.MustParse("1")
		}
		if _, ok := ress["memory"]; !ok {
			ress["memory"] = resource.MustParse("1Gi")
		}

		result := apiplans.PlanSpec{
			Image: apiplans.Image{
				Repository: cfg.Tag.Repository.Name(),
				Tag:        cfg.Tag.TagStr(),
			},
			Inputs: utils.Map(
				utils.KeysOf(inputs), mountpointBuilder("in", env.Tags()),
			),
			Outputs: utils.Map(
				utils.KeysOf(outputs), mountpointBuilder("out", env.Tags()),
			),
			Resources: ress,
			Log: &apiplans.LogPoint{
				Tags: append(
					[]apitag.Tag{{Key: "type", Value: "log"}},
					env.Tags()...,
				),
			},
		}

		return result, nil

	}
}

func mountpointBuilder(ignore string, defaultTags []apitag.Tag) func(p string) apiplans.Mountpoint {
	return func(p string) apiplans.Mountpoint {
		typeTag := ""
		{
			pp := strings.Split(p, string(os.PathSeparator))
		DETECT_TAG:
			for 0 < len(pp) {
				switch tt, rest := pp[len(pp)-1], pp[:len(pp)-1]; tt {
				case "", ignore:
					pp = rest
				default:
					typeTag = tt
					break DETECT_TAG
				}
			}
		}

		tags := defaultTags[:]
		if typeTag != "" {
			tags = append(tags, apitag.Tag{Key: "type", Value: typeTag})
		}

		return apiplans.Mountpoint{Path: p, Tags: tags}
	}
}

var _INPUT *regexp.Regexp
var _OUTPUT *regexp.Regexp

func init() {
	_INPUT = regexp.MustCompile(`(^|/)in(/|$)`)
	_OUTPUT = regexp.MustCompile(`(^|/)out(/|$)`)
}

type namedReader interface {
	Name() string
	io.Reader
}

type image apiplans.Image

func (im image) yamlNode() *yaml.Node {
	base := apiplans.Image(im)
	return y.Text(base.String(), y.WithStyle(yaml.DoubleQuotedStyle))

}

type mountpoint apiplans.Mountpoint

func (m mountpoint) yamlNode() *yaml.Node {
	base := apiplans.Mountpoint(m)

	return y.Map(
		y.Entry(y.Text("path"), y.Text(m.Path, y.WithStyle(yaml.DoubleQuotedStyle))),
		y.Entry(y.Text("tags"), y.Seq(
			utils.Map(
				base.Tags, func(t apitag.Tag) *yaml.Node {
					return y.Text(t.String(), y.WithStyle(yaml.DoubleQuotedStyle))
				},
			)...,
		)),
	)
}

type logpoint apiplans.LogPoint

func (l *logpoint) yamlNode() *yaml.Node {
	if l == nil {
		return y.Null()
	}

	base := apiplans.LogPoint(*l)

	return y.Map(
		y.Entry(y.Text("tags"), y.Seq(
			utils.Map(
				base.Tags,
				func(t apitag.Tag) *yaml.Node { return y.Text(t.String(), y.WithStyle(yaml.DoubleQuotedStyle)) },
			)...,
		)),
	)
}

type planSpecWithDocument struct {
	Image    image
	Inputs   []mountpoint
	Outputs  []mountpoint
	Log      *logpoint
	Resource map[string]string
	Active   bool
}

func (p planSpecWithDocument) MarshalYAML() (interface{}, error) {
	doc := y.Map(
		y.Entry(
			y.Text("image", y.WithHeadComment(`
image:
  Container image to be executed as this Plan.
  This image-tag should be accessible from your knitfab cluster.
`)),
			p.Image.yamlNode(),
		),
		y.Entry(
			y.Text("inputs", y.WithHeadComment(`
inputs:
  List of filepath and Tags as Input of this Plans.
  1 or more Inputs are needed.
  Each filepath should be absolute. Tags should be formatted in "key:value"-style.
`)),
			y.Seq(
				utils.Map(p.Inputs, mountpoint.yamlNode)...,
			),
		),
		y.Entry(
			y.Text("outputs", y.WithHeadComment(`
outputs:
  List of filepathes and Tags as Output of this Plans.
  See "inputs" for detail.
`)),
			y.Seq(
				utils.Map(p.Outputs, mountpoint.yamlNode)...,
			),
		),
		y.Entry(
			y.Text("log", y.WithHeadComment(`
log (optional):
  Set Tags stored log (STDOUT+STDERR of runs of this Plan) as Data.
  If missing or null, log would not be stored.
`)),
			p.Log.yamlNode(),
		),
		y.Entry(
			y.Text("active", y.WithHeadComment(`
active (optional):
  To suspend executing Runs by this Plan, set false explicitly.
  If missing or null, it is assumed as true.
`)),
			y.Bool(p.Active),
		),
		y.Entry(
			y.Text("resouces", y.WithHeadComment(`
resource (optional):
Specify the resource , cpu or memory for example, requirements for this Plan.
This value can be changed after the Plan is applied.

There can be other resources. For them, ask your administrator.

(advanced note: These values are passed to container.resource.limits in kubernetes.)
`)),
			y.Map(
				y.Entry(
					y.Text("cpu", y.WithHeadComment(`
cpu (optional; default = 1):
  Specify the CPU resource requirements for this Plan.
  This value means "how many cores" the plan will use.
  This can be a fraction, like "0.5" or "500m" (= 500 millicore) for a half of a core.
`),
					),
					y.Text(p.Resource["cpu"]),
				),
				y.Entry(
					y.Text("memory", y.WithHeadComment(`
memory (optional; default = 1Gi):
  Specify the memory resource requirements for this Plan.
  This value means "how many bytes" the plan will use.
  You can use suffixes like "Ki", "Mi", "Gi" for kibi-(1024), mebi-(1024^2), gibi-(1024^3) bytes, case sensitive.
  For example, "1Gi" means 1 gibibyte.
  If you omit the suffix, it is assumed as bytes.
`)),
					y.Text(p.Resource["memory"]),
				),
			),
		),
	)

	doc.FootComment = `
# # on_node (optional):
# #   Specify the node where this Plan is executed.
# #
# #   For each level (may, prefer and must), you can put node labels or taints in "key=value" format.
# #   Labels show a node characteristic, and taints show a node restriction.
# #   Ask your administrator for the available labels/taints.
# #
# #   By default (= empty), this plan is executed on any node, if the node does not taint.
# on_node:
#   # may: (optional)
#   #   Allow to execute this plan on nodes with these taints, put here.
#   may:
#     - "label-a=value1"
#     - "label-b=value2"
#
#   # prefer: (optional)
#   #   Execute this plan on nodes with these labels & taints, if possible.
#   prefer:
#     - "vram=large"
#
#   # must: (optional)
#   #   Always execute this plan on nodes with these labels & taints
#   #   (taints on node can be subset of this list).
#   #
#   #   If no node matches, runs of the plan will be scheduled but not started.
#   must:
#     - "accelarator=gpu"
`

	return doc, nil

}
