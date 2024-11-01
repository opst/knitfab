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

	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	"github.com/opst/knitfab/pkg/utils/images/analyzer"
	"github.com/opst/knitfab/pkg/utils/slices"
	y "github.com/opst/knitfab/pkg/utils/yamler"
	"github.com/youta-t/flarc"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/resource"
)

type Option struct {
	fromScratch func(context.Context, *log.Logger, string, env.KnitEnv) (plans.PlanSpec, error)
	fromImage   func(context.Context, *log.Logger, namedReader, string, env.KnitEnv) (plans.PlanSpec, error)
}

func WithTemplateMaker(
	fromScratch func(context.Context, *log.Logger, string, env.KnitEnv) (plans.PlanSpec, error),
	fromImage func(context.Context, *log.Logger, namedReader, string, env.KnitEnv) (plans.PlanSpec, error),
) func(*Option) *Option {
	return func(cmd *Option) *Option {
		cmd.fromScratch = fromScratch
		cmd.fromImage = fromImage
		return cmd
	}
}

type Flag struct {
	Scratch bool   `flag:"scratch" help:"Generate a Plan file without reading any image."`
	Input   string `flag:"" alias:"i" metavar:"path/to/image.tar" help:"Tar file containing image (for example: output of 'docker save') to be used for the Plan."`
}

const (
	ARG_IMAGE_TAG = "image:tag"
)

func New(options ...func(*Option) *Option) (flarc.Command, error) {
	option := &Option{
		fromScratch: FromScratch(),
		fromImage:   FromImage(analyzer.Analyze),
	}
	for _, opt := range options {
		option = opt(option)
	}
	return flarc.NewCommand(
		"Generate a new Plan definition from a container image.",

		Flag{Input: "-", Scratch: false},
		flarc.Args{
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
		common.NewTask(Task(option.fromScratch, option.fromImage)),
		flarc.WithDescription(`
Generate a Plan file from "docker save".

	docker save image:tag | {{ .Command }} > plan.yaml

Generate a Plan file from a container image file.

	docker save image:tag > image.tar
	{{ .Command }} -i image.tar > plan.yaml

You may need to specify image:tag explicitly when the image has multiple tags, like below:

	{{ .Command }} -i image-with-multiple-tag.tar image:tag > plan.yaml
`),
	)

}

func Task(
	fromScratch func(context.Context, *log.Logger, string, env.KnitEnv) (plans.PlanSpec, error),
	fromImage func(context.Context, *log.Logger, namedReader, string, env.KnitEnv) (plans.PlanSpec, error),
) common.Task[Flag] {
	return func(
		ctx context.Context,
		logger *log.Logger,
		knitEnv env.KnitEnv,
		client rest.KnitClient,
		cl flarc.Commandline[Flag],
		params []any,
	) error {
		flags := cl.Flags()
		args := cl.Args()

		var plan plans.PlanSpec
		if flags.Scratch {
			image := "image:version"
			if l := len(args[ARG_IMAGE_TAG]); 0 < l {
				return fmt.Errorf(
					"%w: image:tag and --scratch are exclusive", flarc.ErrUsage,
				)
			}

			spec, err := fromScratch(ctx, logger, image, knitEnv)
			if err != nil {
				return fmt.Errorf("can not generate Plan file: %w", err)
			}
			plan = spec
		} else {
			imageTag := ""
			if 0 < len(args[ARG_IMAGE_TAG]) {
				imageTag = args[ARG_IMAGE_TAG][0]
			}

			var source namedReader = _namedReader{name: "STDIN", Reader: cl.Stdin()}
			if flags.Input != "-" {
				f, err := os.Open(flags.Input)
				if err != nil {
					return fmt.Errorf(
						"cannot open input file: %s: %w", flags.Input, err,
					)
				}
				defer f.Close()
				source = f
			}

			spec, err := fromImage(ctx, logger, source, imageTag, knitEnv)
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
			Image:      image(plan.Image),
			Entrypoint: plan.Entrypoint,
			Args:       plan.Args,
			Inputs:     slices.Map(plan.Inputs, func(i plans.Mountpoint) mountpoint { return mountpoint(i) }),
			Outputs:    slices.Map(plan.Outputs, func(i plans.Mountpoint) mountpoint { return mountpoint(i) }),
			Log:        (*logpoint)(plan.Log),
			Resource:   res,
			Active:     active,
		}

		os.Stdout.WriteString("\n")
		enc := yaml.NewEncoder(os.Stdout)
		defer enc.Close()
		enc.SetIndent(2)
		if err := enc.Encode(yplan); err != nil {
			return fmt.Errorf("cannot write Plan file: %w", err)
		}
		os.Stdout.WriteString("\n")
		logger.Println("# Plan file is generated. modify it as you like.")
		return nil
	}
}

func FromScratch() func(context.Context, *log.Logger, string, env.KnitEnv) (plans.PlanSpec, error) {
	return func(
		ctx context.Context,
		l *log.Logger,
		tag string,
		env env.KnitEnv,
	) (plans.PlanSpec, error) {
		image := new(plans.Image)
		if err := image.Parse(tag); err != nil {
			return plans.PlanSpec{}, err
		}
		ts := env.Tags()
		if len(ts) == 0 {
			ts = []tags.Tag{{Key: "example", Value: "tag"}}
		}

		ress := plans.Resources{}
		for k, v := range env.Resource {
			q, err := resource.ParseQuantity(v)
			if err != nil {
				return plans.PlanSpec{}, fmt.Errorf("invalid resource value: %w", err)
			}
			ress[k] = q
		}
		if _, ok := ress["cpu"]; !ok {
			ress["cpu"] = resource.MustParse("1")
		}
		if _, ok := ress["memory"]; !ok {
			ress["memory"] = resource.MustParse("1Gi")
		}

		result := plans.PlanSpec{
			Image: *image,
			Inputs: []plans.Mountpoint{
				{Path: "/in", Tags: ts},
			},
			Outputs: []plans.Mountpoint{
				{Path: "/out", Tags: ts},
			},
			Resources: ress,
			Log: &plans.LogPoint{Tags: append([]tags.Tag{
				{Key: "type", Value: "log"}}, ts...,
			)},
		}

		return result, nil
	}
}

func FromImage(
	analyze func(context.Context, io.Reader) ([]analyzer.TaggedConfig, error),
) func(context.Context, *log.Logger, namedReader, string, env.KnitEnv) (plans.PlanSpec, error) {
	return func(
		ctx context.Context,
		l *log.Logger,
		source namedReader,
		tag string,
		env env.KnitEnv,
	) (plans.PlanSpec, error) {

		l.Printf(`...analyzing image from "%s"`, source.Name())
		foundConfigs, err := analyze(ctx, source)
		if err != nil {
			return plans.PlanSpec{}, err
		}

		inputs := map[string]struct{}{}
		outputs := map[string]struct{}{}

		var cfg analyzer.TaggedConfig
		if tag == "" {
			if l := len(foundConfigs); 1 < l {
				return plans.PlanSpec{}, fmt.Errorf("multiple images found, specify the image tag")
			} else if l == 0 {
				return plans.PlanSpec{}, fmt.Errorf("no image found")
			}
			cfg = foundConfigs[0]
		} else {
			found := false
		CONFIGS:
			for _, c := range foundConfigs {
				for _, t := range c.Tags {
					if t == tag {
						cfg = c
						found = true
						break CONFIGS
					}
				}
			}
			if !found {
				return plans.PlanSpec{}, fmt.Errorf("specified image tag '%s' is not found", tag)
			}
		}

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

		ress := plans.Resources{}
		for k, v := range env.Resource {
			q, err := resource.ParseQuantity(v)
			if err != nil {
				return plans.PlanSpec{}, fmt.Errorf("invalid resource value: %w", err)
			}
			ress[k] = q
		}
		if _, ok := ress["cpu"]; !ok {
			ress["cpu"] = resource.MustParse("1")
		}
		if _, ok := ress["memory"]; !ok {
			ress["memory"] = resource.MustParse("1Gi")
		}

		var repository string
		var imagetag string
		if i, t, ok := cutImageTag(tag); ok {
			repository = i
			imagetag = t
		}

		if repository == "" && imagetag == "" {
			if 0 < len(cfg.Tags) {
				if i, t, ok := cutImageTag(cfg.Tags[0]); ok {
					repository = i
					imagetag = t
				}
			}
		}

		if repository == "" {
			repository = "IMAGE"
		}
		if imagetag == "" {
			imagetag = "TAG"
		}

		result := plans.PlanSpec{
			Image: plans.Image{
				Repository: repository,
				Tag:        imagetag,
			},
			Entrypoint: cfg.Config.Entrypoint,
			Args:       cfg.Config.Cmd,
			Inputs: slices.Map(
				slices.KeysOf(inputs), mountpointBuilder("in", env.Tags()),
			),
			Outputs: slices.Map(
				slices.KeysOf(outputs), mountpointBuilder("out", env.Tags()),
			),
			Resources: ress,
			Log: &plans.LogPoint{
				Tags: append(
					[]tags.Tag{{Key: "type", Value: "log"}},
					env.Tags()...,
				),
			},
		}

		return result, nil

	}
}

func cutImageTag(imageName string) (repo string, tag string, ok bool) {
	if i := strings.LastIndexByte(imageName, ':'); 0 < i {
		return imageName[:i], imageName[i+1:], true
	}
	return imageName, "", false
}

func mountpointBuilder(ignore string, defaultTags []tags.Tag) func(p string) plans.Mountpoint {
	return func(p string) plans.Mountpoint {
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

		dtags := defaultTags[:]
		if typeTag != "" {
			dtags = append(dtags, tags.Tag{Key: "type", Value: typeTag})
		}

		return plans.Mountpoint{Path: p, Tags: dtags}
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

type _namedReader struct {
	name string
	io.Reader
}

func (r _namedReader) Name() string {
	return r.name
}

type image plans.Image

func (im image) yamlNode() *yaml.Node {
	base := plans.Image(im)
	return y.Text(base.String(), y.WithStyle(yaml.DoubleQuotedStyle))

}

type mountpoint plans.Mountpoint

func (m mountpoint) yamlNode() *yaml.Node {
	base := plans.Mountpoint(m)

	return y.Map(
		y.Entry(y.Text("path"), y.Text(m.Path, y.WithStyle(yaml.DoubleQuotedStyle))),
		y.Entry(y.Text("tags"), y.Seq(
			slices.Map(
				base.Tags, func(t tags.Tag) *yaml.Node {
					return y.Text(t.String(), y.WithStyle(yaml.DoubleQuotedStyle))
				},
			)...,
		)),
	)
}

type logpoint plans.LogPoint

func (l *logpoint) yamlNode() *yaml.Node {
	if l == nil {
		return y.Null()
	}

	base := plans.LogPoint(*l)

	return y.Map(
		y.Entry(y.Text("tags"), y.Seq(
			slices.Map(
				base.Tags,
				func(t tags.Tag) *yaml.Node { return y.Text(t.String(), y.WithStyle(yaml.DoubleQuotedStyle)) },
			)...,
		)),
	)
}

type annotations plans.Annotations

func (a annotations) yamlNode() *yaml.Node {
	base := plans.Annotations(a)

	yannots := make([]*yaml.Node, len(base))
	for i, ann := range base {
		yannots[i] = y.Text(ann.Key+"="+ann.Value, y.WithStyle(yaml.DoubleQuotedStyle))
	}

	return y.Seq(yannots...)
}

type planSpecWithDocument struct {
	Image       image
	Entrypoint  []string
	Args        []string
	Inputs      []mountpoint
	Outputs     []mountpoint
	Log         *logpoint
	Resource    map[string]string
	Active      bool
	Annotations annotations
}

func (p planSpecWithDocument) MarshalYAML() (interface{}, error) {
	doc := y.Map(
		y.Entry(
			y.Text("annotations",
				y.WithHeadComment(`
annotations (optional, mutable):
  Set Annotations of this Plan in list of "key=value" format string.
  You can use this for your own purpose, for example documentation. This does not affect lineage tracking.
  Knitfab Extensions may refer this.
`),
				y.WithFootComment(`  - "key=value"
  - "description=This is a Plan for ..."
`),
			),
			p.Annotations.yamlNode(),
		),
		y.Entry(
			y.Text("image", y.WithHeadComment(`
image:
  Container image to be executed as this Plan.
  This image-tag should be accessible from your knitfab cluster.
`)),
			p.Image.yamlNode(),
		),
		y.Entry(
			y.Text("entrypoint", y.WithHeadComment(`
entrypoint:
  Command to be executed as this Plan image.
  This array overrides the ENTRYPOINT of the image.
`)),
			y.CompactSeq(slices.Map(p.Entrypoint, func(s string) *yaml.Node { return y.Text(s, y.WithStyle(yaml.DoubleQuotedStyle)) })...),
		),
		y.Entry(
			y.Text("args", y.WithHeadComment(`
args:
  Arguments to be passed to this Plan image.
  This array overrides the CMD of the image.
`)),
			y.CompactSeq(slices.Map(p.Args, func(s string) *yaml.Node { return y.Text(s, y.WithStyle(yaml.DoubleQuotedStyle)) })...),
		),
		y.Entry(
			y.Text("inputs", y.WithHeadComment(`
inputs:
  List of filepath and Tags as Input of this Plans.
  1 or more Inputs are needed.
  Each filepath should be absolute. Tags should be formatted in "key:value"-style.
`)),
			y.Seq(
				slices.Map(p.Inputs, mountpoint.yamlNode)...,
			),
		),
		y.Entry(
			y.Text("outputs", y.WithHeadComment(`
outputs:
  List of filepathes and Tags as Output of this Plans.
  See "inputs" for detail.
`)),
			y.Seq(
				slices.Map(p.Outputs, mountpoint.yamlNode)...,
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
active (optional, mutable):
  To suspend executing Runs by this Plan, set false explicitly.
  If missing or null, it is assumed as true.
`)),
			y.Bool(p.Active),
		),
		y.Entry(
			y.Text("resouces", y.WithHeadComment(`
resource (optional, mutable):
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
#
# # service_account (optional, mutable):
# #   Specify the service account to run this Plan.
# #   If missing or null, the service account is not used.
# service_account: "default"
`

	return doc, nil

}
