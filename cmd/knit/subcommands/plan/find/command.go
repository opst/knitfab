package find

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	"github.com/opst/knitfab/pkg/domain"
	kargs "github.com/opst/knitfab/pkg/utils/args"
	"github.com/opst/knitfab/pkg/utils/logic"
	"github.com/youta-t/flarc"
)

type Flag struct {
	Active  string      `flag:"active" metavar:"both|yes|true|no|false" help:"Activeness of Plans to be found. It can be yes(= true)|no(= false)|both."`
	Image   string      `flag:"image" metavar:"image[:tag]" help:"image of Plans to be found."`
	InTags  *kargs.Tags `flag:"in-tag" alias:"i" metavar:"KEY:VALUE..." help:"Tags in input of Plans to be found. Repeatable."`
	OutTags *kargs.Tags `flag:"out-tag" alias:"o" metavar:"KEY:VALUE..." help:"Tags in output of Plan to be found. Repeatable."`
}

type Option struct {
	find func(
		ctx context.Context,
		log *log.Logger,
		client krst.KnitClient,
		active logic.Ternary,
		imageVer domain.ImageIdentifier,
		inTags []tags.Tag,
		outTags []tags.Tag,
	) ([]plans.Detail, error)
}

func WithFind(
	find func(
		ctx context.Context,
		log *log.Logger,
		client krst.KnitClient,
		active logic.Ternary,
		imageVer domain.ImageIdentifier,
		inTags []tags.Tag,
		outTags []tags.Tag,
	) ([]plans.Detail, error),
) func(*Option) *Option {
	return func(dfc *Option) *Option {
		dfc.find = find
		return dfc
	}
}

func New(options ...func(*Option) *Option) (flarc.Command, error) {
	option := &Option{
		find: RunFindPlan,
	}
	for _, o := range options {
		option = o(option)
	}

	return flarc.NewCommand(
		"Display Plans that satisfy all specified conditions.",
		Flag{
			Active:  "both",
			Image:   "",
			InTags:  &kargs.Tags{},
			OutTags: &kargs.Tags{},
		},
		flarc.Args{},
		common.NewTask(Task(option.find)),
		flarc.WithDescription(`
Display Plans that satisfy all specified conditions.

If no condition is specified, all Plans are displayed.

Example
-------

Finding Plan with Input Tag "key1:value1":

	{{ .Command }} --in-tag key1:value1

Finding Plan with Input Tag "key1:value1" AND "key2:value2":

	{{ .Command }} --in-tag key1:value1 --in-tag key2:value2

Finding Plan with Input Tag "key1:value1" AND Output Tag "key2:value2":

	{{ .Command }} --in-tag key1:value1 --out-tag key2:value2

Finding Plan with Input Tag ("key1:value1" AND "key1:value2") AND Output Tag "key2:value2":

	{{ .Command }} --in-tag key1:value1 --in-tag key1:value2 --out-tag key2:value2

Finding Plan with container image "image1", regardless of version:

	{{ .Command }} --image image1

Finding Plan with container image "image1:version1":

	{{ .Command }} --image image1:version1
`),
	)
}

func Task(
	find func(
		ctx context.Context,
		log *log.Logger,
		client krst.KnitClient,
		active logic.Ternary,
		imageVer domain.ImageIdentifier,
		inTags []tags.Tag,
		outTags []tags.Tag,
	) ([]plans.Detail, error),
) common.Task[Flag] {
	return func(
		ctx context.Context,
		logger *log.Logger,
		knitEnv env.KnitEnv,
		client krst.KnitClient,
		cl flarc.Commandline[Flag],
		params []any,
	) error {

		flags := cl.Flags()
		activateFlag := logic.Indeterminate
		switch flags.Active {
		case "yes", "true":
			activateFlag = logic.True
		case "no", "false":
			activateFlag = logic.False
		case "both":
			// default value.
		default:
			return fmt.Errorf(
				`%w: incorrect --active: it shoule be "yes", "true", "no", "false" or "both"`,
				flarc.ErrUsage,
			)
		}

		image, version, ok := strings.Cut(flags.Image, ":")
		if ok && image == "" {
			return fmt.Errorf("%w: --image: only tag is passed", flarc.ErrUsage)
		}
		imageVer := domain.ImageIdentifier{
			Image:   image,
			Version: version,
		}

		inTags := []tags.Tag{}
		if flags.InTags != nil {
			inTags = *flags.InTags
		}

		outTags := []tags.Tag{}
		if flags.OutTags != nil {
			outTags = *flags.OutTags
		}

		plan, err := find(ctx, logger, client, activateFlag, imageVer, inTags, outTags)
		if err != nil {
			return err
		}

		enc := json.NewEncoder(cl.Stdout())
		enc.SetIndent("", "    ")
		if err := enc.Encode(plan); err != nil {
			return err
		}

		return nil
	}
}

func RunFindPlan(
	ctx context.Context,
	log *log.Logger,
	client krst.KnitClient,
	active logic.Ternary,
	imageVer domain.ImageIdentifier,
	inTags []tags.Tag,
	outTags []tags.Tag,
) ([]plans.Detail, error) {

	result, err := client.FindPlan(ctx, active, imageVer, inTags, outTags)
	if err != nil {
		return nil, err
	}

	return result, nil
}
