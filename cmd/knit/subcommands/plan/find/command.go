package find

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	apiplan "github.com/opst/knitfab/pkg/api/types/plans"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	kflag "github.com/opst/knitfab/pkg/commandline/flag"
	"github.com/opst/knitfab/pkg/commandline/usage"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/logic"
)

type Flag struct {
	Active  string      `flag:"active,metavar=both|yes|true|no|false,help=Activeness of Plans to be found. It can be yes(= true)|no(= false)|both."`
	Image   string      `flag:"image,metavar=image[:tag],help=image of Plans to be found."`
	InTags  *kflag.Tags `flag:"in-tag,short=i,metavar=KEY:VALUE...,help=Tags in input of Plans to be found. Repeatable."`
	OutTags *kflag.Tags `flag:"out-tag,short=o,metavar=KEY:VALUE...,help=Tags in output of Plan to be found. Repeatable."`
}

type Command struct {
	task func(
		ctx context.Context,
		log *log.Logger,
		client krst.KnitClient,
		active logic.Ternary,
		imageVer kdb.ImageIdentifier,
		inTags []apitag.Tag,
		outTags []apitag.Tag,
	) ([]apiplan.Detail, error)
}

func WithTask(task func(
	ctx context.Context,
	log *log.Logger,
	client krst.KnitClient,
	active logic.Ternary,
	imageVer kdb.ImageIdentifier,
	inTags []apitag.Tag,
	outTags []apitag.Tag,
) ([]apiplan.Detail, error)) func(*Command) *Command {
	return func(dfc *Command) *Command {
		dfc.task = task
		return dfc
	}
}

func New(
	options ...func(*Command) *Command,
) kcmd.KnitCommand[Flag] {
	return utils.ApplyAll(
		&Command{task: RunFindPlan},
		options...,
	)
}

func (cmd *Command) Name() string {
	return "find"
}

func (*Command) Usage() usage.Usage[Flag] {
	return usage.New(
		Flag{
			Active:  "both",
			Image:   "",
			InTags:  &kflag.Tags{},
			OutTags: &kflag.Tags{},
		},
		usage.Args{},
	)
}

func (*Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "Display Plans that satisfy all specified conditions.",
		Detail: `
Display Plans that satisfy all specified conditions.

If no condition is specified, all Plans are displayed.
`,
		Example: `
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
`,
	}
}

func (cmd *Command) Execute(
	ctx context.Context,
	l *log.Logger,
	_ kenv.KnitEnv,
	c krst.KnitClient,
	f usage.FlagSet[Flag],
) error {
	activateFlag := logic.Indeterminate
	switch f.Flags.Active {
	case "yes", "true":
		activateFlag = logic.True
	case "no", "false":
		activateFlag = logic.False
	case "both":
		// default value.
	default:
		return fmt.Errorf(
			`%w: incorrect --active: it shoule be "yes", "true", "no", "false" or "both"`,
			kcmd.ErrUsage,
		)
	}

	image, version, ok := strings.Cut(f.Flags.Image, ":")
	if ok && image == "" {
		return fmt.Errorf("%w: --image: only tag is passed", kcmd.ErrUsage)
	}
	imageVer := kdb.ImageIdentifier{
		Image:   image,
		Version: version,
	}

	inTags := []apitag.Tag{}
	if f.Flags.InTags != nil {
		inTags = *f.Flags.InTags
	}

	outTags := []apitag.Tag{}
	if f.Flags.OutTags != nil {
		outTags = *f.Flags.OutTags
	}

	plan, err := cmd.task(ctx, l, c, activateFlag, imageVer, inTags, outTags)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "    ")
	if err := enc.Encode(plan); err != nil {
		return err
	}

	return nil
}

func RunFindPlan(
	ctx context.Context,
	log *log.Logger,
	client krst.KnitClient,
	active logic.Ternary,
	imageVer kdb.ImageIdentifier,
	inTags []apitag.Tag,
	outTags []apitag.Tag,
) ([]apiplan.Detail, error) {

	result, err := client.FindPlan(ctx, active, imageVer, inTags, outTags)
	if err != nil {
		return nil, err
	}

	return result, nil
}
