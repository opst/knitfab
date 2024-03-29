package find

import (
	"context"
	"encoding/json"
	"log"
	"os"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	apirun "github.com/opst/knitfab/pkg/api/types/runs"
	kflag "github.com/opst/knitfab/pkg/commandline/flag"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"github.com/opst/knitfab/pkg/utils"
	ptr "github.com/opst/knitfab/pkg/utils/pointer"
)

type Flag struct {
	PlanId    *kflag.Argslice `flag:"planid,short=p,help=Find Run with this Plan Id. Repeatable."`
	KnitIdIn  *kflag.Argslice `flag:"in-knitid,short=i,help=Find run where the Input has this Knit Id. Repeatable."`
	KnitIdOut *kflag.Argslice `flag:"out-knitid,short=o,help=Find run where the Output has this Knit Id. Repeatable."`
	Status    *kflag.Argslice `flag:"status,short=s,metavar=waiting|deactivated|starting|running|done|failed...,help=Find Run in this status. Repeatable."`
}

type Command struct {
	task func(
		ctx context.Context,
		log *log.Logger,
		client krst.KnitClient,
		planId []string,
		knitIdIn []string,
		knitIdOut []string,
		status []string,
	) ([]apirun.Detail, error)
}

func WithTask(
	task func(
		ctx context.Context,
		log *log.Logger,
		client krst.KnitClient,
		planId []string,
		knitIdIn []string,
		knitIdOut []string,
		status []string,
	) ([]apirun.Detail, error),
) func(*Command) *Command {
	return func(dfc *Command) *Command {
		dfc.task = task
		return dfc
	}
}

func New(
	options ...func(*Command) *Command,
) kcmd.KnitCommand[Flag] {
	return utils.ApplyAll(
		&Command{task: RunFindRun},
		options...,
	)
}

func (cmd *Command) Name() string {
	return "find"
}

func (*Command) Usage() usage.Usage[Flag] {
	return usage.New(
		Flag{
			PlanId:    &kflag.Argslice{},
			KnitIdIn:  &kflag.Argslice{},
			KnitIdOut: &kflag.Argslice{},
			Status:    &kflag.Argslice{},
		},
		usage.Args{},
	)
}

func (cmd *Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "Find Runs that satisfy all specified conditions",
		Detail: `
Find Runs that satisfy all specified conditions.
If the same flag is specified multiple times, it will display Runs that satisfy any of the values.
`,
		Example: `
Finding Runs with Plan Id "plan1":

	{{ .Command }} --planid plan1
	{{ .Command }} -p plan1

	(both above are equivalent)

Finding runs with Plan Id "plan1" OR "plan2":

	{{ .Command }} --planid plan1 --planid plan2

Finding runs with Plan Id "plan1" AND Knit Id "knit1" in Input :

	{{ .Command }} --planid plan1 --in-knitid knit1
`,
	}
}

func (cmd *Command) Execute(
	ctx context.Context,
	l *log.Logger,
	e kenv.KnitEnv,
	c krst.KnitClient,
	flags usage.FlagSet[Flag],
) error {

	planId := ptr.SafeDeref(flags.Flags.PlanId)
	knitIdIn := ptr.SafeDeref(flags.Flags.KnitIdIn)
	knitIdOut := ptr.SafeDeref(flags.Flags.KnitIdOut)
	status := ptr.SafeDeref(flags.Flags.Status)

	run, err := cmd.task(ctx, l, c, planId, knitIdIn, knitIdOut, status)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "    ")
	if err := enc.Encode(run); err != nil {
		l.Panicf("fail to dump found Run")
	}
	return nil
}

func RunFindRun(
	ctx context.Context,
	logger *log.Logger,
	client krst.KnitClient,
	planId []string,
	knitIdIn []string,
	knitIdOut []string,
	status []string,
) ([]apirun.Detail, error) {

	result, err := client.FindRun(ctx, planId, knitIdIn, knitIdOut, status)
	if err != nil {
		return nil, err
	}

	return result, nil
}
