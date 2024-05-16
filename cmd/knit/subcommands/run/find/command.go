package find

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

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
	PlanId    *kflag.Argslice     `flag:"planid,short=p,help=Find Run with this Plan Id. Repeatable."`
	KnitIdIn  *kflag.Argslice     `flag:"in-knitid,short=i,help=Find Run where the Input has this Knit Id. Repeatable."`
	KnitIdOut *kflag.Argslice     `flag:"out-knitid,short=o,help=Find Run where the Output has this Knit Id. Repeatable."`
	Status    *kflag.Argslice     `flag:"status,short=s,metavar=waiting|deactivated|starting|running|done|failed...,help=Find Run in this status. Repeatable."`
	Since     *kflag.LooseRFC3339 `flag:"since,help=Find Run only updated at this time or later."`
	Duration  time.Duration       `flag:"duration,help=Find Run only updated at a time earlier than the sum of since and duration."`
}

type Command struct {
	task func(
		ctx context.Context,
		log *log.Logger,
		client krst.KnitClient,
		parameter krst.FindRunParameter,
	) ([]apirun.Detail, error)
}

func WithTask(
	task func(
		ctx context.Context,
		log *log.Logger,
		client krst.KnitClient,
		parameter krst.FindRunParameter,
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
			Since:     &kflag.LooseRFC3339{},
			Duration:  0,
		},
		usage.Args{},
	)
}

func (cmd *Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "Find Runs that satisfy all specified conditions",
		Detail: `
Find Runs that satisfy all specified conditions.
If the same flags except 'since' and 'duration' are specified multiple times, it will display Runs that satisfy any of the values.

'Since' and 'duration' are used to specify the time range to search for Runs.

Since targets Runs that have been updated at equal to or later than since.
The since can be described in RFC3339 date-time format, and it is also possible to omit 
sub-seconds, seconds, minutes, and hours, in the description.
If the time zone is omitted, the local time zone is applied. 
When including a date and time, the following characters are allowed as delimiters between the date and time: "T" or space.

Duration is a flag used in conjunction with since. 
It targets Runs for search that have been updated at a time earlier than the sum of since and duration.
Duration can be described in Go's time.Duration type.
Examples of duration are "300ms", "1.5h" or "2h45m". 
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
	since := time.Time(ptr.SafeDeref(flags.Flags.Since))
	duration := flags.Flags.Duration

	if since == (time.Time{}) && duration != 0 {
		return fmt.Errorf("%w: since and duration must be specified together", kcmd.ErrUsage)
	}

	parameter := krst.FindRunParameter{
		PlanId:    planId,
		KnitIdIn:  knitIdIn,
		KnitIdOut: knitIdOut,
		Status:    status,
		Since:     &since,
		Duration:  &duration,
	}

	run, err := cmd.task(ctx, l, c, parameter)
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
	parameter krst.FindRunParameter,
) ([]apirun.Detail, error) {
	result, err := client.FindRun(ctx, parameter)
	if err != nil {
		return nil, err
	}

	return result, nil
}
