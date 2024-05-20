package find

import (
	"context"
	"encoding/json"
	"fmt"
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
	PlanId    *kflag.Argslice         `flag:"planid,short=p,help=Find Run with this Plan Id. Repeatable."`
	KnitIdIn  *kflag.Argslice         `flag:"in-knitid,short=i,help=Find Run where the Input has this Knit Id. Repeatable."`
	KnitIdOut *kflag.Argslice         `flag:"out-knitid,short=o,help=Find Run where the Output has this Knit Id. Repeatable."`
	Status    *kflag.Argslice         `flag:"status,short=s,metavar=waiting|deactivated|starting|running|done|failed...,help=Find Run in this status. Repeatable."`
	Since     *kflag.LooseRFC3339     `flag:"since,help=Find Run only updated at this time or later."`
	Duration  *kflag.OptionalDuration `flag:"duration,help=Find Run only updated in '--duration' from '--since'."`
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
			Duration:  &kflag.OptionalDuration{},
		},
		usage.Args{},
	)
}

func (cmd *Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "Find Runs that satisfy all specified conditions",
		Detail: `
Find Runs that satisfy all specified conditions.

If the same flags multiple times, it will display Runs that satisfy any of the values.

To limit results with a timespan, use '--since' and '--duration'.

'--since' limits a result to Runs which have been updated at equal to or later than '--since'.
The '--since' is expected to be formatted in RFC3339, and it is also possible to omit sub-seconds, seconds, minutes, hours and ttime offsets.
When the time offset is omitted, it is assumed the local time. Other fields omitted are assumed to be zero.
Delimiter between the date and time can be "T" or " " (space), whichever equiverant.
For example, "2024-10-31T01:23:45.987Z", "2024-10-31 01:23" or "2024-10-31+09:00".

'--duration' limits a result to Runs which have been updated in '--duration' from '--since'.
'--duration' should be used in conjunction with '--since'.
Supported units are "ms" (milliseconds), "s" (seconds), "m" (minutes) and "h" (hours).
For example, "300ms", "1.5h" or "2h45m". Units are required. Negative duration is not supported.
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

Scan over Runs for day by day:

	{{ .Command }} --duration 24h --since 2024-01-01
	{{ .Command }} --duration 24h --since 2024-01-02
	{{ .Command }} --duration 24h --since 2024-01-03
	# And so on. There are no overwraps between days.
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
	since := flags.Flags.Since.Time()
	duration := flags.Flags.Duration.Duration()

	if since == nil && duration != nil {
		return fmt.Errorf("%w: --duration must be together with --since", kcmd.ErrUsage)
	}

	parameter := krst.FindRunParameter{
		PlanId:    planId,
		KnitIdIn:  knitIdIn,
		KnitIdOut: knitIdOut,
		Status:    status,
		Since:     since,
		Duration:  duration,
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
