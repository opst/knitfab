package find

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	apirun "github.com/opst/knitfab/pkg/api/types/runs"
	kflag "github.com/opst/knitfab/pkg/commandline/flag"
	ptr "github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/youta-t/flarc"
)

type Flag struct {
	PlanId    *kflag.Argslice         `flag:"planid" alias:"p" help:"Find Run with this Plan Id. Repeatable."`
	KnitIdIn  *kflag.Argslice         `flag:"in-knitid" alias:"i" help:"Find Run where the Input has this Knit Id. Repeatable."`
	KnitIdOut *kflag.Argslice         `flag:"out-knitid" alias:"o" help:"Find Run where the Output has this Knit Id. Repeatable."`
	Status    *kflag.Argslice         `flag:"status" alias:"s" metavar:"waiting|deactivated|starting|running|done|failed..." help:"Find Run in this status. Repeatable."`
	Since     *kflag.LooseRFC3339     `flag:"since" metavar:"YYYY-mm-dd[THH[:MM[:SS]]][TZ]" help:"Find Run only updated at this time or later."`
	Duration  *kflag.OptionalDuration `flag:"duration" metavar:"DURATION" help:"Find Run only updated in '--duration' from '--since'."`
}

type Option struct {
	find func(
		ctx context.Context,
		log *log.Logger,
		client krst.KnitClient,
		parameter krst.FindRunParameter,
	) ([]apirun.Detail, error)
}

func WithFind(
	find func(
		ctx context.Context,
		log *log.Logger,
		client krst.KnitClient,
		parameter krst.FindRunParameter,
	) ([]apirun.Detail, error),
) func(*Option) *Option {
	return func(dfc *Option) *Option {
		dfc.find = find
		return dfc
	}
}

func New(
	options ...func(*Option) *Option,
) (flarc.Command, error) {
	option := &Option{
		find: RunFindRun,
	}
	for _, opt := range options {
		option = opt(option)
	}

	return flarc.NewCommand(
		"Find Runs that satisfy all specified conditions.",
		Flag{
			PlanId:    &kflag.Argslice{},
			KnitIdIn:  &kflag.Argslice{},
			KnitIdOut: &kflag.Argslice{},
			Status:    &kflag.Argslice{},
			Since:     &kflag.LooseRFC3339{},
			Duration:  &kflag.OptionalDuration{},
		},
		flarc.Args{},
		common.NewTask(Task(option.find)),
		flarc.WithDescription(`
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

Example
-------

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
		),
	)
}

func Task(
	find func(
		ctx context.Context,
		log *log.Logger,
		client krst.KnitClient,
		parameter krst.FindRunParameter,
	) ([]apirun.Detail, error),
) common.Task[Flag] {
	return func(ctx context.Context, logger *log.Logger, knitEnv env.KnitEnv, client krst.KnitClient, cl flarc.Commandline[Flag], params []any) error {
		flags := cl.Flags()
		planId := ptr.SafeDeref(flags.PlanId)
		knitIdIn := ptr.SafeDeref(flags.KnitIdIn)
		knitIdOut := ptr.SafeDeref(flags.KnitIdOut)
		status := ptr.SafeDeref(flags.Status)
		since := flags.Since.Time()
		duration := flags.Duration.Duration()

		if since == nil && duration != nil {
			return fmt.Errorf("%w: --duration must be together with --since", flarc.ErrUsage)
		}

		parameter := krst.FindRunParameter{
			PlanId:    planId,
			KnitIdIn:  knitIdIn,
			KnitIdOut: knitIdOut,
			Status:    status,
			Since:     since,
			Duration:  duration,
		}

		run, err := find(ctx, logger, client, parameter)
		if err != nil {
			return err
		}

		enc := json.NewEncoder(cl.Stdout())
		enc.SetIndent("", "    ")
		if err := enc.Encode(run); err != nil {
			logger.Panicf("fail to dump found Run")
		}
		return nil
	}
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
