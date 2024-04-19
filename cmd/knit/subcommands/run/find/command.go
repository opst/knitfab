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
	"github.com/opst/knitfab/pkg/utils/rfctime"
)

type Flag struct {
	PlanId    *kflag.Argslice `flag:"planid,short=p,help=Find Run with this Plan Id. Repeatable."`
	KnitIdIn  *kflag.Argslice `flag:"in-knitid,short=i,help=Find Run where the Input has this Knit Id. Repeatable."`
	KnitIdOut *kflag.Argslice `flag:"out-knitid,short=o,help=Find Run where the Output has this Knit Id. Repeatable."`
	Status    *kflag.Argslice `flag:"status,short=s,metavar=waiting|deactivated|starting|running|done|failed...,help=Find Run in this status. Repeatable."`
	Since     string          `flag:"since,help=Find Run only updated at this time or later."`
	Duration  string          `flag:"duration,help=Find Run only equal or earlier than '--since' + '--duration'."`
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
		since string,
		duration string,
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
		since string,
		duration string,
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
			Since:     "",
			Duration:  "",
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
Since can be described in the following formats: RFC3339, DateOnly, DateTime. 
The definitions of these formats are in https://pkg.go.dev/time#pkg-constants.
If since are described in DateOnly or DateTime, it will be converted to RFC3339 in the local timezone.

Duration is a flag used in conjunction with Since. 
It targets Runs for search that have been updated at equal to or earlier than 'Since + duration'.
Duration can be described in PostgreSQL interval type. 
Examples of duration are '2 minutes', '3 hours', '4 days', '5 months'.
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
	since := flags.Flags.Since
	duration := flags.Flags.Duration

	if since == "" {
		if duration != "" {
			return fmt.Errorf("%w: since is required when duration is specified", kcmd.ErrUsage)
		}
	} else {
		sinceRFC3339, format, err := rfctime.ParseMultipleFormats(
			since,
			rfctime.RFC3339DateTimeFormat,
			time.DateTime,
			time.DateOnly,
		)
		if err != nil {
			return fmt.Errorf("%w: since is invalid", err)
		}
		switch format {
		case rfctime.RFC3339DateTimeFormat:
		// do nothing
		case time.DateTime, time.DateOnly:
			// convert time to RFC3339DateTimeFormat in local timezone
			since, err = sinceRFC3339.StringWithLocalTimeZone()
			if err != nil {
				return err
			}
		}
	}

	run, err := cmd.task(ctx, l, c, planId, knitIdIn, knitIdOut, status, since, duration)
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
	since string,
	duration string,
) ([]apirun.Detail, error) {

	result, err := client.FindRun(ctx, planId, knitIdIn, knitIdOut, status, since, duration)
	if err != nil {
		return nil, err
	}

	return result, nil
}
