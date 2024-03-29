package stop

import (
	"context"
	"log"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/pkg/commandline/usage"
)

type Command struct{}

func New(
	options ...func(*Command) *Command,
) kcmd.KnitCommand[Flag] {
	return &Command{}
}

func (c *Command) Name() string {
	return "stop"
}

const ARG_RUNID = "RUN_ID"

type Flag struct {
	Fail bool `flag:"fail,short=x,help=Abort Run and let it be failed. Otherwise it will be done as succeeded."`
}

func (c *Command) Usage() usage.Usage[Flag] {
	return usage.New(
		Flag{
			Fail: false,
		},
		usage.Args{
			{
				Name: ARG_RUNID, Required: true,
				Help: "Run Id to be stopped",
			},
		},
	)
}

func (c *Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "Stop running Run.",
		Detail: `
Stop Run and let it be done successfully.
If you want to stop Run and let it be failed, use --fail option.
`,
	}
}

func (c *Command) Execute(
	ctx context.Context,
	l *log.Logger,
	e kenv.KnitEnv,
	client krst.KnitClient,
	flags usage.FlagSet[Flag],
) error {
	runId := flags.Args[ARG_RUNID][0]

	if flags.Flags.Fail {
		_, err := client.Abort(ctx, runId)
		if err == nil {
			l.Printf("Run Id: %s is aborting.", runId)
		}
		return err
	}

	_, err := client.Tearoff(ctx, runId)
	if err == nil {
		l.Printf("Run Id: %s is stopping.", runId)
	}

	return err
}
