package retry

import (
	"context"
	"log"

	"github.com/opst/knitfab/cmd/knit/commandline/command"
	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/pkg/commandline/usage"
)

type Command struct{}

func New() command.KnitCommand[struct{}] {
	cmd := &Command{}

	return cmd
}

func (*Command) Name() string {
	return "retry"
}

const ARG_RUNID = "RUN_ID"

func (*Command) Usage() usage.Usage[struct{}] {
	return usage.New(
		struct{}{},
		usage.Args{
			{
				Name: ARG_RUNID, Required: true,
				Help: "Run Id to retry",
			},
		},
	)
}

func (*Command) Help() command.Help {
	return command.Help{
		Synopsis: "retry a finished Run",
		Detail: `
Retry a Run.

Retriable Runs are:

- finished, means it's status is "done" or "failed",
- NOT a dependency of any other Runs, and
- NOT a root Run.
`,
	}
}

func (cmd *Command) Execute(
	ctx context.Context,
	l *log.Logger,
	_ env.KnitEnv,
	client rest.KnitClient,
	flags usage.FlagSet[struct{}],
) error {
	runId := flags.Args[ARG_RUNID][0]
	if runId == "" {
		l.Println("Run Id is required")
		return command.ErrUsage
	}

	if err := client.Retry(ctx, runId); err != nil {
		return err
	}
	l.Println("requested to retry Run:", runId)
	return nil
}
