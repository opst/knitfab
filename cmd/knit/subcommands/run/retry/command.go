package retry

import (
	"context"
	"log"

	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	"github.com/youta-t/flarc"
)

const ARG_RUNID = "RUN_ID"

func New() (flarc.Command, error) {
	return flarc.NewCommand(
		"Retry a finished Run.",
		struct{}{},
		flarc.Args{
			{
				Name: ARG_RUNID, Required: true,
				Help: "Run Id to retry",
			},
		},
		common.NewTask(Task),
		flarc.WithDescription(
			`
Retry a Run.

Retriable Runs are:

- finished, means it's status is "done" or "failed",
- NOT a dependency of any other Runs, and
- NOT a root Run.
`,
		),
	)
}
func Task(
	ctx context.Context,
	l *log.Logger,
	_ env.KnitEnv,
	client rest.KnitClient,
	cl flarc.Commandline[struct{}],
	_ []any,
) error {
	runId := cl.Args()[ARG_RUNID][0]
	if runId == "" {
		l.Println("Run Id is required")
		return flarc.ErrUsage
	}

	if err := client.Retry(ctx, runId); err != nil {
		return err
	}
	l.Println("requested to retry Run:", runId)
	return nil
}
