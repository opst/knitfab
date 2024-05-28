package stop

import (
	"context"
	"log"

	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/knitcmd"
	"github.com/youta-t/flarc"
)

type Flag struct {
	Fail bool `flag:"fail" alias:"x" help:"Abort Run and let it be failed. Otherwise it will be done as succeeded."`
}

const ARG_RUNID = "RUN_ID"

func New() (flarc.Command, error) {
	return flarc.NewCommand(
		"Stop running Run.",
		Flag{
			Fail: false,
		},
		flarc.Args{
			{
				Name: ARG_RUNID, Required: true,
				Help: "Run Id to be stopped",
			},
		},
		knitcmd.NewTask(Task()),
		flarc.WithDescription(
			`
Stop Run and let it be done successfully.
If you want to stop Run and let it be failed, use --fail option.
`),
	)
}

func Task() knitcmd.Task[Flag] {
	return func(
		ctx context.Context,
		logger *log.Logger,
		knitEnv env.KnitEnv,
		client rest.KnitClient,
		cl flarc.Commandline[Flag],
		params []any,
	) error {
		runId := cl.Args()[ARG_RUNID][0]

		if cl.Flags().Fail {
			_, err := client.Abort(ctx, runId)
			if err == nil {
				logger.Printf("Run Id: %s is aborting.", runId)
			}
			return err
		}

		_, err := client.Tearoff(ctx, runId)
		if err == nil {
			logger.Printf("Run Id: %s is stopping.", runId)
		}

		return err
	}
}
