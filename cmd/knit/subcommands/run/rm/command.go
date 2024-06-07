package rm

import (
	"context"
	"log"

	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	"github.com/youta-t/flarc"
)

type Option struct {
	remove func(
		ctx context.Context,
		client krst.KnitClient,
		runId string,
	) error
}

func WithRemover(
	remove func(
		ctx context.Context,
		client krst.KnitClient,
		runId string,
	) error,
) func(*Option) *Option {
	return func(opt *Option) *Option {
		opt.remove = remove
		return opt
	}
}

const ARG_RUNID = "RUN_ID"

func New(
	options ...func(*Option) *Option,
) (flarc.Command, error) {
	option := &Option{
		remove: RunDeleteRun,
	}
	for _, opt := range options {
		option = opt(option)
	}

	return flarc.NewCommand(
		"Delete Run for the specified Run Id.",
		struct{}{},
		flarc.Args{
			{
				Name:       ARG_RUNID,
				Required:   true,
				Repeatable: false,
				Help:       "Id of the Run to be deleted.",
			},
		},
		common.NewTask(Task(option.remove)),
	)
}

func Task(
	remove func(context.Context, krst.KnitClient, string) error,
) common.Task[struct{}] {
	return func(
		ctx context.Context,
		logger *log.Logger,
		knitEnv env.KnitEnv,
		client krst.KnitClient,
		cl flarc.Commandline[struct{}],
		params []any,
	) error {

		runId := cl.Args()[ARG_RUNID][0]
		if err := remove(ctx, client, runId); err == nil {
			logger.Printf("deleted Run Id:%v", runId)
		} else {
			return err
		}
		return nil
	}
}

func RunDeleteRun(ctx context.Context, client krst.KnitClient, runId string,
) error {
	err := client.DeleteRun(ctx, runId)
	if err != nil {
		return err
	}

	return nil
}
