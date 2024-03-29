package rm

import (
	"context"
	"log"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"github.com/opst/knitfab/pkg/utils"
)

type Command struct {
	task func(
		ctx context.Context,
		client krst.KnitClient,
		runId string,
	) error
}

func WithTask(
	task func(
		ctx context.Context,
		client krst.KnitClient,
		runId string,
	) error,
) func(*Command) *Command {
	return func(dfc *Command) *Command {
		dfc.task = task
		return dfc
	}
}

func New(
	options ...func(*Command) *Command,
) kcmd.KnitCommand[struct{}] {
	return utils.ApplyAll(
		&Command{task: RunDeleteRun},
		options...,
	)
}

func (cmd *Command) Name() string {
	return "rm"
}

func (cmd *Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "Delete Run for the specified Run Id.",
	}
}

const ARG_RUNID = "runId"

func (cmd *Command) Usage() usage.Usage[struct{}] {
	return usage.New(
		struct{}{},
		usage.Args{
			{
				Name:       ARG_RUNID,
				Required:   true,
				Repeatable: false,
				Help:       "Id of the Run to be deleted.",
			},
		},
	)
}

func (cmd *Command) Execute(
	ctx context.Context,
	l *log.Logger,
	e kenv.KnitEnv,
	c krst.KnitClient,
	f usage.FlagSet[struct{}],
) error {
	runId := f.Args[ARG_RUNID][0]
	if err := cmd.task(ctx, c, runId); err == nil {
		l.Printf("deleted Run Id:%v", runId)
	} else {
		return err
	}
	return nil
}

func RunDeleteRun(ctx context.Context, client krst.KnitClient, runId string,
) error {
	err := client.DeleteRun(ctx, runId)
	if err != nil {
		return err
	}

	return nil
}
