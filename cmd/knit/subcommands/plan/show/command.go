package show

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	apiplans "github.com/opst/knitfab/pkg/api/types/plans"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"github.com/opst/knitfab/pkg/utils"
)

type Command struct {
	task func(
		ctx context.Context,
		client krst.KnitClient,
		planId string,
	) (apiplans.Detail, error)
}

func WithDataShowTask(
	task func(
		ctx context.Context,
		client krst.KnitClient,
		planId string,
	) (apiplans.Detail, error),
) func(*Command) *Command {
	return func(cmd *Command) *Command {
		cmd.task = task
		return cmd
	}
}

func New(options ...func(*Command) *Command) kcmd.KnitCommand[struct{}] {
	return utils.ApplyAll(
		&Command{task: RunShowPlan},
		options...,
	)
}

func (cmd *Command) Name() string {
	return "show"
}

func (cmd *Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "Return the Plan information for the specified Plan Id.",
	}
}

func (*Command) Usage() usage.Usage[struct{}] {
	return usage.New(
		struct{}{},
		usage.Args{
			{
				Name: ARG_PLAN_ID, Required: true,
				Help: "Specify the Plan Id you finding",
			},
		},
	)
}

const (
	ARG_PLAN_ID = "PLAN_ID"
)

func (cmd *Command) Execute(
	ctx context.Context,
	l *log.Logger,
	e env.KnitEnv,
	c krst.KnitClient,
	flags usage.FlagSet[struct{}],
) error {
	planId := flags.Args[ARG_PLAN_ID][0]
	data, err := cmd.task(ctx, c, planId)
	if err != nil {
		return fmt.Errorf("%w: Plan Id:%v", err, planId)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "    ")
	if err := enc.Encode(data); err != nil {
		l.Panicf("fail to dump found Plan")
	}

	return nil
}

func RunShowPlan(
	ctx context.Context,
	client krst.KnitClient,
	planId string,
) (apiplans.Detail, error) {

	result, err := client.GetPlans(ctx, planId)
	if err != nil {
		return apiplans.Detail{}, err
	}

	return result, nil
}
