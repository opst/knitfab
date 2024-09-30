package show

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	"github.com/youta-t/flarc"
)

type Option struct {
	show func(
		ctx context.Context,
		client krst.KnitClient,
		planId string,
	) (plans.Detail, error)
}

func WithShow(
	show func(
		ctx context.Context,
		client krst.KnitClient,
		planId string,
	) (plans.Detail, error),
) func(*Option) *Option {
	return func(cmd *Option) *Option {
		cmd.show = show
		return cmd
	}
}

const (
	ARG_PLAN_ID = "PLAN_ID"
)

func New(options ...func(*Option) *Option) (flarc.Command, error) {
	option := &Option{
		show: RunShowPlan,
	}

	for _, opt := range options {
		option = opt(option)
	}

	return flarc.NewCommand(
		"Return the Plan information for the specified Plan Id.",
		struct{}{},
		flarc.Args{
			{
				Name: ARG_PLAN_ID, Required: true,
				Help: "Specify the Plan Id you finding",
			},
		},
		common.NewTask(Task(option.show)),
	)
}

func Task(
	show func(
		ctx context.Context,
		client krst.KnitClient,
		planId string,
	) (plans.Detail, error),
) common.Task[struct{}] {
	return func(
		ctx context.Context,
		logger *log.Logger,
		knitEnv env.KnitEnv,
		client krst.KnitClient,
		cl flarc.Commandline[struct{}],
		params []any,
	) error {
		planId := cl.Args()[ARG_PLAN_ID][0]
		data, err := show(ctx, client, planId)
		if err != nil {
			return fmt.Errorf("%w: Plan Id:%v", err, planId)
		}

		enc := json.NewEncoder(cl.Stdout())
		enc.SetIndent("", "    ")
		if err := enc.Encode(data); err != nil {
			logger.Panicf("fail to dump found Plan")
		}

		return nil
	}
}

func RunShowPlan(
	ctx context.Context,
	client krst.KnitClient,
	planId string,
) (plans.Detail, error) {

	result, err := client.GetPlans(ctx, planId)
	if err != nil {
		return plans.Detail{}, err
	}

	return result, nil
}
