package plan

import (
	"context"
	"encoding/json"
	"fmt"

	"log"

	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	apiplans "github.com/opst/knitfab/pkg/api/types/plans"
	"github.com/youta-t/flarc"
)

type Option struct {
	updateActiveness func(
		ctx context.Context,
		client krst.KnitClient,
		planId string,
		isActive bool,
	) (apiplans.Detail, error)
}

func WithUpdateActiveness(
	updateActiveness func(
		ctx context.Context,
		client krst.KnitClient,
		planId string,
		isActive bool,
	) (apiplans.Detail, error),
) func(*Option) *Option {
	return func(cmd *Option) *Option {
		cmd.updateActiveness = updateActiveness
		return cmd
	}
}

const (
	ARG_PLAN_ID = "PLAN_ID"
	ARG_MODE    = "yes|no"
)

func New(options ...func(*Option) *Option) (flarc.Command, error) {
	opt := &Option{
		updateActiveness: UpdateActivatePlan,
	}

	for _, option := range options {
		opt = option(opt)
	}

	return flarc.NewCommand(
		"Activate or deactivate the Plan.",
		struct{}{},
		flarc.Args{
			{
				Name: ARG_MODE, Required: true,
				Help: "Set 'yes' to activate a Plan, or 'no' to deactivate.",
			},
			{
				Name: ARG_PLAN_ID, Required: true,
				Help: "A Plan id to be changed its activeness.",
			},
		},
		common.NewTask(Task(opt.updateActiveness)),
		flarc.WithDescription(`
When "{{ .Command }} yes", the Plan specified by Plan id is activated.
Runs of the Plan in "deactivated" status are changed to "waiting".
If the Plan is already active, do nothing and return the status as is.

When "{{ .Command }} no", the Plan specified by Plan id is deactivated.
Runs of the Plan in "waiting" status are changed to "deactivated".
If the Plan is already deactivated, do nothing and return the status as is.
`),
	)
}

func Task(
	updateActiveness func(
		ctx context.Context,
		client krst.KnitClient,
		planId string,
		isActive bool,
	) (apiplans.Detail, error),
) func(
	ctx context.Context,
	logger *log.Logger,
	e env.KnitEnv,
	client krst.KnitClient,
	flags flarc.Commandline[struct{}],
	_ []any,
) error {
	return func(
		ctx context.Context,
		logger *log.Logger,
		e env.KnitEnv,
		client krst.KnitClient,
		cl flarc.Commandline[struct{}],
		_ []any,
	) error {
		args := cl.Args()
		mode := args[ARG_MODE][0]
		planId := args[ARG_PLAN_ID][0]

		var isActive bool
		switch mode {
		case "yes":
			isActive = true
		case "no":
			isActive = false
		default:
			logger.Println("mode should be one of: yes, no")
			return flarc.ErrUsage
		}
		data, err := updateActiveness(ctx, client, planId, isActive)
		if err != nil {
			return fmt.Errorf("planId:%v: %w", planId, err)
		}

		enc := json.NewEncoder(cl.Stdout())
		enc.SetIndent("", "    ")
		if err := enc.Encode(data); err != nil {
			logger.Panicf("fail to dump found data")
		}

		return nil
	}

}

func UpdateActivatePlan(
	ctx context.Context,
	client krst.KnitClient,
	planId string,
	isActive bool,
) (apiplans.Detail, error) {

	result, err := client.PutPlanForActivate(ctx, planId, isActive)
	if err != nil {
		return apiplans.Detail{}, err
	}

	return result, nil
}
