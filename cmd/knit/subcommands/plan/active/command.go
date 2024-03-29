package plan

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"log"

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
		isActive bool,
	) (apiplans.Detail, error)
}

func WithTask(
	task func(
		ctx context.Context,
		client krst.KnitClient,
		planId string,
		isActive bool,
	) (apiplans.Detail, error),
) func(*Command) *Command {
	return func(cmd *Command) *Command {
		cmd.task = task
		return cmd
	}
}

func New(options ...func(*Command) *Command) kcmd.KnitCommand[struct{}] {
	return utils.ApplyAll(
		&Command{task: RunActivatePlan},
		options...,
	)
}

func (*Command) Name() string {
	return "active"
}

const (
	ARG_PLAN_ID = "PLAN_ID"
	ARG_MODE    = "yes|no"
)

func (*Command) Usage() usage.Usage[struct{}] {
	return usage.New(
		struct{}{},
		usage.Args{
			{
				Name: ARG_MODE, Required: true,
				Help: "Set 'yes' to activate a Plan, or 'no' to deactivate.",
			},
			{
				Name: ARG_PLAN_ID, Required: true,
				Help: "A Plan id to be changed its activeness.",
			},
		},
	)
}

func (*Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "Activate or deactivate the Plan.",
		Detail: `
When "{{ .Command }} yes", the Plan specified by Plan id is activated.
Runs of the Plan in "deactivated" status are changed to "waiting".
If the Plan is already active, do nothing and return the status as is.

When "{{ .Command }} no", the Plan specified by Plan id is deactivated.
Runs of the Plan in "waiting" status are changed to "deactivated".
If the Plan is already deactivated, do nothing and return the status as is.
`,
	}
}

func (cmd *Command) Execute(
	ctx context.Context,
	logger *log.Logger,
	e env.KnitEnv,
	client krst.KnitClient,
	flags usage.FlagSet[struct{}],
) error {
	mode := flags.Args[ARG_MODE][0]
	planId := flags.Args[ARG_PLAN_ID][0]

	var isActive bool
	switch mode {
	case "yes":
		isActive = true
	case "no":
		isActive = false
	default:
		logger.Println("mode should be one of: yes, no")
		return kcmd.ErrUsage
	}
	data, err := cmd.task(ctx, client, planId, isActive)
	if err != nil {
		return fmt.Errorf("planId:%v: %w", planId, err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "    ")
	if err := enc.Encode(data); err != nil {
		logger.Panicf("fail to dump found data")
	}

	return nil
}

func RunActivatePlan(
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
