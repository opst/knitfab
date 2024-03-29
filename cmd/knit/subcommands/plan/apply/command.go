package apply

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	"github.com/opst/knitfab/cmd/knit/env"
	krest "github.com/opst/knitfab/cmd/knit/rest"
	apiplans "github.com/opst/knitfab/pkg/api/types/plans"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"github.com/opst/knitfab/pkg/utils"
	"gopkg.in/yaml.v3"
)

type Command struct {
	task func(context.Context, krest.KnitClient, apiplans.PlanSpec) (apiplans.Detail, error)
}

func (*Command) Name() string {
	return "apply"
}

const (
	ARG_PLAN_FILE = "PLAN_FILE"
)

func (*Command) Usage() usage.Usage[struct{}] {
	return usage.New(
		struct{}{},
		usage.Args{
			{
				Name: ARG_PLAN_FILE, Required: true,
				Help: "Path to the Plan file. If you need it, try `knit plan template`",
			},
		},
	)
}

func (*Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "Apply Plan file as a Plan in knitfab.",
	}
}

func WithApplyTask(
	task func(context.Context, krest.KnitClient, apiplans.PlanSpec) (apiplans.Detail, error),
) func(*Command) *Command {
	return func(dfc *Command) *Command {
		dfc.task = task
		return dfc
	}
}

func New(options ...func(*Command) *Command) kcmd.KnitCommand[struct{}] {
	return utils.ApplyAll(
		&Command{task: ApplyPlan},
		options...,
	)
}

func (cmd *Command) Execute(
	ctx context.Context,
	logger *log.Logger,
	e env.KnitEnv,
	c krest.KnitClient,
	flags usage.FlagSet[struct{}],
) error {
	buf, err := os.ReadFile(flags.Args[ARG_PLAN_FILE][0])
	if err != nil {
		return fmt.Errorf("fail to read Plan file: %w", err)
	}

	spec := new(apiplans.PlanSpec)
	if err := yaml.Unmarshal(buf, spec); err != nil {
		return fmt.Errorf("fail to parse Plan file: %w", err)
	}

	data, err := cmd.task(ctx, c, *spec)
	if err != nil {
		return fmt.Errorf("failed to apply Plan: %w", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "    ")
	if err := enc.Encode(data); err != nil {
		return err
	}

	return nil
}

func ApplyPlan(
	ctx context.Context,
	client krest.KnitClient,
	spec apiplans.PlanSpec,
) (apiplans.Detail, error) {

	result, err := client.RegisterPlan(ctx, spec)
	if err != nil {
		return apiplans.Detail{}, err
	}
	return result, nil
}
