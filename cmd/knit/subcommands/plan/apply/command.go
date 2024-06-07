package apply

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/opst/knitfab/cmd/knit/env"
	krest "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	apiplans "github.com/opst/knitfab/pkg/api/types/plans"
	"github.com/youta-t/flarc"
	"gopkg.in/yaml.v3"
)

type Option struct {
	applyfunc func(context.Context, krest.KnitClient, apiplans.PlanSpec) (apiplans.Detail, error)
}

func WithApply(
	apply func(context.Context, krest.KnitClient, apiplans.PlanSpec) (apiplans.Detail, error),
) func(*Option) *Option {
	return func(dfc *Option) *Option {
		dfc.applyfunc = apply
		return dfc
	}
}

const (
	ARG_PLAN_FILE = "PLAN_FILE"
)

func New(options ...func(*Option) *Option) (flarc.Command, error) {
	option := &Option{
		applyfunc: ApplyPlan,
	}
	for _, opt := range options {
		option = opt(option)
	}

	return flarc.NewCommand(
		"Apply Plan file as a Plan in knitfab.",
		struct{}{},
		flarc.Args{
			{
				Name: ARG_PLAN_FILE, Required: true,
				Help: "Path to the Plan file. If you need it, try `knit plan template`",
			},
		},
		common.NewTask(Task(option.applyfunc)),
	)
}

func Task(
	applyFunc func(context.Context, krest.KnitClient, apiplans.PlanSpec) (apiplans.Detail, error),
) common.Task[struct{}] {
	return func(
		ctx context.Context,
		logger *log.Logger,
		knitEnv env.KnitEnv,
		client krest.KnitClient,
		cl flarc.Commandline[struct{}],
		params []any,
	) error {
		args := cl.Args()
		buf, err := os.ReadFile(args[ARG_PLAN_FILE][0])
		if err != nil {
			return fmt.Errorf("fail to read Plan file: %w", err)
		}

		spec := new(apiplans.PlanSpec)
		if err := yaml.Unmarshal(buf, spec); err != nil {
			return fmt.Errorf("fail to parse Plan file: %w", err)
		}

		data, err := applyFunc(ctx, client, *spec)
		if err != nil {
			return fmt.Errorf("failed to apply Plan: %w", err)
		}

		enc := json.NewEncoder(cl.Stdout())
		enc.SetIndent("", "    ")
		if err := enc.Encode(data); err != nil {
			return err
		}

		return nil
	}
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
