package serviceaccount

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	"github.com/youta-t/flarc"
)

type Flag struct {
	Set   string `flag:"set" help:"Set the service account name. Exclusive with --unset."`
	Unset bool   `flag:"unset" help:"Unset the service account name. Exclusive with --set."`
}

const ARGS_PLAN_ID = "PLAN_ID"

func New() (flarc.Command, error) {
	return flarc.NewCommand(
		"set or unset the service account name on a plan",
		Flag{},
		flarc.Args{
			{
				Name: ARGS_PLAN_ID, Required: true,
				Help: "Specify the id of the Plan to be updated its service account name.",
			},
		},
		common.NewTask(Task()),
		flarc.WithDescription(`
Set or unset the service account name on a Plan.

The specified service account name will be used to execute Runs of the Plan.

For service accounts which can be used, ask your Knitfab administrator.
`),
	)
}

func Task() common.Task[Flag] {
	return func(
		ctx context.Context,
		logger *log.Logger,
		knitEnv env.KnitEnv,
		client rest.KnitClient,
		cl flarc.Commandline[Flag],
		params []any,
	) error {
		planId := cl.Args()[ARGS_PLAN_ID][0]

		if cl.Flags().Set != "" && cl.Flags().Unset {
			return fmt.Errorf("%w: cannot use --set and --unset at once", flarc.ErrUsage)
		}

		var resp plans.Detail
		if cl.Flags().Set != "" {
			_r, err := client.SetServiceAccount(
				ctx, planId, plans.SetServiceAccount{ServiceAccount: cl.Flags().Set},
			)
			if err != nil {
				return err
			}
			resp = _r
			logger.Printf("Plan %s: serviceaccount is set", planId)
		} else if cl.Flags().Unset {
			_r, err := client.UnsetServiceAccount(ctx, planId)
			if err != nil {
				return err
			}
			resp = _r
			logger.Printf("Plan %s: serviceaccount is unset", planId)
		} else {
			return fmt.Errorf("%w: either --set or --unset is required", flarc.ErrUsage)
		}

		j := json.NewEncoder(cl.Stdout())
		j.SetIndent("", "    ")
		if err := j.Encode(resp); err != nil {
			return err
		}

		return nil
	}
}
