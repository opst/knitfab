package resource

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	apiplans "github.com/opst/knitfab/pkg/api/types/plans"
	"github.com/youta-t/flarc"
	"k8s.io/apimachinery/pkg/api/resource"
)

type Flag struct {
	Set   *ResourceQuantityList `flag:"set" alias:"s" help:"Set resource limits for a Plan. Repeatable"`
	Unset *Types                `flag:"unset" alias:"u" help:"Unset resource limits for a Plan. Repeatable"`
}

const ARGS_PLAN_ID = "PLAN_ID"

func New() (flarc.Command, error) {
	return flarc.NewCommand(
		"Set or unset resource limits for a Plan.",
		Flag{
			Set:   &ResourceQuantityList{},
			Unset: &Types{},
		},
		flarc.Args{
			{
				Name: ARGS_PLAN_ID, Required: true,
				Help: "Specify the id of the Plan to be updated its resource limits.",
			},
		},
		common.NewTask(Task()),
		flarc.WithDescription(`
Set or unset resource limits for a Plan.

knitfab starts a Run if the Plan's resources limits are met with available computing resource.
Otherwise, Runs will be "starting", but not get be "running".

To set, pass --set flag with key-value pairs, like "cpu=1" or "memory=1Gi", where the key is the resource type and the value is the quantity.
The value for "cpu" is number of cores to be used for the Plan, and the value for "memory" is the amount of memory in bytes to be used for the Plan.
For other resource types (like gpu), ask your administrator for the correct values.
Quantity supports the following suffixes: "K"(x1000), "Ki"(x1024), "M"(x1000^2), "Mi"(x1024^2) or "G", "Gi", "T", "Ti", "P", "Pi", "E", "Ei" and "m" (x 1/1000).
Suffixes are case sensitive.

    {{ .Command }} --set cpu=1 --set memory=1Gi

To unset, pass --unset flag with the resource type, like "cpu" or "memory".

    {{ .Command }} --unset cpu --unset memory

Even if you unset cpu or memory, the default value will be used: "1" for cpu and "1Gi" for memory.

Flags --set and --unset are repeatable and can be passed at once.
If you pass --set for same key multiple times, the last value will take precedence.
If you pass --set and --unset for same key, --set will take precedence.`,
		),
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
		flag := cl.Flags()
		if flag.Set.Empty() && flag.Unset.Empty() {
			logger.Println("Nothing to do.")
			return nil
		}

		planId := cl.Args()[ARGS_PLAN_ID][0]

		change := apiplans.ResourceLimitChange{
			Set:   apiplans.Resources(flag.Set.Map()),
			Unset: flag.Unset.Slice(),
		}

		pln, err := client.UpdateResources(ctx, planId, change)
		if err != nil {
			return err
		}

		logger.Printf("Plan:%s is updated.\n", pln.PlanId)

		buf, err := json.MarshalIndent(pln, "", "  ")
		if err != nil {
			return err
		}
		if _, err := cl.Stdout().Write(buf); err != nil {
			return err
		}

		return nil
	}
}

type ResourceQuantity struct {
	Type     string
	Quantity resource.Quantity
}

func (r *ResourceQuantity) String() string {
	return r.Type + "=" + r.Quantity.String()
}

func (r *ResourceQuantity) Set(value string) error {
	return nil
}

type ResourceQuantityList map[string]resource.Quantity

func (rqn *ResourceQuantityList) Empty() bool {
	return rqn == nil || len(*rqn) == 0
}

func (rqn *ResourceQuantityList) Map() map[string]resource.Quantity {
	if rqn == nil {
		return map[string]resource.Quantity{}
	}
	return *rqn
}

func (rqn *ResourceQuantityList) String() string {
	var strrq []string
	for _, rq := range *rqn {
		strrq = append(strrq, rq.String())
	}
	return strings.Join(strrq, " ")
}

func (rqn ResourceQuantityList) Set(value string) error {
	parts := strings.Split(value, "=")
	if len(parts) != 2 {
		return fmt.Errorf("invalid resource quantity: %s", value)
	}
	typ := parts[0]
	q, err := resource.ParseQuantity(parts[1])
	if err != nil {
		return fmt.Errorf("invalid resource quantity: %s", value)
	}
	rqn[typ] = q
	return nil
}

type Types []string

func (n *Types) Empty() bool {
	return n == nil || len(*n) == 0
}

func (n *Types) Slice() []string {
	if n == nil {
		return []string{}
	}
	return *n
}

func (n *Types) String() string {
	return strings.Join(*n, " ")
}

func (n *Types) Set(value string) error {
	*n = append(*n, value)
	return nil
}
