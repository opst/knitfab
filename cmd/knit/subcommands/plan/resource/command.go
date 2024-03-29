package resource

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	apiplans "github.com/opst/knitfab/pkg/api/types/plans"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"k8s.io/apimachinery/pkg/api/resource"
)

type Command struct {
	out io.Writer
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

func (n *Types) String() string {
	return strings.Join(*n, " ")
}

func (n *Types) Set(value string) error {
	*n = append(*n, value)
	return nil
}

type Flag struct {
	Set   ResourceQuantityList `flag:"set,short=s,help=Set resource limits for a Plan. Repeatable"`
	Unset Types                `flag:"unset,short=u,help=Unset resource limits for a Plan. Repeatable"`
}

type Option func(*Command) *Command

func WithOutput(out io.Writer) Option {
	return func(cmd *Command) *Command {
		cmd.out = out
		return cmd
	}
}

func New(opt ...Option) kcmd.KnitCommand[Flag] {
	cmd := &Command{
		out: os.Stdout,
	}
	for _, o := range opt {
		cmd = o(cmd)
	}
	return cmd
}

func (cmd *Command) Name() string {
	return "resource"
}

func (cmd *Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "Set or unset resource limits for a Plan.",
		Detail: `
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
If you pass --set and --unset for same key, --set will take precedence.
`,
	}
}

const ARGS_PLAN_ID = "PLAN_ID"

func (cmd *Command) Usage() usage.Usage[Flag] {
	return usage.New[Flag](
		Flag{
			Set:   ResourceQuantityList{},
			Unset: Types{},
		},
		usage.Args{
			{
				Name: ARGS_PLAN_ID, Required: true,
				Help: "Specify the id of the Plan to be updated its resource limits.",
			},
		},
	)
}

func (cmd *Command) Execute(
	ctx context.Context,
	l *log.Logger,
	e env.KnitEnv,
	client krst.KnitClient,
	flag usage.FlagSet[Flag],
) error {
	if len(flag.Flags.Set) == 0 && len(flag.Flags.Unset) == 0 {
		l.Println("Nothing to do.")
		return nil
	}

	planId := flag.Args[ARGS_PLAN_ID][0]

	change := apiplans.ResourceLimitChange{
		Set:   apiplans.Resources(flag.Flags.Set),
		Unset: flag.Flags.Unset,
	}

	pln, err := client.UpdateResources(ctx, planId, change)
	if err != nil {
		return err
	}

	l.Printf("Plan:%s is updated.\n", pln.PlanId)

	buf, err := json.MarshalIndent(pln, "", "  ")
	if err != nil {
		return err
	}
	if _, err := cmd.out.Write(buf); err != nil {
		return err
	}

	return nil
}
