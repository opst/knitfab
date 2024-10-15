package annotate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/youta-t/flarc"
)

type Annotation plans.Annotation

func (an *Annotation) Set(value string) error {
	k, v, ok := strings.Cut(value, "=")
	if !ok {
		return fmt.Errorf("invalid annotation: %s", value)
	}
	*an = Annotation(plans.Annotation{
		Key:   strings.TrimSpace(k),
		Value: strings.TrimSpace(v),
	})
	return nil
}

func (an *Annotation) String() string {
	return fmt.Sprintf("%s=%s", an.Key, an.Value)
}

type Flag struct {
	Add       []string `flag:"add" help:"Add an annotation in the form key=value. Repeatable."`
	Remove    []string `flag:"remove" help:"Remove an annotation in the form key=value. Repeatable."`
	RemoveKey []string `flag:"remove-key" help:"Remove an annotation by key. Repeatable."`
}

const ARGS_PLAN_ID = "PLAN_ID"

func New() (flarc.Command, error) {
	return flarc.NewCommand(
		"add or remove annotations on a plan",
		Flag{},
		flarc.Args{
			{
				Name: ARGS_PLAN_ID, Required: true,
				Help: "Specify the id of the Plan to be updated its annotations.",
			},
		},
		common.NewTask(Task()),
		flarc.WithDescription(`
Add and/or remove annotations on a Plan.

Annotations are key=value pairs that can be used to put metadata to a Plan.
Annotations are not used by Knitfab itself, but can be used by other tools or users.

To add,

    {{ .Command }} --add key1=value1 --add key2=value2

To remove exact key-value pair,

    {{ .Command }} --remove key1=value1 --remove key2=value2

To remove by key,

    {{ .Command }} --remove-key key1 --remove-key key2

Flags --add, --remove and --remove-key can be passed at once.
If do so, Knitfab applies "remove" first, then "add" (= addition take precedence).
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
		args := cl.Args()
		planId := args[ARGS_PLAN_ID][0]

		flags := cl.Flags()

		parseAnnotation := func(s string) (plans.Annotation, error) {
			k, v, ok := strings.Cut(s, "=")
			if !ok {
				return plans.Annotation{}, fmt.Errorf("invalid annotation: %s", s)
			}
			return plans.Annotation{
				Key:   strings.TrimSpace(k),
				Value: strings.TrimSpace(v),
			}, nil
		}

		add, err := utils.MapUntilError(flags.Add, parseAnnotation)
		if err != nil {
			return errors.Join(flarc.ErrUsage, err)
		}

		remove, err := utils.MapUntilError(flags.Remove, parseAnnotation)
		if err != nil {
			return errors.Join(flarc.ErrUsage, err)
		}

		change := plans.AnnotationChange{
			Add:       plans.Annotations(add),
			Remove:    plans.Annotations(remove),
			RemoveKey: flags.RemoveKey,
		}

		pln, err := client.UpdateAnnotations(ctx, planId, change)
		if err != nil {
			return err
		}

		logger.Printf("Plan:%s is updated.\n", pln.PlanId)

		enc := json.NewEncoder(cl.Stdout())
		enc.SetIndent("", "    ")
		if err := enc.Encode(pln); err != nil {
			return err
		}
		return nil
	}
}
