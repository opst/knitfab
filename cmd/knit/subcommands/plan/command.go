package plan

import (
	"github.com/google/subcommands"
	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	plan_active "github.com/opst/knitfab/cmd/knit/subcommands/plan/active"
	plan_apply "github.com/opst/knitfab/cmd/knit/subcommands/plan/apply"
	plan_find "github.com/opst/knitfab/cmd/knit/subcommands/plan/find"
	plan_resource "github.com/opst/knitfab/cmd/knit/subcommands/plan/resource"
	plan_show "github.com/opst/knitfab/cmd/knit/subcommands/plan/show"
	plan_template "github.com/opst/knitfab/cmd/knit/subcommands/plan/template"
)

func New(cf kcmd.CommonFlags) subcommands.Command {
	commander := kcmd.NewCommander(
		"plan",
		kcmd.Help{
			Synopsis: "manipulating Plan",
		},
	)
	commander.Register(kcmd.Build(plan_show.New(), cf))
	commander.Register(kcmd.Build(plan_find.New(), cf))
	commander.Register(kcmd.Build(plan_template.New(), cf))
	commander.Register(kcmd.Build(plan_apply.New(), cf))
	commander.Register(kcmd.Build(plan_active.New(), cf))
	commander.Register(kcmd.Build(plan_resource.New(), cf))

	return commander
}
