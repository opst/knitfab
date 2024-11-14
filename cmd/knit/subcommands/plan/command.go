package plan

import (
	plan_active "github.com/opst/knitfab/cmd/knit/subcommands/plan/active"
	plan_annotate "github.com/opst/knitfab/cmd/knit/subcommands/plan/annotate"
	plan_apply "github.com/opst/knitfab/cmd/knit/subcommands/plan/apply"
	plan_find "github.com/opst/knitfab/cmd/knit/subcommands/plan/find"
	plan_graph "github.com/opst/knitfab/cmd/knit/subcommands/plan/graph"
	plan_resource "github.com/opst/knitfab/cmd/knit/subcommands/plan/resource"
	plan_serviceaccount "github.com/opst/knitfab/cmd/knit/subcommands/plan/serviceaccount"
	plan_show "github.com/opst/knitfab/cmd/knit/subcommands/plan/show"
	plan_template "github.com/opst/knitfab/cmd/knit/subcommands/plan/template"
	"github.com/youta-t/flarc"
)

func New() (flarc.Command, error) {

	show, err := plan_show.New()
	if err != nil {
		return nil, err
	}

	find, err := plan_find.New()
	if err != nil {
		return nil, err
	}

	template, err := plan_template.New()
	if err != nil {
		return nil, err
	}

	apply, err := plan_apply.New()
	if err != nil {
		return nil, err
	}

	active, err := plan_active.New()
	if err != nil {
		return nil, err
	}

	resource, err := plan_resource.New()
	if err != nil {
		return nil, err
	}

	annotate, err := plan_annotate.New()
	if err != nil {
		return nil, err
	}

	serviceaccount, err := plan_serviceaccount.New()
	if err != nil {
		return nil, err
	}

	graph, err := plan_graph.New()
	if err != nil {
		return nil, err
	}

	return flarc.NewCommandGroup(
		"Manipulate Knitfab Plan.",
		struct{}{},
		flarc.WithSubcommand("show", show),
		flarc.WithSubcommand("find", find),
		flarc.WithSubcommand("graph", graph),
		flarc.WithSubcommand("template", template),
		flarc.WithSubcommand("apply", apply),
		flarc.WithSubcommand("active", active),
		flarc.WithSubcommand("resource", resource),
		flarc.WithSubcommand("annotate", annotate),
		flarc.WithSubcommand("serviceaccount", serviceaccount),
	)

}
