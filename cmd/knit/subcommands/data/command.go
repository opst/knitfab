package data

import (
	data_find "github.com/opst/knitfab/cmd/knit/subcommands/data/find"
	data_lineage "github.com/opst/knitfab/cmd/knit/subcommands/data/lineage"
	data_pull "github.com/opst/knitfab/cmd/knit/subcommands/data/pull"
	data_push "github.com/opst/knitfab/cmd/knit/subcommands/data/push"
	data_tag "github.com/opst/knitfab/cmd/knit/subcommands/data/tag"
	"github.com/youta-t/flarc"
)

func New() (flarc.Command, error) {
	find, err := data_find.New()
	if err != nil {
		return nil, err
	}
	pull, err := data_pull.New()
	if err != nil {
		return nil, err
	}
	push, err := data_push.New()
	if err != nil {
		return nil, err
	}
	tag, err := data_tag.New()
	if err != nil {
		return nil, err
	}

	lineage, err := data_lineage.New()
	if err != nil {
		return nil, err
	}

	return flarc.NewCommandGroup(
		"Manupirate Knifab Data and Tags.",
		struct{}{},
		flarc.WithSubcommand("find", find),
		flarc.WithSubcommand("pull", pull),
		flarc.WithSubcommand("push", push),
		flarc.WithSubcommand("tag", tag),
		flarc.WithSubcommand("lineage", lineage),
	)
}
