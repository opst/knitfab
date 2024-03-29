package data

import (
	"github.com/google/subcommands"
	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	data_find "github.com/opst/knitfab/cmd/knit/subcommands/data/find"
	data_lineage "github.com/opst/knitfab/cmd/knit/subcommands/data/lineage"
	data_pull "github.com/opst/knitfab/cmd/knit/subcommands/data/pull"
	data_push "github.com/opst/knitfab/cmd/knit/subcommands/data/push"
	data_tag "github.com/opst/knitfab/cmd/knit/subcommands/data/tag"
)

func New(cf kcmd.CommonFlags) subcommands.Command {
	cmd := kcmd.NewCommander("data", kcmd.Help{
		Synopsis: "manipulating Data and its Tags",
	})

	cmd.Register(kcmd.Build(data_pull.New(), cf))
	cmd.Register(kcmd.Build(data_push.New(), cf))
	cmd.Register(kcmd.Build(data_tag.New(), cf))
	cmd.Register(kcmd.Build(data_find.New(), cf))
	cmd.Register(kcmd.Build(data_lineage.New(), cf))

	return cmd
}
