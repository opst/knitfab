package run

import (
	"github.com/google/subcommands"
	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	run_find "github.com/opst/knitfab/cmd/knit/subcommands/run/find"
	run_retry "github.com/opst/knitfab/cmd/knit/subcommands/run/retry"
	run_rm "github.com/opst/knitfab/cmd/knit/subcommands/run/rm"
	run_show "github.com/opst/knitfab/cmd/knit/subcommands/run/show"
	run_stop "github.com/opst/knitfab/cmd/knit/subcommands/run/stop"
)

func New(cf kcmd.CommonFlags) subcommands.Command {
	commander := kcmd.NewCommander(
		"run",
		kcmd.Help{
			Synopsis: "manipulating Run",
		},
	)

	commander.Register(kcmd.Build(run_show.New(), cf))
	commander.Register(kcmd.Build(run_find.New(), cf))
	commander.Register(kcmd.Build(run_stop.New(), cf))
	commander.Register(kcmd.Build(run_rm.New(), cf))
	commander.Register(kcmd.Build(run_retry.New(), cf))

	return commander
}
