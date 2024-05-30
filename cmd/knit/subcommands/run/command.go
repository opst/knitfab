package run

import (
	run_find "github.com/opst/knitfab/cmd/knit/subcommands/run/find"
	run_retry "github.com/opst/knitfab/cmd/knit/subcommands/run/retry"
	run_rm "github.com/opst/knitfab/cmd/knit/subcommands/run/rm"
	run_show "github.com/opst/knitfab/cmd/knit/subcommands/run/show"
	run_stop "github.com/opst/knitfab/cmd/knit/subcommands/run/stop"
	"github.com/youta-t/flarc"
)

func New() (flarc.Command, error) {

	show, err := run_show.New()
	if err != nil {
		return nil, err
	}
	find, err := run_find.New()
	if err != nil {
		return nil, err
	}
	stop, err := run_stop.New()
	if err != nil {
		return nil, err
	}
	rm, err := run_rm.New()
	if err != nil {
		return nil, err
	}
	retry, err := run_retry.New()
	if err != nil {
		return nil, err
	}

	return flarc.NewCommandGroup(
		"Manipulate Knitfab Run.",
		struct{}{},
		flarc.WithSubcommand("show", show),
		flarc.WithSubcommand("find", find),
		flarc.WithSubcommand("stop", stop),
		flarc.WithSubcommand("rm", rm),
		flarc.WithSubcommand("retry", retry),
	)
}
