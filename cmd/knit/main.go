//go:generate go run github.com/Songmu/gocredits/cmd/gocredits@v0.3.0 -w
package main

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"os/signal"
	"path"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	subdata "github.com/opst/knitfab/cmd/knit/subcommands/data"
	subinit "github.com/opst/knitfab/cmd/knit/subcommands/init"
	sublic "github.com/opst/knitfab/cmd/knit/subcommands/license"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	subplan "github.com/opst/knitfab/cmd/knit/subcommands/plan"
	subrun "github.com/opst/knitfab/cmd/knit/subcommands/run"
	subver "github.com/opst/knitfab/cmd/knit/subcommands/version"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/youta-t/flarc"
)

//go:embed CREDITS
var CREDITS string

func main() {
	name := path.Base(os.Args[0])
	logger := logger.Default()
	logger.SetPrefix(fmt.Sprintf("[%s] ", name))

	ctx, cancel := signal.NotifyContext(
		context.Background(), os.Interrupt, os.Kill,
	)
	defer cancel()

	cf := try.To(kcmd.DefaultCommonFlags(".")).OrFatal(logger)
	init := try.To(subinit.New()).OrFatal(logger)
	data := try.To(subdata.New()).OrFatal(logger)
	run := try.To(subrun.New()).OrFatal(logger)
	plan := try.To(subplan.New()).OrFatal(logger)
	license := try.To(sublic.New(CREDITS)).OrFatal(logger)
	version := try.To(subver.New()).OrFatal(logger)

	knit := try.To(
		flarc.NewCommandGroup(
			"Knitfab Commandline interface",
			cf,
			flarc.WithSubcommand("init", init),
			flarc.WithSubcommand("data", data),
			flarc.WithSubcommand("run", run),
			flarc.WithSubcommand("plan", plan),
			flarc.WithSubcommand("license", license),
			flarc.WithSubcommand("version", version),
		),
	).OrFatal(logger)

	os.Exit(flarc.Run(ctx, knit, flarc.WithHelp(true)))
}
