//go:generate go run github.com/Songmu/gocredits/cmd/gocredits@v0.3.0 -w
package main

import (
	"context"
	_ "embed"
	"flag"
	"os"
	"path"

	"github.com/google/subcommands"
	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	subdata "github.com/opst/knitfab/cmd/knit/subcommands/data"
	subinit "github.com/opst/knitfab/cmd/knit/subcommands/init"
	sublic "github.com/opst/knitfab/cmd/knit/subcommands/license"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	subplan "github.com/opst/knitfab/cmd/knit/subcommands/plan"
	subrun "github.com/opst/knitfab/cmd/knit/subcommands/run"
	subver "github.com/opst/knitfab/cmd/knit/subcommands/version"
	"github.com/opst/knitfab/pkg/utils/try"
)

//go:embed CREDITS
var CREDITS string

func main() {
	name := path.Base(os.Args[0])
	logger := logger.Default()
	logger.SetPrefix(name)

	ctx := context.Background()

	flag.Parse()

	subcommands.Register(subcommands.HelpCommand(), "help")
	subcommands.Register(subcommands.CommandsCommand(), "help")
	subcommands.Register(subcommands.FlagsCommand(), "help")

	cf := try.To(kcmd.DefaultCommonFlags(".")).OrFatal(logger)
	subcommands.Register(setParent(subinit.New(cf), name), "")
	subcommands.Register(setParent(sublic.New(CREDITS), name), "misc")
	subcommands.Register(setParent(subver.New(), name), "misc")

	subcommands.Register(setParent(subdata.New(cf), name), "")
	subcommands.Register(setParent(subplan.New(cf), name), "")
	subcommands.Register(setParent(subrun.New(cf), name), "")

	result := subcommands.Execute(ctx, logger)
	os.Exit(int(result))
}

func setParent(cmd subcommands.Command, parentName string) subcommands.Command {
	if c, ok := cmd.(interface{ SetParent(string) }); ok {
		c.SetParent(parentName)
	}
	return cmd
}
