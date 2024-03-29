package credits

import (
	"context"
	_ "embed"
	"flag"
	"fmt"

	"github.com/google/subcommands"
	"github.com/opst/knitfab/pkg/buildtime"
)

func New() subcommands.Command {
	return &Command{}
}

type Command struct {
	Messasge string
}

func (c *Command) Name() string {
	return "version"
}

func (c *Command) Synopsis() string {
	return "show version"
}

func (c *Command) Usage() string {
	return "version"
}

func (c *Command) SetFlags(f *flag.FlagSet) {
}

func (c *Command) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	fmt.Println(buildtime.VersionString())
	return subcommands.ExitSuccess
}
