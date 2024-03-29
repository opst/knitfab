package credits

import (
	"context"
	_ "embed"
	"flag"
	"fmt"

	"github.com/google/subcommands"
)

func New(msg string) subcommands.Command {
	return &Command{
		Messasge: msg,
	}
}

type Command struct {
	Messasge string
}

func (c *Command) Name() string {
	return "lisence"
}

func (c *Command) Synopsis() string {
	return "show licenses of dependencies"
}

func (c *Command) Usage() string {
	return "lisence"
}

func (c *Command) SetFlags(f *flag.FlagSet) {
}

func (c *Command) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	fmt.Println(c.Messasge)
	return subcommands.ExitSuccess
}
