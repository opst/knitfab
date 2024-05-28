package credits

import (
	"context"

	"github.com/youta-t/flarc"
)

func New(msg string) (flarc.Command, error) {
	return flarc.NewCommand(
		"Show licenses of dependencies.",
		struct{}{},
		flarc.Args{},
		func(ctx context.Context, c flarc.Commandline[struct{}], a []any) error {
			_, err := c.Stdout().Write([]byte(msg))
			return err
		},
	)
}
