package credits

import (
	"context"
	_ "embed"

	"github.com/opst/knitfab/pkg/buildtime"
	"github.com/youta-t/flarc"
)

func New() (flarc.Command, error) {
	return flarc.NewCommand(
		"Show version of this command.",
		struct{}{},
		flarc.Args{},
		func(ctx context.Context, c flarc.Commandline[struct{}], a []any) error {
			c.Stdout().Write([]byte(buildtime.VersionString()))
			return nil
		},
	)
}
