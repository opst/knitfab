package purge

import (
	"context"
	"log"

	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	"github.com/youta-t/flarc"
)

const KNIT_ID string = "knit-id"

func New() (flarc.Command, error) {
	return flarc.NewCommand(
		"",
		struct{}{},
		flarc.Args{
			{
				Name:     KNIT_ID,
				Required: true,
				Help:     "The Knit ID of the Data to be purged.",
			},
		},
		common.NewTask(Task()),

		flarc.WithDescription(`
Purge Data from the Knitfab.

The Data content will be removed from the Knitfab and the storage.
On the other hand, the Data metadata will be kept. So {{ .Command }} will not brake the lineage.

Purged Data cannot be downloaded (with data pull).
Ayn Runs using the purged Data will not be created.

Once the Data is purged, it cannot be restored.

Purging will fail when the Data is used in the Run or downlaoded by the user.

Exmaple
-------

Purge the Data with a Knit ID:

	{{ .Command }} KNIT-ID
`),
	)
}

func Task() common.Task[struct{}] {
	return func(
		ctx context.Context,
		logger *log.Logger,
		knitEnv env.KnitEnv,
		client rest.KnitClient,
		cl flarc.Commandline[struct{}],
		params []any,
	) error {
		knitId := cl.Args()[KNIT_ID][0]

		err := client.PurgeData(ctx, knitId)

		if err != nil {
			return err
		}

		logger.Printf("Data is purged: knit#id:%s.\n", knitId)

		return nil
	}
}
