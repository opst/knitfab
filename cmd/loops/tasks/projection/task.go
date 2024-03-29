package projection

import (
	"context"
	"log"

	"github.com/opst/knitfab/cmd/loops/recurring"
	kdb "github.com/opst/knitfab/pkg/db"
)

// initial value for task
func Seed() struct{} {
	return struct{}{}
}

// return:
//
// - task : creating new runs in waiting/deactivated state.
func Task(logger *log.Logger, dbrun kdb.RunInterface) recurring.Task[struct{}] {
	return func(ctx context.Context, value struct{}) (struct{}, bool, error) {
		logger.Printf("checking...")
		runId, triggered, err := dbrun.New(ctx)

		if triggered != nil {
			logger.Printf("triggered: %s -> new run id(s) = %v\n", triggered, runId)
		} else {
			logger.Printf("nothing new.")
		}

		return value, triggered != nil, err
	}
}
