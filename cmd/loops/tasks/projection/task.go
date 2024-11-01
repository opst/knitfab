package projection

import (
	"context"
	"log"

	"github.com/opst/knitfab/cmd/loops/loop/recurring"
	kdbrun "github.com/opst/knitfab/pkg/domain/run/db"
)

// initial value for task
func Seed() struct{} {
	return struct{}{}
}

// return:
//
// - task : creating new runs in waiting/deactivated state.
func Task(logger *log.Logger, dbrun kdbrun.Interface) recurring.Task[struct{}] {
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
