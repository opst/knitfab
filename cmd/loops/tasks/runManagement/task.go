package runManagement

import (
	"context"
	"errors"
	"time"

	"github.com/opst/knitfab/cmd/loops/recurring"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager"
	kdb "github.com/opst/knitfab/pkg/db"
)

// Return initial RunCursor value for task
func Seed(pseudoPlans []kdb.PseudoPlanName) kdb.RunCursor {
	return kdb.RunCursor{
		// Status of the runs to be monitored
		Status:   []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
		Pseudo:   pseudoPlans,
		Debounce: 30 * time.Second,
	}
}

// return:
//
// - task: detect status changes of runs (starting -> running -> completing/aborting) and
// update run status.
func Task(
	irun kdb.RunInterface,
	imageManager manager.Manager,
	pseudoManagers map[kdb.PseudoPlanName]manager.Manager,
) recurring.Task[kdb.RunCursor] {
	return func(ctx context.Context, value kdb.RunCursor) (kdb.RunCursor, bool, error) {
		nextCursor, err := irun.PickAndSetStatus(
			ctx, value,
			// The last Status set by PickAndSetStatus() is the return value of func() below.
			func(r kdb.Run) (kdb.KnitRunStatus, error) {
				if r.PlanBody.Pseudo != nil {
					// m is a Manager for a specific PseudoPlan
					m, ok := pseudoManagers[r.PlanBody.Pseudo.Name]
					if !ok {
						return r.Status, nil
					}
					return m(ctx, r)
				}
				return imageManager(ctx, r)
			},
		)

		// Context cancelled/deadline exceeded are okay. It will be retried.
		if err == nil ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, kdb.ErrInvalidRunStateChanging) {
			return nextCursor, !value.Equal(nextCursor), nil
		}
		return nextCursor, !value.Equal(nextCursor), err
	}
}
