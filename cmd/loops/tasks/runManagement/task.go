package runManagement

import (
	"context"
	"errors"
	"time"

	"github.com/opst/knitfab/cmd/loops/loop/recurring"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/runManagementHook"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	"github.com/opst/knitfab/pkg/domain"
	kdbrun "github.com/opst/knitfab/pkg/domain/run/db"
)

// Return initial RunCursor value for task
func Seed(pseudoPlans []domain.PseudoPlanName) domain.RunCursor {
	return domain.RunCursor{
		// Status of the runs to be monitored
		Status:   []domain.KnitRunStatus{domain.Ready, domain.Starting, domain.Running},
		Pseudo:   pseudoPlans,
		Debounce: 30 * time.Second,
	}
}

// return:
//
// - task: detect status changes of runs (starting -> running -> completing/aborting) and
// update run status.
func Task(
	irun kdbrun.Interface,
	imageManager manager.Manager,
	pseudoManagers map[domain.PseudoPlanName]manager.Manager,
	hooks runManagementHook.Hooks,
) recurring.Task[domain.RunCursor] {
	return func(ctx context.Context, value domain.RunCursor) (domain.RunCursor, bool, error) {
		nextCursor, statusChanged, err := irun.PickAndSetStatus(
			ctx, value,
			// The last Status set by PickAndSetStatus() is the return value of func() below.
			func(r domain.Run) (domain.KnitRunStatus, error) {

				var newStatus domain.KnitRunStatus
				var err error
				if r.PlanBody.Pseudo == nil {
					newStatus, err = imageManager(ctx, hooks, r)
				} else {
					// m is a Manager for a specific PseudoPlan
					m, ok := pseudoManagers[r.PlanBody.Pseudo.Name]
					if !ok {
						return r.Status, nil
					}
					newStatus, err = m(ctx, hooks, r)
				}
				return newStatus, err
			},
		)

		if statusChanged {
			if newRuns, _ := irun.Get(ctx, []string{nextCursor.Head}); newRuns != nil {
				if r, ok := newRuns[nextCursor.Head]; ok {
					hookValue := bindruns.ComposeDetail(r)
					switch r.Status {
					case domain.Starting:
						hooks.ToStarting.After(hookValue)
					case domain.Running:
						hooks.ToRunning.After(hookValue)
					case domain.Completing:
						hooks.ToCompleting.After(hookValue)
					case domain.Aborting:
						hooks.ToAborting.After(hookValue)
					}
				}
			}
		}

		curoseMoved := !nextCursor.Equal(value)

		// Context cancelled/deadline exceeded are okay. It will be retried.
		if err == nil ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, domain.ErrInvalidRunStateChanging) {
			return nextCursor, curoseMoved, nil
		}
		return nextCursor, curoseMoved, err
	}
}
