package finishing

import (
	"context"
	"errors"
	"time"

	apiruns "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/loop/recurring"
	"github.com/opst/knitfab/pkg/api-types-binding/runs"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	"github.com/opst/knitfab/pkg/domain"
	k8serrors "github.com/opst/knitfab/pkg/domain/errors/k8serrors"
	kdbrun "github.com/opst/knitfab/pkg/domain/run/db"
	k8srun "github.com/opst/knitfab/pkg/domain/run/k8s"
)

// initial value for task
func Seed(pseudoPlans []domain.PseudoPlanName) domain.RunCursor {
	return domain.RunCursor{
		// statuses of the target runs for finishing
		Status:   []domain.KnitRunStatus{domain.Completing, domain.Aborting},
		Pseudo:   pseudoPlans,
		Debounce: 30 * time.Second,
	}
}

// Task for finishing loop.
//
//	Identify runs that should be stopped and finalize the runs and data while releasing k8s resources.
//
// return:
//
// - task: let the Run finished (completing -> done, aborting -> failed) and
// update run status.
func Task(
	iDbRun kdbrun.Interface,
	iK8sRun k8srun.Interface,
	hook hook.Hook[apiruns.Detail, struct{}],
) recurring.Task[domain.RunCursor] {
	return func(ctx context.Context, cursor domain.RunCursor) (domain.RunCursor, bool, error) {
		nextCursor, statusChanged, err := iDbRun.PickAndSetStatus(
			ctx, cursor,
			func(targetRun domain.Run) (domain.KnitRunStatus, error) {
				var nextState domain.KnitRunStatus
				switch targetRun.Status {
				case domain.Completing:
					nextState = domain.Done
				case domain.Aborting:
					nextState = domain.Failed
				default:
					// fatal
					return targetRun.Status, errors.New("unexpected run status: assertion error")
				}

				hookValue := runs.ComposeDetail(targetRun)

				if _, err := hook.Before(hookValue); err != nil {
					return targetRun.Status, err
				}

				// (1) Delete the worker in k8s if it exists
				// Check if the worker exists
				if name := targetRun.WorkerName; name != "" {
					//
					// worker would exist:
					//

					// find the worker corresponding to targetRun
					worker, err := iK8sRun.FindWorker(ctx, targetRun.RunBody)
					// is err ErrMissing?
					if k8serrors.AsMissingError(err) {
						// NOP: no worker exists.
					} else if err != nil {
						return targetRun.Status, err
					} else {

						// there is worker. shutdown it.
						if err := worker.Close(); err != nil {
							return targetRun.Status, err // fatal error
						}
					}

					// (2) Delete the record corresponding to the run in the DB.
					if err := iDbRun.DeleteWorker(ctx, targetRun.Id); err != nil {
						return targetRun.Status, err
					}
				}

				return nextState, nil
			},
		)

		if statusChanged {
			if runs, _ := iDbRun.Get(ctx, []string{nextCursor.Head}); runs != nil {
				if r, ok := runs[nextCursor.Head]; ok {
					hookVal := bindruns.ComposeDetail(r)
					hook.After(hookVal)
				}
			}
		}

		return nextCursor, !cursor.Equal(nextCursor), err
	}
}
