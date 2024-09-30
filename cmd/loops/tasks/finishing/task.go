package finishing

import (
	"context"
	"errors"
	"time"

	apiruns "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/recurring"
	"github.com/opst/knitfab/pkg/api-types-binding/runs"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/workloads"
	k8s "github.com/opst/knitfab/pkg/workloads/k8s"
	"github.com/opst/knitfab/pkg/workloads/worker"
)

// initial value for task
func Seed(pseudoPlans []kdb.PseudoPlanName) kdb.RunCursor {
	return kdb.RunCursor{
		// statuses of the target runs for finishing
		Status:   []kdb.KnitRunStatus{kdb.Completing, kdb.Aborting},
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
	iDbRun kdb.RunInterface,
	find func(
		ctx context.Context,
		cluster k8s.Cluster,
		runBody kdb.RunBody,
	) (worker.Worker, error),
	cluster k8s.Cluster,
	hook hook.Hook[apiruns.Detail],
) recurring.Task[kdb.RunCursor] {
	return func(ctx context.Context, cursor kdb.RunCursor) (kdb.RunCursor, bool, error) {
		nextCursor, statusChanged, err := iDbRun.PickAndSetStatus(
			ctx, cursor,
			func(targetRun kdb.Run) (kdb.KnitRunStatus, error) {
				var nextState kdb.KnitRunStatus
				switch targetRun.Status {
				case kdb.Completing:
					nextState = kdb.Done
				case kdb.Aborting:
					nextState = kdb.Failed
				default:
					// fatal
					return targetRun.Status, errors.New("unexpected run status: assertion error")
				}

				hookValue := runs.ComposeDetail(targetRun)

				if err := hook.Before(hookValue); err != nil {
					return targetRun.Status, err
				}

				// (1) Delete the worker in k8s if it exists
				// Check if the worker exists
				if name := targetRun.WorkerName; name != "" {
					//
					// worker would exist:
					//

					// find the worker corresponding to targetRun
					worker, err := find(ctx, cluster, targetRun.RunBody)
					// is err ErrMissing?
					if workloads.AsMissingError(err) {
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
