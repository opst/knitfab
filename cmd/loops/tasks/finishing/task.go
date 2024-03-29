package finishing

import (
	"context"
	"errors"
	"time"

	"github.com/opst/knitfab/cmd/loops/recurring"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/workloads"
	k8s "github.com/opst/knitfab/pkg/workloads/k8s"
	"github.com/opst/knitfab/pkg/workloads/worker"
)

// initial value for task
func Seed(
	pseudoPlans []kdb.PseudoPlanName) kdb.RunCursor {
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
) recurring.Task[kdb.RunCursor] {
	return func(ctx context.Context, cursor kdb.RunCursor) (kdb.RunCursor, bool, error) {
		nextCursor, err := iDbRun.PickAndSetStatus(
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
		return nextCursor, !cursor.Equal(nextCursor), err
	}
}
