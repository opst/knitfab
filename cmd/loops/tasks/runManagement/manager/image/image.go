package image

import (
	"context"

	manager "github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager"
	kdb "github.com/opst/knitfab/pkg/db"
	kw "github.com/opst/knitfab/pkg/workloads/worker"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
)

// Returns a manager for starting a worker for a run.
func New(
	getWorker func(context.Context, kdb.Run) (kw.Worker, error),
	startWorker func(context.Context, kdb.Run) error,
	setExit func(ctx context.Context, runId string, exit kdb.RunExit) error,
) manager.Manager {
	return func(
		ctx context.Context, r kdb.Run,
	) (kdb.KnitRunStatus, error) {
		w, err := getWorker(ctx, r)
		if err != nil {
			if !kubeerr.IsNotFound(err) {
				return r.Status, err
			}
			if r.Status != kdb.Ready {
				if err := setExit(ctx, r.Id, kdb.RunExit{
					Code:    254,
					Message: "worker for the run is not found",
				}); err != nil {
					return r.Status, err
				}
				return kdb.Aborting, nil
			}

			err := startWorker(ctx, r)
			if err == nil || kubeerr.IsAlreadyExists(err) {
				return kdb.Starting, nil
			}
			return r.Status, err
		}

		switch s := w.JobStatus(); s {
		case kw.Pending:
			return kdb.Starting, nil
		case kw.Running:
			return kdb.Running, nil
		case kw.Done, kw.Failed:
			if exitCode, reason, ok := w.ExitCode(); ok {
				if err := setExit(ctx, r.Id, kdb.RunExit{
					Code:    exitCode,
					Message: reason,
				}); err != nil {
					return r.Status, err
				}
			}

			if s == kw.Failed {
				return kdb.Aborting, nil
			}
			return kdb.Completing, nil
		}

		return r.Status, nil
	}
}
