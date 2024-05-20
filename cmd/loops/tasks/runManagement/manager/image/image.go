package image

import (
	"context"

	"github.com/opst/knitfab/cmd/loops/hook"
	manager "github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager"
	api_runs "github.com/opst/knitfab/pkg/api/types/runs"
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
		ctx context.Context, h hook.Hook[api_runs.Detail], r kdb.Run,
	) (kdb.KnitRunStatus, error) {
		w, err := getWorker(ctx, r)
		if err != nil {
			if !kubeerr.IsNotFound(err) {
				return r.Status, err
			}
			if err := h.Before(api_runs.ComposeDetail(r)); err != nil {
				return r.Status, err
			}

			if r.Status == kdb.Ready {
				err := startWorker(ctx, r)
				if err != nil && !kubeerr.IsAlreadyExists(err) {
					return r.Status, err
				}
				return kdb.Starting, nil
			}

			if err := setExit(ctx, r.Id, kdb.RunExit{
				Code:    254,
				Message: "worker for the run is not found",
			}); err != nil {
				return r.Status, err
			}
			return kdb.Aborting, nil
		}

		var newStatus kdb.KnitRunStatus
		switch s := w.JobStatus(); s {
		case kw.Pending:
			newStatus = kdb.Starting
		case kw.Running:
			newStatus = kdb.Running
		case kw.Failed:
			newStatus = kdb.Aborting
		case kw.Done:
			newStatus = kdb.Completing
		default:
			return r.Status, nil
		}

		if newStatus == r.Status {
			// no changes.
			return r.Status, nil
		}

		// status should be changed.

		if err := h.Before(api_runs.ComposeDetail(r)); err != nil {
			return r.Status, err
		}

		switch newStatus {
		case kdb.Aborting, kdb.Completing:
			if exitCode, reason, ok := w.ExitCode(); ok {
				if err := setExit(ctx, r.Id, kdb.RunExit{
					Code:    exitCode,
					Message: reason,
				}); err != nil {
					return r.Status, err
				}
			}
		}

		return newStatus, nil
	}
}
