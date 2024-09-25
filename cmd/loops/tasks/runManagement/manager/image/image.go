package image

import (
	"context"

	manager "github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/runManagementHook"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/workloads/k8s"
	kw "github.com/opst/knitfab/pkg/workloads/worker"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
)

// Returns a manager for starting a worker for a run.
func New(
	getWorker func(context.Context, kdb.Run) (kw.Worker, error),
	startWorker func(context.Context, kdb.Run, map[string]string) error,
	setExit func(ctx context.Context, runId string, exit kdb.RunExit) error,
) manager.Manager {
	return func(
		ctx context.Context,
		hooks runManagementHook.Hooks,
		r kdb.Run,
	) (
		kdb.KnitRunStatus,
		error,
	) {
		w, err := getWorker(ctx, r)
		if err != nil {
			if !kubeerr.IsNotFound(err) {
				return r.Status, err
			}

			if r.Status == kdb.Ready {
				resp, err := hooks.ToStarting.Before(bindruns.ComposeDetail(r))
				if err != nil {
					return r.Status, err
				}
				if err := startWorker(ctx, r, resp.KnitfabExtension.Env); err != nil && !kubeerr.IsAlreadyExists(err) {
					return r.Status, err
				}
				return kdb.Starting, nil
			}

			if _, err := hooks.ToAborting.Before(bindruns.ComposeDetail(r)); err != nil {
				return r.Status, err
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

		s := w.JobStatus(ctx)

		switch ty := s.Type; ty {
		case k8s.Pending:
			newStatus = kdb.Starting
		case k8s.Running:
			newStatus = kdb.Running
		case k8s.Failed, k8s.Stucking:
			newStatus = kdb.Aborting
		case k8s.Succeeded:
			newStatus = kdb.Completing
		default:
			return r.Status, nil
		}

		if newStatus == r.Status {
			// no changes.
			return r.Status, nil
		}

		switch newStatus {
		case kdb.Starting:
			// ignore. Since worker is already started, it should not be started again.
		case kdb.Running:
			if _, err := hooks.ToRunning.Before(bindruns.ComposeDetail(r)); err != nil {
				return r.Status, err
			}
		case kdb.Aborting, kdb.Completing:
			if newStatus == kdb.Completing {
				if _, err := hooks.ToCompleting.Before(bindruns.ComposeDetail(r)); err != nil {
					return r.Status, err
				}
			} else {
				if _, err := hooks.ToAborting.Before(bindruns.ComposeDetail(r)); err != nil {
					return r.Status, err
				}
			}

			exit := kdb.RunExit{
				Code:    s.Code,
				Message: s.Message,
			}
			if err := setExit(ctx, r.Id, exit); err != nil {
				return r.Status, err
			}
		}

		return newStatus, nil
	}
}
