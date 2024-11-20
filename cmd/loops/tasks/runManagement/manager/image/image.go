package image

import (
	"context"

	manager "github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/runManagementHook"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	types "github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
	"github.com/opst/knitfab/pkg/domain/run/db"
	"github.com/opst/knitfab/pkg/domain/run/k8s"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
)

// Returns a manager for starting a worker for a run.
func New(
	iK8sRun k8s.Interface,
	iDBRun db.Interface,
) manager.Manager {
	return func(
		ctx context.Context,
		hooks runManagementHook.Hooks,
		r types.Run,
	) (
		types.KnitRunStatus,
		error,
	) {
		w, err := iK8sRun.FindWorker(ctx, r.RunBody)
		if err != nil {
			if !kubeerr.IsNotFound(err) {
				return r.Status, err
			}

			if r.Status == types.Ready {
				resp, err := hooks.ToStarting.Before(bindruns.ComposeDetail(r))
				if err != nil {
					return r.Status, err
				}
				if _, err := iK8sRun.SpawnWorker(ctx, r, resp.KnitfabExtension.Env); err != nil && !kubeerr.IsAlreadyExists(err) {
					return r.Status, err
				}
				return types.Starting, nil
			}

			if _, err := hooks.ToAborting.Before(bindruns.ComposeDetail(r)); err != nil {
				return r.Status, err
			}
			if err := iDBRun.SetExit(ctx, r.Id, types.RunExit{
				Code:    254,
				Message: "worker for the run is not found",
			}); err != nil {
				return r.Status, err
			}
			return types.Aborting, nil
		}

		var newStatus types.KnitRunStatus

		s := w.JobStatus(ctx)

		switch ty := s.Type; ty {
		case cluster.Pending:
			newStatus = types.Starting
		case cluster.Running:
			newStatus = types.Running
		case cluster.Failed, cluster.Stucking:
			newStatus = types.Aborting
		case cluster.Succeeded:
			newStatus = types.Completing
		default:
			return r.Status, nil
		}

		if newStatus == r.Status {
			// no changes.
			return r.Status, nil
		}

		switch newStatus {
		case types.Starting:
			// ignore. Since worker is already started, it should not be started again.
		case types.Running:
			if _, err := hooks.ToRunning.Before(bindruns.ComposeDetail(r)); err != nil {
				return r.Status, err
			}
		case types.Aborting, types.Completing:
			if newStatus == types.Completing {
				if _, err := hooks.ToCompleting.Before(bindruns.ComposeDetail(r)); err != nil {
					return r.Status, err
				}
			} else {
				if _, err := hooks.ToAborting.Before(bindruns.ComposeDetail(r)); err != nil {
					return r.Status, err
				}
			}

			exit := types.RunExit{
				Code:    s.Code,
				Message: s.Message,
			}
			if err := iDBRun.SetExit(ctx, r.Id, exit); err != nil {
				return r.Status, err
			}
		}

		return newStatus, nil
	}
}
