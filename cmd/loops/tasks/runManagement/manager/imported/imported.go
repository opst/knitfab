package imported

import (
	"context"

	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/runManagementHook"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	"github.com/opst/knitfab/pkg/domain"
)

const PLAN_NAME = domain.Imported

func New() manager.Manager {
	return func(
		ctx context.Context,
		hooks runManagementHook.Hooks,
		r domain.Run,
	) (
		domain.KnitRunStatus,
		error,
	) {
		// Imported Runs comes here are expired its `"lifecycle_suspend_until"`.
		// They should be aborted.
		if r.Status == domain.Running {
			if _, err := hooks.ToAborting.Before(bindruns.ComposeDetail(r)); err != nil {
				return r.Status, err
			}
			return domain.Aborting, nil
		}
		return r.Status, nil
	}
}
