package imported

import (
	"context"

	apiruns "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	kdb "github.com/opst/knitfab/pkg/db"
)

const PLAN_NAME = kdb.Imported

func New() manager.Manager {
	return func(ctx context.Context, h hook.Hook[apiruns.Detail], r kdb.Run) (kdb.KnitRunStatus, error) {
		// Imported Runs comes here are expired its `"lifecycle_suspend_until"`.
		// They should be aborted.
		if r.Status == kdb.Running {
			if err := h.Before(bindruns.ComposeDetail(r)); err != nil {
				return r.Status, err
			}
			return kdb.Aborting, nil
		}
		return r.Status, nil
	}
}
