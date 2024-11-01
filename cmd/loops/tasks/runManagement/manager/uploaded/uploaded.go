package uploaded

import (
	"context"

	manager "github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/runManagementHook"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	"github.com/opst/knitfab/pkg/domain"
	kdbdata "github.com/opst/knitfab/pkg/domain/data/db"
	"github.com/opst/knitfab/pkg/utils/slices"
)

const PLAN_NAME = domain.Uploaded

func New(dbdata kdbdata.DataInterface) manager.Manager {
	return func(
		ctx context.Context,
		hooks runManagementHook.Hooks,
		r domain.Run,
	) (
		domain.KnitRunStatus,
		error,
	) {
		if pp := r.RunBody.PlanBody.Pseudo; pp != nil && pp.Name != PLAN_NAME {
			return r.Status, nil
		}

		outputs := slices.Map(
			r.Outputs, func(o domain.Assignment) domain.KnitDataBody { return o.KnitDataBody },
		)
		if r.Log != nil {
			outputs = append(outputs, r.Log.KnitDataBody)
		}

		for _, d := range outputs {
			var agents []string
			agents, err := dbdata.GetAgentName(
				ctx, d.KnitId,
				[]domain.DataAgentMode{domain.DataAgentWrite},
			)
			if err != nil {
				return r.Status, err
			}

			if 0 < len(agents) {
				return r.Status, nil
			}
		}

		if _, err := hooks.ToAborting.Before(bindruns.ComposeDetail(r)); err != nil {
			return r.Status, err
		}
		return domain.Aborting, nil
	}
}
