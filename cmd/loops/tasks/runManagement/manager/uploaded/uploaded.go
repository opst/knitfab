package uploaded

import (
	"context"

	api_runs "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	manager "github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"
)

const PLAN_NAME = kdb.Uploaded

func New(dbdata kdb.DataInterface) manager.Manager {
	return func(ctx context.Context, h hook.Hook[api_runs.Detail], r kdb.Run) (kdb.KnitRunStatus, error) {
		if pp := r.RunBody.PlanBody.Pseudo; pp != nil && pp.Name != PLAN_NAME {
			return r.Status, nil
		}

		outputs := utils.Map(
			r.Outputs, func(o kdb.Assignment) kdb.KnitDataBody { return o.KnitDataBody },
		)
		if r.Log != nil {
			outputs = append(outputs, r.Log.KnitDataBody)
		}

		for _, d := range outputs {
			var agents []string
			agents, err := dbdata.GetAgentName(
				ctx, d.KnitId,
				[]kdb.DataAgentMode{kdb.DataAgentWrite},
			)
			if err != nil {
				return r.Status, err
			}

			if 0 < len(agents) {
				return r.Status, nil
			}
		}

		if err := h.Before(bindruns.ComposeDetail(r)); err != nil {
			return r.Status, err
		}
		return kdb.Aborting, nil
	}
}
