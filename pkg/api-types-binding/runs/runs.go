package runs

import (
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	bindplan "github.com/opst/knitfab/pkg/api-types-binding/plans"
	bindtags "github.com/opst/knitfab/pkg/api-types-binding/tags"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"
)

func ComposeSummary(r kdb.RunBody) runs.Summary {
	var exit *runs.Exit
	if ex := r.Exit; ex != nil {
		exit = &runs.Exit{
			Code:    ex.Code,
			Message: ex.Message,
		}
	}
	return runs.Summary{
		RunId:     r.Id,
		Plan:      bindplan.ComposeSummary(r.PlanBody),
		Status:    string(r.Status),
		Exit:      exit,
		UpdatedAt: rfctime.RFC3339(r.UpdatedAt),
	}
}

func ComposeDetail(r kdb.Run) runs.Detail {
	var logSummary *runs.LogSummary
	if r.Log != nil {
		logSummary = &runs.LogSummary{
			KnitId: r.Log.KnitDataBody.KnitId,
			LogPoint: plans.LogPoint{
				Tags: utils.Map(r.Log.Tags.Slice(), bindtags.Compose),
			},
		}
	}

	return runs.Detail{
		Summary: ComposeSummary(r.RunBody),
		Inputs: utils.Map(
			r.Inputs, func(a kdb.Assignment) runs.Assignment {
				return runs.Assignment{
					KnitId:     a.KnitDataBody.KnitId,
					Mountpoint: bindplan.ComposeMountpoint(a.MountPoint),
				}
			},
		),
		Outputs: utils.Map(
			r.Outputs, func(a kdb.Assignment) runs.Assignment {
				return runs.Assignment{
					KnitId:     a.KnitDataBody.KnitId,
					Mountpoint: bindplan.ComposeMountpoint(a.MountPoint),
				}
			},
		),
		Log: logSummary,
	}
}
