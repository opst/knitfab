package runs

import (
	apiplan "github.com/opst/knitfab/pkg/api/types/plans"
	apitags "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/rfctime"
)

type Exit struct {
	Code    uint8  `json:"code"`
	Message string `json:"message"`
}

func (e *Exit) Equal(o *Exit) bool {
	if e == nil || o == nil {
		return (e == nil) && (o == nil)
	}

	return e.Code == o.Code && e.Message == o.Message
}

type Summary struct {
	RunId     string          `json:"runId"`
	Status    string          `json:"status"`
	UpdatedAt rfctime.RFC3339 `json:"updatedAt"`
	Exit      *Exit           `json:"exit,omitempty"`
	Plan      apiplan.Summary `json:"plan"`
}

func ComposeSummary(r kdb.RunBody) Summary {
	var exit *Exit
	if ex := r.Exit; ex != nil {
		exit = &Exit{
			Code:    ex.Code,
			Message: ex.Message,
		}
	}
	return Summary{
		RunId:     r.Id,
		Plan:      apiplan.ComposeSummary(r.PlanBody),
		Status:    string(r.Status),
		Exit:      exit,
		UpdatedAt: rfctime.RFC3339(r.UpdatedAt),
	}
}

func (s *Summary) Equal(o *Summary) bool {
	if s == nil || o == nil {
		return s == nil && o == nil
	}
	return s.RunId == o.RunId &&
		s.Exit.Equal(o.Exit) &&
		s.Plan.Equal(o.Plan) &&
		s.Status == o.Status &&
		s.UpdatedAt.Equal(&o.UpdatedAt)
}

type Detail struct {
	Summary
	Inputs  []Assignment `json:"inputs"`
	Outputs []Assignment `json:"outputs"`
	Log     *LogSummary  `json:"log"`
}

func (r *Detail) Equal(o *Detail) bool {
	if r == nil || o == nil {
		return r == nil && o == nil
	}

	return r.RunId == o.RunId &&
		r.Plan.Equal(o.Plan) &&
		r.Status == o.Status &&
		r.UpdatedAt.Equal(&o.UpdatedAt) &&
		cmp.SliceContentEqWith(
			r.Inputs, o.Inputs,
			func(a, b Assignment) bool { return a.Equal(&b) },
		) &&
		cmp.SliceContentEqWith(
			r.Outputs, o.Outputs,
			func(a, b Assignment) bool { return a.Equal(&b) },
		) &&
		r.Log.Equal(o.Log)
}

type Assignment struct {
	apiplan.Mountpoint
	KnitId string `json:"knitId"`
}

func (a *Assignment) Equal(o *Assignment) bool {
	if a == nil || o == nil {
		return (a == nil) && (o == nil)
	}

	return a.Mountpoint.Equal(&o.Mountpoint) && a.KnitId == o.KnitId
}

type LogSummary struct {
	apiplan.LogPoint
	KnitId string `json:"knitId"`
}

func (l *LogSummary) Equal(o *LogSummary) bool {
	if l == nil || o == nil {
		return (l == nil) && (o == nil)
	}

	return l.LogPoint.Equal(&o.LogPoint) && l.KnitId == o.KnitId
}

func ComposeDetail(r kdb.Run) Detail {
	var logSummary *LogSummary
	if r.Log != nil {
		logSummary = &LogSummary{
			KnitId: r.Log.KnitDataBody.KnitId,
			LogPoint: apiplan.LogPoint{
				Tags: utils.Map(r.Log.Tags.Slice(), apitags.Convert),
			},
		}
	}

	return Detail{
		Summary: ComposeSummary(r.RunBody),
		Inputs: utils.Map(
			r.Inputs, func(a kdb.Assignment) Assignment {
				return Assignment{
					KnitId:     a.KnitDataBody.KnitId,
					Mountpoint: apiplan.ComposeMountpoint(a.MountPoint),
				}
			},
		),
		Outputs: utils.Map(
			r.Outputs, func(a kdb.Assignment) Assignment {
				return Assignment{
					KnitId:     a.KnitDataBody.KnitId,
					Mountpoint: apiplan.ComposeMountpoint(a.MountPoint),
				}
			},
		),
		Log: logSummary,
	}
}
