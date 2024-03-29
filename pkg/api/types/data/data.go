package data

import (
	apiplan "github.com/opst/knitfab/pkg/api/types/plans"
	apirun "github.com/opst/knitfab/pkg/api/types/runs"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"
)

type Summary struct {
	KnitId string       `json:"knitid"`
	Tags   []apitag.Tag `json:"tags"`
}

func (s *Summary) Equal(o *Summary) bool {
	return s.KnitId == o.KnitId &&
		cmp.SliceContentEqWith(s.Tags, o.Tags, func(a, b apitag.Tag) bool { return a.Equal(&b) })
}

func ComposeSummary(body kdb.KnitDataBody) Summary {
	return Summary{
		KnitId: body.KnitId,
		Tags: utils.Map(
			body.Tags.Slice(),
			func(dt kdb.Tag) apitag.Tag { return apitag.Tag{Key: dt.Key, Value: dt.Value} },
		),
	}
}

type Detail struct {
	KnitId      string        `json:"knitId"`
	Tags        []apitag.Tag  `json:"tags"`
	Upstream    AssignedTo    `json:"upstream"`
	Downstreams []AssignedTo  `json:"downstreams"`
	Nomination  []NominatedBy `json:"nomination"`
}

// assigment representation, looking from data
type AssignedTo struct {
	apiplan.Mountpoint
	Run apirun.Summary `json:"run"`
}

func (a *AssignedTo) Equal(o *AssignedTo) bool {
	if (a == nil) || (o == nil) {
		return a == nil && o == nil
	}

	return a.Run.Equal(&o.Run) && a.Mountpoint.Equal(&o.Mountpoint)
}

func composeAssignTo(r kdb.Dependency) AssignedTo {
	return AssignedTo{
		Mountpoint: apiplan.ComposeMountpoint(r.MountPoint),
		Run:        apirun.ComposeSummary(r.RunBody),
	}
}

// nomination representation, looking from data
type NominatedBy struct {
	apiplan.Mountpoint
	Plan apiplan.Summary `json:"plan"`
}

func (n *NominatedBy) Equal(o *NominatedBy) bool {
	if (n == nil) || (o == nil) {
		return n == nil && o == nil
	}

	return n.Plan.Equal(o.Plan) && n.Mountpoint.Equal(&o.Mountpoint)
}

func composeNominatedBy(n kdb.Nomination) NominatedBy {
	return NominatedBy{
		Mountpoint: apiplan.ComposeMountpoint(n.MountPoint),
		Plan:       apiplan.ComposeSummary(n.PlanBody),
	}
}

func (d *Detail) Equal(o *Detail) bool {
	if d == nil || o == nil {
		return d == nil && o == nil
	}

	return d.KnitId == o.KnitId &&
		d.Upstream.Equal(&o.Upstream) &&
		cmp.SliceContentEqWith(
			d.Tags, o.Tags,
			func(a, b apitag.Tag) bool { return a.Equal(&b) },
		) &&
		cmp.SliceContentEqWith(
			d.Downstreams, o.Downstreams,
			func(a, b AssignedTo) bool { return a.Equal(&b) },
		) &&
		cmp.SliceContentEqWith(
			d.Nomination, o.Nomination,
			func(a, b NominatedBy) bool { return a.Equal(&b) },
		)
}

func ComposeDetail(d kdb.KnitData) Detail {
	downstreams, _ := utils.Group(d.Downstreams, func(d kdb.Dependency) bool {
		return d.Status != kdb.Invalidated
	})

	return Detail{
		KnitId:      d.KnitId,
		Tags:        utils.Map(d.Tags.Slice(), apitag.Convert),
		Upstream:    composeAssignTo(d.Upsteram),
		Downstreams: utils.Map(downstreams, composeAssignTo),
		Nomination:  utils.Map(d.NominatedBy, composeNominatedBy),
	}
}
