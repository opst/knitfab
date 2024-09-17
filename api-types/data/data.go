package data

import (
	"github.com/opst/knitfab-api-types/internal/utils/cmp"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
)

type Summary struct {
	KnitId string     `json:"knitid"`
	Tags   []tags.Tag `json:"tags"`
}

func (s *Summary) Equal(o *Summary) bool {
	return s.KnitId == o.KnitId &&
		cmp.SliceEqualUnordered(s.Tags, o.Tags)
}

type Detail struct {
	KnitId      string        `json:"knitId"`
	Tags        []tags.Tag    `json:"tags"`
	Upstream    AssignedTo    `json:"upstream"`
	Downstreams []AssignedTo  `json:"downstreams"`
	Nomination  []NominatedBy `json:"nomination"`
}

func (d Detail) Equal(o Detail) bool {
	return d.KnitId == o.KnitId &&
		d.Upstream.Equal(o.Upstream) &&
		cmp.SliceEqualUnordered(d.Tags, o.Tags) &&
		cmp.SliceEqualUnordered(d.Downstreams, o.Downstreams) &&
		cmp.SliceEqualUnordered(d.Nomination, o.Nomination)
}

// assigment representation, looking from data
type AssignedTo struct {
	plans.Mountpoint
	Run runs.Summary `json:"run"`
}

func (a AssignedTo) Equal(o AssignedTo) bool {
	return a.Run.Equal(o.Run) && a.Mountpoint.Equal(o.Mountpoint)
}

// nomination representation, looking from data
type NominatedBy struct {
	plans.Mountpoint
	Plan plans.Summary `json:"plan"`
}

func (n NominatedBy) Equal(o NominatedBy) bool {
	return n.Plan.Equal(o.Plan) && n.Mountpoint.Equal(o.Mountpoint)
}
