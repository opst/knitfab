package data

import (
	"github.com/opst/knitfab-api-types/data"
	apitags "github.com/opst/knitfab-api-types/tags"
	bindplan "github.com/opst/knitfab/pkg/api-types-binding/plans"
	bindrun "github.com/opst/knitfab/pkg/api-types-binding/runs"
	bindtags "github.com/opst/knitfab/pkg/api-types-binding/tags"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"
)

func ComposeSummary(body kdb.KnitDataBody) data.Summary {
	return data.Summary{
		KnitId: body.KnitId,
		Tags: utils.Map(
			body.Tags.Slice(),
			func(dt kdb.Tag) apitags.Tag { return apitags.Tag{Key: dt.Key, Value: dt.Value} },
		),
	}
}

func composeAssignTo(r kdb.Dependency) data.AssignedTo {
	return data.AssignedTo{
		Mountpoint: bindplan.ComposeMountpoint(r.MountPoint),
		Run:        bindrun.ComposeSummary(r.RunBody),
	}
}

func composeNominatedBy(n kdb.Nomination) data.NominatedBy {
	return data.NominatedBy{
		Mountpoint: bindplan.ComposeMountpoint(n.MountPoint),
		Plan:       bindplan.ComposeSummary(n.PlanBody),
	}
}

func ComposeDetail(d kdb.KnitData) data.Detail {
	downstreams, _ := utils.Group(d.Downstreams, func(d kdb.Dependency) bool {
		return d.Status != kdb.Invalidated
	})

	return data.Detail{
		KnitId:      d.KnitId,
		Tags:        utils.Map(d.Tags.Slice(), bindtags.Compose),
		Upstream:    composeAssignTo(d.Upsteram),
		Downstreams: utils.Map(downstreams, composeAssignTo),
		Nomination:  utils.Map(d.NominatedBy, composeNominatedBy),
	}
}
