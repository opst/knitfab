package plans

import (
	"errors"

	apiplans "github.com/opst/knitfab-api-types/plans"
	bindtags "github.com/opst/knitfab/pkg/api-types-binding/tags"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"
)

func ComposeMountpoint(mp kdb.MountPoint) apiplans.Mountpoint {
	return apiplans.Mountpoint{
		Path: mp.Path,
		Tags: utils.Map(mp.Tags.Slice(), bindtags.Compose),
	}
}

func ComposeDetail(plan kdb.Plan) apiplans.Detail {
	var log *apiplans.LogPoint
	if plan.Log != nil {
		log = &apiplans.LogPoint{Tags: utils.Map(plan.Log.Tags.Slice(), bindtags.Compose)}
	}

	var onNode *apiplans.OnNode
	if 0 < len(plan.OnNode) {
		onNode = &apiplans.OnNode{}
		for _, on := range plan.OnNode {
			switch on.Mode {
			case kdb.MayOnNode:
				onNode.May = append(onNode.May, apiplans.OnSpecLabel{Key: on.Key, Value: on.Value})
			case kdb.PreferOnNode:
				onNode.Prefer = append(onNode.Prefer, apiplans.OnSpecLabel{Key: on.Key, Value: on.Value})
			case kdb.MustOnNode:
				onNode.Must = append(onNode.Must, apiplans.OnSpecLabel{Key: on.Key, Value: on.Value})
			}
		}
	}

	return apiplans.Detail{
		Summary:        ComposeSummary(plan.PlanBody),
		Active:         plan.Active,
		Inputs:         utils.Map(plan.Inputs, ComposeMountpoint),
		Outputs:        utils.Map(plan.Outputs, ComposeMountpoint),
		Resources:      apiplans.Resources(plan.Resources),
		Log:            log,
		OnNode:         onNode,
		ServiceAccount: plan.ServiceAccount,
	}
}

func ComposeSummary(planBody kdb.PlanBody) apiplans.Summary {
	rst := apiplans.Summary{
		PlanId: planBody.PlanId,
	}
	if i := planBody.Image; i != nil {
		rst.Image = &apiplans.Image{Repository: i.Image, Tag: i.Version}
	}
	if p := planBody.Pseudo; p != nil {
		rst.Name = p.Name.String()
	}

	rst.Annotations = utils.Map(planBody.Annotations, func(a kdb.Annotation) apiplans.Annotation {
		return apiplans.Annotation{Key: a.Key, Value: a.Value}
	})

	return rst
}

var ErrNilArgument = errors.New("nil is prohibited")
