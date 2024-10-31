package plans

import (
	"errors"

	apiplans "github.com/opst/knitfab-api-types/plans"
	bindtags "github.com/opst/knitfab/pkg/api-types-binding/tags"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils"
)

func ComposeMountpoint(mp domain.MountPoint) apiplans.Mountpoint {
	return apiplans.Mountpoint{
		Path: mp.Path,
		Tags: utils.Map(mp.Tags.Slice(), bindtags.Compose),
	}
}

func ComposeDetail(plan domain.Plan) apiplans.Detail {
	var log *apiplans.LogPoint
	if plan.Log != nil {
		log = &apiplans.LogPoint{Tags: utils.Map(plan.Log.Tags.Slice(), bindtags.Compose)}
	}

	var onNode *apiplans.OnNode
	if 0 < len(plan.OnNode) {
		onNode = &apiplans.OnNode{}
		for _, on := range plan.OnNode {
			switch on.Mode {
			case domain.MayOnNode:
				onNode.May = append(onNode.May, apiplans.OnSpecLabel{Key: on.Key, Value: on.Value})
			case domain.PreferOnNode:
				onNode.Prefer = append(onNode.Prefer, apiplans.OnSpecLabel{Key: on.Key, Value: on.Value})
			case domain.MustOnNode:
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

func ComposeSummary(planBody domain.PlanBody) apiplans.Summary {
	rst := apiplans.Summary{
		PlanId:     planBody.PlanId,
		Entrypoint: planBody.Entrypoint,
		Args:       planBody.Args,
	}
	if i := planBody.Image; i != nil {
		rst.Image = &apiplans.Image{Repository: i.Image, Tag: i.Version}
	}
	if p := planBody.Pseudo; p != nil {
		rst.Name = p.Name.String()
	}

	rst.Annotations = utils.Map(planBody.Annotations, func(a domain.Annotation) apiplans.Annotation {
		return apiplans.Annotation{Key: a.Key, Value: a.Value}
	})

	return rst
}

var ErrNilArgument = errors.New("nil is prohibited")
