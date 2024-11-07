package plans

import (
	"errors"

	apiplans "github.com/opst/knitfab-api-types/plans"
	bindtags "github.com/opst/knitfab/pkg/api-types-binding/tags"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/slices"
)

func ComposeMountpoint(mp domain.MountPoint) apiplans.Mountpoint {
	return apiplans.Mountpoint{
		Path: mp.Path,
		Tags: slices.Map(mp.Tags.Slice(), bindtags.Compose),
	}
}

func ComposeLogPoint(log *domain.LogPoint) *apiplans.LogPoint {
	if log == nil {
		return nil
	}
	return &apiplans.LogPoint{Tags: slices.Map(log.Tags.Slice(), bindtags.Compose)}
}

func ComposeDownstream(d domain.PlanDownstream) apiplans.Downstream {
	return apiplans.Downstream{
		Summary:    ComposeSummary(d.PlanBody),
		Mountpoint: ComposeMountpoint(d.Mountpoint),
	}
}

func ComposeInputs(i domain.Input) apiplans.Input {
	return apiplans.Input{
		Mountpoint: ComposeMountpoint(i.MountPoint),
		Upstreams: slices.Map(i.Upstreams, func(u domain.PlanUpstream) apiplans.Upstream {
			var mp *apiplans.Mountpoint = nil
			if u.Mountpoint != nil {
				_mp := ComposeMountpoint(*u.Mountpoint)
				mp = &_mp
			}

			return apiplans.Upstream{
				Summary:    ComposeSummary(u.PlanBody),
				Mountpoint: mp,
				Log:        ComposeLogPoint(u.Log),
			}
		}),
	}
}

func ComposeOutputs(o domain.Output) apiplans.Output {
	return apiplans.Output{
		Mountpoint:  ComposeMountpoint(o.MountPoint),
		Downstreams: slices.Map(o.Downstreams, ComposeDownstream),
	}
}

func ComposeDetail(plan domain.Plan) apiplans.Detail {
	var log *apiplans.Log
	if lp := ComposeLogPoint(plan.Log); lp != nil {
		log = &apiplans.Log{
			LogPoint:    *lp,
			Downstreams: slices.Map(plan.Log.Downstreams, ComposeDownstream),
		}
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
		Inputs:         slices.Map(plan.Inputs, ComposeInputs),
		Outputs:        slices.Map(plan.Outputs, ComposeOutputs),
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

	rst.Annotations = slices.Map(planBody.Annotations, func(a domain.Annotation) apiplans.Annotation {
		return apiplans.Annotation{Key: a.Key, Value: a.Value}
	})

	return rst
}

var ErrNilArgument = errors.New("nil is prohibited")
