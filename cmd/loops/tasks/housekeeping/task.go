package housekeeping

import (
	"context"
	"time"

	"github.com/opst/knitfab/cmd/loops/loop/recurring"
	"github.com/opst/knitfab/pkg/domain"
	kdbdata "github.com/opst/knitfab/pkg/domain/data/db"
	k8sdata "github.com/opst/knitfab/pkg/domain/data/k8s"
	k8serrors "github.com/opst/knitfab/pkg/domain/errors/k8serrors"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
	"github.com/opst/knitfab/pkg/utils/retry"
	kubecore "k8s.io/api/core/v1"
)

// initial value for task
func Seed() domain.DataAgentCursor {
	return domain.DataAgentCursor{
		Debounce: 30 * time.Second,
	}
}

type GetPodder interface {
	GetPod(context.Context, retry.Backoff, string, ...cluster.Requirement[cluster.WithEvents[*kubecore.Pod]]) retry.Promise[cluster.Pod]
}

// return:
//
// - task: terminate orphan run based pseudo-plan
func Task(
	data kdbdata.DataInterface,
	getPodder k8sdata.Interface,
) recurring.Task[domain.DataAgentCursor] {
	return func(ctx context.Context, cursor domain.DataAgentCursor) (domain.DataAgentCursor, bool, error) {
		_cursor, err := data.PickAndRemoveAgent(ctx, cursor, func(da domain.DataAgent) (bool, error) {
			_ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			pod, err := getPodder.FindDataAgent(_ctx, da)
			if err != nil {
				if k8serrors.AsMissingError(err) {
					return true, nil
				}
				return false, err
			}

			switch s := pod.PodPhase(); s {
			case cluster.PodSucceeded, cluster.PodFailed, cluster.PodStucking:
				if err := pod.Close(); err != nil {
					return false, err
				}
				return true, nil
			default:
			}
			return false, nil
		})
		return _cursor, _cursor != cursor, err
	}
}
