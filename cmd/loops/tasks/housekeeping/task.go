package housekeeping

import (
	"context"
	"time"

	"github.com/opst/knitfab/cmd/loops/recurring"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils/retry"
	"github.com/opst/knitfab/pkg/workloads"
	"github.com/opst/knitfab/pkg/workloads/k8s"
	kubecore "k8s.io/api/core/v1"
)

// initial value for task
func Seed() kdb.DataAgentCursor {
	return kdb.DataAgentCursor{
		Debounce: 30 * time.Second,
	}
}

type GetPodder interface {
	GetPod(context.Context, retry.Backoff, string, ...k8s.Requirement[*kubecore.Pod]) retry.Promise[k8s.Pod]
}

// return:
//
// - task: terminate orphan run based pseudo-plan
func Task(
	data kdb.DataInterface,
	kluster GetPodder,
) recurring.Task[kdb.DataAgentCursor] {
	return func(ctx context.Context, cursor kdb.DataAgentCursor) (kdb.DataAgentCursor, bool, error) {
		_cursor, err := data.PickAndRemoveAgent(ctx, cursor, func(da kdb.DataAgent) (bool, error) {
			_ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			ppod := <-kluster.GetPod(
				_ctx, retry.StaticBackoff(50*time.Millisecond), da.Name,
				func(*kubecore.Pod) error { return nil }, // everything is fine
			)
			if err := ppod.Err; err != nil {
				if workloads.AsMissingError(err) {
					return true, nil
				}
				return false, err
			}

			pod := ppod.Value
			switch s := pod.Status(); s {
			case k8s.PodSucceeded, k8s.PodFailed:
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
