package initialize

import (
	"context"
	"errors"
	"time"

	"github.com/opst/knitfab/cmd/loops/recurring"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils/retry"
	wl "github.com/opst/knitfab/pkg/workloads"
	"github.com/opst/knitfab/pkg/workloads/data"
	"github.com/opst/knitfab/pkg/workloads/k8s"
)

// initial value for task
func Seed() kdb.RunCursor {
	return kdb.RunCursor{
		Status: []kdb.KnitRunStatus{kdb.Waiting},
	}
}

// Task for initializing PVCs
//
// # Params
//
// - irun: RunInterface for accessing database
//
// - init: initializer function for PVCs.
// It should create each PVCs per run's output.
//
// # Return
//
// - task : promote waiting run to ready.
func Task(
	irun kdb.RunInterface,
	init func(context.Context, kdb.Run) error,
) recurring.Task[kdb.RunCursor] {
	return func(ctx context.Context, value kdb.RunCursor) (kdb.RunCursor, bool, error) {
		nextCursor, err := irun.PickAndSetStatus(
			ctx, value,
			func(r kdb.Run) (kdb.KnitRunStatus, error) {
				if err := init(ctx, r); err != nil {
					return r.Status, err
				}
				return kdb.Ready, nil
			},
		)

		// Context cancelled/deadline exceeded are okay. It will be retried.
		if err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return nextCursor, !value.Equal(nextCursor), err
		}
		return nextCursor, !value.Equal(nextCursor), nil
	}
}

// factory function for pvc initializer function
//
// # Params
//
// - cluster: where new PVC is created
//
// - template: VolumeTemplate used to build PVC
func PVCInitializer(cluster k8s.Cluster, template data.VolumeTemplate) func(ctx context.Context, r kdb.Run) error {
	return func(ctx context.Context, r kdb.Run) error {
		proms := []retry.Promise[k8s.PVC]{}
		builders, err := data.OfOutputs(r)
		if err != nil {
			return err
		}

		for _, b := range builders {
			pvc := cluster.NewPVC(
				ctx,
				retry.StaticBackoff(200*time.Millisecond),
				b.Build(template),
			)
			proms = append(proms, pvc)
		}

		for nth := range proms {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case p := <-proms[nth]:
				if err := p.Err; !wl.AsConflict(err) {
					return err
				}
			}
		}

		return nil
	}
}
