package gc

import (
	"context"

	"github.com/opst/knitfab/cmd/loops/recurring"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/workloads/k8s"
)

// initial value for task
func Seed() any {
	return nil
}

// return:
//
// - task: remove PVC in garbage
func Task(kclient k8s.K8sClient, namespace string, dbg kdb.GarbageInterface) recurring.Task[any] {
	return func(ctx context.Context, value any) (any, bool, error) {
		pop, err := dbg.Pop(ctx, func(g kdb.Garbage) error {
			if err := kclient.DeletePVC(ctx, namespace, g.VolumeRef); err != nil {
				return err
			}
			return nil
		})
		return value, pop, err
	}
}
