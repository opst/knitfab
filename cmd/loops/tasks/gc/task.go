package gc

import (
	"context"
	"time"

	"github.com/opst/knitfab/cmd/loops/loop/recurring"
	types "github.com/opst/knitfab/pkg/domain"
	kdb "github.com/opst/knitfab/pkg/domain/garbage/db"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
	"github.com/opst/knitfab/pkg/utils/retry"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
)

// initial value for task
func Seed() any {
	return nil
}

// return:
//
// - task: remove PVC in garbage
func Task(kclient cluster.Cluster, dbg kdb.GarbageInterface) recurring.Task[any] {
	return func(ctx context.Context, value any) (any, bool, error) {
		pop, err := dbg.Pop(ctx, func(g types.Garbage) error {
			ret := <-kclient.DeletePVC(ctx, retry.StaticBackoff(50*time.Millisecond), g.VolumeRef)
			if err := ret.Err; err != nil {
				if kubeerr.IsNotFound(err) { // it is okay if the PVC is already deleted
					return nil
				}
				return err
			}
			return nil
		})
		return value, pop, err
	}
}
