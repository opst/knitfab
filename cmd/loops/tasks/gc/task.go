package gc

import (
	"context"

	"github.com/opst/knitfab/cmd/loops/loop/recurring"
	"github.com/opst/knitfab/pkg/domain"
	kdbgarbage "github.com/opst/knitfab/pkg/domain/garbage/db"
	k8sgarbage "github.com/opst/knitfab/pkg/domain/garbage/k8s"
)

// initial value for task
func Seed() any {
	return nil
}

// return:
//
// - task: remove PVC in garbage
func Task(k8sgarbage k8sgarbage.Interface, kdbgarbage kdbgarbage.Interface) recurring.Task[any] {
	return func(ctx context.Context, value any) (any, bool, error) {
		pop, err := kdbgarbage.Pop(ctx, func(g domain.Garbage) error {
			return k8sgarbage.DestroyGarbage(ctx, g)
		})
		return value, pop, err
	}
}
