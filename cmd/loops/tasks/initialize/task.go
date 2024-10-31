package initialize

import (
	"context"
	"errors"

	apiruns "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/loop/recurring"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	"github.com/opst/knitfab/pkg/domain"
	kdbrun "github.com/opst/knitfab/pkg/domain/run/db"
	k8srun "github.com/opst/knitfab/pkg/domain/run/k8s"
)

// initial value for task
func Seed() domain.RunCursor {
	return domain.RunCursor{
		Status: []domain.KnitRunStatus{domain.Waiting},
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
	irun kdbrun.RunInterface,
	init k8srun.Interface,
	hook hook.Hook[apiruns.Detail, struct{}],
) recurring.Task[domain.RunCursor] {
	return func(ctx context.Context, value domain.RunCursor) (domain.RunCursor, bool, error) {
		nextCursor, statusChanged, err := irun.PickAndSetStatus(
			ctx, value,
			func(r domain.Run) (domain.KnitRunStatus, error) {
				hookval := bindruns.ComposeDetail(r)
				if _, err := hook.Before(hookval); err != nil {
					return r.Status, err
				}

				if err := init.Initialize(ctx, r); err != nil {
					return r.Status, err
				}
				return domain.Ready, nil
			},
		)

		if statusChanged {
			if runs, _ := irun.Get(ctx, []string{nextCursor.Head}); runs != nil {
				if r, ok := runs[nextCursor.Head]; ok {
					hookval := bindruns.ComposeDetail(r)
					hook.After(hookval)
				}
			}
		}

		cursorMoved := !value.Equal(nextCursor)
		// Context cancelled/deadline exceeded are okay. It will be retried.
		if err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return nextCursor, cursorMoved, err
		}
		return nextCursor, cursorMoved, nil
	}
}
