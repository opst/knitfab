package recurring

import (
	"context"

	"github.com/opst/knitfab/pkg/loop"
)

// Return:
//
// - T : same as return value T of github.com/opst/knitfab/pkg/loop.Task[T]
//
// - bool : true when this task do something in this cycle, and more backlog can be.
// otherwise false.
//
// - error : same as err of github.com/opst/knitfab/pkg/loop.Break(err)
type Task[T any] func(context.Context, T) (T, bool, error)

// a Task which execute rt ('rt()') and p.Next() with the result.
func (rt Task[T]) Applied(p Policy) loop.Task[T] {
	return func(ctx context.Context, t T) (T, loop.Next) {
		new, ok, err := rt(ctx, t)
		return new, p.Next(ok, err)
	}
}
