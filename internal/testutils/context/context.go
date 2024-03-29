package context

import (
	"context"
	"testing"
	"time"
)

// wrap contest with deadline
//
// the deadline is 1 second before test's deadline, to be able to clean-up resources.
func WithTest(ctx context.Context, t *testing.T) (context.Context, func()) {
	if deadline, ok := t.Deadline(); ok {
		dctx, cancel := context.WithDeadline(ctx, deadline.Add(-time.Second))
		return dctx, cancel
	}
	return ctx, func() {}
}
