package retry

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrRetry = errors.New("retry")

// Backoff is a (blocking) function returns when to retry.
//
// # Args
//
// - context: context. If context is canceled, Backoff should return ctx.Err().
//
// # Returns
//
// - error: nil if retry, non-nil if not.
type Backoff func(context.Context) error

// StaticBackoff returns a Backoff function that waits for a fixed interval.
//
// # Args
//
// - interval: interval to wait.
//
// # Returns
//
// Backoff function, which waits for `interval` or for context to be done.
var StaticBackoff = func(interval time.Duration) Backoff {
	return ExponentialBackoff(interval, 1)
}

// ExponentialBackoff returns a Backoff function that waits with exponential backoff.
//
// # Args
//
// - initialInterval: initial interval.
//
// - r: multiplier of interval.
//
// # Returns
//
// Backoff function.
// For N-th call, it waits for `initialInterval * r^N` or context to be done.
var ExponentialBackoff = func(initialInterval time.Duration, r float64) Backoff {
	interval := initialInterval
	return func(ctx context.Context) error {
		timer := time.NewTimer(interval)
		defer func() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			i := float64(interval) * r
			interval = time.Duration(int64(i))
			return nil
		}
	}
}

// Blocking calls f until it returns nil or non-retry error.
//
// # Args
//
// - ctx: context
//
// - b: backoff function
//
// - f: function to be called. If f returns ErrRetry, Blocking calls f again after backoff.
//
// # Returns
//
// - T: last return value of f
//
// - error: error returned by f
func Blocking[T any](ctx context.Context, b Backoff, f func() (T, error)) (T, error) {
	last := *new(T)
	for {
		if err := b(ctx); err != nil {
			return last, err
		}

		var err error
		last, err = f()
		if err == nil {
			return last, nil
		}
		if errors.Is(err, ErrRetry) {
			continue
		}
		return last, err
	}
}

type Result[T any] struct {
	Value T
	Err   error
}

type Promise[T any] <-chan Result[T]

func Failed[T any](err error) Promise[T] {
	ch := make(chan Result[T], 1)
	ch <- Result[T]{Err: err}
	close(ch)
	return ch
}

func Ok[T any](value T) Promise[T] {
	ch := make(chan Result[T], 1)
	ch <- Result[T]{Value: value}
	close(ch)
	return ch
}

// Go retries function f in background goroutine.
//
// # Args
//
// - ctx: context
//
// - b: backoff function
//
// - f: function to be called. If f returns ErrRetry, it calls f again after backoff.
//
// # Returns
//
// - <-chan Result[T]: channel to receive result.
func Go[T any](ctx context.Context, b Backoff, f func() (T, error)) <-chan Result[T] {
	ch := make(chan Result[T], 1)

	go func() {
		defer close(ch)
		defer func() {
			r := recover()
			var err error
			switch rr := r.(type) {
			case nil:
				return
			case error:
				err = rr
			default:
				err = fmt.Errorf("%+v", rr)
			}

			select {
			case ch <- Result[T]{Err: err}:
			default:
				panic(r)
			}
		}()

		ret, err := Blocking(ctx, b, f)
		ch <- Result[T]{Value: ret, Err: err}
	}()

	return ch
}
