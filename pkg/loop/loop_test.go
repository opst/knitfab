package loop_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/loop"
	"github.com/opst/knitfab/pkg/utils/try"
)

// get latency per waiting channel
//
// return value:
//
// - time.Duration : average of latency
// - time.Duration : standard deviation of latency
func calcurateLatencyOfChan() (time.Duration, time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // let ctx be done

	var average float64 = 0
	var variance float64 = 0
	var sampleSize float64 = 1024 * 1024

	for n := float64(1); n <= sampleSize; n++ {
		before := time.Now()
		<-ctx.Done() // wait channel
		latency := float64(time.Since(before).Nanoseconds())

		// update ave. & var. online ([[WARNING]] numerical instability can be here)
		// average : (previous total + new item) / processed items
		newAverage := ((n-1)*average + latency) / n
		// variance : (previous total variance + new item ** 2) / processed items, and then centering (with subtracting new average ** 2)
		variance = (((n-1)*(variance+math.Pow(average, 2)))+math.Pow(latency, 2))/n - math.Pow(newAverage, 2)

		average = newAverage
	}

	return time.Duration(average), time.Duration(math.Sqrt(variance))
}

func TestStart(t *testing.T) {
	AVERAGE, STDDEV := calcurateLatencyOfChan()
	ESTIMATED_OVERHEAD := AVERAGE + STDDEV // allow 1 sigma

	t.Run("it repeats tasks with interval until context get be done", func(t *testing.T) {
		period := 10 * time.Millisecond
		expectedMaxRepeat := int64(10)
		lifetime := time.Duration(period.Nanoseconds() * expectedMaxRepeat)
		expectedMinRepeat := int64(lifetime / (period + ESTIMATED_OVERHEAD))

		ctx, cancel := context.WithTimeout(context.Background(), lifetime)
		defer cancel()

		actual, err := loop.Start(
			ctx, 0, func(_ context.Context, v int64) (int64, loop.Next) {
				return v + 1, loop.Continue(period)
			},
		)

		if errors.Is(err, context.Canceled) {
			t.Error("expected error (Canceled) is not returned: ", err)
		}

		if actual < expectedMinRepeat || expectedMaxRepeat < actual {
			t.Errorf(
				"task run too much/less (actual, expected) = (%d, %d..%d)",
				actual, expectedMinRepeat, expectedMaxRepeat,
			)
		}
	})

	t.Run("it pass deadlined context when WithTimout is passed", func(t *testing.T) {
		ctx := context.Background()

		timeout := 100 * time.Millisecond

		try.To(loop.Start(
			ctx, 1, func(ctx context.Context, v int64) (int64, loop.Next) {
				now := time.Now()

				if deadline, ok := ctx.Deadline(); !ok {
					t.Errorf("deadline is not set")
				} else if !(deadline.Sub(now) <= timeout) {
					t.Errorf(
						"unexpected deadline\n===actual===\n%s\n===expected===\n(near) %s",
						deadline, now.Add(timeout),
					)
				}

				if 3 <= v {
					return v + 1, loop.Break(nil)
				}
				return v + 1, loop.Continue(20 * time.Millisecond)
			},
			loop.WithTimeout(timeout),
		)).OrFatal(t)
	})

	t.Run("it pass deadline-free context when WithTimout is not passed", func(t *testing.T) {
		ctx := context.Background()

		try.To(loop.Start(
			ctx, 1, func(ctx context.Context, v int64) (int64, loop.Next) {
				if deadline, ok := ctx.Deadline(); ok {
					t.Errorf("deadline is set: %s (now = %s)", deadline, time.Now())
				}

				if 3 <= v {
					return v + 1, loop.Break(nil)
				}
				return v + 1, loop.Continue(20 * time.Millisecond)
			},
		)).OrFatal(t)
	})

	t.Run("when context has been done before starting, it does nothing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		actual, err := loop.Start(
			ctx, 1, func(ctx context.Context, v int) (int, loop.Next) {
				return v + 1, loop.Continue(0)
			},
		)

		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}

		if actual != 1 {
			t.Errorf("loop does not honour context")
		}
	})

	t.Run("it repeats task until it does not Break", func(t *testing.T) {
		ctx := context.Background()

		expected := 10
		actual, err := loop.Start(ctx, 1, func(ctx context.Context, v int) (int, loop.Next) {
			new := v + 1
			if expected <= new {
				return new, loop.Break(nil)
			}
			return new, loop.Continue(0)
		})

		if err != nil {
			t.Fatal(err)
		}

		if actual != expected {
			t.Errorf("repeats too much/less. (actual, expected) = (%d, %d)", actual, expected)
		}
	})

	t.Run("it repeats task until it does not Break with error", func(t *testing.T) {
		ctx := context.Background()

		expectedErr := errors.New("break!")

		expected := 10
		actual, err := loop.Start(ctx, 1, func(ctx context.Context, v int) (int, loop.Next) {
			new := v + 1
			if expected <= new {
				return new, loop.Break(expectedErr)
			}
			return new, loop.Continue(0)
		})

		if !errors.Is(err, expectedErr) {
			t.Errorf("error is unexpected one. (actual, expected) = (%v, %v) ", err, expectedErr)
		}

		if actual != expected {
			t.Errorf("repeats too much/less. (actual, expected) = (%d, %d)", actual, expected)
		}
	})
}
