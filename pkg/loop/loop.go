package loop

import (
	"context"
	"fmt"
	"time"
)

type Next struct {
	// if not nil, breaks with error
	err error

	// if quit == true and err == nil, breaks without error
	quit bool

	// otherwise, continue loop with interval.
	interval time.Duration
}

func (n Next) String() string {
	if n.err != nil {
		return fmt.Sprintf("[break] with error: %v", n.err)
	}
	if n.quit {
		return "[break] without error"
	}

	return fmt.Sprintf("[continue] interval: %s", n.interval)
}

// continue loop.
//
// args:
//
// - interval: sleep before starting next task.
func Continue(interval time.Duration) Next {
	return Next{interval: interval}
}

// break loop.
//
// args:
//
// - err: If you break loop with error, set non nil value.
func Break(err error) Next {
	return Next{quit: true, err: err}
}

// Task for .
//
// args:
//
// - context.Context: (sub-)context which is passed to task.Run.
//
// - Chain[T]
//
type Task[T any] func(context.Context, T) (T, Next)

// Start task in loop.
//
// Task and Loop
//
// Task should return 2 value.
//
// - T : any value the task needs.
// It can be statistics, result of processing, or something else.
//
// - next: it can be Continue(time.Duration) or Break(error).
// To run one more time, return Continue(time.Duration).
// Your task will be called with context and the last T after time.Duration (can be 0).
// If it is enough, return Break(error). When there are no error, you can pass nil.
// Zero value (Next{}) equals Continue(0), that is, "go next ASAP!".
//
// Example
//
// Count 1 to 10:
//
// 	Start(ctx, 1, func(_ context.Context, value int) (int, Next) {
// 		value += 1
// 		if 10 <= value {
// 			return value, Break(nil)
// 		}
// 		return value, Continue(0)
// 	})
//
// send GET request to web server, honouring Location and Retry-After header:
//
// 	Start(ctx, "http://example.org", func(ctx context.Context, url string) (string, Next) {
// 		rer, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
// 		if err != nil {
// 			return url, Break(err)
// 		}
// 		resp, err := http.DefaultClient.Do(req)
// 		if err != nil {
// 			return url, Break(err)
// 		}
// 		defer resp.Body.Close()
//
// 		nextUrl := url
// 		if location := resp.Header.Get("location"); location != "" {
// 			nextUrl = location
// 		}
//
// 		retryAfter := resp.Header.Get("retry-after")
//		interval := 0
// 		if delay, err := sttrconv.Atoi(retryAfter); err == nil {
//			interval = delay * time.Second
//		} else if retry, err := http.parseTime(retryAfter); err != nil {
// 			return url, Break(err)
// 		} else {
//			interval = time.Until(retry)
//		}
//		if interval < 0 {
//			interval = 0
//		}
//
// 		if 300 <= resp.StatusCode && resp.StatusCode < 400 ||  // redirection
// 			resp.StatusCode == 201 { // get created resource
// 			return nextUrl, Continue(interval)  // follow location
// 		}
// 		if resp.StatusCode == 429 ||  // Too Many Requests
// 			500 <= resp.StatusCode {  // other serverside error
// 			return url, Continue(interval)  // retry after interval
// 		}
//
// 		if 200 <= resp.StatusCode && resp.StatusCode < 300 { // OK!
// 			payload, err := io.ReadAll(resp.Body)
// 			if err != nil {
// 				return url, Break(err)
// 			}
// 			return string(payload), Break(nil)
// 		}
// 		if 400 <= resp.StatusCode && resp.StatusCode < 500 {  // request matters...
// 			payload, err := io.ReadAll(resp.Body)
// 			if err != nil {
// 				return url, Break(err)
// 			}
// 			return url, Break(fmt.Errorf("error (status = %d): %s", resp.StatusCode, string(payload)))
// 		}
//
// 		return url, Break(fmt.Errorf("unsupportted status code: %d", resp.StatusCode))
// 	})
//
// Args
//
// - ctx : context. When this context get be Done, loop will be break with ctx.Err().
//
// - init : your task will be called as task(ctx, init) at the first time.
//
// - task : task receiving (context, last value), then return (new value, Continue() or Break()).
//
// Returns
//
// - T: T task returns at last.
// This value is always returned wheather or not it returns non-nil error together.
//
// - error: error in Break(error). It is nil when loop breaks with Break(nil).
//
// - options: options for loop.
//
func Start[T any](ctx context.Context, init T, task Task[T], options ...LoopOption) (T, error) {
	select {
	case <-ctx.Done():
		return init, ctx.Err()
	default:
	}

	value := init
	for {
		interval := 0 * time.Nanosecond

		lc := &loopConfig{ctx: ctx}
		for _, opt := range options {
			lc = opt(lc)
		}

		v, n := func() (T, Next) {
			ctx := lc.ctx
			if lc.deferred != nil {
				defer lc.deferred()
			}
			return task(ctx, value)
		}()

		if n.err != nil {
			return v, n.err
		} else if n.quit {
			return v, nil
		} else {
			value = v
			interval = n.interval
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			// shitting down is priority. it should come first, and checking timer later.
			if !timer.Stop() {
				<-timer.C // drain. see: time.Timer.Stop's document
			}
			return value, ctx.Err()

		case <-timer.C:
			continue
		}
	}
}

type loopConfig struct {
	ctx      context.Context
	deferred func()
}

type LoopOption func(*loopConfig) *loopConfig

// set timeout per loop
//
// this timeout is set on context.Context passed to task.
func WithTimeout(d time.Duration) LoopOption {
	return func(lc *loopConfig) *loopConfig {
		ctx, cancel := context.WithTimeout(lc.ctx, d)
		return &loopConfig{
			ctx: ctx,
			deferred: func() {
				if lc.deferred != nil {
					defer lc.deferred()
				}
				cancel()
			},
		}
	}
}
