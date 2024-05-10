package filewatch

import (
	"context"
	"fmt"

	"github.com/fsnotify/fsnotify"
)

// UntilModifyContext returns a context that is canceled
// when one of target files is modified (= written, created, removed, or renamed).
//
// # Args
//
// - ctx: context.Context
//
// - targetFilePath ...string: file pathes to be watched.
// When any of the files is modified, the context is canceled.
//
// # Returns
//
// - context.Context: context that is canceled when one of target files is modified.
//
// - func(): cancel function.
//
// - error: error caused when it fails to start watching files.
//
// If error is not nil, both of the the context and the cancel function are nil.
func UntilModifyContext(ctx context.Context, targetFilePath ...string) (context.Context, func(), error) {
	cctx, cancel := context.WithCancelCause(ctx)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		cancel(err)
		return nil, nil, err
	}

	go func() {
		defer w.Close()

		for {
			select {
			case <-cctx.Done():
				return
			case event, ok := <-w.Events:
				if !ok {
					return
				}
				cancel(fmt.Errorf("%s is updated (%s)", event.Name, event.Op.String()))
			}
		}
	}()

	for _, f := range targetFilePath {
		if err = w.Add(f); err != nil {
			cancel(err)
			return nil, nil, err
		}
	}
	return cctx, func() { cancel(nil) }, nil
}
