package main

import (
	"context"
	"log"
	"time"

	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/loop"
	"github.com/opst/knitfab/cmd/loops/loop/recurring"
	"github.com/opst/knitfab/cmd/loops/tasks/finishing"
	"github.com/opst/knitfab/cmd/loops/tasks/gc"
	"github.com/opst/knitfab/cmd/loops/tasks/housekeeping"
	"github.com/opst/knitfab/cmd/loops/tasks/initialize"
	"github.com/opst/knitfab/cmd/loops/tasks/projection"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager/image"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager/imported"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager/uploaded"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/runManagementHook"
	cfg_hook "github.com/opst/knitfab/pkg/configs/hook"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/knitfab"
	"github.com/opst/knitfab/pkg/utils/slices"
)

type LoggerOptions func(*log.Logger) *log.Logger

func byLogger(l *log.Logger, opt ...LoggerOptions) *log.Logger {
	for _, o := range opt {
		l = o(l)
	}
	return l
}

func Copied() LoggerOptions {
	return func(l *log.Logger) *log.Logger {
		return log.New(l.Writer(), l.Prefix(), l.Flags())
	}
}

func WithPrefix(pre string) LoggerOptions {
	return func(l *log.Logger) *log.Logger {
		l.SetPrefix(pre)
		return l
	}
}

func WithTimestamp() LoggerOptions {
	return func(l *log.Logger) *log.Logger {
		l.SetFlags(l.Flags() | log.Ldate | log.Ltime | log.Lmicroseconds)
		return l
	}
}

// Wrapper for monitoring loop tasks
//
//	Log the start and end of each time a task is executed. Essentially, it executes a task.
func monitor[T any](logger *log.Logger, task loop.Task[T]) loop.Task[T] {
	// counter for execution of the task
	var counter uint64
	return func(ctx context.Context, t T) (ret T, next loop.Next) {
		// func() capture the 'counter' variable
		counter += 1
		timestamp := time.Now()

		logger.Printf("task start: #0x%X: ", counter)

		// log at the end of the task
		defer func() {
			logger.Printf(
				"task end: #0x%X (takes %s): %s\n with value = %#v",
				counter, time.Since(timestamp), next, ret,
			)
		}()

		// execute the task specified by the argument
		ret, next = task(ctx, t)
		return
	}
}

// Manifest for starting a loop, which determines how the loop should behave.
type LoopManifest struct {
	// Policy for the looping
	Policy recurring.Policy

	// Hooks for the looping
	Hooks cfg_hook.Config
}

func mergeEmptyStruct(a, b struct{}) struct{} {
	return struct{}{}
}

// Start proection loop
//
// Args:
//
// - ctx
//
// - logger : logger for monitoring loop.
//
// - kcluster : k8s cluster client
//
// - manifest
func StartProjectionLoop(
	ctx context.Context,
	logger *log.Logger,
	knit knitfab.Knitfab,
	manifest LoopManifest,
) error {
	l := byLogger(logger, Copied(), WithPrefix("[projection loop]"))
	_, err := loop.Start(
		ctx, projection.Seed(),
		monitor(
			l,
			projection.Task(
				l, knit.Run().Database(),
			).Applied(manifest.Policy),
		),
	)
	return err
}

func StartInitializeLoop(
	ctx context.Context,
	logger *log.Logger,
	knit knitfab.Knitfab,
	manifest LoopManifest,
) error {
	_, err := loop.Start(
		ctx, initialize.Seed(),
		monitor(
			byLogger(logger, Copied(), WithPrefix("[initialize loop]")),
			initialize.Task(
				knit.Run().Database(),
				knit.Run().K8s(),
				hook.Build(manifest.Hooks.Lifecycle, mergeEmptyStruct),
			).Applied(manifest.Policy),
		),
		loop.WithTimeout(30*time.Second),
	)
	return err
}

func StartRunManagementLoop(
	ctx context.Context,
	logger *log.Logger,
	knit knitfab.Knitfab,
	manifest LoopManifest,
) error {
	// A map psuedo plan name to the psuedo plan manager
	pseudoPlanManagers := map[domain.PseudoPlanName]manager.Manager{
		domain.Uploaded: uploaded.New(
			knit.Data().Database(),
		),
		domain.Imported: imported.New(),
	}
	_, err := loop.Start(
		ctx,
		// Initial RunCursor
		runManagement.Seed(slices.KeysOf(pseudoPlanManagers)),
		monitor(
			byLogger(logger, Copied(), WithPrefix("[run management loop]")),
			// loop body
			runManagement.Task(
				// Runs from DB
				knit.Run().Database(),
				// A manager for starting a worker for a run.
				image.New(
					knit.Run().K8s(),
					knit.Run().Database(),
				),
				// A map of psuedo plan name to the psuedo plan manager
				pseudoPlanManagers,

				runManagementHook.Hooks{
					ToStarting:   hook.Build(manifest.Hooks.Lifecycle, runManagementHook.Merge), // ready -> starting
					ToRunning:    hook.Build(manifest.Hooks.Lifecycle, mergeEmptyStruct),        // starting -> running
					ToCompleting: hook.Build(manifest.Hooks.Lifecycle, mergeEmptyStruct),        // running -> completing
					ToAborting:   hook.Build(manifest.Hooks.Lifecycle, mergeEmptyStruct),        // running -> aborting
				},
			).Applied(manifest.Policy),
		),
		loop.WithTimeout(30*time.Second),
	)
	return err
}

func StartFinishingLoop(
	ctx context.Context,
	logger *log.Logger,
	knit knitfab.Knitfab,
	manifest LoopManifest,
) error {
	pseudoPlanNames := []domain.PseudoPlanName{
		domain.Uploaded,
		domain.Imported,
	}
	// Initial RunCursor
	runCursor := finishing.Seed(pseudoPlanNames)

	_, err := loop.Start(
		ctx, runCursor,
		monitor(
			byLogger(logger, Copied(), WithPrefix("[finishing loop]")),
			// loop body
			finishing.Task(
				knit.Run().Database(),
				knit.Run().K8s(),
				hook.Build(manifest.Hooks.Lifecycle, mergeEmptyStruct),
			).Applied(manifest.Policy),
		),
	)
	return err
}

func StartGarbageCollectionLoop(
	ctx context.Context,
	logger *log.Logger,
	knit knitfab.Knitfab,
	manifest LoopManifest,
) error {
	_, err := loop.Start(
		ctx, gc.Seed(),
		monitor(
			byLogger(logger, Copied(), WithPrefix("[gc loop]")),
			gc.Task(
				knit.Garbage().K8s(),
				knit.Garbage().Database(),
			).Applied(manifest.Policy),
		),
	)
	return err
}

func StartHousekeepingLoop(
	ctx context.Context,
	logger *log.Logger,
	knit knitfab.Knitfab,
	manifest LoopManifest,
) error {
	_, err := loop.Start(
		ctx, housekeeping.Seed(),
		monitor(
			byLogger(logger, Copied(), WithPrefix("[housekeepoing loop]")),
			housekeeping.Task(
				knit.Data().Database(),
				knit.Data().K8s(),
			).Applied(manifest.Policy),
		),
	)
	return err
}
