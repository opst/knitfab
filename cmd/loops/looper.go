package main

import (
	"context"
	"fmt"
	"log"
	"time"

	apiruns "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/recurring"
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
	knit "github.com/opst/knitfab/pkg"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/loop"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/workloads/data"
	"github.com/opst/knitfab/pkg/workloads/k8s"
	"github.com/opst/knitfab/pkg/workloads/worker"
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
	// Loop type to be started
	Type kdb.LoopType

	// Policy for the looping
	Policy recurring.Policy

	// Hooks for the looping
	Hooks hook.Hook[apiruns.Detail]
}

// Start loop if needed.
//
// Args:
//
// - ctx
//
// - logger : logger for monitoring loop.
//
// - dbLoop
//
// - lType : loop type to be started
//
// - policy : How the loop control behaves for the task.
//
// - force : If false, loop is started only when the loop pool has some rooms.
// otherwise, loop is emploied forcibly.
func StartLoop(
	ctx context.Context,
	logger *log.Logger,
	kcluster knit.KnitCluster,
	kclient k8s.K8sClient,
	manifest LoopManifest,
) error {

	switch manifest.Type {
	case kdb.Projection:
		l := byLogger(logger, Copied(), WithPrefix("[projection loop]"))
		_, err := loop.Start(
			ctx, projection.Seed(),
			monitor(
				l,
				projection.Task(
					l, kcluster.Database().Runs(),
				).Applied(manifest.Policy),
			),
		)
		return err

	case kdb.Initialize:
		volumeConfig := kcluster.Config().DataAgent().Volume()
		_, err := loop.Start(
			ctx, initialize.Seed(),
			monitor(
				byLogger(logger, Copied(), WithPrefix("[initialize loop]")),
				initialize.Task(
					kcluster.Database().Runs(),
					initialize.PVCInitializer(
						kcluster.BaseCluster(),
						data.VolumeTemplate{
							Namespece:    kcluster.Namespace(),
							StorageClass: volumeConfig.StorageClassName(),
							Capacity:     volumeConfig.InitialCapacity(),
						},
					),
					manifest.Hooks,
				).Applied(manifest.Policy),
			),
			loop.WithTimeout(30*time.Second),
		)
		return err

	case kdb.RunManagement:
		// A map psuedo plan name to the psuedo plan manager
		pseudoPlanManagers := map[kdb.PseudoPlanName]manager.Manager{
			kdb.Uploaded: uploaded.New(
				kcluster.Database().Data(),
			),
			kdb.Imported: imported.New(),
		}
		_, err := loop.Start(
			ctx,
			// Initial RunCursor
			runManagement.Seed(utils.KeysOf(pseudoPlanManagers)),
			monitor(
				byLogger(logger, Copied(), WithPrefix("[run management loop]")),
				// loop body
				runManagement.Task(
					// Runs from DB
					kcluster.Database().Runs(),
					// A manager for starting a worker for a run.
					image.New(
						kcluster.GetWorker,
						kcluster.SpawnWorker,
						kcluster.Database().Runs().SetExit,
					),
					// A map of psuedo plan name to the psuedo plan manager
					pseudoPlanManagers,
					manifest.Hooks,
				).Applied(manifest.Policy),
			),
			loop.WithTimeout(30*time.Second),
		)
		return err

	case kdb.Finishing:
		pseudoPlanNames := []kdb.PseudoPlanName{
			kdb.Uploaded,
			kdb.Imported,
		}
		// Initial RunCursor
		runCursor := finishing.Seed(pseudoPlanNames)

		_, err := loop.Start(
			ctx, runCursor,
			monitor(
				byLogger(logger, Copied(), WithPrefix("[finishing loop]")),
				// loop body
				finishing.Task(
					// Runs from DB
					kcluster.Database().Runs(),
					// A worker finder function
					worker.Find,
					// A k8s cluster
					kcluster.BaseCluster(),
					manifest.Hooks,
				).Applied(manifest.Policy),
			),
		)
		return err

	case kdb.GarbageCollection:
		_, err := loop.Start(
			ctx, gc.Seed(),
			monitor(
				byLogger(logger, Copied(), WithPrefix("[gc loop]")),
				gc.Task(
					kclient, kcluster.Namespace(), kcluster.Database().Garbage(),
				).Applied(manifest.Policy),
			),
		)
		return err

	case kdb.Housekeeping:
		_, err := loop.Start(
			ctx, housekeeping.Seed(),
			monitor(
				byLogger(logger, Copied(), WithPrefix("[housekeepoing loop]")),
				housekeeping.Task(
					kcluster.Database().Data(),
					kcluster.BaseCluster(),
				).Applied(manifest.Policy),
			),
		)
		return err

	default:
		return fmt.Errorf("unsupported loop type: %s", manifest.Type)
	}
}
