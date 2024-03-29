package worker

import (
	"context"
	"io"
	"time"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils/retry"
	k8s "github.com/opst/knitfab/pkg/workloads/k8s"
	"github.com/opst/knitfab/pkg/workloads/metasource"
	kubebatch "k8s.io/api/batch/v1"
)

type Status string

const (
	Pending Status = "pending"
	Running Status = "running"
	Done    Status = "done"
	Failed  Status = "failed"
)

type Worker interface {
	// RunId returns the run ID of the worker
	RunId() string

	// JobStatus returns the status of the job
	JobStatus() Status

	// ExitCode returns the exit code of the main container of job
	//
	// # Returns
	//
	// - exitCode : the exit code of the main container.
	//
	// - reason: the reason of the exit.
	//
	// - ok : true if the worker has been stopped, false otherwise.
	ExitCode() (uint8, string, bool)

	// Log returns the log of the worker's main container.
	//
	// # Returns
	//
	// - io.ReadCloser : the log stream of the main container.
	//
	// - error : error if any.
	Log(ctx context.Context) (io.ReadCloser, error)

	// Close closes the worker
	Close() error
}

type worker struct {
	runId string
	job   k8s.Job
}

func (w *worker) RunId() string {
	return w.runId
}

func (w *worker) JobStatus() Status {
	switch w.job.Status() {
	case k8s.Succeeded:
		return Done
	case k8s.Failed:
		return Failed
	case k8s.Pending:
		return Pending
	default:
		return Running
	}
}

func (w *worker) ExitCode() (uint8, string, bool) {
	return w.job.ExitCode("main")
}

func (w *worker) Log(ctx context.Context) (io.ReadCloser, error) {
	return w.job.Log(ctx, "main")
}

func (w *worker) Close() error {
	return w.job.Close()
}

// spawn new Worker and start to Run
//
// # params:
//
// - ctx
//
// - cluster : where the Worker is spawned into
//
//   - run : the spec of the run to be start.
//     New Workers are created based on the run spec "as-is basis",
//     and do not complement anything.
//     PVCs for data should have been provisioned already.
func Spawn(
	ctx context.Context,
	cluster k8s.Cluster,
	kc *bconf.KnitClusterConfig,
	ex metasource.ResourceBuilder[*bconf.KnitClusterConfig, *kubebatch.Job],
) (Worker, error) {
	prom := <-cluster.NewJob(
		ctx,
		retry.StaticBackoff(3*time.Second),
		ex.Build(kc),
	)

	if prom.Err != nil {
		return nil, prom.Err
	}

	return &worker{
		runId: ex.Id(),
		job:   prom.Value,
	}, nil
}

// Find Workers that match runBody's criteria
func Find(
	ctx context.Context,
	cluster k8s.Cluster,
	runBody kdb.RunBody,
) (Worker, error) {
	prom := <-cluster.GetJob(
		ctx,
		retry.StaticBackoff(3*time.Second),
		runBody.WorkerName,
	)

	if prom.Err != nil {
		return nil, prom.Err
	}

	return &worker{
		runId: runBody.Id,
		job:   prom.Value,
	}, nil
}
