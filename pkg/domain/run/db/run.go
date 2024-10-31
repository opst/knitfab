package db

import (
	"context"
	"time"

	"github.com/opst/knitfab/pkg/domain"
)

type RunInterface interface {
	// create new run based a pseudo plan
	//
	// Along with this, data & mountpoint for output will be created.
	//
	// Args
	//
	// - context.Context
	//
	// - PseudoPlanName: name of the (pseudo) plan of the new run.
	//
	// - lifecycleSuspend: duration to suspend lifecycle of run.
	//
	// Returns
	//
	// - string: run id which is newly created. If success, means error is nil, the run is Running state.
	//
	// - error
	NewPseudo(ctx context.Context, planName domain.PseudoPlanName, lifecycleSuspend time.Duration) (string, error)

	// create a new Runs with image (in other words, perform Projection).
	//
	// This method creates new Runs for a randomly selected plan (with image) having activated nominations.
	//
	// Runs created by this way are...
	//
	// - set as waiting state
	//
	// - having inputs
	//
	// - having no output mountpoints nor workers
	//
	// thus, they can run (= have inputs enough), but not are ready (= outputs and worker are not determined).
	//
	// Returns
	//
	// - []string: created run ids
	//
	// - *ProjectionTrigger: (KnitId, MountPointId) pair which trigger creating runs.
	// If there are no nominations to be processed, this is nil.
	// This value can be non-nil even if []string is empty or nil,
	// when the nomination causes projection but no new runs are needed.
	//
	// - error
	//
	// Both []string and error can be nil when no runs are created.
	New(context.Context) (runId []string, triggeredBy *domain.ProjectionTrigger, err error)

	// update run status.
	//
	// To change state to Invalidated, use Delete.
	//
	// Args
	//
	// - context.Context
	//
	// - string: runid to be finished.
	//
	// - KnitRunStaus: new state.
	//
	// Returns
	//
	// - error: ErrInvalidRunStateChanging (when newState is Invalidated,
	// or not next of current state),
	// ErrMissing (when run is not found for given runId)
	SetStatus(ctx context.Context, runId string, newStatus domain.KnitRunStatus) error

	// update run exit.
	SetExit(ctx context.Context, runId string, exit domain.RunExit) error

	// pick next run of cursor, and change its status to the return value of func()
	//
	// Args
	//
	// - context.Context
	//
	// - cursorFrom: initial RunCursor
	//
	// - KnitRunStatus: status which run should be picked is.
	//
	// - func(Run) (KnitRunStatus, error): some task should occur along with Run state is transiting.
	//             The return value of this func is to be the next state of the run.
	//
	// Return
	//
	// - RunCursor: cursor points on picked (and updated, if succeeded) run.
	// If no runs can be picked, cursor state is as it was passed.
	//
	// - bool: it can be true only when the status is changed and saved in database.
	//
	// - error
	// ErrInvalidRunStateChanging (when the run with given runId is not completing nor aborting),
	PickAndSetStatus(ctx context.Context, cursorFrom domain.RunCursor, task func(domain.Run) (domain.KnitRunStatus, error)) (domain.RunCursor, bool, error)

	// update run status as "done" when completing or "failed" when aborting.
	//
	// Along with this, update system tags of output data.
	//
	// If run is in other state, it returns error ( ErrInvalidRunStateChanging )
	// and data are kept as it is.
	//
	// Args
	//
	// - context.Context
	//
	// - string: runid to be finished.
	//
	// Returns
	//
	// - error:
	// ErrInvalidRunStateChanging (when the run with given runId is not completing nor aborting),
	// ErrMissing (when run is not found for given runId)
	Finish(ctx context.Context, runId string) error

	// find runs which...
	//
	// - is based on a plan in argument "planId",
	//
	// - is passed a input data with a knitId in argument "knitIdInput",
	//
	// - makes a output data with a knitId in argument "knitIdOutput" and
	//
	// - is in a status which is in argument "status"
	//
	// (all conditions should be met).
	//
	// when some conditions are empty, such empty conditions are ignored and do not narrow results.
	//
	// Args
	// - context.Context
	//
	// - RunFindQuery: find runs which the query mathces
	//
	// Returns
	//
	// - []string: found runIds.
	//
	// - error
	Find(context.Context, domain.RunFindQuery) ([]string, error)

	// Retreive Run
	//
	// Args
	//
	// - context.Context:
	//
	// - []string: runId
	//
	// Returns
	//
	// - map[string][]Run: mapping runId->Run
	//
	// - error
	Get(ctx context.Context, runId []string) (map[string]domain.Run, error)

	// Delete Run.
	//
	// It means that,
	//
	// - change the run status to "invalidated",
	//
	// - delete its output data (from database and k8s eventually), and
	//
	// - delete its downstream runs which are "invalidated".
	//
	// To keep soundness of lineages,
	// it should cause error if the run to be deleted has
	// one or more not-invalidated runs in its downstream.
	//
	// If and only if the deleted run is "root" of lineage (should be pseudo-plan based run),
	// also the run will be deleted from database.
	//
	// Args
	//
	// - context.Context
	//
	// - string: runId to be deleted
	//
	// Returns
	//
	// - error
	//
	Delete(ctx context.Context, runId string) error

	// Delete Worker of runId.
	DeleteWorker(ctx context.Context, runId string) error

	// Retry Run.
	//
	// It means that, discard outputs of run and change the run status to "waiting".
	//
	// Runs which are "done" or "failed" and have no downstream runs can be retried.
	//
	// Args
	//
	// - context.Context
	//
	// - string: runId to be retried
	//
	// Returns
	//
	// - error:
	// ErrInvalidRunStateChanging, when the run with given runId is not done nor failed.;
	// ErrMissing, when run is not found for given runId.;
	// ErrRunHasDownstreams, when the run has downstream runs.;
	// ErrRunIsProtected, when the run is protected from retry, because it is pseudo run.;
	// and other errors from database.
	//
	Retry(ctx context.Context, runId string) error
}
