package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
)

type KnitRunStatus string

const (
	// This Run is deactivate the linked plan.
	Deactivated KnitRunStatus = "deactivated"

	// This Run is waiting to be run.
	Waiting KnitRunStatus = "waiting"

	// This run has fulfilled to start as a Worker.
	//
	// - WorkerName is decided
	// - VolumeRefs of output mountpoints are decided
	Ready KnitRunStatus = "ready"

	// This Run is starting
	Starting KnitRunStatus = "starting"

	// This Run is running.
	Running KnitRunStatus = "running"

	// It is observed that the run's worker has stopped successfully.
	Completing KnitRunStatus = "completing"

	// It is observed, or should be done that the run's worker has stopped insuccessfully.
	Aborting KnitRunStatus = "aborting"

	// This Run has been done, successfuly.
	Done KnitRunStatus = "done"

	// This Run stopped with error.
	Failed KnitRunStatus = "failed"

	// This run was discarded
	Invalidated KnitRunStatus = "invalidated"
)

func (krs KnitRunStatus) String() string {
	return string(krs)
}

// If run is in these KnitRunStatuses,
// data generated from that should have tag "knit#transient: processing"
func ProcessingStatuses() []KnitRunStatus {
	return []KnitRunStatus{
		Deactivated, Waiting, Ready, Starting,
		Running, Completing, Aborting,
	}
}

// If run is in these KnitRunStatuses,
// data generated from that should have tag "knit#transient: failed"
func FailedStatuses() []KnitRunStatus {
	return []KnitRunStatus{Failed, Invalidated}
}

func AsKnitRunStatus(status string) (KnitRunStatus, error) {
	switch status {
	case string(Waiting):
		return Waiting, nil
	case string(Ready):
		return Ready, nil
	case string(Starting):
		return Starting, nil
	case string(Running):
		return Running, nil
	case string(Completing):
		return Completing, nil
	case string(Aborting):
		return Aborting, nil
	case string(Done):
		return Done, nil
	case string(Failed):
		return Failed, nil
	case string(Deactivated):
		return Deactivated, nil
	case string(Invalidated):
		return Invalidated, nil
	default:
		return "", fmt.Errorf("'%s' is not KnitRunStatus", status)
	}
}

func (krs KnitRunStatus) HasStarted() bool {
	switch krs {
	case Waiting, Deactivated, Ready, Starting:
		return false
	default:
		return true
	}
}

func (krs KnitRunStatus) Invalidated() bool {
	switch krs {
	case Invalidated:
		return true
	default:
		return false
	}
}

func (krs KnitRunStatus) Processing() bool {
	switch krs {
	case Running, Completing, Aborting:
		return true
	default:
		return false
	}
}

// pair of run status [after updated, to be updated]
func GetRunStatusesForPlanActivate(activenessToBe bool) (KnitRunStatus, KnitRunStatus) {
	if activenessToBe {
		return Waiting, Deactivated // for activating plan
	}

	return Deactivated, Waiting // for deactivating plan
}

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
	NewPseudo(ctx context.Context, planName PseudoPlanName, lifecycleSuspend time.Duration) (string, error)

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
	New(context.Context) (runId []string, triggeredBy *ProjectionTrigger, err error)

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
	SetStatus(ctx context.Context, runId string, newStatus KnitRunStatus) error

	// update run exit.
	SetExit(ctx context.Context, runId string, exit RunExit) error

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
	// - error
	// ErrInvalidRunStateChanging (when the run with given runId is not completing nor aborting),
	// ErrMissing (when run is not found for given runId)
	PickAndSetStatus(ctx context.Context, cursorFrom RunCursor, task func(Run) (KnitRunStatus, error)) (RunCursor, error)

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
	Find(context.Context, RunFindQuery) ([]string, error)

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
	Get(ctx context.Context, runId []string) (map[string]Run, error)

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

type ProjectionTrigger struct {
	PlanId  string
	InputId int
	KnitId  string
}

func (pt *ProjectionTrigger) String() string {
	if pt == nil {
		return "(not triggered)"
	}
	return fmt.Sprintf(
		"plan{%s}.mountpoint{%d} = knit id %s",
		pt.PlanId, pt.InputId, pt.KnitId,
	)
}

type RunCursor struct {
	// Id of run which is picked at last time
	Head string

	// interval to pick same run without changing status.
	Debounce time.Duration

	// names of pseudo plan which picked run are based on.
	//
	// If it is nil or empty, it means "match plan-with-image based runs only".
	Pseudo []PseudoPlanName

	// strictly pick only pseudo plan based runs
	PseudoOnly bool

	// status of run which is picked
	Status []KnitRunStatus
}

func (r RunCursor) Equal(other RunCursor) bool {
	return r.Head == other.Head &&
		r.PseudoOnly == other.PseudoOnly &&
		cmp.SliceContentEq(r.Pseudo, other.Pseudo) &&
		cmp.SliceContentEq(r.Status, other.Status)
}

// parameter to query runs
//
// When all dimension matches a run, this query matches the run.
type RunFindQuery struct {
	// match if run is based on one of these plans.
	//
	// If it is nil or empty, it means "match any".
	PlanId []string

	// match if run's input is one of these knitId.
	//
	// If it is nil or empty, it means "match any".
	InputKnitId []string

	// match if run's output is one of these knitId.
	//
	// If it is nil or empty, it means "match any".
	OutputKnitId []string

	// match if run's status is one of these statuses.
	//
	// If it is nil or empty, it means "match any".
	Status []KnitRunStatus

	// match if run's updated time is equal or later than this UpdatedSince.
	UpdatedSince *time.Time

	// match if run's updated time is earlier than this UpdatedUntil.
	UpdatedUntil *time.Time
}

func (rfq RunFindQuery) Equal(other RunFindQuery) bool {
	return cmp.SliceContentEq(rfq.PlanId, other.PlanId) &&
		cmp.SliceContentEq(rfq.InputKnitId, other.InputKnitId) &&
		cmp.SliceContentEq(rfq.OutputKnitId, other.OutputKnitId) &&
		cmp.SliceContentEq(rfq.Status, other.Status) &&
		((rfq.UpdatedSince == nil && other.UpdatedSince == nil) ||
			(rfq.UpdatedSince != nil && other.UpdatedSince != nil && rfq.UpdatedSince.Equal(*other.UpdatedSince))) &&
		((rfq.UpdatedUntil == nil && other.UpdatedUntil == nil) ||
			(rfq.UpdatedUntil != nil && other.UpdatedUntil != nil && rfq.UpdatedUntil.Equal(*other.UpdatedUntil)))
}

// relation between run and data; "How does the run uses data?"
type Assignment struct {
	MountPoint

	// If no data has been assigned yet, this field is nil.
	KnitDataBody KnitDataBody
}

func (a *Assignment) Equal(o *Assignment) bool {
	return a.KnitDataBody.Equal(&o.KnitDataBody) &&
		a.MountPoint.Equal(&o.MountPoint)
}

type Log struct {
	Id           int
	Tags         *TagSet
	KnitDataBody KnitDataBody
}

func (l *Log) Equal(o *Log) bool {
	if (l == nil) || (o == nil) {
		return (l == nil) && (o == nil)
	}
	return l.Tags.Equal(o.Tags) &&
		l.KnitDataBody.Equal(&o.KnitDataBody)
}

// Core part of run.
type RunBody struct {
	Id     string
	Status KnitRunStatus

	// Name of worker, if any.
	//
	// When there are no worker for the run, this is left as zero-value.
	WorkerName string

	// last update timestamp.
	UpdatedAt time.Time

	// Exit status of the run, if any.
	Exit *RunExit

	// plan which the run is based.
	PlanBody
}

type RunExit struct {
	Code    uint8
	Message string
}

func (rb *RunBody) Equal(o *RunBody) bool {
	if (rb == nil) || (o == nil) {
		return (rb == nil) && (o == nil)
	}

	return rb.Id == o.Id &&
		rb.Status == o.Status &&
		rb.WorkerName == o.WorkerName &&
		rb.UpdatedAt.Equal(o.UpdatedAt) &&
		rb.PlanBody.Equal(&o.PlanBody)
}

type Run struct {
	RunBody

	// Name of worker of the run, if any.
	//
	// This field is empty if no worker is running, or going to run.
	Inputs  []Assignment
	Outputs []Assignment
	Log     *Log
}

func (r *Run) Equal(other *Run) bool {
	return r.RunBody.Equal(&other.RunBody) &&
		cmp.SliceContentEqWith(
			r.Inputs, other.Inputs,
			func(a, b Assignment) bool { return a.Equal(&b) },
		) &&
		cmp.SliceContentEqWith(
			r.Outputs, other.Outputs,
			func(a, b Assignment) bool { return a.Equal(&b) },
		) &&
		r.Log.Equal(other.Log)
}

var (
	ErrRunIsProtected    = errors.New("the run is protected")
	ErrRunHasDownstreams = fmt.Errorf("%w: depended on by downstream runs", ErrRunIsProtected)
	ErrWorkerActive      = fmt.Errorf("%w: possibly running", ErrRunIsProtected)

	ErrInvalidRunStateChanging = errors.New("cannot change run state")
)

func NewErrInvalidRunStateChanging(from, to KnitRunStatus) error {
	return fmt.Errorf("%w: %s -> %s", ErrInvalidRunStateChanging, from, to)
}
