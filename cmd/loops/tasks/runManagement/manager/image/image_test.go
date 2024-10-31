package image_test

import (
	"context"
	"errors"
	"io"
	"testing"

	apiruns "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager/image"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/runManagementHook"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
	kw "github.com/opst/knitfab/pkg/domain/run/k8s/worker"
	"github.com/opst/knitfab/pkg/utils/cmp"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
	kubeshm "k8s.io/apimachinery/pkg/runtime/schema"
)

type FakeWorker struct {
	runId     string
	jobStatus cluster.JobStatus
}

var _ kw.Worker = (*FakeWorker)(nil)

func (w *FakeWorker) RunId() string {
	return w.runId
}

func (w *FakeWorker) JobStatus(context.Context) cluster.JobStatus {
	return w.jobStatus
}

func (w *FakeWorker) Log(_ context.Context) (io.ReadCloser, error) {
	return nil, nil
}

func (w FakeWorker) Interface() kw.Worker {
	return &w
}

func (w *FakeWorker) Close() error {
	return nil
}

func TestManager_GetWorkerHasFailed(t *testing.T) {

	type When struct {
		run            domain.Run
		errSetExit     error
		errGetWorker   error
		errStartWorker error

		respBeforeToStartingHook runManagementHook.HookResponse
		errBeforeHook            error
	}

	type Then struct {
		wantBeforeHookInvoked bool

		wantStatus domain.KnitRunStatus
		wantError  error

		wantSetExitInvoked     bool
		wantStartWorkerInvoked bool
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			getWorker := func(context.Context, domain.Run) (kw.Worker, error) {
				return nil, when.errGetWorker
			}
			startWorkerInvoked := false
			startWorker := func(_ context.Context, _ domain.Run, env map[string]string) error {
				startWorkerInvoked = true
				if !cmp.MapEq(env, when.respBeforeToStartingHook.KnitfabExtension.Env) {
					t.Errorf("got env %v, want %v", env, when.respBeforeToStartingHook.KnitfabExtension.Env)
				}
				return when.errStartWorker
			}

			setExitInvoked := false
			setExit := func(_ context.Context, runId string, exit domain.RunExit) error {
				setExitInvoked = true
				if runId != when.run.Id {
					t.Errorf("got runId %v, want %v", runId, when.run.Id)
				}
				want := domain.RunExit{
					Code:    254,
					Message: "worker for the run is not found",
				}
				if exit != want {
					t.Errorf("got exit %v, want %v", exit, want)
				}
				return when.errSetExit
			}

			testee := image.New(getWorker, startWorker, setExit)

			beforeToStartingHookInvoked := false
			beforeToRunningHookInvoked := false
			beforeToCompletingHookInvoked := false
			beforeToAbortingHookInvoked := false
			hooks := runManagementHook.Hooks{
				ToStarting: hook.Func[apiruns.Detail, runManagementHook.HookResponse]{
					BeforeFn: func(d apiruns.Detail) (runManagementHook.HookResponse, error) {
						beforeToStartingHookInvoked = true
						want := bindruns.ComposeDetail(when.run)
						if !d.Equal(want) {
							t.Errorf("got detail %v, want %v", d, want)
						}
						return when.respBeforeToStartingHook, when.errBeforeHook
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("after hook should not be invoked")
						return nil
					},
				},
				ToRunning: hook.Func[apiruns.Detail, struct{}]{
					BeforeFn: func(d apiruns.Detail) (struct{}, error) {
						beforeToRunningHookInvoked = true
						want := bindruns.ComposeDetail(when.run)
						if !d.Equal(want) {
							t.Errorf("got detail %v, want %v", d, want)
						}
						return struct{}{}, when.errBeforeHook
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("after hook should not be invoked")
						return nil
					},
				},
				ToCompleting: hook.Func[apiruns.Detail, struct{}]{
					BeforeFn: func(d apiruns.Detail) (struct{}, error) {
						beforeToCompletingHookInvoked = true
						want := bindruns.ComposeDetail(when.run)
						if !d.Equal(want) {
							t.Errorf("got detail %v, want %v", d, want)
						}
						return struct{}{}, when.errBeforeHook
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("after hook should not be invoked")
						return nil
					},
				},
				ToAborting: hook.Func[apiruns.Detail, struct{}]{
					BeforeFn: func(d apiruns.Detail) (struct{}, error) {
						beforeToAbortingHookInvoked = true
						want := bindruns.ComposeDetail(when.run)
						if !d.Equal(want) {
							t.Errorf("got detail %v, want %v", d, want)
						}
						return struct{}{}, when.errBeforeHook
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("after hook should not be invoked")
						return nil
					},
				},
			}

			gotStatus, gotError := testee(ctx, hooks, when.run)

			if then.wantBeforeHookInvoked {
				switch when.run.Status {
				case domain.Ready:
					if !beforeToStartingHookInvoked {
						t.Errorf("ToStarting before hook should be invoked")
					}
					if beforeToRunningHookInvoked || beforeToCompletingHookInvoked || beforeToAbortingHookInvoked {
						t.Errorf("before hooks except ToStarting should not be invoked")
					}
				case domain.Starting, domain.Running, domain.Completing:
					if !beforeToAbortingHookInvoked {
						t.Errorf("ToAborting before hook should be invoked")
					}
					if beforeToStartingHookInvoked || beforeToRunningHookInvoked || beforeToCompletingHookInvoked {
						t.Errorf("before hooks except ToAborting should not be invoked")
					}
				}
			}

			if setExitInvoked != then.wantSetExitInvoked {
				t.Errorf("got setExitInvoked %v, want %v", setExitInvoked, then.wantSetExitInvoked)
			}

			if startWorkerInvoked != then.wantStartWorkerInvoked {
				t.Errorf("got startWorkerInvoked %v, want %v", startWorkerInvoked, then.wantStartWorkerInvoked)
			}

			if gotStatus != then.wantStatus {
				t.Errorf("got status %v, want %v", gotStatus, then.wantStatus)
			}

			if !errors.Is(gotError, then.wantError) {
				t.Errorf("got error %v, want %v", gotError, then.wantError)
			}
		}
	}

	{
		wantErr := errors.New("unexpected error")
		t.Run("when getWorker returns unexpected error, it should return the error", theory(
			When{
				run: domain.Run{
					RunBody: domain.RunBody{
						Id:         "run/ready",
						Status:     domain.Ready,
						WorkerName: "worker/ready",
						PlanBody: domain.PlanBody{
							PlanId: "plan/ready",
							Image: &domain.ImageIdentifier{
								Image:   "example.repo.invalid/ready",
								Version: "v1.0.0",
							},
						},
					},
				},
				errGetWorker:   wantErr,
				errStartWorker: nil,
			},
			Then{
				wantStatus:             domain.Ready,
				wantError:              wantErr,
				wantBeforeHookInvoked:  false,
				wantSetExitInvoked:     false,
				wantStartWorkerInvoked: false,
			},
		))
	}

	{
		t.Run("when getWorker for Ready Run returns NotFound error, it should start the worker for run in ready status", theory(
			When{
				run: domain.Run{
					RunBody: domain.RunBody{
						Id:         "run/ready",
						Status:     domain.Ready,
						WorkerName: "worker/ready",
						PlanBody: domain.PlanBody{
							PlanId: "plan/ready",
							Image: &domain.ImageIdentifier{
								Image:   "example.repo.invalid/ready",
								Version: "v1.0.0",
							},
						},
					},
				},
				errGetWorker: kubeerr.NewNotFound(
					kubeshm.GroupResource{Resource: "job"}, "worker/ready",
				),
				errStartWorker: nil,
				errBeforeHook:  nil,
				respBeforeToStartingHook: runManagementHook.HookResponse{
					KnitfabExtension: runManagementHook.KnitfabExtension{
						Env: map[string]string{
							"foo": "bar",
							"baz": "qux",
						},
					},
				},
			},
			Then{
				wantStatus:             domain.Starting,
				wantError:              nil,
				wantBeforeHookInvoked:  true,
				wantSetExitInvoked:     false,
				wantStartWorkerInvoked: true,
			},
		))
	}

	{
		wantErr := errors.New("unexpected error")
		t.Run("when setExit returns unexpected error, it should return the error", theory(
			When{
				run: domain.Run{
					RunBody: domain.RunBody{
						Id:         "run/starting",
						Status:     domain.Starting,
						WorkerName: "worker/starting",
						PlanBody: domain.PlanBody{
							PlanId: "plan/starting",
							Image: &domain.ImageIdentifier{
								Image:   "example.repo.invalid/starting",
								Version: "v1.0.0",
							},
						},
					},
				},
				errGetWorker: kubeerr.NewNotFound(
					kubeshm.GroupResource{Resource: "job"}, "worker/starting",
				),
				errSetExit:     wantErr,
				errStartWorker: nil,
			},
			Then{
				wantStatus:            domain.Starting,
				wantError:             wantErr,
				wantBeforeHookInvoked: true,
				wantSetExitInvoked:    true,
			},
		))
	}

	{
		wantErr := errors.New("unexpected error")
		t.Run("when getWorker returns NotFound error for a non-Ready Run but Before hook caused an error, it should return the error", theory(
			When{
				run: domain.Run{
					RunBody: domain.RunBody{
						Id:         "run/starting",
						Status:     domain.Starting,
						WorkerName: "worker/starting",
						PlanBody: domain.PlanBody{
							PlanId: "plan/ready",
							Image: &domain.ImageIdentifier{
								Image:   "example.repo.invalid/starting",
								Version: "v1.0.0",
							},
						},
					},
				},
				errGetWorker: kubeerr.NewNotFound(
					kubeshm.GroupResource{Resource: "job"}, "worker/starting",
				),
				errBeforeHook:  wantErr,
				errSetExit:     nil,
				errStartWorker: nil,
			},
			Then{
				wantStatus:             domain.Starting,
				wantError:              wantErr,
				wantBeforeHookInvoked:  true,
				wantSetExitInvoked:     false,
				wantStartWorkerInvoked: false,
			},
		))
	}

	{
		t.Run("when getWorker returns NotFound error for a non-Ready Run, it should translate from non-ready status to aborting", theory(
			When{
				run: domain.Run{
					RunBody: domain.RunBody{
						Id:         "run/starting",
						Status:     domain.Starting,
						WorkerName: "worker/starting",
						PlanBody: domain.PlanBody{
							PlanId: "plan/starting",
							Image: &domain.ImageIdentifier{
								Image:   "example.repo.invalid/starting",
								Version: "v1.0.0",
							},
						},
					},
				},
				errGetWorker: kubeerr.NewNotFound(
					kubeshm.GroupResource{Resource: "job"}, "worker/starting",
				),
				errBeforeHook:  nil,
				errSetExit:     nil,
				errStartWorker: nil,
			},
			Then{
				wantStatus:             domain.Aborting,
				wantError:              nil,
				wantBeforeHookInvoked:  true,
				wantSetExitInvoked:     true,
				wantStartWorkerInvoked: false,
			},
		))
	}

	{
		t.Run("when getWorker returns NotFound error for a Ready Run and startWorker returns already exists error, it should translate state from ready to starting", theory(
			When{
				run: domain.Run{
					RunBody: domain.RunBody{
						Id:         "run/ready",
						Status:     domain.Ready,
						WorkerName: "worker/ready",
						PlanBody: domain.PlanBody{
							PlanId: "plan/ready",
							Image: &domain.ImageIdentifier{
								Image:   "example.repo.invalid/ready",
								Version: "v1.0.0",
							},
						},
					},
				},
				errGetWorker: kubeerr.NewNotFound(
					kubeshm.GroupResource{Resource: "job"}, "worker/ready",
				),
				errStartWorker: kubeerr.NewAlreadyExists(
					kubeshm.GroupResource{Resource: "job"}, "worker/ready",
				),
				errBeforeHook: nil,
			},
			Then{
				wantStatus:             domain.Starting,
				wantError:              nil,
				wantBeforeHookInvoked:  true,
				wantSetExitInvoked:     false,
				wantStartWorkerInvoked: true,
			},
		))
	}

	{
		wantErr := errors.New("unexpected error")
		t.Run("when getWorker returns NotFound error and start worker returns unexpected error, it should return the error", theory(
			When{
				run: domain.Run{
					RunBody: domain.RunBody{
						Id:         "run/ready",
						Status:     domain.Ready,
						WorkerName: "worker/ready",
						PlanBody: domain.PlanBody{
							PlanId: "plan/ready",
							Image: &domain.ImageIdentifier{
								Image:   "example.repo.invalid/ready",
								Version: "v1.0.0",
							},
						},
					},
				},
				errGetWorker: kubeerr.NewNotFound(
					kubeshm.GroupResource{Resource: "job"}, "worker/ready",
				),
				errStartWorker: wantErr,
				errBeforeHook:  nil,
			},
			Then{
				wantStatus:             domain.Ready,
				wantError:              wantErr,
				wantBeforeHookInvoked:  true,
				wantSetExitInvoked:     false,
				wantStartWorkerInvoked: true,
			},
		))
	}
}

func TestManager_GetWorkerSucceeded(t *testing.T) {

	type When struct {
		runStatus     domain.KnitRunStatus
		jobStatus     cluster.JobStatus
		errBeforeHook error
		errSetExit    error
	}

	type Then struct {
		wantStatus            domain.KnitRunStatus
		wantErr               error
		wantSetExitInvoked    bool
		wantBeforeHookInvoked bool
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			run := domain.Run{
				RunBody: domain.RunBody{
					Id:         "run/example",
					Status:     when.runStatus,
					WorkerName: "worker/example",
					PlanBody: domain.PlanBody{
						PlanId: "plan/example",
						Image: &domain.ImageIdentifier{
							Image:   "example.repo.invalid/running",
							Version: "v1.0.0",
						},
					},
				},
			}

			getWorker := func(context.Context, domain.Run) (kw.Worker, error) {
				return &FakeWorker{
					runId:     run.Id,
					jobStatus: when.jobStatus,
				}, nil
			}

			setExitInvoked := false
			setExit := func(_ context.Context, runId string, exit domain.RunExit) error {
				setExitInvoked = true
				if runId != run.Id {
					t.Errorf("got runId %v, want %v", runId, run.Id)
				}
				want := domain.RunExit{
					Code:    when.jobStatus.Code,
					Message: when.jobStatus.Message,
				}
				if exit != want {
					t.Errorf("got exit %v, want %v", exit, want)
				}
				return when.errSetExit
			}

			beforeToStartingHookInvoked := false
			beforeToRunningHookInvoked := false
			beforeToCompletingHookInvoked := false
			beforeToAbortingHookInvoked := false
			hooks := runManagementHook.Hooks{
				ToStarting: hook.Func[apiruns.Detail, runManagementHook.HookResponse]{
					BeforeFn: func(d apiruns.Detail) (runManagementHook.HookResponse, error) {
						beforeToStartingHookInvoked = true
						want := bindruns.ComposeDetail(run)
						if !d.Equal(want) {
							t.Errorf("got detail %v, want %v", d, want)
						}
						return runManagementHook.HookResponse{}, when.errBeforeHook
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("after hook should not be invoked")
						return nil
					},
				},
				ToRunning: hook.Func[apiruns.Detail, struct{}]{
					BeforeFn: func(d apiruns.Detail) (struct{}, error) {
						beforeToRunningHookInvoked = true
						want := bindruns.ComposeDetail(run)
						if !d.Equal(want) {
							t.Errorf("got detail %v, want %v", d, want)
						}
						return struct{}{}, when.errBeforeHook
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("after hook should not be invoked")
						return nil
					},
				},
				ToCompleting: hook.Func[apiruns.Detail, struct{}]{
					BeforeFn: func(d apiruns.Detail) (struct{}, error) {
						beforeToCompletingHookInvoked = true
						want := bindruns.ComposeDetail(run)
						if !d.Equal(want) {
							t.Errorf("got detail %v, want %v", d, want)
						}
						return struct{}{}, when.errBeforeHook
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("after hook should not be invoked")
						return nil
					},
				},
				ToAborting: hook.Func[apiruns.Detail, struct{}]{
					BeforeFn: func(d apiruns.Detail) (struct{}, error) {
						beforeToAbortingHookInvoked = true
						want := bindruns.ComposeDetail(run)
						if !d.Equal(want) {
							t.Errorf("got detail %v, want %v", d, want)
						}
						return struct{}{}, when.errBeforeHook
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("after hook should not be invoked")
						return nil
					},
				},
			}

			testee := image.New(getWorker, nil, setExit)
			gotStatus, gotError := testee(ctx, hooks, run)

			if setExitInvoked != then.wantSetExitInvoked {
				t.Errorf("got setExitInvoked %v, want %v", setExitInvoked, then.wantSetExitInvoked)
			}

			if then.wantBeforeHookInvoked {
				switch when.jobStatus.Type {
				case cluster.Pending:
					if beforeToStartingHookInvoked || beforeToRunningHookInvoked || beforeToCompletingHookInvoked || beforeToAbortingHookInvoked {
						t.Errorf("before hooks should not be invoked")
					}
				case cluster.Running:
					if !beforeToRunningHookInvoked {
						t.Errorf("ToRunning before hook should be invoked")
					}
					if beforeToStartingHookInvoked || beforeToCompletingHookInvoked || beforeToAbortingHookInvoked {
						t.Errorf("before hooks except ToRunning should not be invoked")
					}
				case cluster.Succeeded:
					if !beforeToCompletingHookInvoked {
						t.Errorf("ToCompleting before hook should be invoked")
					}
					if beforeToStartingHookInvoked || beforeToRunningHookInvoked || beforeToAbortingHookInvoked {
						t.Errorf("before hooks except ToCompleting should not be invoked")
					}
				case cluster.Failed, cluster.Stucking:
					if !beforeToAbortingHookInvoked {
						t.Errorf("ToAborting before hook should be invoked")
					}
					if beforeToStartingHookInvoked || beforeToRunningHookInvoked || beforeToCompletingHookInvoked {
						t.Errorf("before hooks except ToAborting should not be invoked")
					}
				}
			}

			if gotStatus != then.wantStatus {
				t.Errorf("got status %v, want %v", gotStatus, then.wantStatus)
			}

			if !errors.Is(gotError, then.wantErr) {
				t.Errorf("got error %v, want nil", gotError)
			}
		}
	}

	t.Run("When worker for Starting Run is Pending, it stays Starting", theory(
		When{
			runStatus: domain.Starting,
			jobStatus: cluster.JobStatus{Type: cluster.Pending, Message: "Pending"},
		},
		Then{
			wantBeforeHookInvoked: false,
			wantSetExitInvoked:    false,
			wantStatus:            domain.Starting,
		},
	))

	t.Run("When worker for a Running Run is also Running, it stays Running", theory(
		When{
			runStatus: domain.Running,
			jobStatus: cluster.JobStatus{Type: cluster.Running},
		},
		Then{
			wantBeforeHookInvoked: false,
			wantSetExitInvoked:    false,
			wantStatus:            domain.Running,
		},
	))

	t.Run("When worker for Starting Run is Running, it translate run status to Running", theory(
		When{
			runStatus: domain.Starting,
			jobStatus: cluster.JobStatus{Type: cluster.Running},
		},
		Then{
			wantBeforeHookInvoked: true,
			wantStatus:            domain.Running,
		},
	))

	{
		wantErr := errors.New("unexpected error")
		t.Run("When worker for Starting Run is Running and Before hook returns an error, it should return the error", theory(
			When{
				errBeforeHook: wantErr,
				runStatus:     domain.Starting,
				jobStatus:     cluster.JobStatus{Type: cluster.Running},
			},
			Then{
				wantBeforeHookInvoked: true,
				wantSetExitInvoked:    false,
				wantStatus:            domain.Starting,
				wantErr:               wantErr,
			},
		))
	}

	{
		wantErr := errors.New("unexpected error")
		t.Run("When worker for Running Run is Done and Before hook returns an error, it should return the error", theory(
			When{
				errBeforeHook: wantErr,
				runStatus:     domain.Running,
				jobStatus:     cluster.JobStatus{Type: cluster.Succeeded, Code: 0, Message: "Completed"},
			},
			Then{
				wantBeforeHookInvoked: true,
				wantSetExitInvoked:    false,
				wantStatus:            domain.Running,
				wantErr:               wantErr,
			},
		))
	}

	{
		wantErr := errors.New("unexpected error")
		t.Run("When worker for Running Run is Failed and Before hook returns an error, it should return the error", theory(
			When{
				errBeforeHook: wantErr,
				runStatus:     domain.Running,
				jobStatus:     cluster.JobStatus{Type: cluster.Failed, Code: 1, Message: "Error"},
			},
			Then{
				wantBeforeHookInvoked: true,
				wantSetExitInvoked:    false,
				wantStatus:            domain.Running,
				wantErr:               wantErr,
			},
		))
	}

	t.Run("When worker is done, it translate run status to completing", theory(
		When{
			runStatus: domain.Running,
			jobStatus: cluster.JobStatus{Type: cluster.Succeeded, Code: 0, Message: "Completed"},
		},
		Then{
			wantBeforeHookInvoked: true,
			wantSetExitInvoked:    true,
			wantStatus:            domain.Completing,
		},
	))

	{
		wantError := errors.New("unexpected error")
		t.Run("When worker is done and setExit returnd an error, it should return the error", theory(
			When{
				runStatus:  domain.Running,
				jobStatus:  cluster.JobStatus{Type: cluster.Succeeded, Code: 0, Message: "Completed"},
				errSetExit: wantError,
			},
			Then{
				wantBeforeHookInvoked: true,
				wantSetExitInvoked:    true,

				wantStatus: domain.Running,
				wantErr:    wantError,
			},
		))
	}

	t.Run("When worker is failed, it translate run status to aborting", theory(
		When{
			runStatus: domain.Running,
			jobStatus: cluster.JobStatus{Type: cluster.Failed, Code: 1, Message: "Error"},
		},
		Then{
			wantBeforeHookInvoked: true,
			wantSetExitInvoked:    true,

			wantStatus: domain.Aborting,
			wantErr:    nil,
		},
	))

	{
		fakeErr := errors.New("fake error")
		t.Run("When worker is failed and setExit returns an error, it should return the error", theory(
			When{
				runStatus:  domain.Running,
				jobStatus:  cluster.JobStatus{Type: cluster.Failed, Code: 1, Message: "Error"},
				errSetExit: fakeErr,
			},
			Then{
				wantBeforeHookInvoked: true,
				wantSetExitInvoked:    true,

				wantStatus: domain.Running,
				wantErr:    fakeErr,
			},
		))

	}

	{
		wantErr := errors.New("unexpected error")
		t.Run("When worker for Starting Run is Stucked and Before hook returns an error, it should return the error", theory(
			When{
				errBeforeHook: wantErr,
				runStatus:     domain.Starting,
				jobStatus:     cluster.JobStatus{Type: cluster.Stucking, Code: 255, Message: "Stucking"},
			},
			Then{
				wantBeforeHookInvoked: true,
				wantSetExitInvoked:    false,
				wantStatus:            domain.Starting,
				wantErr:               wantErr,
			},
		))
	}

	t.Run("When worker is stucking, it translate run status to aborting", theory(
		When{
			runStatus: domain.Starting,
			jobStatus: cluster.JobStatus{Type: cluster.Stucking, Code: 255, Message: "Stucking"},
		},
		Then{
			wantBeforeHookInvoked: true,
			wantSetExitInvoked:    true,

			wantStatus: domain.Aborting,
			wantErr:    nil,
		},
	))

	{
		wantError := errors.New("unexpected error")
		t.Run("When worker is stucking and setExit returnd an error, it should return the error", theory(
			When{
				runStatus:  domain.Starting,
				jobStatus:  cluster.JobStatus{Type: cluster.Stucking, Code: 255, Message: "Stucking"},
				errSetExit: wantError,
			},
			Then{
				wantBeforeHookInvoked: true,
				wantSetExitInvoked:    true,

				wantStatus: domain.Starting,
				wantErr:    wantError,
			},
		))
	}
}
