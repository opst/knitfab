package image_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager/image"
	api_runs "github.com/opst/knitfab/pkg/api/types/runs"
	kdb "github.com/opst/knitfab/pkg/db"
	kw "github.com/opst/knitfab/pkg/workloads/worker"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
	kubeshm "k8s.io/apimachinery/pkg/runtime/schema"
)

type FakeWorker struct {
	runId      string
	jobStatus  kw.Status
	exitCode   uint8
	exitReason string
	exitOk     bool
}

var _ kw.Worker = (*FakeWorker)(nil)

func (w *FakeWorker) RunId() string {
	return w.runId
}

func (w *FakeWorker) JobStatus() kw.Status {
	return w.jobStatus
}

func (w *FakeWorker) ExitCode() (uint8, string, bool) {
	return w.exitCode, w.exitReason, w.exitOk
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
		run            kdb.Run
		errSetExit     error
		errGetWorker   error
		errStartWorker error
		errBeforeHook  error
	}

	type Then struct {
		wantBeforeHookInvoked bool

		wantStatus kdb.KnitRunStatus
		wantError  error

		wantSetExitInvoked     bool
		wantStartWorkerInvoked bool
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			getWorker := func(context.Context, kdb.Run) (kw.Worker, error) {
				return nil, when.errGetWorker
			}
			startWorkerInvoked := false
			startWorker := func(context.Context, kdb.Run) error {
				startWorkerInvoked = true
				return when.errStartWorker
			}

			setExitInvoked := false
			setExit := func(_ context.Context, runId string, exit kdb.RunExit) error {
				setExitInvoked = true
				if runId != when.run.Id {
					t.Errorf("got runId %v, want %v", runId, when.run.Id)
				}
				want := kdb.RunExit{
					Code:    254,
					Message: "worker for the run is not found",
				}
				if exit != want {
					t.Errorf("got exit %v, want %v", exit, want)
				}
				return when.errSetExit
			}

			testee := image.New(getWorker, startWorker, setExit)

			beforeHookInvoked := false
			h := hook.Func[api_runs.Detail]{
				BeforeFn: func(d api_runs.Detail) error {
					beforeHookInvoked = true
					want := api_runs.ComposeDetail(when.run)
					if !d.Equal(&want) {
						t.Errorf("got detail %v, want %v", d, want)
					}
					return when.errBeforeHook
				},
				AfterFn: func(d api_runs.Detail) error {
					t.Error("after hook should not be invoked")
					return nil
				},
			}
			gotStatus, gotError := testee(ctx, h, when.run)

			if beforeHookInvoked != then.wantBeforeHookInvoked {
				t.Errorf("got beforeHookInvoked %v, want %v", beforeHookInvoked, then.wantBeforeHookInvoked)
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
				run: kdb.Run{
					RunBody: kdb.RunBody{
						Id:         "run/ready",
						Status:     kdb.Ready,
						WorkerName: "worker/ready",
						PlanBody: kdb.PlanBody{
							PlanId: "plan/ready",
							Image: &kdb.ImageIdentifier{
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
				wantStatus:             kdb.Ready,
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
				run: kdb.Run{
					RunBody: kdb.RunBody{
						Id:         "run/ready",
						Status:     kdb.Ready,
						WorkerName: "worker/ready",
						PlanBody: kdb.PlanBody{
							PlanId: "plan/ready",
							Image: &kdb.ImageIdentifier{
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
			},
			Then{
				wantStatus:             kdb.Starting,
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
				run: kdb.Run{
					RunBody: kdb.RunBody{
						Id:         "run/starting",
						Status:     kdb.Starting,
						WorkerName: "worker/starting",
						PlanBody: kdb.PlanBody{
							PlanId: "plan/starting",
							Image: &kdb.ImageIdentifier{
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
				wantStatus:            kdb.Starting,
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
				run: kdb.Run{
					RunBody: kdb.RunBody{
						Id:         "run/starting",
						Status:     kdb.Starting,
						WorkerName: "worker/starting",
						PlanBody: kdb.PlanBody{
							PlanId: "plan/ready",
							Image: &kdb.ImageIdentifier{
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
				wantStatus:             kdb.Starting,
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
				run: kdb.Run{
					RunBody: kdb.RunBody{
						Id:         "run/starting",
						Status:     kdb.Starting,
						WorkerName: "worker/starting",
						PlanBody: kdb.PlanBody{
							PlanId: "plan/starting",
							Image: &kdb.ImageIdentifier{
								Image:   "example.repo.invalid/starting",
								Version: "v1.0.0",
							},
						},
					},
				},
				errGetWorker: kubeerr.NewNotFound(
					kubeshm.GroupResource{Resource: "job"}, "worker/starting",
				),
				errStartWorker: nil,
				errSetExit:     nil,
				errBeforeHook:  nil,
			},
			Then{
				wantStatus:             kdb.Aborting,
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
				run: kdb.Run{
					RunBody: kdb.RunBody{
						Id:         "run/ready",
						Status:     kdb.Ready,
						WorkerName: "worker/ready",
						PlanBody: kdb.PlanBody{
							PlanId: "plan/ready",
							Image: &kdb.ImageIdentifier{
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
				wantStatus:             kdb.Starting,
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
				run: kdb.Run{
					RunBody: kdb.RunBody{
						Id:         "run/ready",
						Status:     kdb.Ready,
						WorkerName: "worker/ready",
						PlanBody: kdb.PlanBody{
							PlanId: "plan/ready",
							Image: &kdb.ImageIdentifier{
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
				wantStatus:             kdb.Ready,
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
		runStatus     kdb.KnitRunStatus
		jobStatus     kw.Status
		errBeforeHook error
		exitCode      uint8
		exitReason    string
		exitOk        bool
		errSetExit    error
	}

	type Then struct {
		wantStatus            kdb.KnitRunStatus
		wantErr               error
		wantSetExitInvoked    bool
		wantBeforeHookInvoked bool
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			run := kdb.Run{
				RunBody: kdb.RunBody{
					Id:         "run/example",
					Status:     when.runStatus,
					WorkerName: "worker/example",
					PlanBody: kdb.PlanBody{
						PlanId: "plan/example",
						Image: &kdb.ImageIdentifier{
							Image:   "example.repo.invalid/running",
							Version: "v1.0.0",
						},
					},
				},
			}

			getWorker := func(context.Context, kdb.Run) (kw.Worker, error) {
				return &FakeWorker{
					runId:      run.Id,
					jobStatus:  when.jobStatus,
					exitCode:   when.exitCode,
					exitReason: when.exitReason,
					exitOk:     when.exitOk,
				}, nil
			}

			setExitInvoked := false
			setExit := func(_ context.Context, runId string, exit kdb.RunExit) error {
				setExitInvoked = true
				if runId != run.Id {
					t.Errorf("got runId %v, want %v", runId, run.Id)
				}
				want := kdb.RunExit{
					Code:    when.exitCode,
					Message: when.exitReason,
				}
				if exit != want {
					t.Errorf("got exit %v, want %v", exit, want)
				}
				return when.errSetExit
			}

			beforeHookInvoked := false
			h := hook.Func[api_runs.Detail]{
				BeforeFn: func(d api_runs.Detail) error {
					beforeHookInvoked = true
					want := api_runs.ComposeDetail(run)
					if !d.Equal(&want) {
						t.Errorf("got detail %v, want %v", d, want)
					}
					return when.errBeforeHook
				},
				AfterFn: func(d api_runs.Detail) error {
					t.Error("after hook should not be invoked")
					return nil
				},
			}

			testee := image.New(getWorker, nil, setExit)
			gotStatus, gotError := testee(ctx, h, run)

			if setExitInvoked != then.wantSetExitInvoked {
				t.Errorf("got setExitInvoked %v, want %v", setExitInvoked, then.wantSetExitInvoked)
			}

			if beforeHookInvoked != then.wantBeforeHookInvoked {
				t.Errorf("got beforeHookInvoked %v, want %v", beforeHookInvoked, then.wantBeforeHookInvoked)
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
			runStatus: kdb.Starting,
			jobStatus: kw.Pending,
		},
		Then{
			wantBeforeHookInvoked: false,
			wantSetExitInvoked:    false,
			wantStatus:            kdb.Starting,
		},
	))

	t.Run("When worker for a Running Run is also Running, it stays Running", theory(
		When{
			runStatus: kdb.Running,
			jobStatus: kw.Running,
		},
		Then{
			wantBeforeHookInvoked: false,
			wantSetExitInvoked:    false,
			wantStatus:            kdb.Running,
		},
	))

	t.Run("When worker for Starting Run is Running, it translate run status to Running", theory(
		When{
			runStatus: kdb.Starting,
			jobStatus: kw.Running,
		},
		Then{
			wantBeforeHookInvoked: true,
			wantStatus:            kdb.Running,
		},
	))

	{
		wantErr := errors.New("unexpected error")
		t.Run("When worker for Starting Run is Running and Before hook returns an error, it should return the error", theory(
			When{
				errBeforeHook: wantErr,
				runStatus:     kdb.Starting,
				jobStatus:     kw.Running,
			},
			Then{
				wantBeforeHookInvoked: true,
				wantSetExitInvoked:    false,
				wantStatus:            kdb.Starting,
				wantErr:               wantErr,
			},
		))
	}

	{
		wantErr := errors.New("unexpected error")
		t.Run("When worker for Running Run is Done and Before hook returns an error, it should return the error", theory(
			When{
				errBeforeHook: wantErr,
				runStatus:     kdb.Running,
				jobStatus:     kw.Done,
				exitCode:      0,
				exitReason:    "Completed",
				exitOk:        true,
			},
			Then{
				wantBeforeHookInvoked: true,
				wantSetExitInvoked:    false,
				wantStatus:            kdb.Running,
				wantErr:               wantErr,
			},
		))
	}

	{
		wantErr := errors.New("unexpected error")
		t.Run("When worker for Running Run is Failed and Before hook returns an error, it should return the error", theory(
			When{
				errBeforeHook: wantErr,
				runStatus:     kdb.Running,
				jobStatus:     kw.Failed,
				exitCode:      1,
				exitReason:    "Error",
				exitOk:        true,
			},
			Then{
				wantBeforeHookInvoked: true,
				wantSetExitInvoked:    false,
				wantStatus:            kdb.Running,
				wantErr:               wantErr,
			},
		))
	}

	t.Run("When worker is done, it translate run status to completing", theory(
		When{
			runStatus:  kdb.Running,
			jobStatus:  kw.Done,
			exitCode:   0,
			exitReason: "Completed",
			exitOk:     true,
		},
		Then{
			wantBeforeHookInvoked: true,
			wantSetExitInvoked:    true,
			wantStatus:            kdb.Completing,
		},
	))

	{
		wantError := errors.New("unexpected error")
		t.Run("When worker is done and setExit returnd an error, it should return the error", theory(
			When{
				runStatus:  kdb.Running,
				jobStatus:  kw.Done,
				exitCode:   0,
				exitReason: "Completed",
				exitOk:     true,
				errSetExit: wantError,
			},
			Then{
				wantBeforeHookInvoked: true,
				wantSetExitInvoked:    true,

				wantStatus: kdb.Running,
				wantErr:    wantError,
			},
		))
	}

	t.Run("When worker is failed, it translate run status to aborting", theory(
		When{
			runStatus:  kdb.Running,
			jobStatus:  kw.Failed,
			exitCode:   1,
			exitReason: "Error",
			exitOk:     true,
		},
		Then{
			wantBeforeHookInvoked: true,
			wantSetExitInvoked:    true,

			wantStatus: kdb.Aborting,
			wantErr:    nil,
		},
	))

	{
		fakeErr := errors.New("fake error")
		t.Run("When worker is failed and setExit returns an error, it should return the error", theory(
			When{
				runStatus:  kdb.Running,
				jobStatus:  kw.Failed,
				exitCode:   1,
				exitReason: "Error",
				exitOk:     true,
				errSetExit: fakeErr,
			},
			Then{
				wantBeforeHookInvoked: true,
				wantSetExitInvoked:    true,

				wantStatus: kdb.Running,
				wantErr:    fakeErr,
			},
		))

	}
}
