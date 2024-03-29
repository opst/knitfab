package image_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager/image"
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
	}

	type Then struct {
		wantStatus kdb.KnitRunStatus
		wantError  error

		wantSetExitInvoked bool
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			getWorker := func(context.Context, kdb.Run) (kw.Worker, error) {
				return nil, when.errGetWorker
			}
			startWorker := func(context.Context, kdb.Run) error {
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
			gotStatus, gotError := testee(ctx, when.run)

			if setExitInvoked != then.wantSetExitInvoked {
				t.Errorf("got setExitInvoked %v, want %v", setExitInvoked, then.wantSetExitInvoked)
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
		t.Run("get worker returns unexpected error, it should return the error", theory(
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
				errGetWorker: wantErr,
			},
			Then{
				wantStatus: kdb.Ready,
				wantError:  wantErr,
			},
		))
	}

	{
		t.Run("get worker returns not found error, it should start the worker for run in ready status", theory(
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
			},
			Then{
				wantStatus: kdb.Starting,
				wantError:  nil,
			},
		))
	}

	{
		t.Run("get worker returns not found error, it should translate from non-ready status to aborting", theory(
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
			},
			Then{
				wantStatus:         kdb.Aborting,
				wantError:          nil,
				wantSetExitInvoked: true,
			},
		))
	}

	{
		wantErr := errors.New("unexpected error")
		t.Run("setExit returns unexpected error, it should return the error", theory(
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
				errSetExit:     wantErr,
			},
			Then{
				wantStatus:         kdb.Starting,
				wantError:          wantErr,
				wantSetExitInvoked: true,
			},
		))
	}

	{
		t.Run("get worker returns not found error and start worker returns already exists error, it should translate state from ready to starting", theory(
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
			},
			Then{
				wantStatus: kdb.Starting,
				wantError:  nil,
			},
		))
	}

	{
		wantErr := errors.New("unexpected error")
		t.Run("get worker returns not found error and start worker returns unexpected error, it should return the error", theory(
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
			},
			Then{
				wantStatus: kdb.Ready,
				wantError:  wantErr,
			},
		))
	}
}

func TestManager_GetWorkerSucceeded(t *testing.T) {

	type When struct {
		runStatus  kdb.KnitRunStatus
		jobStatus  kw.Status
		exitCode   uint8
		exitReason string
		exitOk     bool
		errSetExit error
	}

	type Then struct {
		wantStatus         kdb.KnitRunStatus
		wantSetExitInvoked bool
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

			testee := image.New(getWorker, nil, setExit)
			gotStatus, gotError := testee(ctx, run)

			if setExitInvoked != then.wantSetExitInvoked {
				t.Errorf("got setExitInvoked %v, want %v", setExitInvoked, then.wantSetExitInvoked)
			}

			if gotStatus != then.wantStatus {
				t.Errorf("got status %v, want %v", gotStatus, then.wantStatus)
			}

			if !errors.Is(gotError, when.errSetExit) {
				t.Errorf("got error %v, want nil", gotError)
			}
		}
	}

	t.Run("When worker is pending, it translate run status to starting", theory(
		When{
			runStatus: kdb.Starting,
			jobStatus: kw.Pending,
		},
		Then{
			wantStatus: kdb.Starting,
		},
	))

	t.Run("When worker is running, it translate run status to running", theory(
		When{
			runStatus: kdb.Starting,
			jobStatus: kw.Running,
		},
		Then{
			wantStatus: kdb.Running,
		},
	))

	t.Run("When worker is done, it translate run status to completing", theory(
		When{
			runStatus:  kdb.Running,
			jobStatus:  kw.Done,
			exitCode:   0,
			exitReason: "Completed",
			exitOk:     true,
		},
		Then{
			wantSetExitInvoked: true,
			wantStatus:         kdb.Completing,
		},
	))

	t.Run("When worker is failed, it translate run status to aborting", theory(
		When{
			runStatus:  kdb.Running,
			jobStatus:  kw.Failed,
			exitCode:   1,
			exitReason: "Error",
			exitOk:     true,
		},
		Then{
			wantSetExitInvoked: true,
			wantStatus:         kdb.Aborting,
		},
	))

	{
		fakeErr := errors.New("fake error")
		t.Run("When worker is failed and setExit returns unexpected error, it should return the error", theory(
			When{
				runStatus:  kdb.Running,
				jobStatus:  kw.Failed,
				exitCode:   1,
				exitReason: "Error",
				exitOk:     true,
				errSetExit: fakeErr,
			},
			Then{
				wantSetExitInvoked: true,
				wantStatus:         kdb.Running,
			},
		))

	}
}
