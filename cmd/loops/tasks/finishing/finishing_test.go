package finishing_test

import (
	"context"
	"errors"
	"io"
	"testing"

	apiruns "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/tasks/finishing"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	"github.com/opst/knitfab/pkg/domain"
	workloads "github.com/opst/knitfab/pkg/domain/errors/k8serrors"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
	kdbmock "github.com/opst/knitfab/pkg/domain/run/db/mock"
	mockK8sRun "github.com/opst/knitfab/pkg/domain/run/k8s/mock"
	"github.com/opst/knitfab/pkg/domain/run/k8s/worker"
)

func TestTaskFinishing_Outside_PickAndSetStatus(t *testing.T) {

	type When struct {
		givenCursor domain.RunCursor

		newCursor     domain.RunCursor
		statusChanged bool
		err           error

		pickedRun          domain.Run
		iDbRunGetReturnNil bool
	}

	type Then struct {
		wantedCursor           domain.RunCursor
		wantedOk               bool
		wantedErr              error
		hookAfterHasBeenCalled bool
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			iDbRun := kdbmock.NewRunInterface()

			pickAndSetStatusCalled := false
			iDbRun.Impl.PickAndSetStatus = func(
				ctx context.Context, cursor domain.RunCursor,
				_ func(domain.Run) (domain.KnitRunStatus, error), // ignore
			) (domain.RunCursor, bool, error) {
				pickAndSetStatusCalled = true
				return when.newCursor, when.statusChanged, when.err
			}
			iDbRun.Impl.Get = func(ctx context.Context, runIds []string) (map[string]domain.Run, error) {
				if len(runIds) != 1 || runIds[0] != when.newCursor.Head {
					t.Errorf("runIds: actual=%+v, expect=%+v", runIds, []string{when.newCursor.Head})
				}
				if when.iDbRunGetReturnNil {
					return nil, errors.New("iDbRun.Get: should be ignored")
				}
				return map[string]domain.Run{when.newCursor.Head: when.pickedRun}, nil
			}

			// Testee
			hookAfterHasBeenCalled := false
			testee := finishing.Task(iDbRun, nil, hook.Func[apiruns.Detail, struct{}]{
				AfterFn: func(hookValue apiruns.Detail) error {
					hookAfterHasBeenCalled = true
					if want := bindruns.ComposeDetail(when.pickedRun); !want.Equal(hookValue) {
						t.Errorf("hookValue: actual=%+v, expect=%+v", hookValue, want)
					}
					return errors.New("hook.After: should be ignored")
				},
			})
			cursor, ok, err := testee(ctx, when.givenCursor)
			t.Logf("from testee, cursor:=%+v,\n ok=%+v, err=%+v", cursor, ok, err)

			// assertion
			if !pickAndSetStatusCalled {
				t.Errorf("callback: not called")
			}

			if !cursor.Equal(then.wantedCursor) {
				t.Errorf("cursor: actual=%+v, expect=%+v", cursor, then.wantedCursor)
			}

			if ok != then.wantedOk {
				t.Errorf("ok: actual=%+v, expect=%+v", ok, then.wantedOk)
			}

			if !errors.Is(err, then.wantedErr) {
				t.Errorf("err: actual=%+v, expect=%+v", err, then.wantedErr)
			}

			if hookAfterHasBeenCalled != when.statusChanged {
				t.Errorf(
					"hookAfter: called=%+v, want=%+v",
					hookAfterHasBeenCalled, when.statusChanged,
				)
			}
		}
	}

	t.Run("when PickAndSetStatus do not cause error, the task should return no error (status changed)", theory(
		When{
			givenCursor: domain.RunCursor{
				Head:   "run-id-0",
				Status: []domain.KnitRunStatus{domain.Completing, domain.Aborting},
				Pseudo: []domain.PseudoPlanName{},
			},
			newCursor: domain.RunCursor{
				Head:   "run-id-1",
				Status: []domain.KnitRunStatus{domain.Completing, domain.Aborting},
				Pseudo: []domain.PseudoPlanName{},
			},
			statusChanged: true,
			err:           nil,
			pickedRun: domain.Run{
				RunBody: domain.RunBody{
					Id:         "run-id-1",
					WorkerName: "worker-name-1",
					Status:     domain.Completing,
					PlanBody: domain.PlanBody{
						PlanId: "plan-id-1",
						Hash:   "hash-1",
						Active: true,
						Image: &domain.ImageIdentifier{
							Image: "repo-1", Version: "tag-1",
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						MountPoint: domain.MountPoint{
							Id:   100_100,
							Path: "/path/to/input",
							Tags: domain.NewTagSet([]domain.Tag{{Key: "type", Value: "csv"}}),
						},
						KnitDataBody: domain.KnitDataBody{
							KnitId:    "knit-id-1",
							VolumeRef: "#knit-id-1",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "type", Value: "csv"},
								{Key: "input", Value: "1"},
							}),
						},
					},
				},
				Outputs: []domain.Assignment{
					{
						MountPoint: domain.MountPoint{
							Id:   100_110,
							Path: "/path/to/output",
							Tags: domain.NewTagSet([]domain.Tag{{Key: "type", Value: "model"}}),
						},
						KnitDataBody: domain.KnitDataBody{
							KnitId:    "knit-id-2",
							VolumeRef: "#knit-id-2",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "type", Value: "model"},
								{Key: "output", Value: "1"},
							}),
						},
					},
				},
				Log: &domain.Log{
					Id: 100_001,
					Tags: domain.NewTagSet([]domain.Tag{
						{Key: "type", Value: "log"},
					}),
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "knit-id-log",
						VolumeRef: "#knit-id-log",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "type", Value: "text"},
							{Key: "log", Value: "1"},
						}),
					},
				},
			},
		},
		Then{
			wantedCursor: domain.RunCursor{
				Head:   "run-id-1",
				Status: []domain.KnitRunStatus{domain.Completing, domain.Aborting},
				Pseudo: []domain.PseudoPlanName{},
			},
			wantedOk:               true,
			wantedErr:              nil,
			hookAfterHasBeenCalled: true,
		},
	))

	t.Run("when PickAndSetStatus do not cause error, the task should return no error (status not changed)", theory(
		When{
			givenCursor: domain.RunCursor{
				Head:   "run-id-0",
				Status: []domain.KnitRunStatus{domain.Completing, domain.Aborting},
				Pseudo: []domain.PseudoPlanName{},
			},
			newCursor: domain.RunCursor{
				Head:   "run-id-1",
				Status: []domain.KnitRunStatus{domain.Completing, domain.Aborting},
				Pseudo: []domain.PseudoPlanName{},
			},
			statusChanged: false,
			err:           nil,
			pickedRun: domain.Run{
				RunBody: domain.RunBody{
					Id:         "run-id-1",
					WorkerName: "worker-name-1",
					Status:     domain.Completing,
					PlanBody: domain.PlanBody{
						PlanId: "plan-id-1",
						Hash:   "hash-1",
						Active: true,
						Image: &domain.ImageIdentifier{
							Image: "repo-1", Version: "tag-1",
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						MountPoint: domain.MountPoint{
							Id:   100_100,
							Path: "/path/to/input",
							Tags: domain.NewTagSet([]domain.Tag{{Key: "type", Value: "csv"}}),
						},
						KnitDataBody: domain.KnitDataBody{
							KnitId:    "knit-id-1",
							VolumeRef: "#knit-id-1",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "type", Value: "csv"},
								{Key: "input", Value: "1"},
							}),
						},
					},
				},
				Outputs: []domain.Assignment{
					{
						MountPoint: domain.MountPoint{
							Id:   100_110,
							Path: "/path/to/output",
							Tags: domain.NewTagSet([]domain.Tag{{Key: "type", Value: "model"}}),
						},
						KnitDataBody: domain.KnitDataBody{
							KnitId:    "knit-id-2",
							VolumeRef: "#knit-id-2",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "type", Value: "model"},
								{Key: "output", Value: "1"},
							}),
						},
					},
				},
				Log: &domain.Log{
					Id: 100_001,
					Tags: domain.NewTagSet([]domain.Tag{
						{Key: "type", Value: "log"},
					}),
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "knit-id-log",
						VolumeRef: "#knit-id-log",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "type", Value: "text"},
							{Key: "log", Value: "1"},
						}),
					},
				},
			},
		},
		Then{
			wantedCursor: domain.RunCursor{
				Head:   "run-id-1",
				Status: []domain.KnitRunStatus{domain.Completing, domain.Aborting},
				Pseudo: []domain.PseudoPlanName{},
			},
			wantedOk:               true,
			wantedErr:              nil,
			hookAfterHasBeenCalled: true,
		},
	))

	t.Run("when PickAndSetStatus is not effected, the task should return non-ok", theory(
		When{
			givenCursor: domain.RunCursor{
				Head:   "run-id-0",
				Status: []domain.KnitRunStatus{domain.Completing, domain.Aborting},
				Pseudo: []domain.PseudoPlanName{},
			},
			newCursor: domain.RunCursor{
				Head:   "run-id-0",
				Status: []domain.KnitRunStatus{domain.Completing, domain.Aborting},
				Pseudo: []domain.PseudoPlanName{},
			},
			statusChanged: false,
			err:           nil,
		},
		Then{
			wantedCursor: domain.RunCursor{
				Head:   "run-id-0",
				Status: []domain.KnitRunStatus{domain.Completing, domain.Aborting},
				Pseudo: []domain.PseudoPlanName{},
			},
			wantedErr:              nil,
			hookAfterHasBeenCalled: false,
		},
	))

	{
		expectedErr := errors.New("fake error")
		t.Run("when PickAndSetStatus returns error, the task should return the error", theory(
			When{
				givenCursor: domain.RunCursor{
					Head:   "run-id-0",
					Status: []domain.KnitRunStatus{domain.Completing, domain.Aborting},
					Pseudo: []domain.PseudoPlanName{},
				},
				newCursor: domain.RunCursor{
					Head:   "run-id-1",
					Status: []domain.KnitRunStatus{domain.Completing, domain.Aborting},
					Pseudo: []domain.PseudoPlanName{},
				},
				statusChanged: false,
				err:           expectedErr,
			},
			Then{
				wantedCursor: domain.RunCursor{
					Head:   "run-id-1",
					Status: []domain.KnitRunStatus{domain.Completing, domain.Aborting},
					Pseudo: []domain.PseudoPlanName{},
				},
				wantedOk:               true,
				wantedErr:              expectedErr,
				hookAfterHasBeenCalled: false,
			},
		))
	}
}

type FakeWorker struct {
	runId     string
	jobStatus cluster.JobStatus
	closed    bool
	closeErr  error

	exitCode   uint8
	exitReason string
	exitOk     bool
}

func (fw *FakeWorker) RunId() string {
	return fw.runId
}

func (fw *FakeWorker) JobStatus(context.Context) cluster.JobStatus {
	return fw.jobStatus
}

func (fw *FakeWorker) ExitCode() (uint8, string, bool) {
	return fw.exitCode, fw.exitReason, fw.exitOk
}

func (fw *FakeWorker) Log(ctx context.Context) (io.ReadCloser, error) {
	return nil, nil
}

func (fw *FakeWorker) Close() error {
	fw.closed = true
	return fw.closeErr
}

var _ worker.Worker = &FakeWorker{}

func TestTaskFinishing_Inside_PickAndSetStatus(t *testing.T) {

	type When struct {
		runPassedToCallback domain.Run
		workerFromFind      *FakeWorker
		errBefore           error
		errFromFind         error
		errFromDeleteWorker error
	}

	type Then struct {
		runStatus             domain.KnitRunStatus
		wantHookBeforeCalled  bool
		wantFindHasBeenCalled bool
		wantError             error
		wantAnyError          bool
		wantWorkerClosed      bool
		wantDeleteWorker      bool
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			iDbRun := kdbmock.NewRunInterface()
			// build mock of PickAndSetStatus

			iDbRun.Impl.PickAndSetStatus = func(
				ctx context.Context, cursor domain.RunCursor,
				callback func(domain.Run) (domain.KnitRunStatus, error), // ignore
			) (domain.RunCursor, bool, error) {
				newStatus, err := callback(when.runPassedToCallback)

				if then.wantAnyError && (err == nil) {
					t.Errorf("err: actual=%+v, expect=%+v", err, then.wantError)
				}
				if !then.wantAnyError && !errors.Is(err, then.wantError) {
					t.Errorf("err: actual=%+v, expect=%+v", err, then.wantError)
				}
				if newStatus != then.runStatus {
					t.Errorf("runStatus: actual=%+v, expect=%+v", newStatus, then.runStatus)
				}

				return cursor, true, nil
			}
			iDbRun.Impl.DeleteWorker = func(ctx context.Context, runId string) error {
				if runId != when.runPassedToCallback.Id {
					t.Errorf("runId: actual=%+v, expect=%+v", runId, when.runPassedToCallback.Id)
				}
				return when.errFromDeleteWorker
			}
			iDbRun.Impl.Get = func(ctx context.Context, runIds []string) (map[string]domain.Run, error) {
				return map[string]domain.Run{}, nil
			}

			findHasBeenCalled := false
			fakeRunInterfafce := mockK8sRun.New(t)
			fakeRunInterfafce.Impl.FindWorker = func(ctx context.Context, runBody domain.RunBody) (worker.Worker, error) {
				findHasBeenCalled = true
				if !runBody.Equal(&when.runPassedToCallback.RunBody) {
					t.Errorf("find: runBody: actual=%+v, expect=%+v", runBody, when.runPassedToCallback.RunBody)
				}
				return when.workerFromFind, when.errFromFind
			}

			// Testee
			beforeHasBeenCalled := false
			testee := finishing.Task(iDbRun, fakeRunInterfafce, hook.Func[apiruns.Detail, struct{}]{
				BeforeFn: func(hookValue apiruns.Detail) (struct{}, error) {
					beforeHasBeenCalled = true
					if want := bindruns.ComposeDetail(when.runPassedToCallback); !want.Equal(hookValue) {
						t.Errorf("hookValue: actual=%+v, expect=%+v", hookValue, want)
					}
					return struct{}{}, when.errBefore
				},
			})
			testee(context.Background(), domain.RunCursor{
				Head:   "run-id-0",
				Status: []domain.KnitRunStatus{domain.Completing, domain.Aborting},
				Pseudo: []domain.PseudoPlanName{domain.Uploaded},
			})

			// assertion
			if len(iDbRun.Calls.PickAndSetStatus) < 1 {
				t.Errorf("callback: not called")
			}

			if beforeHasBeenCalled != then.wantHookBeforeCalled {
				t.Errorf("before: called=%+v, want=%+v", beforeHasBeenCalled, then.wantHookBeforeCalled)
			}

			if then.wantFindHasBeenCalled != findHasBeenCalled {
				t.Errorf("find: called=%+v", findHasBeenCalled)
			}

			if then.wantDeleteWorker {
				if len(iDbRun.Calls.DeleteWorker) < 1 {
					t.Errorf("deleteWorker: not called")
				}
			} else {
				if 0 < len(iDbRun.Calls.DeleteWorker) {
					t.Errorf("deleteWorker: called")
				}
			}

			if w := when.workerFromFind; w != nil && w.closed != then.wantWorkerClosed {
				t.Errorf(
					"workerClosed: actual=%+v, expect=%+v",
					when.workerFromFind.closed, then.wantWorkerClosed,
				)
			}
		}
	}

	t.Run("for completeing run with worker name, it returns Done as new status", theory(
		When{
			runPassedToCallback: domain.Run{
				RunBody: domain.RunBody{
					Id:         "run-id-0",
					WorkerName: "worker-name-0",
					Status:     domain.Completing,
					PlanBody: domain.PlanBody{
						PlanId: "plan-id-0",
						Hash:   "hash-0",
						Active: true,
						Image: &domain.ImageIdentifier{
							Image: "repo-0", Version: "tag-0",
						},
					},
				},
			},
			workerFromFind: &FakeWorker{
				runId: "run-id-0",
				jobStatus: cluster.JobStatus{
					Type: cluster.Succeeded,
				},
				closed: false,
			},
		},
		Then{
			runStatus:             domain.Done,
			wantHookBeforeCalled:  true,
			wantWorkerClosed:      true,
			wantFindHasBeenCalled: true,
			wantDeleteWorker:      true,
		},
	))

	t.Run("for aborting run with worker name, it returns Failed as new status", theory(
		When{
			runPassedToCallback: domain.Run{
				RunBody: domain.RunBody{
					Id:         "run-id-0",
					WorkerName: "worker-name-0",
					Status:     domain.Aborting,
					PlanBody: domain.PlanBody{
						PlanId: "plan-id-0",
						Hash:   "hash-0",
						Active: true,
						Image: &domain.ImageIdentifier{
							Image: "repo-0", Version: "tag-0",
						},
					},
				},
			},
			workerFromFind: &FakeWorker{
				runId:     "run-id-0",
				jobStatus: cluster.JobStatus{Type: cluster.Failed, Code: 1},
				closed:    false,
			},
		},
		Then{
			runStatus:             domain.Failed,
			wantHookBeforeCalled:  true,
			wantFindHasBeenCalled: true,
			wantWorkerClosed:      true,
			wantDeleteWorker:      true,
		},
	))

	t.Run("for completeing run without worker name, it returns Done as new status", theory(
		When{
			runPassedToCallback: domain.Run{
				RunBody: domain.RunBody{
					Id:     "run-id-0",
					Status: domain.Completing,
					PlanBody: domain.PlanBody{
						PlanId: "plan-id-0",
						Hash:   "hash-0",
						Active: true,
						Image: &domain.ImageIdentifier{
							Image: "repo-0", Version: "tag-0",
						},
					},
				},
			},
			workerFromFind: &FakeWorker{},
		},
		Then{
			runStatus:             domain.Done,
			wantHookBeforeCalled:  true,
			wantFindHasBeenCalled: false,
			wantWorkerClosed:      false,
			wantDeleteWorker:      false,
		},
	))

	t.Run("for aborting run without worker name, it returns Failed as new status", theory(
		When{
			runPassedToCallback: domain.Run{
				RunBody: domain.RunBody{
					Id:     "run-id-0",
					Status: domain.Aborting,
					PlanBody: domain.PlanBody{
						PlanId: "plan-id-0",
						Hash:   "hash-0",
						Active: true,
						Image: &domain.ImageIdentifier{
							Image: "repo-0", Version: "tag-0",
						},
					},
				},
			},
			workerFromFind: &FakeWorker{},
		},
		Then{
			runStatus:             domain.Failed,
			wantHookBeforeCalled:  true,
			wantFindHasBeenCalled: false,
			wantWorkerClosed:      false,
			wantDeleteWorker:      false,
		},
	))

	{
		fakeError := errors.New("fake error")
		t.Run("when before hook returns error, it returns the error and stay its state", theory(
			When{
				runPassedToCallback: domain.Run{
					RunBody: domain.RunBody{
						Id:         "run-id-0",
						WorkerName: "worker-name-0",
						Status:     domain.Completing,
						PlanBody: domain.PlanBody{
							PlanId: "plan-id-0",
							Hash:   "hash-0",
							Active: true,
							Image: &domain.ImageIdentifier{
								Image: "repo-0", Version: "tag-0",
							},
						},
					},
				},
				errBefore: fakeError,
			},
			Then{
				runStatus:             domain.Completing,
				wantHookBeforeCalled:  true,
				wantFindHasBeenCalled: false,
				wantWorkerClosed:      false,
				wantDeleteWorker:      false,
				wantError:             fakeError,
			},
		))
	}

	t.Run("when find returns ErrMissing, it returns no error and update state", theory(
		When{
			runPassedToCallback: domain.Run{
				RunBody: domain.RunBody{
					Id:         "run-id-0",
					WorkerName: "worker-name-0",
					Status:     domain.Completing,
					PlanBody: domain.PlanBody{
						PlanId: "plan-id-0",
						Hash:   "hash-0",
						Active: true,
						Image: &domain.ImageIdentifier{
							Image: "repo-0", Version: "tag-0",
						},
					},
				},
			},
			errFromFind: workloads.NewMissing("fake missing error"),
		},
		Then{
			runStatus:             domain.Done,
			wantHookBeforeCalled:  true,
			wantFindHasBeenCalled: true,
			wantWorkerClosed:      false,
			wantDeleteWorker:      true,
		},
	))

	{
		fakeError := errors.New("fake error")
		t.Run("when find returns other error, it returns the error and stay its state", theory(
			When{
				runPassedToCallback: domain.Run{
					RunBody: domain.RunBody{
						Id:         "run-id-0",
						WorkerName: "worker-name-0",
						Status:     domain.Completing,
						PlanBody: domain.PlanBody{
							PlanId: "plan-id-0",
							Hash:   "hash-0",
							Active: true,
							Image: &domain.ImageIdentifier{
								Image: "repo-0", Version: "tag-0",
							},
						},
					},
				},
				errFromFind: fakeError,
			},
			Then{
				runStatus:             domain.Completing,
				wantHookBeforeCalled:  true,
				wantFindHasBeenCalled: true,
				wantWorkerClosed:      false,
				wantDeleteWorker:      false,
				wantError:             fakeError,
			},
		))
	}

	{
		fakeError := errors.New("fake error")
		t.Run("when worker.Close returns error, it returns the error and stay its state", theory(
			When{
				runPassedToCallback: domain.Run{
					RunBody: domain.RunBody{
						Id:         "run-id-0",
						WorkerName: "worker-name-0",
						Status:     domain.Completing,
						PlanBody: domain.PlanBody{
							PlanId: "plan-id-0",
							Hash:   "hash-0",
							Active: true,
							Image: &domain.ImageIdentifier{
								Image: "repo-0", Version: "tag-0",
							},
						},
					},
				},
				workerFromFind: &FakeWorker{
					runId:     "run-id-0",
					jobStatus: cluster.JobStatus{Type: cluster.Succeeded},
					closeErr:  fakeError,
				},
			},
			Then{
				runStatus:             domain.Completing,
				wantHookBeforeCalled:  true,
				wantFindHasBeenCalled: true,
				wantWorkerClosed:      true,
				wantDeleteWorker:      false,
				wantError:             fakeError,
			},
		))
	}

	{
		fakeError := errors.New("fake error")
		t.Run("when iDbRun.DeleteWorker returns error, it returns the error and stay its state", theory(
			When{
				runPassedToCallback: domain.Run{
					RunBody: domain.RunBody{
						Id:         "run-id-0",
						WorkerName: "worker-name-0",
						Status:     domain.Completing,
						PlanBody: domain.PlanBody{
							PlanId: "plan-id-0",
							Hash:   "hash-0",
							Active: true,
							Image: &domain.ImageIdentifier{
								Image: "repo-0", Version: "tag-0",
							},
						},
					},
				},
				workerFromFind:      &FakeWorker{},
				errFromDeleteWorker: fakeError,
			},
			Then{
				runStatus:             domain.Completing,
				wantHookBeforeCalled:  true,
				wantFindHasBeenCalled: true,
				wantWorkerClosed:      true,
				wantDeleteWorker:      true,
				wantError:             fakeError,
			},
		))
	}

	t.Run("when run status is unexpected, it returns error", theory(
		When{
			runPassedToCallback: domain.Run{
				RunBody: domain.RunBody{
					Id:         "run-id-0",
					WorkerName: "worker-name-0",
					Status:     domain.Running,
					PlanBody: domain.PlanBody{
						PlanId: "plan-id-0",
						Hash:   "hash-0",
						Active: true,
						Image: &domain.ImageIdentifier{
							Image: "repo-0", Version: "tag-0",
						},
					},
				},
			},
			workerFromFind: &FakeWorker{},
		},
		Then{
			runStatus:             domain.Running,
			wantHookBeforeCalled:  false,
			wantFindHasBeenCalled: false,
			wantAnyError:          true,
			wantWorkerClosed:      false,
			wantDeleteWorker:      false,
			wantError:             errors.New("unexpected run status: assertion error"),
		},
	))
}
