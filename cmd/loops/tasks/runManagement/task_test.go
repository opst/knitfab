package runManagement_test

import (
	"context"
	"errors"
	"testing"

	api_runs "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	kdbmock "github.com/opst/knitfab/pkg/db/mocks"
)

func TestTask_Outside_of_PickAndSetStatus(t *testing.T) {

	type When struct {
		cursorToBePassed kdb.RunCursor

		returnCursor       kdb.RunCursor
		returnStateChanged bool
		returnErr          error

		updatedRun kdb.Run

		getRunReturnsNil bool
	}

	type Then struct {
		wantedAfterHookInvoked bool
		wantedContinue         bool
		wantedErr              error
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			irun := kdbmock.NewRunInterface()
			irun.Impl.PickAndSetStatus = func(
				ctx context.Context, cursor kdb.RunCursor,
				_ func(kdb.Run) (kdb.KnitRunStatus, error),
			) (kdb.RunCursor, bool, error) {
				if !cursor.Equal(when.cursorToBePassed) {
					t.Errorf(
						"cursor: actual=%+v, expect=%+v",
						cursor, when.cursorToBePassed,
					)
				}
				return when.returnCursor, when.returnStateChanged, when.returnErr
			}

			irun.Impl.Get = func(context.Context, []string) (map[string]kdb.Run, error) {
				if when.getRunReturnsNil {
					return nil, errors.New("irun.Get: should be ignored")
				}
				return map[string]kdb.Run{when.returnCursor.Head: when.updatedRun}, nil
			}

			afterHasBeenCalled := false
			testee := runManagement.Task(irun, nil, nil, hook.Func[api_runs.Detail]{
				BeforeFn: func(d api_runs.Detail) error {
					t.Error("before hook: should not be invoked")
					return nil
				},
				AfterFn: func(d api_runs.Detail) error {
					afterHasBeenCalled = true
					if want := bindruns.ComposeDetail(when.updatedRun); !d.Equal(want) {
						t.Errorf("hookValue: actual=%+v, expect=%+v", d, want)
					}
					return errors.New("after hook: should be ignored")
				},
			})

			cursor, cont, err := testee(ctx, when.cursorToBePassed)

			if !cursor.Equal(when.returnCursor) {
				t.Errorf("cursor: actual=%+v, expect=%+v", cursor, when.returnCursor)
			}

			if cont != then.wantedContinue {
				t.Errorf("ok: actual=%+v, expect=%+v", cont, then.wantedContinue)
			}

			if !errors.Is(err, then.wantedErr) {
				t.Errorf("err: actual=%+v, expect=%+v", err, then.wantedErr)
			}

			if afterHasBeenCalled != then.wantedAfterHookInvoked {
				t.Errorf(
					"after: actual=%+v, expect=%+v",
					afterHasBeenCalled, then.wantedAfterHookInvoked,
				)
			}
		}
	}

	{
		expectedErr := errors.New("fake error")
		t.Run("when PickAndSetStatus returns error, the task should return the error", theory(
			When{
				cursorToBePassed: kdb.RunCursor{
					Head:   "some-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnCursor: kdb.RunCursor{
					Head:   "new-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnStateChanged: false,
				returnErr:          expectedErr,
			},
			Then{
				wantedContinue:         true,
				wantedAfterHookInvoked: false,
				wantedErr:              expectedErr,
			},
		))
	}

	{
		t.Run("when PickAndSetStatus returns same cursor, the task should return false", theory(
			When{
				cursorToBePassed: kdb.RunCursor{
					Head:   "some-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnCursor: kdb.RunCursor{
					Head:   "some-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnStateChanged: true,
				returnErr:          nil,
			},
			Then{
				wantedAfterHookInvoked: true,
				wantedContinue:         false,
			},
		))
	}

	{
		t.Run("when PickAndSetStatus returns different cursor, the task should return true", theory(
			When{
				cursorToBePassed: kdb.RunCursor{
					Head:   "some-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnCursor: kdb.RunCursor{
					Head:   "new-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnStateChanged: true,
				returnErr:          nil,
			},
			Then{
				wantedAfterHookInvoked: true,
				wantedContinue:         true,
			},
		))
	}

	{
		t.Run("when irun.Get returns nil, the after hook should not be invoked", theory(
			When{
				cursorToBePassed: kdb.RunCursor{
					Head:   "some-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnCursor: kdb.RunCursor{
					Head:   "new-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnStateChanged: true,
				returnErr:          nil,
				getRunReturnsNil:   true,
			},
			Then{
				wantedAfterHookInvoked: false,
				wantedContinue:         true,
			},
		))
	}

	{
		t.Run("when PickAndSetStatus returns context.Canceled, no error should be returned", theory(
			When{
				cursorToBePassed: kdb.RunCursor{
					Head:   "some-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnCursor: kdb.RunCursor{
					Head:   "new-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnStateChanged: false,
				returnErr:          context.Canceled,
			},
			Then{
				wantedAfterHookInvoked: false,
				wantedContinue:         true,
			},
		))
	}

	{
		t.Run("when PickAndSetStatus returns context.DeadlineExceeded, no error should be returned", theory(
			When{
				cursorToBePassed: kdb.RunCursor{
					Head:   "some-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnCursor: kdb.RunCursor{
					Head:   "new-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnStateChanged: false,
				returnErr:          context.DeadlineExceeded,
			},
			Then{
				wantedAfterHookInvoked: false,
				wantedContinue:         true,
			},
		))
	}

	{
		t.Run("when PickAndSetStatus returns kdb.ErrInvalidRunStateChanging, no error should be returned", theory(
			When{
				cursorToBePassed: kdb.RunCursor{
					Head:   "some-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnCursor: kdb.RunCursor{
					Head:   "new-run-id",
					Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
					Pseudo: []kdb.PseudoPlanName{},
				},
				returnStateChanged: false,
				returnErr:          kdb.ErrInvalidRunStateChanging,
			},
			Then{
				wantedAfterHookInvoked: false,
				wantedContinue:         true,
			},
		))
	}
}

func TestTask_Inside_of_PickAndSetStatus(t *testing.T) {
	type When struct {
		pickedRun kdb.Run

		newStatus    kdb.KnitRunStatus // to be returned by imageManager or pseudoManager
		managerError error
	}
	type Then struct {
		newStatus                kdb.KnitRunStatus // expected status of the run after the task
		wantHookBeforeInvoked    bool
		wantImageManagerInvoked  bool
		pseudoManagerToBeInvoked []kdb.PseudoPlanName
		err                      error
	}

	const (
		planName1 kdb.PseudoPlanName = "plan-name-1"
		planName2 kdb.PseudoPlanName = "plan-name-2"
	)

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			irun := kdbmock.NewRunInterface()
			irun.Impl.PickAndSetStatus = func(
				ctx context.Context, _ kdb.RunCursor,
				f func(kdb.Run) (kdb.KnitRunStatus, error),
			) (kdb.RunCursor, bool, error) {
				state, err := f(when.pickedRun)
				if state != then.newStatus {
					t.Errorf("state: actual=%+v, expect=%+v", state, then.newStatus)
				}

				if !errors.Is(err, then.err) {
					t.Errorf("err: actual=%+v, expect=%+v", err, when.managerError)
				}

				return kdb.RunCursor{}, true, nil
			}

			irun.Impl.Get = func(context.Context, []string) (map[string]kdb.Run, error) {
				return map[string]kdb.Run{}, nil
			}

			imageManagerHasBeenInvoked := false
			invokedPseudoManager := []kdb.PseudoPlanName{}

			imageManager := func(_ context.Context, h hook.Hook[api_runs.Detail], _ kdb.Run) (kdb.KnitRunStatus, error) {
				imageManagerHasBeenInvoked = true

				h.Before(bindruns.ComposeDetail(when.pickedRun))
				return when.newStatus, when.managerError
			}
			pseudoManagers := map[kdb.PseudoPlanName]manager.Manager{
				planName1: func(_ context.Context, h hook.Hook[api_runs.Detail], _ kdb.Run) (kdb.KnitRunStatus, error) {
					h.Before(bindruns.ComposeDetail(when.pickedRun))
					invokedPseudoManager = append(invokedPseudoManager, planName1)
					return when.newStatus, when.managerError
				},
				planName2: func(_ context.Context, h hook.Hook[api_runs.Detail], _ kdb.Run) (kdb.KnitRunStatus, error) {
					h.Before(bindruns.ComposeDetail(when.pickedRun))
					invokedPseudoManager = append(invokedPseudoManager, planName2)
					return when.newStatus, when.managerError
				},
			}

			beforeHookInvoked := false
			testee := runManagement.Task(irun, imageManager, pseudoManagers, hook.Func[api_runs.Detail]{
				BeforeFn: func(d api_runs.Detail) error {
					beforeHookInvoked = true
					if want := bindruns.ComposeDetail(when.pickedRun); !d.Equal(want) {
						t.Errorf("hookValue: actual=%+v, expect=%+v", d, want)
					}
					return nil
				},
			})

			_, _, err := testee(ctx, kdb.RunCursor{
				Head:   when.pickedRun.Id,
				Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
				Pseudo: []kdb.PseudoPlanName{},
			})

			if err != nil {
				t.Errorf("err: actual=%+v, expect=%+v", err, nil)
			}

			if beforeHookInvoked != then.wantHookBeforeInvoked {
				t.Errorf(
					"hookBefore: actual=%+v, expect=%+v",
					beforeHookInvoked, then.wantHookBeforeInvoked,
				)
			}

			if imageManagerHasBeenInvoked != then.wantImageManagerInvoked {
				t.Errorf(
					"imageManager: actual=%+v, expect=%+v",
					imageManagerHasBeenInvoked, then.wantImageManagerInvoked,
				)
			}

			if !cmp.SliceContentEq(invokedPseudoManager, then.pseudoManagerToBeInvoked) {
				t.Errorf(
					"pseudoManager: actual=%+v, expect=%+v",
					invokedPseudoManager, then.pseudoManagerToBeInvoked,
				)
			}
		}
	}

	{
		t.Run("when picked run has no pseudo plan, imageManager should be invoked", theory(
			When{
				pickedRun: kdb.Run{
					RunBody: kdb.RunBody{
						Id: "some-run-id",
						PlanBody: kdb.PlanBody{
							Pseudo: nil,
							Image: &kdb.ImageIdentifier{
								Image: "repo.invalid/image", Version: "v1.0",
							},
						},
						Status: kdb.Ready,
					},
				},
				newStatus:    kdb.Running,
				managerError: nil,
			},
			Then{
				wantHookBeforeInvoked:    true,
				wantImageManagerInvoked:  true,
				pseudoManagerToBeInvoked: []kdb.PseudoPlanName{},
				newStatus:                kdb.Running,
			},
		))
	}

	{
		t.Run("when picked run has pseudo plan, pseudoManager should be invoked", theory(
			When{
				pickedRun: kdb.Run{
					RunBody: kdb.RunBody{
						Id: "some-run-id",
						PlanBody: kdb.PlanBody{
							Pseudo: &kdb.PseudoPlanDetail{Name: planName1},
							Image:  nil,
						},
						Status: kdb.Ready,
					},
				},
				newStatus:    kdb.Running,
				managerError: nil,
			},
			Then{
				wantHookBeforeInvoked:    true,
				wantImageManagerInvoked:  false,
				pseudoManagerToBeInvoked: []kdb.PseudoPlanName{planName1},
				newStatus:                kdb.Running,
			},
		))
	}

	{
		wantError := errors.New("fake error")
		t.Run("when manager returns error, the task should return the error", theory(
			When{
				pickedRun: kdb.Run{
					RunBody: kdb.RunBody{
						Id: "some-run-id",
						PlanBody: kdb.PlanBody{
							Pseudo: nil,
							Image: &kdb.ImageIdentifier{
								Image: "repo.invalid/image", Version: "v1.0",
							},
						},
						Status: kdb.Ready,
					},
				},
				newStatus:    kdb.Running,
				managerError: wantError,
			},
			Then{
				wantHookBeforeInvoked:    true,
				wantImageManagerInvoked:  true,
				pseudoManagerToBeInvoked: []kdb.PseudoPlanName{},
				err:                      wantError,
				newStatus:                kdb.Running,
			},
		))
	}

}
