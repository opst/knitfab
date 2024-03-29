package runManagement_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab/cmd/loops/tasks/runManagement"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager"
	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	kdbmock "github.com/opst/knitfab/pkg/db/mocks"
)

func TestTask_OutsideOfPickAndSetStatus(t *testing.T) {

	type CallPickAndSetStatus struct {
		cursorToBePassed kdb.RunCursor

		returnCursor kdb.RunCursor
		returnErr    error
	}

	type Then struct {
		expectedOk  bool
		expectedErr error
	}

	theory := func(when CallPickAndSetStatus, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			irun := kdbmock.NewRunInterface()
			irun.Impl.PickAndSetStatus = func(
				ctx context.Context, cursor kdb.RunCursor,
				_ func(kdb.Run) (kdb.KnitRunStatus, error),
			) (kdb.RunCursor, error) {
				if !cursor.Equal(when.cursorToBePassed) {
					t.Errorf(
						"cursor: actual=%+v, expect=%+v",
						cursor, when.cursorToBePassed,
					)
				}
				return when.returnCursor, when.returnErr
			}

			testee := runManagement.Task(irun, nil, nil)

			cursor, ok, err := testee(ctx, when.cursorToBePassed)

			if !cursor.Equal(when.returnCursor) {
				t.Errorf("cursor: actual=%+v, expect=%+v", cursor, when.returnCursor)
			}

			if ok != then.expectedOk {
				t.Errorf("ok: actual=%+v, expect=%+v", ok, then.expectedOk)
			}

			if !errors.Is(err, then.expectedErr) {
				t.Errorf("err: actual=%+v, expect=%+v", err, then.expectedErr)
			}
		}
	}

	{
		expectedErr := errors.New("fake error")
		t.Run("when PickAndSetStatus returns error, the task should return the error", theory(
			CallPickAndSetStatus{
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
				returnErr: expectedErr,
			},
			Then{
				expectedOk:  true,
				expectedErr: expectedErr,
			},
		))
	}

	{
		t.Run("when PickAndSetStatus returns same cursor, the task should return false", theory(
			CallPickAndSetStatus{
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
				returnErr: nil,
			},
			Then{
				expectedOk: false,
			},
		))
	}

	{
		t.Run("when PickAndSetStatus returns different cursor, the task should return true", theory(
			CallPickAndSetStatus{
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
				returnErr: nil,
			},
			Then{
				expectedOk: true,
			},
		))
	}

	{
		t.Run("when PickAndSetStatus returns context.Canceled, no error should be returned", theory(
			CallPickAndSetStatus{
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
				returnErr: context.Canceled,
			},
			Then{
				expectedOk: true,
			},
		))
	}

	{
		t.Run("when PickAndSetStatus returns context.DeadlineExceeded, no error should be returned", theory(
			CallPickAndSetStatus{
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
				returnErr: context.DeadlineExceeded,
			},
			Then{
				expectedOk: true,
			},
		))
	}

	{
		t.Run("when PickAndSetStatus returns kdb.ErrInvalidRunStateChanging, no error should be returned", theory(
			CallPickAndSetStatus{
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
				returnErr: kdb.ErrInvalidRunStateChanging,
			},
			Then{
				expectedOk: true,
			},
		))
	}
}

func TestTask_InsideOfPickAndSetStatus(t *testing.T) {
	type When struct {
		pickedRun kdb.Run
		newStatus kdb.KnitRunStatus
		newError  error
	}
	type Then struct {
		wantImageManagerInvoked  bool
		pseudoManagerToBeInvoked []kdb.PseudoPlanName
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
			) (kdb.RunCursor, error) {
				state, err := f(when.pickedRun)
				if state != when.newStatus {
					t.Errorf("state: actual=%+v, expect=%+v", state, when.newStatus)
				}

				if !errors.Is(err, when.newError) {
					t.Errorf("err: actual=%+v, expect=%+v", err, when.newError)
				}

				return kdb.RunCursor{}, nil
			}

			imageManagerHasBeenInvoked := false
			invokedPseudoManager := []kdb.PseudoPlanName{}

			imageManager := func(_ context.Context, _ kdb.Run) (kdb.KnitRunStatus, error) {
				imageManagerHasBeenInvoked = true
				return when.newStatus, when.newError
			}
			pseudoManagers := map[kdb.PseudoPlanName]manager.Manager{
				planName1: func(_ context.Context, _ kdb.Run) (kdb.KnitRunStatus, error) {
					invokedPseudoManager = append(invokedPseudoManager, planName1)
					return when.newStatus, when.newError
				},
				planName2: func(_ context.Context, _ kdb.Run) (kdb.KnitRunStatus, error) {
					invokedPseudoManager = append(invokedPseudoManager, planName2)
					return when.newStatus, when.newError
				},
			}

			testee := runManagement.Task(irun, imageManager, pseudoManagers)

			_, _, err := testee(ctx, kdb.RunCursor{
				Head:   when.pickedRun.Id,
				Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Starting, kdb.Running},
				Pseudo: []kdb.PseudoPlanName{},
			})

			if err != nil {
				t.Errorf("err: actual=%+v, expect=%+v", err, nil)
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
				newStatus: kdb.Running,
				newError:  nil,
			},
			Then{
				wantImageManagerInvoked:  true,
				pseudoManagerToBeInvoked: []kdb.PseudoPlanName{},
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
				newStatus: kdb.Running,
				newError:  nil,
			},
			Then{
				wantImageManagerInvoked:  false,
				pseudoManagerToBeInvoked: []kdb.PseudoPlanName{planName1},
			},
		))
	}

	{
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
				newStatus: kdb.Running,
				newError:  errors.New("fake error"),
			},
			Then{
				wantImageManagerInvoked:  true,
				pseudoManagerToBeInvoked: []kdb.PseudoPlanName{},
			},
		))
	}

}
