package initialize_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab-api-types/misc/rfctime"
	apiruns "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/tasks/initialize"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	types "github.com/opst/knitfab/pkg/domain"
	kdbrunmock "github.com/opst/knitfab/pkg/domain/run/db/mock"
	k8srunmock "github.com/opst/knitfab/pkg/domain/run/k8s/mock"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestTask_Outside_of_PickAndSetStatus(t *testing.T) {

	type When struct {
		Cursor            types.RunCursor
		NextCursor        types.RunCursor
		StatusChanged     bool
		Err               error
		IRunGetReturnsNil bool
		UpdatedRun        types.Run
	}

	type Then struct {
		Cursor   types.RunCursor
		Continue bool
		Err      error
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			run := kdbrunmock.NewRunInterface()
			run.Impl.PickAndSetStatus = func(
				ctx context.Context, value types.RunCursor,
				f func(types.Run) (types.KnitRunStatus, error),
			) (types.RunCursor, bool, error) {
				return when.NextCursor, when.StatusChanged, when.Err
			}

			run.Impl.Get = func(ctx context.Context, ids []string) (map[string]types.Run, error) {
				if when.IRunGetReturnsNil {
					return nil, errors.New("irun.Get: should be ignored")
				}
				return map[string]types.Run{when.NextCursor.Head: when.UpdatedRun}, nil
			}

			hookAfterHasBeenCalled := false
			testee := initialize.Task(run, nil, hook.Func[apiruns.Detail, struct{}]{
				AfterFn: func(d apiruns.Detail) error {
					hookAfterHasBeenCalled = true
					want := bindruns.ComposeDetail(when.UpdatedRun)
					if !d.Equal(want) {
						t.Errorf(
							"unexpected detail:\n===actual==\n%+v\n===expected===\n%+v",
							d, want,
						)
					}
					return errors.New("hook after: should be ignored")
				},
			})

			value, ok, err := testee(ctx, when.Cursor)

			if !errors.Is(err, then.Err) {
				t.Errorf("unexpected error: %+v", err)
			}
			if ok != then.Continue {
				t.Errorf("unexpected Continue: %v", ok)
			}
			if !value.Equal(then.Cursor) {
				t.Errorf(
					"unexpected value:\n===actual==\n%+v\n===expected===\n%+v",
					value, then.Cursor,
				)
			}
			if when.StatusChanged != hookAfterHasBeenCalled {
				t.Errorf("unexpected hook.After has been called: %v", hookAfterHasBeenCalled)
			}
		}
	}

	t.Run("it continues when PickAndSetStatus returns a new cursor", theory(
		When{
			Cursor: types.RunCursor{
				Head:   "previous-run",
				Status: []types.KnitRunStatus{types.Waiting},
			},

			NextCursor: types.RunCursor{
				Head:   "next-run",
				Status: []types.KnitRunStatus{types.Waiting},
			},
			StatusChanged: true,
			Err:           nil,

			UpdatedRun: types.Run{
				RunBody: types.RunBody{
					Id:         "next-run",
					Status:     types.Ready,
					WorkerName: "worker-name",
					UpdatedAt: try.To(
						rfctime.ParseRFC3339DateTime("2021-10-11T12:13:14+09:00"),
					).OrFatal(t).Time(),
					PlanBody: types.PlanBody{
						PlanId: "plan-id",
						Image: &types.ImageIdentifier{
							Image:   "example.repo.invalid/image",
							Version: "v1.0.0",
						},
					},
				},
				Inputs: []types.Assignment{
					{
						MountPoint: types.MountPoint{
							Id:   100_100,
							Path: "/in/1",
							Tags: types.NewTagSet([]types.Tag{{Key: "type", Value: "csv"}}),
						},
						KnitDataBody: types.KnitDataBody{
							KnitId:    "next-run-input-1",
							VolumeRef: "ref-next-run-input-1",
							Tags: types.NewTagSet([]types.Tag{
								{Key: "type", Value: "csv"},
								{Key: "input", Value: "1"},
							}),
						},
					},
				},
				Outputs: []types.Assignment{
					{
						MountPoint: types.MountPoint{
							Id:   100_010,
							Path: "/out/1",
							Tags: types.NewTagSet([]types.Tag{
								{Key: "type", Value: "model"},
								{Key: "output", Value: "1"},
							}),
						},
					},
				},
				Log: &types.Log{
					Id: 100_001,
					Tags: types.NewTagSet([]types.Tag{
						{Key: "type", Value: "jsonl"},
					}),
					KnitDataBody: types.KnitDataBody{
						KnitId:    "next-run-log",
						VolumeRef: "ref-next-run-log",
						Tags: types.NewTagSet([]types.Tag{
							{Key: "type", Value: "jsonl"},
							{Key: "log", Value: "1"},
						}),
					},
				},
			},
		},
		Then{
			Cursor: types.RunCursor{
				Head: "next-run", Status: []types.KnitRunStatus{types.Waiting},
			},
			Continue: true,
			Err:      nil,
		},
	))

	t.Run("it stops when PickAndSetStatus does not move cursor", theory(
		When{
			Cursor: types.RunCursor{
				Head:   "previous-run",
				Status: []types.KnitRunStatus{types.Waiting},
			},

			NextCursor: types.RunCursor{
				Head:   "previous-run",
				Status: []types.KnitRunStatus{types.Waiting},
			},
			StatusChanged: false,
			Err:           nil,
		},
		Then{
			Cursor: types.RunCursor{
				Head: "previous-run", Status: []types.KnitRunStatus{types.Waiting},
			},
			Continue: false,
			Err:      nil,
		},
	))

	t.Run("it ignores context.Canceled", theory(
		When{
			Cursor: types.RunCursor{
				Head:   "previous-run",
				Status: []types.KnitRunStatus{types.Waiting},
			},

			NextCursor: types.RunCursor{
				Head:   "next-run",
				Status: []types.KnitRunStatus{types.Waiting},
			},
			StatusChanged: false,
			Err:           context.Canceled,
		},
		Then{
			Cursor: types.RunCursor{
				Head: "next-run", Status: []types.KnitRunStatus{types.Waiting},
			},
			Continue: true,
		},
	))

	t.Run("it ignores context.DeadlineExceeded", theory(
		When{
			Cursor: types.RunCursor{
				Head:   "previous-run",
				Status: []types.KnitRunStatus{types.Waiting},
			},

			NextCursor: types.RunCursor{
				Head:   "next-run",
				Status: []types.KnitRunStatus{types.Waiting},
			},
			StatusChanged: false,
			Err:           context.DeadlineExceeded,
		},
		Then{
			Cursor: types.RunCursor{
				Head: "next-run", Status: []types.KnitRunStatus{types.Waiting},
			},
			Continue: true,
			Err:      nil,
		},
	))
}

func TestTask_Inside_of_PickAndSetStatus(t *testing.T) {
	ctx := context.Background()

	pickedRun := types.Run{
		RunBody: types.RunBody{
			Id:         "picked-run",
			Status:     types.Waiting,
			WorkerName: "worker-name",
			UpdatedAt: try.To(
				rfctime.ParseRFC3339DateTime("2021-10-11T12:13:14+09:00"),
			).OrFatal(t).Time(),
			PlanBody: types.PlanBody{
				PlanId: "plan-id",
				Image: &types.ImageIdentifier{
					Image:   "example.repo.invalid/image",
					Version: "v1.0.0",
				},
			},
		},
		Inputs: []types.Assignment{
			{
				MountPoint: types.MountPoint{
					Id:   100_100,
					Path: "/in/1",
					Tags: types.NewTagSet([]types.Tag{{Key: "type", Value: "csv"}}),
				},
				KnitDataBody: types.KnitDataBody{
					KnitId:    "picked-run-input-1",
					VolumeRef: "ref-picked-run-input-1",
					Tags: types.NewTagSet([]types.Tag{
						{Key: "type", Value: "csv"},
						{Key: "input", Value: "1"},
					}),
				},
			},
		},
		Outputs: []types.Assignment{
			{
				MountPoint: types.MountPoint{
					Id:   100_010,
					Path: "/out/1",
					Tags: types.NewTagSet([]types.Tag{
						{Key: "type", Value: "model"},
						{Key: "output", Value: "1"},
					}),
				},
			},
		},
		Log: &types.Log{
			Id: 100_001,
			Tags: types.NewTagSet([]types.Tag{
				{Key: "type", Value: "jsonl"},
				{Key: "log", Value: "1"},
			}),
		},
	}
	seed := types.RunCursor{
		Head:   "previous-run",
		Status: []types.KnitRunStatus{types.Waiting},
	}

	type When struct {
		BeforeErr error
		InitErr   error
	}

	type Then struct {
		NewStatus types.KnitRunStatus
		Err       error
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			run := kdbrunmock.NewRunInterface()
			run.Impl.PickAndSetStatus = func(
				ctx context.Context, value types.RunCursor,
				f func(types.Run) (types.KnitRunStatus, error),
			) (types.RunCursor, bool, error) {
				gotStatus, err := f(pickedRun)

				if when.BeforeErr != nil {
					if !errors.Is(err, when.BeforeErr) {
						t.Errorf("unexpected error: %+v", err)
					}
				} else {
					if !errors.Is(err, when.InitErr) {
						t.Errorf("unexpected error: %+v", err)
					}
				}

				if gotStatus != then.NewStatus {
					t.Errorf("unexpected new status: %s (expected: %s)", gotStatus, then.NewStatus)
				}

				return seed, true, err
			}
			run.Impl.Get = func(ctx context.Context, ids []string) (map[string]types.Run, error) {
				return map[string]types.Run{pickedRun.Id: pickedRun}, nil
			}

			initHasBeenCalled := false
			mockIRun := k8srunmock.New(t)
			mockIRun.Impl.Initialize = func(ctx context.Context, r types.Run) error {
				initHasBeenCalled = true
				if !r.Equal(&pickedRun) {
					t.Errorf(
						"unexpected run is passed to PVC Initializer:\n===actual==\n%+v\n===expected===\n%+v",
						r, pickedRun,
					)
				}
				return when.InitErr
			}

			beforeFnHasBeenCalled := false
			testee := initialize.Task(run, mockIRun, hook.Func[apiruns.Detail, struct{}]{
				BeforeFn: func(d apiruns.Detail) (struct{}, error) {
					beforeFnHasBeenCalled = true
					if want := bindruns.ComposeDetail(pickedRun); !d.Equal(want) {
						t.Errorf(
							"unexpected detail:\n===actual==\n%+v\n===expected===\n%+v",
							d, want,
						)
					}

					return struct{}{}, when.BeforeErr
				},
			})

			testee(ctx, seed)

			if !beforeFnHasBeenCalled {
				t.Error("BeforeFn has not been called")
			}

			if when.BeforeErr == nil {
				if !initHasBeenCalled {
					t.Error("PVCInitializer has not been called")
				}
			}
		}
	}

	beforeErr := errors.New("fake error (before)")
	initErr := errors.New("fake error (init)")

	t.Run("it continues when BeforeFn and PVCInitializer successes", theory(
		When{
			BeforeErr: nil,
			InitErr:   nil,
		},
		Then{
			NewStatus: types.Ready,
			Err:       nil,
		},
	))

	t.Run("it stops when BeforeFn returns an error", theory(
		When{
			BeforeErr: beforeErr,
			InitErr:   nil,
		},
		Then{
			NewStatus: pickedRun.Status,
			Err:       beforeErr,
		},
	))

	t.Run("it stops when PVCInitializer returns an error", theory(
		When{
			BeforeErr: nil,
			InitErr:   initErr,
		},
		Then{
			NewStatus: pickedRun.Status,
			Err:       initErr,
		},
	))
}
