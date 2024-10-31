package uploaded_test

import (
	"context"
	"errors"
	"testing"

	apiruns "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager/uploaded"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/runManagementHook"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	types "github.com/opst/knitfab/pkg/domain"
	mocks "github.com/opst/knitfab/pkg/domain/data/db/mock"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestManager_callGetAgentName(t *testing.T) {
	type When struct {
		inputs  []types.Assignment
		outputs []types.Assignment
		log     *types.Log
	}
	type Then struct {
		knitIds []string
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			run := types.Run{
				RunBody: types.RunBody{
					Id:     "runId",
					Status: types.Running,
					PlanBody: types.PlanBody{
						Pseudo: &types.PseudoPlanDetail{Name: types.Uploaded},
					},
				},
				Inputs:  when.inputs,
				Outputs: when.outputs,
				Log:     when.log,
			}

			dbdata := mocks.NewDataInterface()

			knitIds := []string{}

			dbdata.Impl.GetAgentName = func(
				ctx context.Context, knitId string, modes []types.DataAgentMode,
			) ([]string, error) {
				knitIds = append(knitIds, knitId)
				if !cmp.SliceEq(modes, []types.DataAgentMode{types.DataAgentWrite}) {
					t.Errorf(
						"modes should be [kdb.DataAgentWrite]: actual = %+v",
						modes,
					)
				}
				return []string{"agentName"}, nil
			}

			ctx := context.Background()
			testee := uploaded.New(dbdata)

			hooks := runManagementHook.Hooks{
				ToStarting: hook.Func[apiruns.Detail, runManagementHook.HookResponse]{
					BeforeFn: func(d apiruns.Detail) (runManagementHook.HookResponse, error) {
						t.Errorf("Starting Before Hook should not be called")
						return runManagementHook.HookResponse{}, nil
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("Starting After Hook should not be called")
						return nil
					},
				},
				ToRunning: hook.Func[apiruns.Detail, struct{}]{
					BeforeFn: func(d apiruns.Detail) (struct{}, error) {
						t.Errorf("Running Before Hook should not be called")
						return struct{}{}, nil
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("Running After Hook should not be called")
						return nil
					},
				},
				ToCompleting: hook.Func[apiruns.Detail, struct{}]{
					BeforeFn: func(d apiruns.Detail) (struct{}, error) {
						t.Error("completing before hook should not be called")
						return struct{}{}, nil
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("completing after hook should not be called")
						return nil
					},
				},
				ToAborting: hook.Func[apiruns.Detail, struct{}]{
					BeforeFn: func(d apiruns.Detail) (struct{}, error) {
						return struct{}{}, nil
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("aboring after hook should not be called")
						return nil
					},
				},
			}
			try.To(testee(ctx, hooks, run)).OrFatal(t)

			if !cmp.SliceEq(knitIds, then.knitIds) {
				t.Errorf(
					"knitIds should be %+v: actual = %+v",
					then.knitIds, knitIds,
				)
			}
		}
	}

	t.Run("when input assignments only, GetAgentName should not be called", theory(
		When{
			inputs: []types.Assignment{
				{
					MountPoint:   types.MountPoint{Path: "/in/1", Id: 1},
					KnitDataBody: types.KnitDataBody{KnitId: "knitId-in-1"},
				},
				{
					MountPoint:   types.MountPoint{Path: "/in/2", Id: 2},
					KnitDataBody: types.KnitDataBody{KnitId: "knitId-in-2"},
				},
			}},
		Then{knitIds: []string{}},
	))

	t.Run("when there are output assignments, GetAgentName should be called", theory(
		When{
			inputs: []types.Assignment{
				{
					MountPoint:   types.MountPoint{Path: "/in/1", Id: 1},
					KnitDataBody: types.KnitDataBody{KnitId: "knitId-in-1"},
				},
				{
					MountPoint:   types.MountPoint{Path: "/in/2", Id: 2},
					KnitDataBody: types.KnitDataBody{KnitId: "knitId-in-2"},
				},
			},
			outputs: []types.Assignment{
				{
					MountPoint:   types.MountPoint{Path: "/out/1", Id: 1},
					KnitDataBody: types.KnitDataBody{KnitId: "knitId-out-1"},
				},
			}},
		Then{knitIds: []string{"knitId-out-1"}},
	))
	t.Run("when there are log assignments, GetAgentName should be called", theory(
		When{
			log: &types.Log{
				Id:           1,
				KnitDataBody: types.KnitDataBody{KnitId: "knitId-log"},
			}},
		Then{knitIds: []string{"knitId-log"}},
	))
}

func TestManager_after_calling_GetAgentName(t *testing.T) {
	given := types.Run{
		RunBody: types.RunBody{
			Id:     "runId",
			Status: types.Running,
			PlanBody: types.PlanBody{
				Pseudo: &types.PseudoPlanDetail{Name: types.Uploaded},
			},
		},
		Outputs: []types.Assignment{
			{
				MountPoint:   types.MountPoint{Path: "/out/1", Id: 1},
				KnitDataBody: types.KnitDataBody{KnitId: "knitId-out-1"},
			},
		},
	}

	type When struct {
		knitIds       []string
		errGetAgent   error
		errBeforeHook error
	}

	type Then struct {
		invokeBeforeHook bool
		status           types.KnitRunStatus
		err              error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {

			dbdata := mocks.NewDataInterface()

			dbdata.Impl.GetAgentName = func(
				ctx context.Context, knitId string, modes []types.DataAgentMode,
			) ([]string, error) {
				return when.knitIds, when.errGetAgent
			}

			ctx := context.Background()
			testee := uploaded.New(dbdata)

			beforeHookHasBeenInvoked := false

			hooks := runManagementHook.Hooks{
				ToStarting: hook.Func[apiruns.Detail, runManagementHook.HookResponse]{
					BeforeFn: func(d apiruns.Detail) (runManagementHook.HookResponse, error) {
						t.Errorf("Starting Before Hook should not be called")
						return runManagementHook.HookResponse{}, nil
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("Starting After Hook should not be called")
						return nil
					},
				},
				ToRunning: hook.Func[apiruns.Detail, struct{}]{
					BeforeFn: func(d apiruns.Detail) (struct{}, error) {
						t.Errorf("Running Before Hook should not be called")
						return struct{}{}, nil
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("Running After Hook should not be called")
						return nil
					},
				},
				ToCompleting: hook.Func[apiruns.Detail, struct{}]{
					BeforeFn: func(d apiruns.Detail) (struct{}, error) {
						t.Error("completing before hook should not be called")
						return struct{}{}, nil
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("completing after hook should not be called")
						return nil
					},
				},
				ToAborting: hook.Func[apiruns.Detail, struct{}]{
					BeforeFn: func(d apiruns.Detail) (struct{}, error) {
						beforeHookHasBeenInvoked = true

						want := bindruns.ComposeDetail(given)
						if !d.Equal(want) {
							t.Errorf(
								"detail should be %+v: actual = %+v",
								want, d,
							)
						}

						return struct{}{}, when.errBeforeHook
					},
					AfterFn: func(d apiruns.Detail) error {
						t.Error("aborting after hook should not be called")
						return nil
					},
				},
			}
			status, err := testee(ctx, hooks, given)

			if status != then.status {
				t.Errorf(
					"status should be %v: actual = %v",
					then.status, status,
				)
			}

			if !errors.Is(err, then.err) {
				t.Errorf(
					"err should be %v: actual = %v",
					then.err, err,
				)
			}

			if then.invokeBeforeHook != beforeHookHasBeenInvoked {
				t.Errorf(
					"before invoked: actual = %v, expected = %v",
					beforeHookHasBeenInvoked, then.invokeBeforeHook,
				)
			}
		}
	}

	{
		expectedErr := errors.New("expected error")
		t.Run("when GetAgentName returns an error, the error should be returned", theory(
			When{
				errGetAgent:   expectedErr,
				errBeforeHook: nil,
			},
			Then{
				status:           given.Status,
				err:              expectedErr,
				invokeBeforeHook: false,
			},
		))
	}

	{
		t.Run("when GetAgentName returns an empty list, it should return Aborting", theory(
			When{
				knitIds:       []string{},
				errBeforeHook: nil,
			},
			Then{
				status:           types.Aborting,
				err:              nil,
				invokeBeforeHook: true,
			},
		))
	}

	{
		expetedErr := errors.New("expected error")
		t.Run("when before hook returns error, it should return the status as it was", theory(
			When{
				knitIds:       []string{},
				errBeforeHook: expetedErr,
			},
			Then{
				status:           types.Running,
				err:              expetedErr,
				invokeBeforeHook: true,
			},
		))
	}

	{
		t.Run("when GetAgentName returns a non-empty list, it should return status as it was", theory(
			When{knitIds: []string{"knitId-out-1"}},
			Then{
				status:           given.Status,
				err:              nil,
				invokeBeforeHook: false,
			},
		))
	}
}
