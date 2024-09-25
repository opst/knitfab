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
	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/db/mocks"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestManager_callGetAgentName(t *testing.T) {
	type When struct {
		inputs  []kdb.Assignment
		outputs []kdb.Assignment
		log     *kdb.Log
	}
	type Then struct {
		knitIds []string
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			run := kdb.Run{
				RunBody: kdb.RunBody{
					Id:     "runId",
					Status: kdb.Running,
					PlanBody: kdb.PlanBody{
						Pseudo: &kdb.PseudoPlanDetail{Name: kdb.Uploaded},
					},
				},
				Inputs:  when.inputs,
				Outputs: when.outputs,
				Log:     when.log,
			}

			dbdata := mocks.NewDataInterface()

			knitIds := []string{}

			dbdata.Impl.GetAgentName = func(
				ctx context.Context, knitId string, modes []kdb.DataAgentMode,
			) ([]string, error) {
				knitIds = append(knitIds, knitId)
				if !cmp.SliceEq(modes, []kdb.DataAgentMode{kdb.DataAgentWrite}) {
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
			inputs: []kdb.Assignment{
				{
					MountPoint:   kdb.MountPoint{Path: "/in/1", Id: 1},
					KnitDataBody: kdb.KnitDataBody{KnitId: "knitId-in-1"},
				},
				{
					MountPoint:   kdb.MountPoint{Path: "/in/2", Id: 2},
					KnitDataBody: kdb.KnitDataBody{KnitId: "knitId-in-2"},
				},
			}},
		Then{knitIds: []string{}},
	))

	t.Run("when there are output assignments, GetAgentName should be called", theory(
		When{
			inputs: []kdb.Assignment{
				{
					MountPoint:   kdb.MountPoint{Path: "/in/1", Id: 1},
					KnitDataBody: kdb.KnitDataBody{KnitId: "knitId-in-1"},
				},
				{
					MountPoint:   kdb.MountPoint{Path: "/in/2", Id: 2},
					KnitDataBody: kdb.KnitDataBody{KnitId: "knitId-in-2"},
				},
			},
			outputs: []kdb.Assignment{
				{
					MountPoint:   kdb.MountPoint{Path: "/out/1", Id: 1},
					KnitDataBody: kdb.KnitDataBody{KnitId: "knitId-out-1"},
				},
			}},
		Then{knitIds: []string{"knitId-out-1"}},
	))
	t.Run("when there are log assignments, GetAgentName should be called", theory(
		When{
			log: &kdb.Log{
				Id:           1,
				KnitDataBody: kdb.KnitDataBody{KnitId: "knitId-log"},
			}},
		Then{knitIds: []string{"knitId-log"}},
	))
}

func TestManager_after_calling_GetAgentName(t *testing.T) {
	given := kdb.Run{
		RunBody: kdb.RunBody{
			Id:     "runId",
			Status: kdb.Running,
			PlanBody: kdb.PlanBody{
				Pseudo: &kdb.PseudoPlanDetail{Name: kdb.Uploaded},
			},
		},
		Outputs: []kdb.Assignment{
			{
				MountPoint:   kdb.MountPoint{Path: "/out/1", Id: 1},
				KnitDataBody: kdb.KnitDataBody{KnitId: "knitId-out-1"},
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
		status           kdb.KnitRunStatus
		err              error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {

			dbdata := mocks.NewDataInterface()

			dbdata.Impl.GetAgentName = func(
				ctx context.Context, knitId string, modes []kdb.DataAgentMode,
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
				status:           kdb.Aborting,
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
				status:           kdb.Running,
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
