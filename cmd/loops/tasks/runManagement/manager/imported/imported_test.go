package imported_test

import (
	"context"
	"errors"
	"testing"

	apiruns "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager/imported"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/runManagementHook"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestImportedManager(t *testing.T) {
	t.Run("Do not change state:", func(t *testing.T) {

		theory := func(when domain.KnitRunStatus) func(*testing.T) {
			return func(t *testing.T) {
				run := domain.Run{
					RunBody: domain.RunBody{
						Status: when,
					},
				}

				hookIsCalled := false
				hooks := runManagementHook.Hooks{
					ToStarting: hook.Func[apiruns.Detail, runManagementHook.HookResponse]{
						BeforeFn: func(d apiruns.Detail) (runManagementHook.HookResponse, error) {
							t.Errorf("Starting Before Hook should not be called")
							return runManagementHook.HookResponse{}, nil
						},
					},
					ToRunning: hook.Func[apiruns.Detail, struct{}]{
						BeforeFn: func(d apiruns.Detail) (struct{}, error) {
							t.Errorf("Running Before Hook should not be called")
							return struct{}{}, nil
						},
					},
					ToCompleting: hook.Func[apiruns.Detail, struct{}]{
						BeforeFn: func(d apiruns.Detail) (struct{}, error) {
							t.Errorf("Completeing Before Hook should not be called")
							return struct{}{}, nil
						},
					},
					ToAborting: hook.Func[apiruns.Detail, struct{}]{
						BeforeFn: func(d apiruns.Detail) (struct{}, error) {
							hookIsCalled = true
							return struct{}{}, nil
						},
					},
				}

				testee := imported.New()
				got := try.To(testee(context.Background(), hooks, run)).OrFatal(t)

				if got != when {
					t.Errorf("Expected status %v, got %v", when, got)
				}

				if hookIsCalled {
					t.Errorf("Hook should not be called")
				}
			}
		}

		t.Run("Starting", theory(domain.Starting))
		t.Run("Ready", theory(domain.Ready))
	})

	t.Run("Change Run state from Running to Aborting", func(t *testing.T) {
		run := domain.Run{
			RunBody: domain.RunBody{
				Status: domain.Running,
			},
		}

		hookIsCalled := false
		hooks := runManagementHook.Hooks{
			ToStarting: hook.Func[apiruns.Detail, runManagementHook.HookResponse]{
				BeforeFn: func(d apiruns.Detail) (runManagementHook.HookResponse, error) {
					t.Errorf("Starting Before Hook should not be called")
					return runManagementHook.HookResponse{}, nil
				},
			},
			ToRunning: hook.Func[apiruns.Detail, struct{}]{
				BeforeFn: func(d apiruns.Detail) (struct{}, error) {
					t.Errorf("Running Before Hook should not be called")
					return struct{}{}, nil
				},
			},
			ToCompleting: hook.Func[apiruns.Detail, struct{}]{
				BeforeFn: func(d apiruns.Detail) (struct{}, error) {
					t.Errorf("Completeing Before Hook should not be called")
					return struct{}{}, nil
				},
			},
			ToAborting: hook.Func[apiruns.Detail, struct{}]{
				BeforeFn: func(d apiruns.Detail) (struct{}, error) {
					hookIsCalled = true
					return struct{}{}, nil
				},
			},
		}

		testee := imported.New()
		got := try.To(testee(context.Background(), hooks, run)).OrFatal(t)

		if got != domain.Aborting {
			t.Errorf("Expected status %v, got %v", domain.Aborting, got)
		}

		if !hookIsCalled {
			t.Errorf("Hook should be called")
		}
	})

	t.Run("Do not change Run state from Running if hook causes an error", func(t *testing.T) {
		run := domain.Run{
			RunBody: domain.RunBody{
				Status: domain.Running,
			},
		}

		expectedErr := errors.New("expected error")
		hooks := runManagementHook.Hooks{
			ToStarting: hook.Func[apiruns.Detail, runManagementHook.HookResponse]{
				BeforeFn: func(d apiruns.Detail) (runManagementHook.HookResponse, error) {
					t.Errorf("Starting Before Hook should not be called")
					return runManagementHook.HookResponse{}, nil
				},
			},
			ToRunning: hook.Func[apiruns.Detail, struct{}]{
				BeforeFn: func(d apiruns.Detail) (struct{}, error) {
					t.Errorf("Running Before Hook should not be called")
					return struct{}{}, nil
				},
			},
			ToCompleting: hook.Func[apiruns.Detail, struct{}]{
				BeforeFn: func(d apiruns.Detail) (struct{}, error) {
					t.Errorf("Completeing Before Hook should not be called")
					return struct{}{}, nil
				},
			},
			ToAborting: hook.Func[apiruns.Detail, struct{}]{
				BeforeFn: func(d apiruns.Detail) (struct{}, error) {
					return struct{}{}, expectedErr
				},
			},
		}

		testee := imported.New()
		got, gotErr := testee(context.Background(), hooks, run)

		if got != domain.Running {
			t.Errorf("Expected status %v, got %v", domain.Running, got)
		}
		if !errors.Is(gotErr, expectedErr) {
			t.Errorf("Expected error %v, got %v", expectedErr, gotErr)
		}
	})
}
