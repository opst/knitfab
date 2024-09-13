package imported_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/manager/imported"
	api_runs "github.com/opst/knitfab/pkg/api/types/runs"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestImportedManager(t *testing.T) {
	t.Run("Do not change state:", func(t *testing.T) {

		theory := func(when kdb.KnitRunStatus) func(*testing.T) {
			return func(t *testing.T) {
				run := kdb.Run{
					RunBody: kdb.RunBody{
						Status: when,
					},
				}

				hookIsCalled := false
				hooks := hook.Func[api_runs.Detail]{
					BeforeFn: func(d api_runs.Detail) error {
						hookIsCalled = true
						return nil
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

		t.Run("Starting", theory(kdb.Starting))
		t.Run("Ready", theory(kdb.Ready))
	})

	t.Run("Change Run state from Running to Aborting", func(t *testing.T) {
		run := kdb.Run{
			RunBody: kdb.RunBody{
				Status: kdb.Running,
			},
		}

		hookIsCalled := false
		hooks := hook.Func[api_runs.Detail]{
			BeforeFn: func(d api_runs.Detail) error {
				hookIsCalled = true
				return nil
			},
		}

		testee := imported.New()
		got := try.To(testee(context.Background(), hooks, run)).OrFatal(t)

		if got != kdb.Aborting {
			t.Errorf("Expected status %v, got %v", kdb.Aborting, got)
		}

		if !hookIsCalled {
			t.Errorf("Hook should be called")
		}
	})

	t.Run("Do not change Run state from Running if hook causes an error", func(t *testing.T) {
		run := kdb.Run{
			RunBody: kdb.RunBody{
				Status: kdb.Running,
			},
		}

		expectedErr := errors.New("expected error")
		hooks := hook.Func[api_runs.Detail]{
			BeforeFn: func(d api_runs.Detail) error {
				return expectedErr
			},
		}

		testee := imported.New()
		got, gotErr := testee(context.Background(), hooks, run)

		if got != kdb.Running {
			t.Errorf("Expected status %v, got %v", kdb.Running, got)
		}
		if !errors.Is(gotErr, expectedErr) {
			t.Errorf("Expected error %v, got %v", expectedErr, gotErr)
		}
	})
}
