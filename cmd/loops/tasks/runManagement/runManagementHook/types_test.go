package runManagementHook_test

import (
	"testing"

	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/runManagementHook"
	"github.com/opst/knitfab/pkg/utils/cmp"
)

func TestMerge(t *testing.T) {
	a := runManagementHook.HookResponse{
		KnitfabExtension: runManagementHook.KnitfabExtension{
			Env: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
	}

	b := runManagementHook.HookResponse{
		KnitfabExtension: runManagementHook.KnitfabExtension{
			Env: map[string]string{
				"key2": "value3",
				"key3": "value4",
			},
		},
	}

	expected := runManagementHook.HookResponse{
		KnitfabExtension: runManagementHook.KnitfabExtension{
			Env: map[string]string{
				"key1": "value1",
				"key2": "value3",
				"key3": "value4",
			},
		},
	}

	if got := runManagementHook.Merge(a, b); !cmp.MapEq(got.KnitfabExtension.Env, expected.KnitfabExtension.Env) {
		t.Errorf("Merge(%v, %v) = %v; expected %v", a, b, got, expected)
	}
}
