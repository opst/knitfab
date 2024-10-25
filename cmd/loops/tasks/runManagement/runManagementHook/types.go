package runManagementHook

import (
	apiruns "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
)

type KnitfabExtension struct {
	Env map[string]string `json:"env"`
}

func mergeKnitfabExtension(a, b KnitfabExtension) KnitfabExtension {

	env := make(map[string]string)
	for k, v := range a.Env {
		env[k] = v
	}
	for k, v := range b.Env {
		env[k] = v
	}

	return KnitfabExtension{
		Env: env,
	}
}

type HookResponse struct {
	KnitfabExtension KnitfabExtension `json:"knitfabExtension"`
}

func Merge(a, b HookResponse) HookResponse {
	return HookResponse{
		KnitfabExtension: mergeKnitfabExtension(a.KnitfabExtension, b.KnitfabExtension),
	}
}

type Hooks struct {
	ToStarting   hook.Hook[apiruns.Detail, HookResponse]
	ToRunning    hook.Hook[apiruns.Detail, struct{}]
	ToCompleting hook.Hook[apiruns.Detail, struct{}]
	ToAborting   hook.Hook[apiruns.Detail, struct{}]
}
