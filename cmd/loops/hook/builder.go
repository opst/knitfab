package hook

import (
	api_runs "github.com/opst/knitfab/pkg/api/types/runs"
	cfg_hook "github.com/opst/knitfab/pkg/configs/hook"
)

type Hooks struct {
	Initialize    Web[api_runs.Detail]
	RunManagement Web[api_runs.Detail]
	Finishing     Web[api_runs.Detail]
}

func Build(cfg cfg_hook.WebHook) Web[api_runs.Detail] {
	return Web[api_runs.Detail]{
		BeforeURL: cfg.Before,
		AfterURL:  cfg.After,
	}
}
