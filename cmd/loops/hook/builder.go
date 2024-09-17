package hook

import (
	apiruns "github.com/opst/knitfab-api-types/runs"
	cfg_hook "github.com/opst/knitfab/pkg/configs/hook"
)

func Build(cfg cfg_hook.WebHook) Web[apiruns.Detail] {
	return Web[apiruns.Detail]{
		BeforeURL: cfg.Before,
		AfterURL:  cfg.After,
	}
}
