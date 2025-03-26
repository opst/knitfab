package hook

import (
	apiruns "github.com/opst/knitfab-api-types/v2/runs"
	cfg_hook "github.com/opst/knitfab/v2/pkg/configs/hook"
)

func Build[R any](cfg cfg_hook.WebHook, merge func(a, b R) R) Web[apiruns.Detail, R] {
	return Web[apiruns.Detail, R]{
		BeforeURL: cfg.Before,
		AfterURL:  cfg.After,
		Merge:     merge,
	}
}
