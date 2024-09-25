package hook

import (
	apiruns "github.com/opst/knitfab-api-types/runs"
	cfg_hook "github.com/opst/knitfab/pkg/configs/hook"
)

func Build[R any](cfg cfg_hook.WebHook, merge func(a, b R) R) Web[apiruns.Detail, R] {
	return Web[apiruns.Detail, R]{
		BeforeURL: cfg.Before,
		AfterURL:  cfg.After,
		Merge:     merge,
	}
}
