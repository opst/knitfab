package manager

import (
	"context"

	"github.com/opst/knitfab/cmd/loops/hook"
	api_runs "github.com/opst/knitfab/pkg/api/types/runs"
	kdb "github.com/opst/knitfab/pkg/db"
)

type Manager func(context.Context, hook.Hook[api_runs.Detail], kdb.Run) (kdb.KnitRunStatus, error)
