package manager

import (
	"context"

	apiruns "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	kdb "github.com/opst/knitfab/pkg/db"
)

type Manager func(context.Context, hook.Hook[apiruns.Detail], kdb.Run) (kdb.KnitRunStatus, error)
