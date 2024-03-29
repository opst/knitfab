package manager

import (
	"context"

	kdb "github.com/opst/knitfab/pkg/db"
)

type Manager func(context.Context, kdb.Run) (kdb.KnitRunStatus, error)
