package manager

import (
	"context"

	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/runManagementHook"
	kdb "github.com/opst/knitfab/pkg/db"
)

type Manager func(
	ctx context.Context,
	hooks runManagementHook.Hooks,
	run kdb.Run,
) (
	kdb.KnitRunStatus,
	error,
)
