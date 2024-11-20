package manager

import (
	"context"

	"github.com/opst/knitfab/cmd/loops/tasks/runManagement/runManagementHook"
	"github.com/opst/knitfab/pkg/domain"
)

type Manager func(
	ctx context.Context,
	hooks runManagementHook.Hooks,
	run domain.Run,
) (
	domain.KnitRunStatus,
	error,
)
