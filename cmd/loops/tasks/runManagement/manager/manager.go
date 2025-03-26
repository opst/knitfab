package manager

import (
	"context"

	"github.com/opst/knitfab/v2/cmd/loops/tasks/runManagement/runManagementHook"
	"github.com/opst/knitfab/v2/pkg/domain"
)

type Manager func(
	ctx context.Context,
	hooks runManagementHook.Hooks,
	run domain.Run,
) (
	domain.KnitRunStatus,
	error,
)
