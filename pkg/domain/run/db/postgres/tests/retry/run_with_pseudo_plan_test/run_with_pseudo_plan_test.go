package tests_test

import (
	"context"
	"testing"

	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/domain"
	th "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/domain/run/db/postgres/tests/retry/internal/dataset"
	"github.com/opst/knitfab/pkg/domain/run/db/postgres/tests/retry/internal/theory"
)

func TestRetry_ForRunWithoutImage(t *testing.T) {

	ctx := context.Background()
	poolBroaker := testenv.NewPoolBroaker(ctx, t)
	pgpool := poolBroaker.GetPool(ctx, t)

	t.Run("not finished run can not be retried [pseudo, deactivated]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-1-pseudo/run-deactivated")},
		theory.Then{Err: domain.ErrRunIsProtected},
		pgpool,
	))
	t.Run("not finished run can not be retried [pseudo, waiting]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-1-pseudo/run-waiting")},
		theory.Then{Err: domain.ErrRunIsProtected},
		pgpool,
	))
	t.Run("not finished run can not be retried [pseudo, ready]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-1-pseudo/run-ready")},
		theory.Then{Err: domain.ErrRunIsProtected},
		pgpool,
	))
	t.Run("not finished run can not be retried [pseudo, starting]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-1-pseudo/run-starting")},
		theory.Then{Err: domain.ErrRunIsProtected},
		pgpool,
	))
	t.Run("not finished run can not be retried [pseudo, running]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-1-pseudo/run-running")},
		theory.Then{Err: domain.ErrRunIsProtected},
		pgpool,
	))
	t.Run("not finished run can not be retried [pseudo, completing]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-1-pseudo/run-completing")},
		theory.Then{Err: domain.ErrRunIsProtected},
		pgpool,
	))
	t.Run("not finished run can not be retried [pseudo, aborting]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-1-pseudo/run-aborting")},
		theory.Then{Err: domain.ErrRunIsProtected},
		pgpool,
	))
	t.Run("not finished run can not be retried [pseudo, invalidated]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-1-pseudo/run-invalidated")},
		theory.Then{Err: domain.ErrRunIsProtected},
		pgpool,
	))

	t.Run("pseudo run can not be retried [failed]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-1-pseudo/run-failed")},
		theory.Then{Err: domain.ErrRunIsProtected},
		pgpool,
	))
	t.Run("pseudo run can not be retried [done]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-1-pseudo/run-done-leaf")},
		theory.Then{Err: domain.ErrRunIsProtected},
		pgpool,
	))
}
