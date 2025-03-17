package tests_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/domain"
	kerr "github.com/opst/knitfab/pkg/domain/errors"
	th "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/domain/run/db/postgres/tests/retry/internal/dataset"
	"github.com/opst/knitfab/pkg/domain/run/db/postgres/tests/retry/internal/theory"
)

func TestRetry_ForRunWithImage(t *testing.T) {

	ctx := context.Background()
	poolBroaker := testenv.NewPoolBroaker(ctx, t)
	pgpool := poolBroaker.GetPool(ctx, t)

	t.Run("not finished run can not be retried [image, deactivated]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-2-image/run-deactivated")},
		theory.Then{Err: domain.ErrInvalidRunStateChanging},
		pgpool,
	))
	t.Run("not finished run can not be retried [image, waiting]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-2-image/run-waiting")},
		theory.Then{Err: domain.ErrInvalidRunStateChanging},
		pgpool,
	))
	t.Run("not finished run can not be retried [image, ready]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-2-image/run-ready")},
		theory.Then{Err: domain.ErrInvalidRunStateChanging},
		pgpool,
	))
	t.Run("not finished run can not be retried [image, starting]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-2-image/run-starting")},
		theory.Then{Err: domain.ErrInvalidRunStateChanging},
		pgpool,
	))
	t.Run("not finished run can not be retried [image, running]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-2-image/run-running")},
		theory.Then{Err: domain.ErrInvalidRunStateChanging},
		pgpool,
	))
	t.Run("not finished run can not be retried [image, completing]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-2-image/run-completing")},
		theory.Then{Err: domain.ErrInvalidRunStateChanging},
		pgpool,
	))
	t.Run("not finished run can not be retried [image, aborting]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-2-image/run-aborting")},
		theory.Then{Err: domain.ErrInvalidRunStateChanging},
		pgpool,
	))

	t.Run("finished run with downstream can not be retried", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-2-image/run-done")},
		theory.Then{Err: domain.ErrRunHasDownstreams},
		pgpool,
	))

	t.Run("finished run without downstream can be retried [done]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-3-image/run-done-leaf")},
		theory.Then{
			RemovedRunIds: []string{},
		},
		pgpool,
	))
	t.Run("finished run with only invalidated downstream can be retried [done]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-3-image/run-done")},
		theory.Then{
			RemovedRunIds: []string{
				th.Padding36("plan-3-image/run-invalidated"),
			},
		},
		pgpool,
	))
	t.Run("finished run with only invalidated downstream can be retried [done (purged)]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-2-image/run-done-purged")},
		theory.Then{},
		pgpool,
	))

	t.Run("finished run without downstream can be retried [failed]", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-2-image/run-failed")},
		theory.Then{},
		pgpool,
	))

	t.Run("retrying not existing run should be failed", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("no-such-run"), DoNotLockTheRun: true},
		theory.Then{Err: kerr.ErrMissing},
		pgpool,
	))

	t.Run("retrying invalidated run should be failed", theory.Theory(
		dataset.GivenDatabase,
		theory.When{RunId: th.Padding36("plan-3-image/run-invalidated")},
		theory.Then{Err: kerr.ErrMissing},
		pgpool,
	))

	err := errors.New("fake error")
	t.Run("if nominator.DropData returns error, it retuns that error", theory.Theory(
		dataset.GivenDatabase,
		theory.When{
			RunId:        th.Padding36("plan-3-image/run-done-leaf"),
			NominatorErr: err,
		},
		theory.Then{Err: err},
		pgpool,
	))
}
