package changing_status_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/domain"
	kerr "github.com/opst/knitfab/pkg/domain/errors"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	kpgrun "github.com/opst/knitfab/pkg/domain/run/db/postgres"
	"github.com/opst/knitfab/pkg/utils/try"

	. "github.com/opst/knitfab/pkg/domain/run/db/postgres/tests/changing_status/internal"
)

func TestRun_ChangingStatus_DatabaseIsEmpty(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	t.Run("[SetStatus] cause ErrMissing", func(t *testing.T) {
		for _, status := range []domain.KnitRunStatus{
			domain.Deactivated, domain.Waiting,
			domain.Ready, domain.Starting, domain.Running,
			domain.Aborting, domain.Completing,
			domain.Failed, domain.Done,
			domain.Invalidated,
		} {
			t.Run(fmt.Sprintf("(status: %s)", status), func(t *testing.T) {
				ctx := context.Background()
				pgpool := poolBroaker.GetPool(ctx, t)
				conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()
				now := try.To(testhelpers.PGNow(ctx, conn)).OrFatal(t)
				given := Testdata(t, now)
				if err := given.Plans.Apply(ctx, pgpool); err != nil {
					t.Fatal(err)
				}
				wpool := proxy.Wrap(pgpool)
				testee := kpgrun.New(wpool) // no new run
				err := testee.SetStatus(ctx, "there-are-no-runs", status)
				if err == nil || !errors.Is(err, kerr.ErrMissing) {
					t.Errorf("unexpected error: %+v", err)
				}
			})
		}
	})

	{
		theory := func(c domain.RunCursor) func(t *testing.T) {
			return func(t *testing.T) {
				ctx := context.Background()
				pgpool := poolBroaker.GetPool(ctx, t)
				conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()
				now := try.To(testhelpers.PGNow(ctx, conn)).OrFatal(t)
				given := Testdata(t, now)
				if err := given.Plans.Apply(ctx, pgpool); err != nil {
					t.Fatal(err)
				}
				wpool := proxy.Wrap(pgpool)
				testee := kpgrun.New(wpool) // no new run
				nextCursor, statusChanged, err := testee.PickAndSetStatus(
					ctx, c, func(r domain.Run) (domain.KnitRunStatus, error) {
						t.Fatal("callback should not be called")
						return domain.Aborting, nil
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				if statusChanged {
					t.Error("status should not be changed")
				}
				if !nextCursor.Equal(c) {
					t.Errorf("unmatch: picked is not false. cursor = %+v", nextCursor)
				}

				var count int

				if err := conn.QueryRow(
					ctx, `select count(*) from "run"`,
				).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Errorf("unexpected runs are inserted: count: %d", count)
				}
			}
		}

		t.Run("[PickAndSetStatus] any runs should not be picked (image plan & pseudo plan)", theory(domain.RunCursor{
			Status: []domain.KnitRunStatus{
				domain.Deactivated, domain.Waiting,
				domain.Ready, domain.Starting, domain.Running,
				domain.Aborting, domain.Completing,
				domain.Failed, domain.Done,
				domain.Invalidated,
			},
			Pseudo: []domain.PseudoPlanName{PseudoActive, PseudoInactive},
		}))
		t.Run("[PickAndSetStatus] any runs should not be picked (pseudo plan only)", theory(domain.RunCursor{
			Status: []domain.KnitRunStatus{
				domain.Deactivated, domain.Waiting,
				domain.Ready, domain.Starting, domain.Running,
				domain.Aborting, domain.Completing,
				domain.Failed, domain.Done,
				domain.Invalidated,
			},
			Pseudo:     []domain.PseudoPlanName{PseudoActive, PseudoInactive},
			PseudoOnly: true,
		}))
		t.Run("[PickAndSetStatus] any runs should not be picked (image plan only)", theory(domain.RunCursor{
			Status: []domain.KnitRunStatus{
				domain.Deactivated, domain.Waiting,
				domain.Ready, domain.Starting, domain.Running,
				domain.Aborting, domain.Completing,
				domain.Failed, domain.Done,
				domain.Invalidated,
			},
			Pseudo: []domain.PseudoPlanName{},
		}))
		t.Run("[PickAndSetStatus] any runs should not be picked (no states)", theory(domain.RunCursor{
			Status: []domain.KnitRunStatus{},
			Pseudo: []domain.PseudoPlanName{PseudoActive, PseudoInactive},
		}))
		t.Run("[PickAndSetStatus] any runs should not be picked (pseudo plan only, but plan names are empty)", theory(domain.RunCursor{
			Status: []domain.KnitRunStatus{
				domain.Deactivated, domain.Waiting,
				domain.Ready, domain.Starting, domain.Running,
				domain.Aborting, domain.Completing,
				domain.Failed, domain.Done,
				domain.Invalidated,
			},
			Pseudo:     []domain.PseudoPlanName{},
			PseudoOnly: true,
		}))
	}
}

func TestRun_ChangingStatus_DatabaseHasRuns_AllRunsAreLocked(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	t.Run("[PickAndSetStatus] it does not pick any run", func(t *testing.T) {
		for _, status := range []domain.KnitRunStatus{
			domain.Deactivated, domain.Waiting,
			domain.Ready, domain.Starting, domain.Running,
			domain.Aborting, domain.Completing,
			domain.Failed, domain.Done,
			domain.Invalidated,
		} {
			t.Run(fmt.Sprintf("(status: %s)", status), func(t *testing.T) {
				ctx := context.Background()
				pgpool := poolBroaker.GetPool(ctx, t)
				conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()
				now := try.To(testhelpers.PGNow(ctx, conn)).OrFatal(t)
				given := Testdata(t, now)
				for _, op := range []tables.Operation{given.Plans, given.Runs} {
					if err := op.Apply(ctx, pgpool); err != nil {
						t.Fatal(err)
					}
				}

				{
					// lock!
					tx := try.To(pgpool.Begin(ctx)).OrFatal(t)
					defer tx.Rollback(ctx)
					if rows, err := tx.Query(
						ctx, `select "run_id" from "run" for update`,
					); err != nil {
						t.Fatal(err)
					} else {
						rows.Close()
					}
				}

				wpool := proxy.Wrap(pgpool)
				testee := kpgrun.New(wpool) // no new run
				cursor := domain.RunCursor{
					Status: []domain.KnitRunStatus{status},
					Pseudo: []domain.PseudoPlanName{PseudoActive, PseudoInactive},
				}
				nextCursor, statusChanged, err := testee.PickAndSetStatus(
					ctx, cursor,
					func(r domain.Run) (domain.KnitRunStatus, error) {
						t.Fatal("callback should not be called")
						return domain.Failed, nil
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				if statusChanged {
					t.Error("status should not be changed")
				}

				if !cursor.Equal(nextCursor) {
					t.Error("run should not picked")
				}
			})
		}
	})

}

func TestRun_ChangingStatus_DatabaseHasRuns_AllRunsAreSuspended(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	t.Run("[PickAndSetStatus] it does not pick any run", func(t *testing.T) {
		for _, status := range []domain.KnitRunStatus{
			domain.Deactivated, domain.Waiting,
			domain.Ready, domain.Starting, domain.Running,
			domain.Aborting, domain.Completing,
			domain.Failed, domain.Done,
			domain.Invalidated,
		} {
			t.Run(fmt.Sprintf("(status: %s)", status), func(t *testing.T) {
				ctx := context.Background()
				pgpool := poolBroaker.GetPool(ctx, t)
				conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()
				now := try.To(testhelpers.PGNow(ctx, conn)).OrFatal(t)
				given := Testdata(t, now)
				for _, op := range []tables.Operation{given.Plans, given.Runs} {
					if err := op.Apply(ctx, pgpool); err != nil {
						t.Fatal(err)
					}
				}

				{
					// suspend!
					tx := try.To(pgpool.Begin(ctx)).OrFatal(t)
					defer tx.Rollback(ctx)
					if rows, err := tx.Query(
						ctx,
						`update "run" set "lifecycle_suspend_until" = 'infinity'`,
					); err != nil {
						t.Fatal(err)
					} else {
						rows.Close()
					}
				}

				wpool := proxy.Wrap(pgpool)
				testee := kpgrun.New(wpool) // no new run
				cursor := domain.RunCursor{
					Status: []domain.KnitRunStatus{status},
					Pseudo: []domain.PseudoPlanName{PseudoActive, PseudoInactive},
				}
				nextCursor, statusChanged, err := testee.PickAndSetStatus(
					ctx, cursor,
					func(r domain.Run) (domain.KnitRunStatus, error) {
						t.Fatal("callback should not be called")
						return domain.Failed, nil
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				if statusChanged {
					t.Error("status should not be changed")
				}

				if !cursor.Equal(nextCursor) {
					t.Error("run should not picked")
				}
			})
		}
	})

}
