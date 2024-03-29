package changing_status_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	kpgrun "github.com/opst/knitfab/pkg/db/postgres/run"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	"github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils/try"

	. "github.com/opst/knitfab/pkg/db/postgres/run/tests/changing_status/internal"
)

func TestRun_ChangingStatus_DatabaseIsEmpty(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	t.Run("[SetState] cause ErrMissing", func(t *testing.T) {
		for _, status := range []kdb.KnitRunStatus{
			kdb.Deactivated, kdb.Waiting,
			kdb.Ready, kdb.Starting, kdb.Running,
			kdb.Aborting, kdb.Completing,
			kdb.Failed, kdb.Done,
			kdb.Invalidated,
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
				if err == nil || !errors.Is(err, kdb.ErrMissing) {
					t.Errorf("unexpected error: %+v", err)
				}
			})
		}
	})

	{
		theory := func(c kdb.RunCursor) func(t *testing.T) {
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
				nextCursor, err := testee.PickAndSetStatus(
					ctx, c, func(r kdb.Run) (kdb.KnitRunStatus, error) {
						t.Fatal("callback should not be called")
						return kdb.Aborting, nil
					},
				)
				if err != nil {
					t.Fatal(err)
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

		t.Run("[PickAndSetState] any runs should not be picked (image plan & pseudo plan)", theory(kdb.RunCursor{
			Status: []kdb.KnitRunStatus{
				kdb.Deactivated, kdb.Waiting,
				kdb.Ready, kdb.Starting, kdb.Running,
				kdb.Aborting, kdb.Completing,
				kdb.Failed, kdb.Done,
				kdb.Invalidated,
			},
			Pseudo: []kdb.PseudoPlanName{PseudoActive, PseudoInactive},
		}))
		t.Run("[PickAndSetState] any runs should not be picked (pseudo plan only)", theory(kdb.RunCursor{
			Status: []kdb.KnitRunStatus{
				kdb.Deactivated, kdb.Waiting,
				kdb.Ready, kdb.Starting, kdb.Running,
				kdb.Aborting, kdb.Completing,
				kdb.Failed, kdb.Done,
				kdb.Invalidated,
			},
			Pseudo:     []kdb.PseudoPlanName{PseudoActive, PseudoInactive},
			PseudoOnly: true,
		}))
		t.Run("[PickAndSetState] any runs should not be picked (image plan only)", theory(kdb.RunCursor{
			Status: []kdb.KnitRunStatus{
				kdb.Deactivated, kdb.Waiting,
				kdb.Ready, kdb.Starting, kdb.Running,
				kdb.Aborting, kdb.Completing,
				kdb.Failed, kdb.Done,
				kdb.Invalidated,
			},
			Pseudo: []kdb.PseudoPlanName{},
		}))
		t.Run("[PickAndSetState] any runs should not be picked (no states)", theory(kdb.RunCursor{
			Status: []kdb.KnitRunStatus{},
			Pseudo: []kdb.PseudoPlanName{PseudoActive, PseudoInactive},
		}))
		t.Run("[PickAndSetState] any runs should not be picked (pseudo plan only, but plan names are empty)", theory(kdb.RunCursor{
			Status: []kdb.KnitRunStatus{
				kdb.Deactivated, kdb.Waiting,
				kdb.Ready, kdb.Starting, kdb.Running,
				kdb.Aborting, kdb.Completing,
				kdb.Failed, kdb.Done,
				kdb.Invalidated,
			},
			Pseudo:     []kdb.PseudoPlanName{},
			PseudoOnly: true,
		}))
	}
}

func TestRun_ChangingStatus_DatabaseHasRuns_AllRunsAreLocked(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	t.Run("[PickAndSetStatus] it does not pick any run", func(t *testing.T) {
		for _, status := range []kdb.KnitRunStatus{
			kdb.Deactivated, kdb.Waiting,
			kdb.Ready, kdb.Starting, kdb.Running,
			kdb.Aborting, kdb.Completing,
			kdb.Failed, kdb.Done,
			kdb.Invalidated,
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
				cursor := kdb.RunCursor{
					Status: []kdb.KnitRunStatus{status},
					Pseudo: []kdb.PseudoPlanName{PseudoActive, PseudoInactive},
				}
				nextCursor, err := testee.PickAndSetStatus(
					ctx, cursor,
					func(r kdb.Run) (kdb.KnitRunStatus, error) {
						t.Fatal("callback should not be called")
						return kdb.Failed, nil
					},
				)
				if err != nil {
					t.Fatal(err)
				}

				if !cursor.Equal(nextCursor) {
					t.Error("run should not picked")
				}
			})
		}
	})

}
