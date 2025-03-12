package no_inputs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgconn"
	pgerrcode "github.com/jackc/pgerrcode"
	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	. "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	kpgnom "github.com/opst/knitfab/pkg/domain/nomination/db/postgres"
	ds "github.com/opst/knitfab/pkg/domain/nomination/db/postgres/tests/nominate_data/internal/dataset"
	fn "github.com/opst/knitfab/pkg/utils/function"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestNominator_NominateData_Nominate_NoInput(t *testing.T) {
	t.Run("if no inputs are in DB, it does not cause error", func(t *testing.T) {
		poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

		plans := tables.Operation{
			Plan: []tables.Plan{
				{PlanId: Padding36("pseudo"), Active: true, Hash: Padding64("hash-pseudo")},
			},
			PlanPseudo: []tables.PlanPseudo{
				{PlanId: Padding36("pseudo"), Name: "knit#uploaded"},
			},
			Outputs: map[tables.Output]tables.OutputAttr{
				{OutputId: 1, PlanId: Padding36("pseudo"), Path: "/out"}: {},
			},
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     Padding36("upload"),
						PlanId:    Padding36("pseudo"),
						Status:    domain.Done,
						UpdatedAt: ds.DAY_1,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   Padding36("knit-1"),
							OutputId: 1,
							RunId:    Padding36("upload"),
							PlanId:   Padding36("pseudo"),
						}: {
							VolumeRef: "vol",
							Timestamp: &ds.DAY_2,
							UserTag:   ds.TAGSET_1,
						},
					},
				},
			},
		}

		ctx := context.Background()
		pool := poolBroaker.GetPool(ctx, t)

		if err := plans.Apply(ctx, pool); err != nil {
			t.Fatal(err)
		}

		wpool := proxy.Wrap(pool)
		wpool.Events().Query.After(func() {
			BeginFuncToRollback(ctx, pool, fn.Void[error](func(tx kpool.Tx) {
				if _, err := tx.Exec(ctx, `lock table "nomination" in ROW EXCLUSIVE mode nowait`); err == nil {
					t.Errorf("nomination is not locked")
				} else if pgerr := new(pgconn.PgError); !errors.As(err, &pgerr) || pgerr.Code != pgerrcode.LockNotAvailable {
					t.Errorf(
						"unexpected error: expected error code is %s, but %s",
						pgerrcode.LockNotAvailable, err,
					)
				}
			}))
		})

		tx := try.To(wpool.Begin(ctx)).OrFatal(t)
		defer tx.Rollback(ctx)
		testee := kpgnom.DefaultNominator()
		if err := testee.NominateData(ctx, tx, []string{Padding36("knit-1")}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		actual := try.To(scanner.New[tables.Nomination]().QueryAll(
			ctx, conn, `table "nomination"`,
		)).OrFatal(t)

		if len(actual) != 0 {
			t.Error("unexpected nominations are found:", actual)
		}
	})
}
