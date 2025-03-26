package tests_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab/v2/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/v2/pkg/conn/db/postgres/pool/proxy"
	"github.com/opst/knitfab/v2/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/v2/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/v2/pkg/domain"
	"github.com/opst/knitfab/v2/pkg/domain/internal/db/postgres/tables"
	th "github.com/opst/knitfab/v2/pkg/domain/internal/db/postgres/testhelpers"
	nom_mock "github.com/opst/knitfab/v2/pkg/domain/nomination/db/mock"
	kpgrun "github.com/opst/knitfab/v2/pkg/domain/run/db/postgres"
	"github.com/opst/knitfab/v2/pkg/domain/run/db/postgres/tests/delete/internal/dataset"
	"github.com/opst/knitfab/v2/pkg/utils/cmp"
	"github.com/opst/knitfab/v2/pkg/utils/try"
)

func TestRun_Delete_When_Nominator_Cause_Error(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	type When struct {
		RunId                  string
		NominatorDropDataError error
	}
	type Then struct {
		WantNominatorDropData           []string
		RunIdsShouldBeDeletedAdditional []string
		ReasonNotDeleted                error
	}

	theory := func(given tables.Operation, when When, _ Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pgpool := poolBroaker.GetPool(ctx, t)
			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			if err := dataset.GivenDatabase.ApplyWithConn(ctx, conn); err != nil {
				t.Fatal(err)
			}
			if err := given.ApplyWithConn(ctx, conn); err != nil {
				t.Fatal(err)
			}

			wrapped := proxy.Wrap(pgpool)
			nom := nom_mock.New(t)
			nom.Impl.DropData = func(_ context.Context, _ pool.Tx, knitIds []string) error {
				return when.NominatorDropDataError
			}

			runBefore := try.To(
				scanner.New[string]().QueryAll(
					ctx, conn,
					`select "run_id" from "run"`,
				),
			).OrFatal(t)

			dataBefore := try.To(
				scanner.New[string]().QueryAll(
					ctx, conn,
					`select "knit_id" from "data"`,
				),
			).OrFatal(t)

			testee := kpgrun.New(wrapped, kpgrun.WithNominator(nom))

			if err := testee.Delete(ctx, when.RunId); !errors.Is(err, when.NominatorDropDataError) {
				t.Errorf("unexpected error: %v", err)
			}
			{
				runAfter := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "run_id" from "run"`,
					),
				).OrFatal(t)

				if !cmp.SliceContentEq(runBefore, runAfter) {
					t.Errorf("unexpected runs: %v", runAfter)
				}
			}

			{
				dataAfter := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "knit_id" from "data"`,
					),
				).OrFatal(t)

				if !cmp.SliceContentEq(dataBefore, dataAfter) {
					t.Errorf("unexpected data: %v", dataAfter)
				}
			}
		}
	}

	t.Run("when nominator cause error during deleting gen1/done-leaf", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done-leaf"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done-leaf/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done-leaf"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-leaf-out-1",
						},
					},
				},
			},
		},
		When{
			RunId:                  th.Padding36("gen1/done-leaf"),
			NominatorDropDataError: errors.New("nominator cause error"),
		},
		Then{},
	))
}
