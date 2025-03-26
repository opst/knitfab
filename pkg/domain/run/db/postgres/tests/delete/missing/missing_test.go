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
	kerr "github.com/opst/knitfab/v2/pkg/domain/errors"
	"github.com/opst/knitfab/v2/pkg/domain/internal/db/postgres/tables"
	th "github.com/opst/knitfab/v2/pkg/domain/internal/db/postgres/testhelpers"
	nom_mock "github.com/opst/knitfab/v2/pkg/domain/nomination/db/mock"
	kpgrun "github.com/opst/knitfab/v2/pkg/domain/run/db/postgres"
	"github.com/opst/knitfab/v2/pkg/domain/run/db/postgres/tests/delete/internal/dataset"
	"github.com/opst/knitfab/v2/pkg/utils/cmp"
	"github.com/opst/knitfab/v2/pkg/utils/try"
)

func TestRun_Delete_Should_Be_Protected(t *testing.T) {

	type When struct {
		RunId string
	}
	type Then struct {
		ReasonNotDeleted   error
		RunsShouldBeLocked []string
	}

	ctx := context.Background()
	poolBroaker := testenv.NewPoolBroaker(ctx, t)
	theory := func(given tables.Operation, when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			pgpool := poolBroaker.GetPool(ctx, t)
			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			testenv.ClearTablesWithConn(ctx, conn, t)
			if err := dataset.GivenDatabase.ApplyWithConn(ctx, conn); err != nil {
				t.Fatal(err)
			}
			if err := given.ApplyWithConn(ctx, conn); err != nil {
				t.Fatal(err)
			}

			wrapped := proxy.Wrap(pgpool)
			wrapped.Events().Events().Query.After(func() {
				lockedRunIds := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`
						with "all" as (
							select "run_id" from "run"
						),
						"unlocked" as (
							select "run_id" from "run" for update skip locked
						)
						select "run_id" from "all"
						except
						select "run_id" from "unlocked"
						`,
					),
				).OrFatal(t)

				if !cmp.SliceContentEq(then.RunsShouldBeLocked, lockedRunIds) {
					t.Errorf("unexpected locked run ids: %v", lockedRunIds)
				}
			})

			nom := nom_mock.New(t)
			nom.Impl.DropData = func(_ context.Context, _ pool.Tx, knitIds []string) error {
				return nil
			}

			beforeRuns := try.To(
				scanner.New[string]().QueryAll(
					ctx, conn, `select "run_id" from "run"`,
				),
			).OrFatal(t)

			beforeData := try.To(
				scanner.New[string]().QueryAll(
					ctx, conn, `select "knit_id" from "data"`,
				),
			).OrFatal(t)

			testee := kpgrun.New(wrapped, kpgrun.WithNominator(nom))

			if err := testee.Delete(ctx, when.RunId); !errors.Is(err, then.ReasonNotDeleted) {
				t.Errorf("unexpected error: %v", err)
			}

			{
				afterRuns := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn, `select "run_id" from "run"`,
					),
				).OrFatal(t)

				if !cmp.SliceContentEq(beforeRuns, afterRuns) {
					t.Errorf("unexpected runs: %v", afterRuns)
				}
			}

			{
				afterData := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn, `select "knit_id" from "data"`,
					),
				).OrFatal(t)

				if !cmp.SliceContentEq(beforeData, afterData) {
					t.Errorf("unexpected data: %v", afterData)
				}
			}
		}
	}

	t.Run("non existing Run", theory(
		tables.Operation{},
		When{RunId: th.Padding36("no-such-run")},
		Then{
			ReasonNotDeleted: kerr.ErrMissing,
		},
	))

	t.Run("gen1/invalidated", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1"),
						Status:    domain.Invalidated,
						PlanId:    th.Padding36("plan-pseudo"),
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen1")},
		Then{
			ReasonNotDeleted: kerr.ErrMissing,
			RunsShouldBeLocked: []string{
				th.Padding36("gen1"),
			},
		},
	))

	t.Run("gen2/invalidated", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1"),
						Status:    domain.Done,
						PlanId:    th.Padding36("plan-pseudo"),
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2"),
						Status:    domain.Invalidated,
						PlanId:    th.Padding36("plan-gen2"),
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {
							VolumeRef: "pvc-gen2-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen2")},
		Then{
			ReasonNotDeleted: kerr.ErrMissing,
			RunsShouldBeLocked: []string{
				th.Padding36("gen2"),
			},
		},
	))

	t.Run("gen3/invalidated", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1"),
						Status:    domain.Done,
						PlanId:    th.Padding36("plan-pseudo"),
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2"),
						Status:    domain.Done,
						PlanId:    th.Padding36("plan-gen2"),
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {
							VolumeRef: "pvc-gen2-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen3"),
						Status:    domain.Invalidated,
						PlanId:    th.Padding36("plan-gen3"),
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen2/:out/1"),
							InputId: 3_100,
							RunId:   th.Padding36("gen3"),
							PlanId:  th.Padding36("plan-gen3"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen3/:out/1"),
							OutputId: 3_010,
							RunId:    th.Padding36("gen3"),
							PlanId:   th.Padding36("plan-gen3"),
						}: {
							VolumeRef: "pvc-gen3-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen3")},
		Then{
			ReasonNotDeleted: kerr.ErrMissing,
			RunsShouldBeLocked: []string{
				th.Padding36("gen3"),
			},
		},
	))

}
