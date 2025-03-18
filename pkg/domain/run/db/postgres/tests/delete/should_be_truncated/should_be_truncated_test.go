package tests_test

import (
	"context"
	"testing"

	"github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	nom_mock "github.com/opst/knitfab/pkg/domain/nomination/db/mock"
	kpgrun "github.com/opst/knitfab/pkg/domain/run/db/postgres"
	"github.com/opst/knitfab/pkg/domain/run/db/postgres/tests/delete/internal/dataset"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/slices"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestRun_Delete_Should_Be_Truncated(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	type When struct {
		RunId string
	}
	type Then struct {
		WantNominatorDropData           []string
		RunIdsShouldBeDeletedAdditional []string
		ReasonNotDeleted                error
	}

	theory := func(given tables.Operation, when When, then Then) func(*testing.T) {
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

			var outputKnitIds []string
			var outputVolumeRefs []tables.VolumeRef
			var outputDataPurged []string
			{
				_output := try.To(
					scanner.New[tables.VolumeRef]().QueryAll(
						ctx, conn,
						`
						select "knit_id", coalesce("volume_ref", '') as "volume_ref" from "data"
						left join "volume_ref" using ("knit_id")
						where "run_id" = $1
						`,
						when.RunId,
					),
				).OrFatal(t)

				outputKnitIds = slices.Map(
					_output,
					func(vr tables.VolumeRef) string { return vr.KnitId },
				)

				_outputVolumeRefs, _outputWithoutVolumeRefs := slices.Group(
					_output,
					func(vr tables.VolumeRef) bool {
						return vr.VolumeRef != ""
					},
				)

				outputVolumeRefs = _outputVolumeRefs
				outputDataPurged = slices.Map(
					_outputWithoutVolumeRefs,
					func(vr tables.VolumeRef) string { return vr.KnitId },
				)
			}

			wrapped := proxy.Wrap(pgpool)
			wrapped.Events().Events().Query.After(func() {
				lockedRunIds := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`
						with "unlocked" as (
							select "run_id" from "run" for update skip locked
						)
						select "run_id" from "run"
						except
						select "run_id" from "unlocked"
						`,
					),
				).OrFatal(t)

				toBeLocked := []string{when.RunId}
				toBeLocked = append(toBeLocked, then.RunIdsShouldBeDeletedAdditional...)
				if !cmp.SliceContentEq(toBeLocked, lockedRunIds) {
					t.Errorf("unexpected locked run ids: want %v, got: %v", toBeLocked, lockedRunIds)
				}

				lockedData := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`
						with "unlocked" as (
							select "knit_id" from "data"
							where "run_id" = $1
							for update skip locked
						),
						"all" as (
							select "knit_id" from "data" where "run_id" = $1
						)
						select "knit_id" from "all"
						except
						select "knit_id" from "unlocked"
						`,
						when.RunId,
					),
				).OrFatal(t)
				if !cmp.SliceContentEq(outputKnitIds, lockedData) {
					t.Errorf("unexpected locked data: want %v, got: %v", outputKnitIds, lockedData)
				}
			})

			nom := nom_mock.New(t)
			nom.Impl.DropData = func(_ context.Context, _ pool.Tx, knitIds []string) error {
				return nil
			}

			testee := kpgrun.New(wrapped, kpgrun.WithNominator(nom))

			before := try.To(th.PGNow(ctx, conn)).OrFatal(t)
			if err := testee.Delete(ctx, when.RunId); err != nil {
				t.Fatal(err)
			}
			after := try.To(th.PGNow(ctx, conn)).OrFatal(t)

			{
				got := []string{}
				for _, dd := range nom.Calls.DropData {
					got = append(got, dd...)
				}

				if !cmp.SliceContentEq(then.WantNominatorDropData, got) {
					t.Errorf("unexpected nominator drop data: %v", got)
				}
			}

			{
				got := try.To(
					scanner.New[tables.Run]().QueryAll(
						ctx, conn,
						`select * from "run" where "run_id" = $1`,
						when.RunId,
					),
				).OrFatal(t)
				if len(got) != 1 {
					t.Errorf("unexpected run: %v", got)
				} else {
					g := got[0]
					if g.Status != domain.Invalidated {
						t.Errorf("unexpected run status: %v", g.Status)
					}

					if g.UpdatedAt.Before(before) || g.UpdatedAt.After(after) {
						t.Errorf(
							"unexpected updated at: %v (should between %s and %s)",
							g.UpdatedAt, before, after,
						)
					}
				}
			}

			{
				got := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "knit_id" from "data" where "run_id" = $1`,
						when.RunId,
					),
				).OrFatal(t)
				if len(got) != 0 {
					t.Errorf("unexpected data: %v", got)
				}
			}

			{
				got := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "run_id" from "run_exit" where "run_id" = $1`,
						when.RunId,
					),
				).OrFatal(t)
				if len(got) != 0 {
					t.Errorf("unexpected run exit: %v", got)
				}
			}

			{
				got := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "run_id" from "run" where "run_id" = any($1)`,
						then.RunIdsShouldBeDeletedAdditional,
					),
				).OrFatal(t)
				if len(got) != 0 {
					t.Errorf("unexpected run: %v", got)
				}
			}

			{
				got := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "knit_id" from "knit_id" where "knit_id" = any($1)`,
						outputDataPurged,
					),
				).OrFatal(t)
				if len(got) != 0 {
					t.Errorf("unexpected data: %v", got)
				}
			}

			{
				got := try.To(
					scanner.New[tables.Garbage]().QueryAll(
						ctx, conn,
						`select * from "garbage" where "knit_id" = any($1)`,
						slices.Map(
							outputVolumeRefs,
							func(vr tables.VolumeRef) string { return vr.KnitId },
						),
					),
				).OrFatal(t)
				if !cmp.SliceContentEqWith(
					outputVolumeRefs, got,
					func(a tables.VolumeRef, b tables.Garbage) bool {
						return a.KnitId == b.KnitId && a.VolumeRef == b.VolumeRef
					},
				) {
					t.Errorf("unexpected garbage: want: %v, got: %v", outputVolumeRefs, got)
				}
			}
		}
	}

	t.Run("gen2/deactivated", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/deactivated"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Deactivated,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/deactivated"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/deactivated/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/deactivated"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {
							VolumeRef: "pvc-gen2-deactivated-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen2/deactivated")},
		Then{ReasonNotDeleted: domain.ErrRunIsProtected},
	))

	t.Run("gen2/waiting", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/waiting"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Waiting,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/waiting"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/waiting/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/waiting"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {
							VolumeRef: "pvc-gen2-waiting-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen2/waiting")},
		Then{ReasonNotDeleted: domain.ErrRunIsProtected},
	))

	t.Run("gen2/failed", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/failed"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Failed,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/failed"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/failed/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/failed"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {
							VolumeRef: "pvc-gen2-failed-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen2/failed")},
		Then{},
	))

	t.Run("gen2/failed (Output is purged)", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/failed"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Failed,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/failed"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/failed/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/failed"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen2/failed")},
		Then{},
	))

	t.Run("gen2/done (no downstreams)", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/done-leaf"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/done-leaf"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/done-leaf/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/done-leaf"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {
							VolumeRef: "pvc-gen2-done-leaf-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen2/done-leaf")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen2/done-leaf/:out/1"),
			},
		},
	))

	t.Run("gen2/done (no downstreams, output is purged)", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/done-leaf"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/done-leaf"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/done-leaf/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/done-leaf"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen2/done-leaf")},
		Then{},
	))

	t.Run("gen2/done (with invalidated downstream)", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/done"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/done"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/done/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/done"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {
							VolumeRef: "pvc-gen2-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen3/invalidated"),
						PlanId:    th.Padding36("plan-gen3"),
						Status:    domain.Invalidated,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen2/done/:out/1"),
							InputId: 3_100,
							RunId:   th.Padding36("gen3/invalidated"),
							PlanId:  th.Padding36("plan-gen3"),
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen2/done")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen2/done/:out/1"),
			},
			RunIdsShouldBeDeletedAdditional: []string{
				th.Padding36("gen3/invalidated"),
			},
		},
	))

	t.Run("gen2/done (with invalidated downstream, output is purged)", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/done"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/done"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/done/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/done"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen3/invalidated"),
						PlanId:    th.Padding36("plan-gen3"),
						Status:    domain.Invalidated,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen2/done/:out/1"),
							InputId: 3_100,
							RunId:   th.Padding36("gen3/invalidated"),
							PlanId:  th.Padding36("plan-gen3"),
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen2/done")},
		Then{
			RunIdsShouldBeDeletedAdditional: []string{
				th.Padding36("gen3/invalidated"),
			},
		},
	))

	t.Run("gen3/deactivated", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/done"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/done"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/done/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/done"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {
							VolumeRef: "pvc-gen2-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen3/deactivated"),
						PlanId:    th.Padding36("plan-gen3"),
						Status:    domain.Deactivated,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen2/done/:out/1"),
							InputId: 3_100,
							RunId:   th.Padding36("gen3/deactivated"),
							PlanId:  th.Padding36("plan-gen3"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen3/deactivated/:out/1"),
							OutputId: 3_010,
							RunId:    th.Padding36("gen3/deactivated"),
							PlanId:   th.Padding36("plan-gen3"),
						}: {
							VolumeRef: "pvc-gen3-deactivated-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen3/deactivated")},
		Then{ReasonNotDeleted: domain.ErrRunIsProtected},
	))

	t.Run("gen3/waiting", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/done"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/done"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/done/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/done"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {
							VolumeRef: "pvc-gen2-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen3/waiting"),
						PlanId:    th.Padding36("plan-gen3"),
						Status:    domain.Waiting,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen2/done/:out/1"),
							InputId: 3_100,
							RunId:   th.Padding36("gen3/waiting"),
							PlanId:  th.Padding36("plan-gen3"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen3/waiting/:out/1"),
							OutputId: 3_010,
							RunId:    th.Padding36("gen3/waiting"),
							PlanId:   th.Padding36("plan-gen3"),
						}: {
							VolumeRef: "pvc-gen3-waiting-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen3/waiting")},
		Then{ReasonNotDeleted: domain.ErrRunIsProtected},
	))

	t.Run("gen3/failed", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/done"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/done"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/done/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/done"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {
							VolumeRef: "pvc-gen2-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen3/failed"),
						PlanId:    th.Padding36("plan-gen3"),
						Status:    domain.Failed,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen2/done/:out/1"),
							InputId: 3_100,
							RunId:   th.Padding36("gen3/failed"),
							PlanId:  th.Padding36("plan-gen3"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen3/failed/:out/1"),
							OutputId: 3_010,
							RunId:    th.Padding36("gen3/failed"),
							PlanId:   th.Padding36("plan-gen3"),
						}: {
							VolumeRef: "pvc-gen3-failed-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen3/failed")},
		Then{},
	))

	t.Run("gen3/failed (Output is purged)", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/done"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/done"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/done/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/done"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {
							VolumeRef: "pvc-gen2-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen3/failed"),
						PlanId:    th.Padding36("plan-gen3"),
						Status:    domain.Failed,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen2/done/:out/1"),
							InputId: 3_100,
							RunId:   th.Padding36("gen3/failed"),
							PlanId:  th.Padding36("plan-gen3"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen3/failed/:out/1"),
							OutputId: 3_010,
							RunId:    th.Padding36("gen3/failed"),
							PlanId:   th.Padding36("plan-gen3"),
						}: {},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen3/failed")},
		Then{},
	))

	t.Run("gen3/done (no downstreams)", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/done"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/done"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/done/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/done"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {
							VolumeRef: "pvc-gen2-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen3/done"),
						PlanId:    th.Padding36("plan-gen3"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen2/done/:out/1"),
							InputId: 3_100,
							RunId:   th.Padding36("gen3/done"),
							PlanId:  th.Padding36("plan-gen3"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen3/done/:out/1"),
							OutputId: 3_010,
							RunId:    th.Padding36("gen3/done"),
							PlanId:   th.Padding36("plan-gen3"),
						}: {
							VolumeRef: "pvc-gen3-done-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen3/done")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen3/done/:out/1"),
			},
		},
	))

	t.Run("gen3/done (no downstreams, output is purged)", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/done"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/done/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/done"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/done"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							RunId:   th.Padding36("gen2/done"),
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen2/done/:out/1"),
							OutputId: 2_010,
							RunId:    th.Padding36("gen2/done"),
							PlanId:   th.Padding36("plan-gen2"),
						}: {
							VolumeRef: "pvc-gen2-done-out-1",
						},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen3/done"),
						PlanId:    th.Padding36("plan-gen3"),
						Status:    domain.Done,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							KnitId:  th.Padding36("gen2/done/:out/1"),
							InputId: 3_100,
							RunId:   th.Padding36("gen3/done"),
							PlanId:  th.Padding36("plan-gen3"),
						},
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen3/done/:out/1"),
							OutputId: 3_010,
							RunId:    th.Padding36("gen3/done"),
							PlanId:   th.Padding36("plan-gen3"),
						}: {},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen3/done")},
		Then{},
	))
}
