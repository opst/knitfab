package tests_test

import (
	"context"
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
	"github.com/opst/knitfab/v2/pkg/utils/slices"
	"github.com/opst/knitfab/v2/pkg/utils/try"
)

func TestRun_Delete_Should_Be_Deleted(t *testing.T) {
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

			var outputVolumeRefs []tables.VolumeRef
			var outputDataPurged []string
			{

				_outputVolumeRefs, _outputWithoutVolumeRefs := slices.Group(
					try.To(
						scanner.New[tables.VolumeRef]().QueryAll(
							ctx, conn,
							`
							select "knit_id", coalesce("volume_ref", '') as "volume_ref" from "data"
							left join "volume_ref" using ("knit_id")
							where "run_id" = $1
							`,
							when.RunId,
						),
					).OrFatal(t),
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
				unlockedRunIds := try.To(
					// in this case, target run may be deleted.
					// so, we should check it is not-not locked.
					scanner.New[string]().QueryAll(
						ctx, conn,
						`
						select "run_id" from "run"
						where "run_id" = $1
						for update skip locked
						`,
						when.RunId,
					),
				).OrFatal(t)

				if len(unlockedRunIds) != 0 {
					t.Errorf("unexpected locked run ids: %v", unlockedRunIds)
				}
			})

			nom := nom_mock.New(t)
			nom.Impl.DropData = func(_ context.Context, _ pool.Tx, knitIds []string) error {
				return nil
			}

			testee := kpgrun.New(wrapped, kpgrun.WithNominator(nom))

			if err := testee.Delete(ctx, when.RunId); err != nil {
				t.Fatal(err)
			}

			{
				got := []string{}
				for _, dd := range nom.Calls.DropData {
					got = append(got, dd...)
				}

				if !cmp.SliceContentEq(then.WantNominatorDropData, got) {
					t.Errorf(
						"unexpected nominator drop data: want: %v, got %v",
						then.WantNominatorDropData, got,
					)
				}
			}

			{
				runIdsToBeDeleted := []string{when.RunId}
				runIdsToBeDeleted = append(runIdsToBeDeleted, then.RunIdsShouldBeDeletedAdditional...)
				got := try.To(
					scanner.New[tables.Run]().QueryAll(
						ctx, conn,
						`select * from "run" where "run_id" = any($1)`,
						runIdsToBeDeleted,
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
					t.Errorf("unexpected knit_id: %v", got)
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

	t.Run("gen1/deactivated", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/deactivated"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Deactivated,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/deactivated/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/deactivated"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-deactivated-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen1/deactivated")},
		Then{},
	))

	t.Run("gen1/waiting", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/waiting"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Waiting,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/waiting/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/waiting"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-waiting-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen1/waiting")},
		Then{},
	))

	t.Run("gen1/failed", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/failed"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Failed,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/failed/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/failed"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {
							VolumeRef: "pvc-gen1-failed-out-1",
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen1/failed")},
		Then{},
	))

	t.Run("gen1/failed (Data is purged)", theory(
		tables.Operation{
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen1/failed"),
						PlanId:    th.Padding36("plan-pseudo"),
						Status:    domain.Failed,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId:   th.Padding36("gen1/failed/:out/1"),
							OutputId: 1_010,
							RunId:    th.Padding36("gen1/failed"),
							PlanId:   th.Padding36("plan-pseudo"),
						}: {},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen1/failed")},
		Then{},
	))

	t.Run("gen1/done (no downstream)", theory(
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
		When{RunId: th.Padding36("gen1/done-leaf")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen1/done-leaf/:out/1"),
			},
		},
	))

	t.Run("gen1/done (no downstream, purged)", theory(
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
						}: {},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen1/done-leaf")},
		Then{},
	))

	t.Run("gen1/done (with invalidated downstream)", theory(
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
						RunId:     th.Padding36("gen2/invalidated"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Invalidated,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							RunId:   th.Padding36("gen2/invalidated"),
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen1/done")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen1/done/:out/1"),
			},
			RunIdsShouldBeDeletedAdditional: []string{
				th.Padding36("gen2/invalidated"),
			},
		},
	))

	t.Run("gen1/done (with invalidated downstream, output is purged)", theory(
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
						}: {},
					},
				},
				{
					Run: tables.Run{
						RunId:     th.Padding36("gen2/invalidated"),
						PlanId:    th.Padding36("plan-gen2"),
						Status:    domain.Invalidated,
						UpdatedAt: dataset.UPLOADED_AT,
					},
					Assign: []tables.Assign{
						{
							RunId:   th.Padding36("gen2/invalidated"),
							KnitId:  th.Padding36("gen1/done/:out/1"),
							InputId: 2_100,
							PlanId:  th.Padding36("plan-gen2"),
						},
					},
				},
			},
		},
		When{RunId: th.Padding36("gen1/done")},
		Then{
			WantNominatorDropData: []string{},
			RunIdsShouldBeDeletedAdditional: []string{
				th.Padding36("gen2/invalidated"),
			},
		},
	))
}
