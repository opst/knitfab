package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/domain"
	kerr "github.com/opst/knitfab/pkg/domain/errors"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	kpgnommock "github.com/opst/knitfab/pkg/domain/nomination/db/mock"
	kpgrun "github.com/opst/knitfab/pkg/domain/run/db/postgres"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/slices"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestRetry(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	given := tables.Operation{
		Plan: []tables.Plan{
			{
				PlanId: th.Padding36("plan-1-pseudo"),
				Active: true, Hash: th.Padding36("#plan-1-pseudo"),
			},
			{
				PlanId: th.Padding36("plan-2-image"),
				Active: true, Hash: th.Padding36("#plan-2-image"),
			},
			{
				PlanId: th.Padding36("plan-3-image"),
				Active: true, Hash: th.Padding36("#plan-3-image"),
			},
		},
		PlanPseudo: []tables.PlanPseudo{
			{
				PlanId: th.Padding36("plan-1-pseudo"),
				Name:   "pseudo",
			},
		},
		PlanImage: []tables.PlanImage{
			{
				PlanId: th.Padding36("plan-2-image"),
				Image:  "image", Version: "v1.2",
			},
			{
				PlanId: th.Padding36("plan-3-image"),
				Image:  "image", Version: "v1.3",
			},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			{
				PlanId:  th.Padding36("plan-2-image"),
				InputId: 2_100, Path: "/in/1",
			}: {},
			{
				PlanId:  th.Padding36("plan-3-image"),
				InputId: 3_100, Path: "/in/1",
			}: {},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{
				PlanId:   th.Padding36("plan-1-pseudo"),
				OutputId: 1_010, Path: "/out/1",
			}: {},
			{
				PlanId:   th.Padding36("plan-2-image"),
				OutputId: 2_010, Path: "/out/1",
			}: {},
			{
				PlanId:   th.Padding36("plan-3-image"),
				OutputId: 3_010, Path: "/out/1",
			}: {},
		},
		Steps: []tables.Step{
			{
				Run: tables.Run{
					RunId:                 th.Padding36("plan-1-pseudo/run-deactivated"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                domain.Deactivated,
					UpdatedAt:             time.Now().Add(-time.Hour),
					LifecycleSuspendUntil: time.Now().Add(-time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/run-deactivated/out/1"),
						RunId:     th.Padding36("plan-1-pseudo/run-deactivated"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
						VolumeRef: "plan-1/run-dectivted/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("plan-1-pseudo/run-waiting"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                domain.Waiting,
					UpdatedAt:             time.Now().Add(-time.Hour),
					LifecycleSuspendUntil: time.Now().Add(-time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/run-waiting/out/1"),
						RunId:     th.Padding36("plan-1-pseudo/run-waiting"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
						VolumeRef: "plan-1/run-waiting/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("plan-1-pseudo/run-ready"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                domain.Ready,
					UpdatedAt:             time.Now().Add(-time.Hour),
					LifecycleSuspendUntil: time.Now().Add(-time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/run-read	/out/1"),
						RunId:     th.Padding36("plan-1-pseudo/run-ready"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
						VolumeRef: "plan-1/run-ready/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("plan-1-pseudo/run-starting"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                domain.Starting,
					UpdatedAt:             time.Now().Add(-time.Hour),
					LifecycleSuspendUntil: time.Now().Add(-time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/run-starting/out/1"),
						RunId:     th.Padding36("plan-1-pseudo/run-starting"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
						VolumeRef: "plan-1/run-starting/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("plan-1-pseudo/run-running"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                domain.Running,
					UpdatedAt:             time.Now().Add(-time.Hour),
					LifecycleSuspendUntil: time.Now().Add(-time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/run-running/out/1"),
						RunId:     th.Padding36("plan-1-pseudo/run-running"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
						VolumeRef: "plan-1/run-running/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("plan-1-pseudo/run-completing"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                domain.Completing,
					UpdatedAt:             time.Now().Add(-time.Hour),
					LifecycleSuspendUntil: time.Now().Add(-time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/run-completing/out/1"),
						RunId:     th.Padding36("plan-1-pseudo/run-completing"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
						VolumeRef: "plan-1/run-completing/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("plan-1-pseudo/run-aborting"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                domain.Aborting,
					UpdatedAt:             time.Now().Add(-time.Hour),
					LifecycleSuspendUntil: time.Now().Add(-time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/run-aborting/out/1"),
						RunId:     th.Padding36("plan-1-pseudo/run-aborting"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
						VolumeRef: "plan-1/run-aborting/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("plan-1-pseudo/run-done"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                domain.Done,
					UpdatedAt:             time.Now().Add(-time.Hour),
					LifecycleSuspendUntil: time.Now().Add(-time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/run-done/out/1"),
						RunId:     th.Padding36("plan-1-pseudo/run-done"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
						VolumeRef: "plan-1/run-done/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("plan-1-pseudo/run-done-leaf"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                domain.Done,
					UpdatedAt:             time.Now().Add(-time.Hour),
					LifecycleSuspendUntil: time.Now().Add(-time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/run-done-leaf/out/1"),
						RunId:     th.Padding36("plan-1-pseudo/run-done-leaf"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
						VolumeRef: "plan-1/run-done-leaf/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("plan-1-pseudo/run-failed"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                domain.Failed,
					UpdatedAt:             time.Now().Add(-time.Hour),
					LifecycleSuspendUntil: time.Now().Add(-time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/run-failed/out/1"),
						RunId:     th.Padding36("plan-1-pseudo/run-failed"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
						VolumeRef: "plan-1/run-failed/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("plan-1-pseudo/run-invalidated"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                domain.Invalidated,
					UpdatedAt:             time.Now().Add(-time.Hour),
					LifecycleSuspendUntil: time.Now().Add(-time.Hour),
				},
			},

			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2-image/run-deactivated"),
					PlanId:    th.Padding36("plan-2-image"),
					Status:    domain.Deactivated,
					UpdatedAt: time.Now().Add(-time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
						RunId:   th.Padding36("plan-2-image/run-deactivated"),
						PlanId:  th.Padding36("plan-2-image"),
						InputId: 2_100,
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-2-image/run-deactivated/out/1"),
						RunId:     th.Padding36("plan-2-image/run-deactivated"),
						PlanId:    th.Padding36("plan-2-image"),
						OutputId:  2_010,
						VolumeRef: "plan-2/run-deactivated/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2-image/run-waiting"),
					PlanId:    th.Padding36("plan-2-image"),
					Status:    domain.Waiting,
					UpdatedAt: time.Now().Add(-time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
						RunId:   th.Padding36("plan-2-image/run-waiting"),
						PlanId:  th.Padding36("plan-2-image"),
						InputId: 2_100,
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						VolumeRef: "plan-2/run-waiting/out/1",
						KnitId:    th.Padding36("plan-2-image/run-waiting/out/1"),
						RunId:     th.Padding36("plan-2-image/run-waiting"),
						PlanId:    th.Padding36("plan-2-image"),
						OutputId:  2_010,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2-image/run-ready"),
					PlanId:    th.Padding36("plan-2-image"),
					Status:    domain.Ready,
					UpdatedAt: time.Now().Add(-time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
						RunId:   th.Padding36("plan-2-image/run-ready"),
						PlanId:  th.Padding36("plan-2-image"),
						InputId: 2_100,
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-2-image/run-ready/out/1"),
						RunId:     th.Padding36("plan-2-image/run-ready"),
						PlanId:    th.Padding36("plan-2-image"),
						OutputId:  2_010,
						VolumeRef: "plan-2/run-ready/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2-image/run-starting"),
					PlanId:    th.Padding36("plan-2-image"),
					Status:    domain.Starting,
					UpdatedAt: time.Now().Add(-time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
						RunId:   th.Padding36("plan-2-image/run-starting"),
						PlanId:  th.Padding36("plan-2-image"),
						InputId: 2_100,
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-2-image/run-starting/out/1"),
						RunId:     th.Padding36("plan-2-image/run-starting"),
						PlanId:    th.Padding36("plan-2-image"),
						OutputId:  2_010,
						VolumeRef: "plan-2/run-starting/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2-image/run-running"),
					PlanId:    th.Padding36("plan-2-image"),
					Status:    domain.Running,
					UpdatedAt: time.Now().Add(-time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
						RunId:   th.Padding36("plan-2-image/run-running"),
						PlanId:  th.Padding36("plan-2-image"),
						InputId: 2_100,
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-2-image/run-running/out/1"),
						RunId:     th.Padding36("plan-2-image/run-running"),
						PlanId:    th.Padding36("plan-2-image"),
						OutputId:  2_010,
						VolumeRef: "plan-2/run-running/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2-image/run-completing"),
					PlanId:    th.Padding36("plan-2-image"),
					Status:    domain.Completing,
					UpdatedAt: time.Now().Add(-time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
						RunId:   th.Padding36("plan-2-image/run-completing"),
						PlanId:  th.Padding36("plan-2-image"),
						InputId: 2_100,
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-2-image/run-completing/out/1"),
						RunId:     th.Padding36("plan-2-image/run-completing"),
						PlanId:    th.Padding36("plan-2-image"),
						OutputId:  2_010,
						VolumeRef: "plan-2/run-completing/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2-image/run-aborting"),
					PlanId:    th.Padding36("plan-2-image"),
					Status:    domain.Aborting,
					UpdatedAt: time.Now().Add(-time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
						RunId:   th.Padding36("plan-2-image/run-aborting"),
						PlanId:  th.Padding36("plan-2-image"),
						InputId: 2_100,
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-2-image/run-aborting/out/1"),
						RunId:     th.Padding36("plan-2-image/run-aborting"),
						PlanId:    th.Padding36("plan-2-image"),
						OutputId:  2_010,
						VolumeRef: "plan-2/run-aborting/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2-image/run-done"),
					PlanId:    th.Padding36("plan-2-image"),
					Status:    domain.Done,
					UpdatedAt: time.Now().Add(-time.Hour),
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("plan-2-image/run-done"),
					ExitCode: 0,
					Message:  "done",
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
						RunId:   th.Padding36("plan-2-image/run-done"),
						PlanId:  th.Padding36("plan-2-image"),
						InputId: 2_100,
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:   th.Padding36("plan-2-image/run-done/out/1"),
						RunId:    th.Padding36("plan-2-image/run-done"),
						PlanId:   th.Padding36("plan-2-image"),
						OutputId: 2_010,
					}: {
						Timestamp: pointer.Ref(time.Now()),
						UserTag:   []domain.Tag{{Key: "key", Value: "value"}},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2-image/run-failed"),
					PlanId:    th.Padding36("plan-2-image"),
					Status:    domain.Failed,
					UpdatedAt: time.Now().Add(-time.Hour),
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("plan-2-image/run-failed"),
					ExitCode: 2,
					Message:  "Error",
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
						RunId:   th.Padding36("plan-2-image/run-failed"),
						PlanId:  th.Padding36("plan-2-image"),
						InputId: 2_100,
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-2-image/run-failed/out/1"),
						RunId:     th.Padding36("plan-2-image/run-failed"),
						PlanId:    th.Padding36("plan-2-image"),
						OutputId:  2_010,
						VolumeRef: "plan-2/run-failed/out/1",
					}: {
						Timestamp: pointer.Ref(time.Now()),
						UserTag:   []domain.Tag{{Key: "key", Value: "value"}},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2-image/run-invalidated"),
					PlanId:    th.Padding36("plan-2-image"),
					Status:    domain.Invalidated,
					UpdatedAt: time.Now().Add(-time.Hour),
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("plan-2-image/run-invalidated"),
					ExitCode: 0,
					Message:  "done",
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
						RunId:   th.Padding36("plan-2-image/run-invalidated"),
						PlanId:  th.Padding36("plan-2-image"),
						InputId: 2_100,
					},
				},
			},

			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-3-image/run-done-leaf"),
					PlanId:    th.Padding36("plan-3-image"),
					Status:    domain.Done,
					UpdatedAt: time.Now().Add(-time.Hour),
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("plan-3-image/run-done-leaf"),
					ExitCode: 0,
					Message:  "done",
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("plan-2-image/run-done/out/1"),
						RunId:   th.Padding36("plan-3-image/run-done-leaf"),
						PlanId:  th.Padding36("plan-3-image"),
						InputId: 3_100,
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-3-image/run-done-leaf/out/1"),
						RunId:     th.Padding36("plan-3-image/run-done-leaf"),
						PlanId:    th.Padding36("plan-3-image"),
						OutputId:  3_010,
						VolumeRef: "plan-3/run-done-leaf/out/1",
					}: {
						Timestamp: pointer.Ref(time.Now()),
						UserTag:   []domain.Tag{{Key: "key", Value: "value"}},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-3-image/run-done"),
					PlanId:    th.Padding36("plan-3-image"),
					Status:    domain.Done,
					UpdatedAt: time.Now().Add(-time.Hour),
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("plan-3-image/run-done"),
					ExitCode: 0,
					Message:  "done",
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("plan-2-image/run-done/out/1"),
						RunId:   th.Padding36("plan-3-image/run-done"),
						PlanId:  th.Padding36("plan-3-image"),
						InputId: 3_100,
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-3-image/run-done/out/1"),
						RunId:     th.Padding36("plan-3-image/run-done"),
						PlanId:    th.Padding36("plan-3-image"),
						OutputId:  3_010,
						VolumeRef: "plan-3/run-done/out/1",
					}: {
						Timestamp: pointer.Ref(time.Now()),
						UserTag:   []domain.Tag{{Key: "key", Value: "value"}},
					},
				},
			},

			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-3-image/run-invalidated"),
					PlanId:    th.Padding36("plan-3-image"),
					Status:    domain.Invalidated,
					UpdatedAt: time.Now().Add(-time.Hour),
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("plan-3-image/run-invalidated"),
					ExitCode: 0,
					Message:  "done",
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("plan-3-image/run-done/out/1"),
						RunId:   th.Padding36("plan-3-image/run-invalidated"),
						PlanId:  th.Padding36("plan-3-image"),
						InputId: 3_100,
					},
				},
			},
		},
	}
	type When struct {
		RunId           string
		DoNotLockTheRun bool
		NominatorErr    error
	}
	type Then struct {
		RemovedRunIds []string
		Err           error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pgpool := poolBroaker.GetPool(ctx, t)
			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			if err := given.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			outputDataFromTheRun := try.To(
				scanner.New[string]().QueryAll(
					ctx, conn,
					`select "knit_id" from "data" where "run_id" = $1`,
					when.RunId,
				),
			).OrFatal(t)

			runExitAtFirst := try.To(
				scanner.New[tables.RunExit]().QueryAll(
					ctx, conn,
					`select * from "run_exit"`,
				),
			).OrFatal(t)

			nomi := kpgnommock.New(t)
			nomi.Impl.DropData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
				return when.NominatorErr
			}
			wpool := proxy.Wrap(pgpool)

			wpool.Events().Query.After(func() {
				conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()

				runIdsTobeLocked := slices.Concat([]string{}, then.RemovedRunIds)
				if !when.DoNotLockTheRun {
					runIdsTobeLocked = append(runIdsTobeLocked, when.RunId)
				}
				lockedRun := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`
						with "unlocked" as (
							SELECT run_id FROM "run" for update skip locked
						)
						SELECT run_id FROM "run" except table "unlocked"
						`,
					),
				).OrFatal(t)
				if !cmp.SliceContentEq(lockedRun, runIdsTobeLocked) {
					t.Errorf(
						"locked run id:\nexpected: %v\ngot: %v",
						runIdsTobeLocked, lockedRun,
					)
				}

				lockedKnitIds := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`
						with "unlocked" as (
							SELECT "knit_id" FROM "data" for update skip locked
						)
						SELECT knit_id FROM "data" except table "unlocked"
						`,
					),
				).OrFatal(t)
				if !cmp.SliceContentEq(lockedKnitIds, outputDataFromTheRun) {
					t.Errorf(
						"locked knit id:\nexpected: %v\ngot: %v",
						outputDataFromTheRun, lockedKnitIds,
					)
				}
			})

			testee := kpgrun.New(wpool, kpgrun.WithNominator(nomi))

			before := try.To(th.PGNow(ctx, conn)).OrFatal(t)
			err := testee.Retry(ctx, when.RunId)
			after := try.To(th.PGNow(ctx, conn)).OrFatal(t)

			if !errors.Is(err, then.Err) {
				t.Fatalf("returned error:\n  expected: %v\n  got %v", then.Err, err)
			}
			if err != nil {
				runIds := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "run_id" from "run" where "run_id" = any($1)`,
						then.RemovedRunIds,
					),
				).OrFatal(t)
				if !cmp.SliceContentEq(runIds, then.RemovedRunIds) {
					t.Errorf(
						"downstream runs should not be removed, but missing: %v",
						runIds,
					)
				}
				runExitAtLater := try.To(
					scanner.New[tables.RunExit]().QueryAll(
						ctx, conn,
						`select * from "run_exit"`,
					),
				).OrFatal(t)

				if !cmp.SliceContentEq(runExitAtFirst, runExitAtLater) {
					t.Errorf("run_exit should not be changed, but got %v", runExitAtLater)
				}

				knitIds := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "knit_id" from "data" where "knit_id" = any($1)`,
						outputDataFromTheRun,
					),
				).OrFatal(t)
				if !cmp.SliceContentEq(knitIds, outputDataFromTheRun) {
					t.Errorf(
						"downstream data should not be removed, but missing: %v",
						knitIds,
					)
				}

				return
			}

			{
				remainedRunIds := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn, `SELECT run_id FROM "run" where "run_id" = any($1)`,
						then.RemovedRunIds,
					),
				).OrFatal(t)
				if len(remainedRunIds) != 0 {
					t.Errorf("run ids to be removed, but not: %v", remainedRunIds)
				}
			}
			{
				remainedKnitIds := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn, `SELECT knit_id FROM "data" where "knit_id" = any($1)`,
						outputDataFromTheRun,
					),
				).OrFatal(t)
				if len(remainedKnitIds) != 0 {
					t.Errorf("knit ids to be removed, but not: %v", remainedKnitIds)
				}
			}
			{
				newOutputs := try.To(
					scanner.New[int]().QueryAll(
						ctx, conn, `SELECT "output_id" FROM "data" where "run_id" = $1`,
						when.RunId,
					),
				).OrFatal(t)

				expected := try.To(
					scanner.New[int]().QueryAll(
						ctx, conn,
						`
						SELECT "output_id" FROM "output"
						inner join "run" using ("plan_id")
						where "run_id" = $1
						`,
						when.RunId,
					),
				).OrFatal(t)

				if !cmp.SliceContentEq(newOutputs, expected) {
					t.Errorf("expected outputs %v, got %v", expected, newOutputs)
				}
			}
			{
				worker := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`SELECT "name" FROM "worker" where "run_id" = $1`,
						when.RunId,
					),
				).OrFatal(t)
				if len(worker) != 1 {
					t.Error("expected worker is not found. got:", worker)
				}
			}
			{
				unnominatedKnitIds := slices.Concat(nomi.Calls.DropData...)
				if !cmp.SliceContentEq(unnominatedKnitIds, outputDataFromTheRun) {
					t.Errorf(
						"expected knit ids to be unnominated %v, got %v",
						outputDataFromTheRun, unnominatedKnitIds,
					)
				}
			}
			{
				actual := try.To(
					scanner.New[tables.RunExit]().QueryAll(
						ctx, conn, `SELECT * FROM "run_exit" where "run_id" = $1`,
						when.RunId,
					),
				).OrFatal(t)
				if len(actual) != 0 {
					t.Fatalf("expected no run_exit, got %v", actual)
				}
			}
			{
				var actual tables.Run
				{
					runs := try.To(
						scanner.New[tables.Run]().QueryAll(
							ctx, conn, `SELECT * FROM "run" where "run_id" = $1`,
							when.RunId,
						),
					).OrFatal(t)
					if len(runs) != 1 {
						t.Fatalf("expected 1 run, got %v", runs)
					}
					actual = runs[0]
				}

				if actual.Status != domain.Waiting {
					t.Errorf("expected status %v, got %v", domain.Waiting, actual.Status)
				}
				if actual.UpdatedAt.Before(before) {
					t.Errorf("expected started_at after %v, got %v", before, actual.UpdatedAt)
				}
				if actual.UpdatedAt.After(after) {
					t.Errorf("expected started_at before %v, got %v", after, actual.UpdatedAt)
				}
				if !actual.UpdatedAt.Equal(actual.LifecycleSuspendUntil) {
					t.Errorf("expected lifecycle_suspend_until equal to updated_at, got %v", actual.LifecycleSuspendUntil)
				}
			}
		}
	}

	t.Run("not finished run can not be retried [pseudo, deactivated]", theory(
		When{RunId: th.Padding36("plan-1-pseudo/run-deactivated")},
		Then{Err: domain.ErrRunIsProtected},
	))
	t.Run("not finished run can not be retried [image, deactivated]", theory(
		When{RunId: th.Padding36("plan-2-image/run-deactivated")},
		Then{Err: domain.ErrInvalidRunStateChanging},
	))
	t.Run("not finished run can not be retried [pseudo, waiting]", theory(
		When{RunId: th.Padding36("plan-1-pseudo/run-waiting")},
		Then{Err: domain.ErrRunIsProtected},
	))
	t.Run("not finished run can not be retried [image, waiting]", theory(
		When{RunId: th.Padding36("plan-2-image/run-waiting")},
		Then{Err: domain.ErrInvalidRunStateChanging},
	))
	t.Run("not finished run can not be retried [pseudo, ready]", theory(
		When{RunId: th.Padding36("plan-1-pseudo/run-ready")},
		Then{Err: domain.ErrRunIsProtected},
	))
	t.Run("not finished run can not be retried [image, ready]", theory(
		When{RunId: th.Padding36("plan-2-image/run-ready")},
		Then{Err: domain.ErrInvalidRunStateChanging},
	))
	t.Run("not finished run can not be retried [pseudo, starting]", theory(
		When{RunId: th.Padding36("plan-1-pseudo/run-starting")},
		Then{Err: domain.ErrRunIsProtected},
	))
	t.Run("not finished run can not be retried [image, starting]", theory(
		When{RunId: th.Padding36("plan-2-image/run-starting")},
		Then{Err: domain.ErrInvalidRunStateChanging},
	))
	t.Run("not finished run can not be retried [pseudo, running]", theory(
		When{RunId: th.Padding36("plan-1-pseudo/run-running")},
		Then{Err: domain.ErrRunIsProtected},
	))
	t.Run("not finished run can not be retried [image, running]", theory(
		When{RunId: th.Padding36("plan-2-image/run-running")},
		Then{Err: domain.ErrInvalidRunStateChanging},
	))
	t.Run("not finished run can not be retried [pseudo, completing]", theory(
		When{RunId: th.Padding36("plan-1-pseudo/run-completing")},
		Then{Err: domain.ErrRunIsProtected},
	))
	t.Run("not finished run can not be retried [image, completing]", theory(
		When{RunId: th.Padding36("plan-2-image/run-completing")},
		Then{Err: domain.ErrInvalidRunStateChanging},
	))
	t.Run("not finished run can not be retried [pseudo, aborting]", theory(
		When{RunId: th.Padding36("plan-1-pseudo/run-aborting")},
		Then{Err: domain.ErrRunIsProtected},
	))
	t.Run("not finished run can not be retried [image, aborting]", theory(
		When{RunId: th.Padding36("plan-2-image/run-aborting")},
		Then{Err: domain.ErrInvalidRunStateChanging},
	))
	t.Run("not finished run can not be retried [pseudo, invalidated]", theory(
		When{RunId: th.Padding36("plan-1-pseudo/run-invalidated")},
		Then{Err: domain.ErrRunIsProtected},
	))

	t.Run("pseudo run can not be retried [failed]", theory(
		When{RunId: th.Padding36("plan-1-pseudo/run-failed")},
		Then{Err: domain.ErrRunIsProtected},
	))
	t.Run("pseudo run can not be retried [done]", theory(
		When{RunId: th.Padding36("plan-1-pseudo/run-done-leaf")},
		Then{Err: domain.ErrRunIsProtected},
	))
	t.Run("finished run with downstream can not be retried", theory(
		When{RunId: th.Padding36("plan-2-image/run-done")},
		Then{Err: domain.ErrRunHasDownstreams},
	))

	t.Run("finished run without downstream can be retried [done]", theory(
		When{RunId: th.Padding36("plan-3-image/run-done-leaf")},
		Then{},
	))
	t.Run("finished run with only invalidated downstream can be retried [done]", theory(
		When{RunId: th.Padding36("plan-3-image/run-done")},
		Then{
			RemovedRunIds: []string{th.Padding36("plan-3-image/run-invalidated")},
		},
	))

	t.Run("finished run without downstream can be retried [failed]", theory(
		When{RunId: th.Padding36("plan-2-image/run-failed")},
		Then{},
	))

	t.Run("retrying not existing run should be failed", theory(
		When{RunId: th.Padding36("no-such-run"), DoNotLockTheRun: true},
		Then{Err: kerr.ErrMissing},
	))

	t.Run("retrying invalidated run should be failed", theory(
		When{RunId: th.Padding36("plan-3-image/run-invalidated")},
		Then{Err: kerr.ErrMissing},
	))

	err := errors.New("fake error")
	t.Run("if nominator.DropData returns error, it retuns that error", theory(
		When{
			RunId:        th.Padding36("plan-3-image/run-done-leaf"),
			NominatorErr: err,
		},
		Then{Err: err},
	))
}
