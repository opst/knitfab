package tests_test

import (
	"context"
	"errors"
	"testing"
	"time"

	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	kpg_run "github.com/opst/knitfab/pkg/db/postgres/run"
	"github.com/opst/knitfab/pkg/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestRunSetExit(t *testing.T) {

	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	given := tables.Operation{
		Plan: []tables.Plan{
			{
				PlanId: th.Padding36("plan-1-pseudo"),
				Active: true, Hash: th.Padding36("#plan-1-pseudo"),
			},
		},
		PlanPseudo: []tables.PlanPseudo{
			{
				PlanId: th.Padding36("plan-1-pseudo"),
				Name:   "pseudo",
			},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{
				PlanId:   th.Padding36("plan-1-pseudo"),
				OutputId: 1_010, Path: "/out/1",
			}: {},
		},
		Steps: []tables.Step{
			{
				Run: tables.Run{
					RunId:                 th.Padding36("plan-1-pseudo/run-running"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                kdb.Running,
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
					RunId:                 th.Padding36("plan-1-pseudo/run-done"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                kdb.Done,
					UpdatedAt:             time.Now().Add(-time.Hour),
					LifecycleSuspendUntil: time.Now().Add(-time.Hour),
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("plan-1-pseudo/run-done"),
					ExitCode: 0,
					Message:  "done",
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/run-done/out/1"),
						RunId:     th.Padding36("plan-1-pseudo/run-done"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
						VolumeRef: "plan-1/run-don1/out/1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("plan-1-pseudo/run-failed"),
					PlanId:                th.Padding36("plan-1-pseudo"),
					Status:                kdb.Failed,
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
		},
	}

	type When struct {
		runId string
		exit  kdb.RunExit
	}
	type Then struct {
		wantError error
		exits     []tables.RunExit
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pool := poolBroaker.GetPool(ctx, t)

			// Given
			if err := given.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			testee := kpg_run.New(pool)

			// When
			err := testee.SetExit(ctx, when.runId, when.exit)
			// Then
			if err != nil {
				if !errors.Is(err, then.wantError) {
					t.Errorf("got error %v, want nil", err)
				}
				return
			}

			if then.wantError != nil {
				t.Errorf("got nil, want error %v", then.wantError)
				return
			}

			conn := try.To(pool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			got := try.To(scanner.New[tables.RunExit]().QueryAll(
				ctx, conn, `table "run_exit"`,
			)).OrFatal(t)

			if !cmp.SliceContentEq(got, then.exits) {
				t.Errorf("got %v, want %v", got, then.exits)
			}
		}
	}

	t.Run("set new RunExit", theory(
		When{
			runId: th.Padding36("plan-1-pseudo/run-failed"),
			exit: kdb.RunExit{
				Code:    1,
				Message: "failed",
			},
		},
		Then{
			exits: []tables.RunExit{
				{
					RunId:    th.Padding36("plan-1-pseudo/run-done"),
					ExitCode: 0,
					Message:  "done",
				},
				{
					RunId:    th.Padding36("plan-1-pseudo/run-failed"),
					ExitCode: 1,
					Message:  "failed",
				},
			},
		},
	))

	t.Run("update RunExit", theory(
		When{
			runId: th.Padding36("plan-1-pseudo/run-done"),
			exit: kdb.RunExit{
				Code:    2,
				Message: "done",
			},
		},
		Then{
			exits: []tables.RunExit{
				{
					RunId:    th.Padding36("plan-1-pseudo/run-done"),
					ExitCode: 2,
					Message:  "done",
				},
			},
		},
	))

}
