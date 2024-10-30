package tests_test

import (
	"context"
	"errors"
	"testing"

	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	kpgrun "github.com/opst/knitfab/pkg/db/postgres/run"
	"github.com/opst/knitfab/pkg/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestDelete_Worker(t *testing.T) {
	run1Id := "pseudo-plan-1:run"
	run2Id := "image-plan-2:run"
	run3Id := "image-plan-3:run"
	worker2Id := "image-plan-2-worker"
	// init test data
	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: th.Padding36("pseudo-plan-1"), Active: true, Hash: th.Padding64("#pseudo-plan-1")},
			{PlanId: th.Padding36("image-plan-2"), Active: true, Hash: th.Padding64("#pseudo-plan-2")},
			{PlanId: th.Padding36("image-plan-3"), Active: true, Hash: th.Padding64("#pseudo-plan-3")},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: th.Padding36("pseudo-plan-1"), Name: "pseudo-plan-1"},
		},
		PlanImage: []tables.PlanImage{
			{PlanId: th.Padding36("image-plan-2"), Image: "repo.invalid/image", Version: "v1"},
			{PlanId: th.Padding36("image-plan-3"), Image: "repo.invalid/image", Version: "v1"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 1_010, PlanId: th.Padding36("pseudo-plan-1"), Path: "/out/1"}: {},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			{InputId: 2_100, PlanId: th.Padding36("image-plan-2"), Path: "/in/2"}: {},
			{InputId: 3_100, PlanId: th.Padding36("image-plan-3"), Path: "/in/3"}: {},
		},
		Steps: []tables.Step{
			// make Run-1
			{
				Run: tables.Run{
					RunId:  th.Padding36(run1Id),
					PlanId: th.Padding36("pseudo-plan-1"),
					Status: kdb.Done,
				},
				// NO worker
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("pseudo-plan-1:run:/out/1"),
						VolumeRef: "&pseudo-plan-1:run:/out/1",
						PlanId:    th.Padding36("pseudo-plan-1"),
						RunId:     th.Padding36(run1Id),
						OutputId:  1_010,
					}: {},
				},
			},
			// make Run-2
			{
				Run: tables.Run{
					RunId:  th.Padding36(run2Id),
					PlanId: th.Padding36("image-plan-2"),
					Status: kdb.Completing,
				},
				Worker: worker2Id,
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36(run2Id),
						PlanId:  th.Padding36("image-plan-2"),
						InputId: 2_100,
						KnitId:  th.Padding36("pseudo-plan-1:run:/out/1"),
					},
				},
			},
			// make Run-3
			{
				Run: tables.Run{
					RunId:  th.Padding36(run3Id),
					PlanId: th.Padding36("image-plan-3"),
					Status: kdb.Failed,
				},
				// No worker ( same as Run-1 )
				Worker: "",
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36(run3Id),
						PlanId:  th.Padding36("image-plan-3"),
						InputId: 3_100,
						KnitId:  th.Padding36("pseudo-plan-1:run:/out/1"),
					},
				},
			},
		},
	}

	// Try deleting the runId ...
	type When struct {
		runId string
	}

	// What kind of error will occur?
	// What state will the RDB records be in?
	type Then struct {
		expectedError     error
		registeredWorkers []tables.Worker
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pb := testenv.NewPoolBroaker(ctx, t)
			pgpool := pb.GetPool(ctx, t)
			pool := proxy.Wrap(pgpool)

			// instanciate test data
			if err := given.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			// test case
			testee_db := kpgrun.New(pool)
			err := testee_db.DeleteWorker(ctx, when.runId)
			if !errors.Is(err, then.expectedError) {
				t.Errorf(
					"returned error:\n===actual===\n%v\n===expected===\n%v",
					err, then.expectedError,
				)
			}

			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			actualWorkers := try.To(scanner.New[tables.Worker]().QueryAll(
				ctx, conn, `select * from "worker"`,
			)).OrFatal(t)

			if !cmp.SliceContentEq(actualWorkers, then.registeredWorkers) {
				t.Errorf(
					"returned error:\n===actual===\n%v\n===expected===\n%v",
					actualWorkers, then.registeredWorkers,
				)
			}
		}
	}

	t.Run("when DeleteWorker() with runId having worker, it deletes that's worker", theory(
		When{runId: th.Padding36(run2Id)},
		Then{
			expectedError:     nil,
			registeredWorkers: []tables.Worker{},
		},
	))

	t.Run("when DeleteWorker() with runId not having worker, it would delete nothing", theory(
		When{runId: th.Padding36(run1Id)},
		Then{
			expectedError: nil,
			registeredWorkers: []tables.Worker{
				{RunId: th.Padding36(run2Id), Name: worker2Id},
			},
		},
	))

	t.Run("when DeleteWorker() with runId not having worker, it would delete nothing", theory(
		When{runId: th.Padding36(run3Id)},
		Then{
			expectedError: nil,
			registeredWorkers: []tables.Worker{
				{RunId: th.Padding36(run2Id), Name: worker2Id},
			},
		},
	))

	t.Run("when DeleteWorker() with runId not exists, it would delete nothing", theory(
		When{runId: th.Padding36("no-run-id")},
		Then{
			expectedError: nil, // This is ignored, so no error will occur
			registeredWorkers: []tables.Worker{ // Only workers associated with run2Id
				{RunId: th.Padding36(run2Id), Name: worker2Id},
			},
		},
	))
}
