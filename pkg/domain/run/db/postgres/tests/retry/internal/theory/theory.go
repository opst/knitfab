package theory

import (
	"context"
	"errors"
	"slices"
	"testing"

	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	kpgnommock "github.com/opst/knitfab/pkg/domain/nomination/db/mock"
	kpgrun "github.com/opst/knitfab/pkg/domain/run/db/postgres"
	"github.com/opst/knitfab/pkg/utils/cmp"
	kslices "github.com/opst/knitfab/pkg/utils/slices"
	"github.com/opst/knitfab/pkg/utils/try"
)

type When struct {
	RunId           string
	DoNotLockTheRun bool
	NominatorErr    error
}
type Then struct {
	RemovedRunIds []string
	Err           error
}

func Theory(given tables.Operation, when When, then Then, pgpool kpool.Pool) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		testenv.ClearTablesWithConn(ctx, conn, t)
		if err := given.ApplyWithConn(ctx, conn); err != nil {
			t.Fatal(err)
		}

		var outputDataFromTheRun []string
		var outputVolumeRefFromTheRun []tables.VolumeRef
		{
			_outputVolumeRefFromTheRun := try.To(
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
			outputDataFromTheRun = kslices.Map(
				_outputVolumeRefFromTheRun,
				func(v tables.VolumeRef) string { return v.KnitId },
			)
			outputVolumeRefFromTheRun = kslices.Filter(
				_outputVolumeRefFromTheRun,
				func(v tables.VolumeRef) bool { return v.VolumeRef != "" },
			)
		}
		runExitAtFirst := try.To(
			scanner.New[tables.RunExit]().QueryAll(
				ctx, conn, `select * from "run_exit"`,
			),
		).OrFatal(t)

		knitIdsToBeUnnominated := try.To(
			scanner.New[string]().QueryAll(
				ctx, conn,
				`
				with "run" as (
					select "run_id" from "run" where "run_id" = $1 and "status" = $2
				)
				select "knit_id" from "data"
				where "run_id" in (select "run_id" from "run")
				`,
				when.RunId, domain.Done,
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
				t.Errorf("Runs are to be removed, but not: %v", remainedRunIds)
			}
		}
		{
			remainedData := try.To(
				scanner.New[string]().QueryAll(
					ctx, conn, `SELECT knit_id FROM "data" where "knit_id" = any($1)`,
					outputDataFromTheRun,
				),
			).OrFatal(t)
			if len(remainedData) != 0 {
				t.Errorf("Data are to be removed, but not: %v", remainedData)
			}
		}
		{
			remainedKnitIds := try.To(
				scanner.New[string]().QueryAll(
					ctx, conn, `SELECT knit_id FROM "knit_id" where "knit_id" = any($1)`,
					outputDataFromTheRun,
				),
			).OrFatal(t)

			if want := kslices.Map(
				outputVolumeRefFromTheRun,
				func(v tables.VolumeRef) string { return v.KnitId },
			); !cmp.SliceContentEq(remainedKnitIds, want) {
				t.Errorf("KnitIds are to be removed, but not: %v", remainedKnitIds)
			}
		}
		{
			garbage := try.To(
				scanner.New[tables.Garbage]().QueryAll(
					ctx, conn, `SELECT "knit_id", "volume_ref" FROM "garbage" where "knit_id" = any($1)`,
					outputDataFromTheRun,
				),
			).OrFatal(t)

			if !cmp.SliceContentEqWith(
				garbage, outputVolumeRefFromTheRun,
				func(a tables.Garbage, b tables.VolumeRef) bool {
					return a.KnitId == b.KnitId && a.VolumeRef == b.VolumeRef
				},
			) {
				t.Errorf("Garbage are to be added, but not: %v", garbage)
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
			if !cmp.SliceContentEq(
				unnominatedKnitIds, knitIdsToBeUnnominated,
			) {
				t.Errorf(
					"expected knit ids to be unnominated %v, got %v",
					knitIdsToBeUnnominated, unnominatedKnitIds,
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
