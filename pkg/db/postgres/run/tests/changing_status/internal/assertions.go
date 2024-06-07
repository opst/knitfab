package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	kdb "github.com/opst/knitfab/pkg/db"
	kpgnommock "github.com/opst/knitfab/pkg/db/postgres/nominator/mock"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
	"github.com/opst/knitfab/pkg/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	kpgrun "github.com/opst/knitfab/pkg/db/postgres/run"
	"github.com/opst/knitfab/pkg/db/postgres/tables/matcher"
	th "github.com/opst/knitfab/pkg/db/postgres/testhelpers"

	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/try"
)

type When struct {
	Target kdb.Run
	Cursor kdb.RunCursor
}

type Then struct {
	NewStatus         kdb.KnitRunStatus
	RunIdsToBeLocked  []string
	KnitIdsToBeLocked []string
}
type Assertion func(
	context.Context, *testing.T, testenv.PoolBroaker,
	[]tables.Operation, When, Then,
)

func shouldLock(
	ctx context.Context, t *testing.T, pool *proxy.Pool, then Then,
) {
	t.Helper()
	pool.Events().Query.After(func() {
		conn := try.To(pool.Base.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		lockedRunIds := try.To(scanner.New[string]().QueryAll(
			ctx, conn,
			`
			with "unlocked" as (
				select "run_id" from "run"
				order by "run_id"
				for update skip locked
			)
			select "run_id" from "run"
			except
			select "run_id" from "unlocked"
			`,
		)).OrFatal(t)
		if !cmp.SliceContentEq(lockedRunIds, then.RunIdsToBeLocked) {
			t.Errorf(
				"unmatch locked run id\n===actual===\n%+v\n===expected===\n%+v",
				lockedRunIds, then.RunIdsToBeLocked,
			)
		}

		lockedKnitIds := try.To(scanner.New[string]().QueryAll(
			ctx, conn,
			`
			with "unlocked" as (
				select "knit_id" from "data"
				order by "knit_id"
				for update skip locked
			)
			select "knit_id" from "data"
			except
			select "knit_id" from "unlocked"
			`,
		)).OrFatal(t)
		if !cmp.SliceContentEq(lockedKnitIds, then.KnitIdsToBeLocked) {
			t.Errorf(
				"unmatch locked knit id\n===actual===\n%+v\n===expected===\n%+v",
				lockedKnitIds, then.KnitIdsToBeLocked,
			)
		}
	})
}

func CanBeChanged(
	ctx context.Context, t *testing.T, poolBroaker testenv.PoolBroaker,
	given []tables.Operation, when When, then Then,
) {
	t.Helper()
	t.Run(fmt.Sprintf("status should be changed %s -> %s [SetStatus]", when.Target.Status, then.NewStatus), func(t *testing.T) {
		pgpool := poolBroaker.GetPool(ctx, t)
		for _, op := range given {
			op.Apply(ctx, pgpool)
		}
		wpool := proxy.Wrap(pgpool)
		shouldLock(ctx, t, wpool, then)
		nomi := kpgnommock.New(t)
		nomi.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
			return nil
		}

		testee := kpgrun.New(wpool, kpgrun.WithNominator(nomi))

		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		runBeforeChanged := try.To(scanner.New[tables.Run]().QueryAll(
			ctx, conn, `select * from "run" where "run_id" = $1`,
			when.Target.RunBody.Id,
		)).OrFatal(t)
		if len(runBeforeChanged) == 0 {
			t.Fatal("target run is tnot found in DB!")
		}

		before := try.To(th.PGNow(ctx, conn)).OrFatal(t)
		err := testee.SetStatus(ctx, when.Target.RunBody.Id, then.NewStatus)
		if err != nil {
			t.Fatal(err)
		}
		after := try.To(th.PGNow(ctx, conn)).OrFatal(t)

		runAfterChanged := try.To(scanner.New[tables.Run]().QueryAll(
			ctx, conn, `select * from "run" where "run_id" = $1`,
			when.Target.RunBody.Id,
		)).OrFatal(t)
		if len(runAfterChanged) == 0 {
			t.Fatal("changed run is not found in DB!")
		}

		{
			actual, base := runAfterChanged[0], runBeforeChanged[0]
			var expected matcher.Run
			if base.Status == then.NewStatus {
				expected = matcher.Run{
					RunId:                 matcher.EqEq(base.RunId),
					PlanId:                matcher.EqEq(base.PlanId),
					Status:                matcher.EqEq(base.Status),
					LifecycleSuspendUntil: matcher.Between(before, after),
					UpdatedAt:             matcher.Equal(base.UpdatedAt),
				}
			} else {
				expected = matcher.Run{
					RunId:                 matcher.EqEq(base.RunId),
					PlanId:                matcher.EqEq(base.PlanId),
					Status:                matcher.EqEq(then.NewStatus),
					LifecycleSuspendUntil: matcher.Between(before, after),
					UpdatedAt:             matcher.Between(before, after),
				}
			}
			if !expected.Match(actual) {
				t.Errorf(
					"run record:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		}

		outputData := utils.Map(
			when.Target.Outputs,
			func(a kdb.Assignment) string { return a.KnitDataBody.KnitId },
		)

		if when.Target.Log != nil && when.Target.Log.KnitDataBody.KnitId != "" {
			outputData = append(outputData, when.Target.Log.KnitDataBody.KnitId)
		}

		toBeNominated := []string{}
		if then.NewStatus == kdb.Done {
			toBeNominated = outputData
		}
		toBeTimestamped := []string{}
		if then.NewStatus == kdb.Done || then.NewStatus == kdb.Failed {
			toBeTimestamped = outputData
		}
		if !cmp.SliceContentEq(
			utils.Concat(nomi.Calls.NominateData...),
			toBeNominated,
		) {
			t.Errorf(
				"nominated data:\n===actual===\n%+v\n===expected===\n%+v",
				utils.Concat(nomi.Calls.NominateData...),
				toBeNominated,
			)
		}

		ret := try.To(
			scanner.New[tables.DataTimeStamp]().QueryAll(
				ctx, conn,
				`
				select
					"knit_id", "timestamp"
				from "knit_timestamp"
				where "knit_id" = any($1)
				`,
				outputData,
			),
		).OrFatal(t)

		knitIdsWhichHaveTimestamp := utils.Map(
			ret,
			func(r tables.DataTimeStamp) string { return r.KnitId },
		)
		if !cmp.SliceContentEq(knitIdsWhichHaveTimestamp, toBeTimestamped) {
			t.Errorf(
				"unexpectedly timestamped:\n===actual===\n%+v\n===expected===\n%+v",
				knitIdsWhichHaveTimestamp, toBeTimestamped,
			)
		}
		_, ng := utils.Group(
			ret,
			func(r tables.DataTimeStamp) bool {
				return !r.Timestamp.Before(before) && !r.Timestamp.After(after)
			},
		)
		if len(ng) != 0 {
			t.Errorf(
				"unexpectedly timestamped:\n===actual===\n%+v\n===expected===\nbetween %s and %s",
				ret, before, after,
			)
		}
	})

	t.Run(fmt.Sprintf("status should be changed %s -> %s [PickAndSetStatus]", when.Target.Status, then.NewStatus), func(t *testing.T) {
		pgpool := poolBroaker.GetPool(ctx, t)
		for _, op := range given {
			op.Apply(ctx, pgpool)
		}
		wpool := proxy.Wrap(pgpool)
		nomi := kpgnommock.New(t)
		nomi.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
			return nil
		}
		testee := kpgrun.New(wpool, kpgrun.WithNominator(nomi))
		shouldLock(ctx, t, wpool, then)

		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		runBeforeChanged := try.To(scanner.New[tables.Run]().QueryAll(
			ctx, conn, `select * from "run" where "run_id" = $1`,
			when.Target.RunBody.Id,
		)).OrFatal(t)
		if len(runBeforeChanged) == 0 {
			t.Fatalf("target run is not found in DB!")
		}

		if runBeforeChanged[0].Status != when.Target.Status {
			t.Fatalf("bad assumption: statuses of targe run and run in table are not same")
		}

		before := try.To(th.PGNow(ctx, conn)).OrFatal(t)
		called := false
		nextCursor, statusChanged, err := testee.PickAndSetStatus(
			ctx, when.Cursor,
			func(r kdb.Run) (kdb.KnitRunStatus, error) {
				called = true
				if !r.Equal(&when.Target) {
					t.Errorf(
						"unmatch run (passed to callback)\n===actual===\n%+v\n===expected===\n%+v",
						r, when.Target,
					)
				}
				return then.NewStatus, nil
			},
		)
		after := try.To(th.PGNow(ctx, conn)).OrFatal(t)
		if err != nil {
			t.Fatal(err)
		}
		if wantStatusChanged := when.Target.Status != then.NewStatus; statusChanged != wantStatusChanged {
			t.Errorf("unexpectedly status changed: want %+v, but got %+v", wantStatusChanged, statusChanged)
		}
		{
			expected := kdb.RunCursor{
				Status:     when.Cursor.Status,
				Head:       when.Target.RunBody.Id,
				Pseudo:     when.Cursor.Pseudo,
				PseudoOnly: when.Cursor.PseudoOnly,
				Debounce:   when.Cursor.Debounce,
			}
			if !nextCursor.Equal(expected) {
				t.Errorf(
					"unexpected next cursor:\n===actual===\n%+v\n===expected===\n%+v",
					nextCursor, expected,
				)
			}
		}

		t.Run("(record in run table)", func(t *testing.T) {
			runAfterChanged := try.To(scanner.New[tables.Run]().QueryAll(
				ctx, conn, `select * from "run" where "run_id" = $1`,
				when.Target.RunBody.Id,
			)).OrFatal(t)
			if len(runAfterChanged) == 0 {
				t.Fatalf("changed run is not found in DB!")
			}

			if !called {
				t.Fatal("callback has not called")
			}
			{
				actual, base := runAfterChanged[0], runBeforeChanged[0]
				var expected matcher.Run
				if base.Status == then.NewStatus {
					expected = matcher.Run{
						RunId:                 matcher.EqEq(base.RunId),
						PlanId:                matcher.EqEq(base.PlanId),
						Status:                matcher.EqEq(base.Status),
						LifecycleSuspendUntil: matcher.After(before.Add(when.Cursor.Debounce)),
						UpdatedAt:             matcher.Equal(base.UpdatedAt),
					}
				} else {
					expected = matcher.Run{
						RunId:                 matcher.EqEq(base.RunId),
						PlanId:                matcher.EqEq(base.PlanId),
						Status:                matcher.EqEq(then.NewStatus),
						LifecycleSuspendUntil: matcher.Between(before, after),
						UpdatedAt:             matcher.Between(before, after),
					}
				}
				if !expected.Match(actual) {
					t.Errorf(
						"run record:\n===actual===\n%+v\n===expected===\n%+v",
						actual, expected,
					)
				}
			}
		})

		outputData := utils.Map(
			when.Target.Outputs,
			func(a kdb.Assignment) string { return a.KnitDataBody.KnitId },
		)
		if when.Target.Log != nil && when.Target.Log.KnitDataBody.KnitId != "" {
			outputData = append(outputData, when.Target.Log.KnitDataBody.KnitId)
		}
		toBeNominated := []string{}
		if then.NewStatus == kdb.Done {
			toBeNominated = outputData
		}
		toBeTimestamped := []string{}
		if then.NewStatus == kdb.Done || then.NewStatus == kdb.Failed {
			toBeTimestamped = outputData
		}
		if !cmp.SliceContentEq(
			utils.Concat(nomi.Calls.NominateData...),
			toBeNominated,
		) {
			t.Errorf(
				"nominated data:\n===actual===\n%+v\n===expected===\n%+v",
				utils.Concat(nomi.Calls.NominateData...),
				toBeNominated,
			)
		}

		ret := try.To(
			scanner.New[tables.DataTimeStamp]().QueryAll(
				ctx, conn,
				`
				select
					"knit_id", "timestamp"
				from "knit_timestamp"
				where "knit_id" = any($1)
				`,
				outputData,
			),
		).OrFatal(t)

		knitIdsWhichHaveTimestamp := utils.Map(
			ret,
			func(r tables.DataTimeStamp) string { return r.KnitId },
		)
		if !cmp.SliceContentEq(knitIdsWhichHaveTimestamp, toBeTimestamped) {
			t.Errorf(
				"unexpectedly timestamped:\n===actual===\n%+v\n===expected===\n%+v",
				knitIdsWhichHaveTimestamp, toBeTimestamped,
			)
		}
		_, ng := utils.Group(
			ret,
			func(r tables.DataTimeStamp) bool {
				return !r.Timestamp.Before(before) && !r.Timestamp.After(after)
			},
		)
		if len(ng) != 0 {
			t.Errorf(
				"unexpectedly timestamped:\n===actual===\n%+v\n===expected===\nbetween %s and %s",
				ret, before, after,
			)
		}
	})

	t.Run(fmt.Sprintf("status should NOT be changed from %s if callback causes error [PickAndSetStatus]", when.Target.Status), func(t *testing.T) {
		pgpool := poolBroaker.GetPool(ctx, t)
		for _, op := range given {
			op.Apply(ctx, pgpool)
		}
		wpool := proxy.Wrap(pgpool)
		nomi := kpgnommock.New(t)
		nomi.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
			return nil
		}
		testee := kpgrun.New(wpool) // no new run
		shouldLock(ctx, t, wpool, then)

		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		allRuns := utils.ToMap(
			try.To(scanner.New[tables.Run]().QueryAll(
				ctx, conn, `table "run"`,
			)).OrFatal(t),
			func(r tables.Run) string { return r.RunId },
		)

		expectedError := errors.New("fake error")
		nextCursor, statusChanged, err := testee.PickAndSetStatus(
			ctx, when.Cursor,
			func(r kdb.Run) (kdb.KnitRunStatus, error) {
				if !r.Equal(&when.Target) {
					t.Errorf(
						"unmatch run (passed to callback)\n===actual===\n%+v\n===expected===\n%+v",
						r, when.Target,
					)
				}
				return then.NewStatus, expectedError
			},
		)
		if statusChanged {
			t.Errorf("unexpectedly status changed")
		}
		if !errors.Is(err, expectedError) {
			t.Errorf(
				"unexpected error\n===actual===\n%+v\n===expected===\n%+v",
				err, expectedError,
			)
		}

		actual := try.To(scanner.New[tables.Run]().QueryAll(
			ctx, conn, `table "run"`,
		)).OrFatal(t)

		for _, a := range actual {
			whatItWas := allRuns[a.RunId]
			if !a.Equal(&whatItWas) {
				t.Errorf(
					"unexpected change: runId %s\n===actual===\n%+v\n===expected===\n%+v",
					when.Target.RunBody.Id,
					a, whatItWas,
				)
			}
		}

		{
			expected := kdb.RunCursor{
				Status:     when.Cursor.Status,
				Head:       when.Target.RunBody.Id,
				Pseudo:     when.Cursor.Pseudo,
				PseudoOnly: when.Cursor.PseudoOnly,
			}
			if !nextCursor.Equal(expected) {
				t.Errorf(
					"unexpected next cursor:\n===actual===\n%+v\n===expected===\n%+v",
					nextCursor, expected,
				)
			}
		}

		if !errors.Is(err, expectedError) {
			t.Errorf(
				"unmatch error:\n===actual===\n%v\n===expected===\n%v",
				err, expectedError,
			)
		}

		var outputData []string
		var outputDataWithTimestamp []string
		{
			_outputData := utils.Map(
				when.Target.Outputs,
				func(a kdb.Assignment) kdb.KnitDataBody { return a.KnitDataBody },
			)
			if when.Target.Log != nil && when.Target.Log.KnitDataBody.KnitId != "" {
				_outputData = append(_outputData, when.Target.Log.KnitDataBody)
			}
			outputData = utils.Map(
				_outputData, func(a kdb.KnitDataBody) string { return a.KnitId },
			)

			_outWithTimestamp, _ := utils.Group(
				_outputData,
				func(a kdb.KnitDataBody) bool {
					_, ok := utils.First(
						a.Tags.SystemTag(),
						func(tag kdb.Tag) bool { return tag.Key == kdb.KeyKnitTimestamp },
					)
					return ok
				},
			)

			outputDataWithTimestamp = utils.Map(
				_outWithTimestamp,
				func(a kdb.KnitDataBody) string { return a.KnitId },
			)
		}

		toBeNominated := []string{}
		if !cmp.SliceContentEq(
			utils.Concat(nomi.Calls.NominateData...),
			toBeNominated,
		) {
			t.Errorf(
				"nominated data:\n===actual===\n%+v\n===expected===\n%+v",
				utils.Concat(nomi.Calls.NominateData...),
				toBeNominated,
			)
		}

		ret := try.To(
			scanner.New[tables.DataTimeStamp]().QueryAll(
				ctx, conn,
				`
				select
					"knit_id", "timestamp"
				from "knit_timestamp"
				where "knit_id" = any($1)
				`,
				outputData,
			),
		).OrFatal(t)

		knitIdsWhichHaveTimestamp := utils.Map(
			ret,
			func(r tables.DataTimeStamp) string { return r.KnitId },
		)
		if !cmp.SliceContentEq(knitIdsWhichHaveTimestamp, outputDataWithTimestamp) {
			t.Errorf(
				"unexpectedly timestamped:\n===actual===\n%+v\n===expected===\n%+v",
				knitIdsWhichHaveTimestamp, outputDataWithTimestamp,
			)
		}
	})
}

func ShouldNotBeChanged(
	ctx context.Context, t *testing.T, poolBroaker testenv.PoolBroaker,
	given []tables.Operation, when When, then Then,
) {
	t.Helper()
	t.Run(fmt.Sprintf("status shold NOT be changed %s -> %s [SetStatus]", when.Target.Status, then.NewStatus), func(t *testing.T) {
		pgpool := poolBroaker.GetPool(ctx, t)
		wpool := proxy.Wrap(pgpool)
		shouldLock(ctx, t, wpool, then)
		nomi := kpgnommock.New(t)
		nomi.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
			return nil
		}
		testee := kpgrun.New(wpool, kpgrun.WithNominator(nomi))
		for _, op := range given {
			op.Apply(ctx, pgpool)
		}

		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		allRuns := try.To(scanner.New[tables.Run]().QueryAll(
			ctx, conn, `table "run"`,
		)).OrFatal(t)

		err := testee.SetStatus(ctx, when.Target.RunBody.Id, then.NewStatus)
		if err == nil || !errors.Is(err, kdb.ErrInvalidRunStateChanging) {
			t.Errorf("unexpected error: %+v", err)
		}
		actualRuns := try.To(scanner.New[tables.Run]().QueryAll(
			ctx, conn, `table "run"`,
		)).OrFatal(t)
		if !cmp.SliceContentEqWith(
			actualRuns, allRuns,
			func(acr, alr tables.Run) bool { return acr.Equal(&alr) },
		) {
			t.Errorf(
				"unexpectedly runs changed\n===actual===\n%+v\n===expected===\n%+v",
				actualRuns, allRuns,
			)
		}

		var outputData []string
		var outputDataWithTimestamp []string
		{
			_out := utils.Map(
				when.Target.Outputs,
				func(a kdb.Assignment) kdb.KnitDataBody { return a.KnitDataBody },
			)
			if when.Target.Log != nil && when.Target.Log.KnitDataBody.KnitId != "" {
				_out = append(_out, when.Target.Log.KnitDataBody)
			}
			outputData = utils.Map(
				_out,
				func(a kdb.KnitDataBody) string { return a.KnitId },
			)

			_outWithTimestamp, _ := utils.Group(
				_out,
				func(a kdb.KnitDataBody) bool {
					_, ok := utils.First(
						a.Tags.SystemTag(),
						func(tag kdb.Tag) bool { return tag.Key == kdb.KeyKnitTimestamp },
					)
					return ok
				},
			)
			outputDataWithTimestamp = utils.Map(
				_outWithTimestamp,
				func(a kdb.KnitDataBody) string { return a.KnitId },
			)
		}

		toBeNominated := []string{}
		if !cmp.SliceContentEq(
			utils.Concat(nomi.Calls.NominateData...),
			toBeNominated,
		) {
			t.Errorf(
				"nominated data:\n===actual===\n%+v\n===expected===\n%+v",
				utils.Concat(nomi.Calls.NominateData...),
				toBeNominated,
			)
		}

		ret := try.To(
			scanner.New[tables.DataTimeStamp]().QueryAll(
				ctx, conn,
				`
				select
					"knit_id", "timestamp"
				from "knit_timestamp"
				where "knit_id" = any($1)
				`,
				outputData,
			),
		).OrFatal(t)

		knitIdsWhichHaveTimestamp := utils.Map(
			ret,
			func(r tables.DataTimeStamp) string { return r.KnitId },
		)
		if !cmp.SliceContentEq(knitIdsWhichHaveTimestamp, outputDataWithTimestamp) {
			t.Errorf(
				"unexpectedly timestamped:\n===actual===\n%+v\n===expected===\n%+v",
				knitIdsWhichHaveTimestamp, outputDataWithTimestamp,
			)
		}
	})

	t.Run(fmt.Sprintf("status should NOT be changed %s -> %s [PickAndSetStatus]", when.Target.Status, then.NewStatus), func(t *testing.T) {
		pgpool := poolBroaker.GetPool(ctx, t)
		wpool := proxy.Wrap(pgpool)
		shouldLock(ctx, t, wpool, then)
		nomi := kpgnommock.New(t)
		nomi.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
			return nil
		}
		testee := kpgrun.New(wpool, kpgrun.WithNominator(nomi))
		for _, op := range given {
			op.Apply(ctx, pgpool)
		}

		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		allRuns := try.To(scanner.New[tables.Run]().QueryAll(
			ctx, conn, `table "run"`,
		)).OrFatal(t)

		nextCursor, statusChanged, err := testee.PickAndSetStatus(
			ctx, when.Cursor,
			func(r kdb.Run) (kdb.KnitRunStatus, error) {
				return then.NewStatus, nil
			},
		)
		if err == nil || !errors.Is(err, kdb.ErrInvalidRunStateChanging) {
			t.Errorf("unexpected error: %+v", err)
		}
		if statusChanged {
			t.Errorf("unexpectedly status changed")
		}
		{
			expected := kdb.RunCursor{
				Status:     when.Cursor.Status,
				Head:       when.Target.RunBody.Id,
				Pseudo:     when.Cursor.Pseudo,
				PseudoOnly: when.Cursor.PseudoOnly,
			}
			if !nextCursor.Equal(expected) {
				t.Errorf(
					"unexpected next cursor:\n===actual===\n%+v\n===expected===\n%+v",
					nextCursor, expected,
				)
			}
		}
		actualRuns := try.To(scanner.New[tables.Run]().QueryAll(
			ctx, conn, `table "run"`,
		)).OrFatal(t)
		if !cmp.SliceContentEqWith(
			actualRuns, allRuns,
			func(acr, alr tables.Run) bool { return acr.Equal(&alr) },
		) {
			t.Errorf(
				"unexpectedly runs changed\n===actual===\n%+v\n===expected===\n%+v",
				actualRuns, allRuns,
			)
		}

		var outputData []string
		var outputDataWithTimestamp []string
		{
			_out := utils.Map(
				when.Target.Outputs,
				func(a kdb.Assignment) kdb.KnitDataBody { return a.KnitDataBody },
			)
			if when.Target.Log != nil && when.Target.Log.KnitDataBody.KnitId != "" {
				_out = append(_out, when.Target.Log.KnitDataBody)
			}
			_outWithTimestamp, _ := utils.Group(
				_out,
				func(a kdb.KnitDataBody) bool {
					_, ok := utils.First(
						a.Tags.SystemTag(),
						func(tag kdb.Tag) bool { return tag.Key == kdb.KeyKnitTimestamp },
					)
					return ok
				},
			)
			outputData = utils.Map(
				_out,
				func(a kdb.KnitDataBody) string { return a.KnitId },
			)
			outputDataWithTimestamp = utils.Map(
				_outWithTimestamp,
				func(a kdb.KnitDataBody) string { return a.KnitId },
			)
		}

		toBeNominated := []string{}
		if !cmp.SliceContentEq(
			utils.Concat(nomi.Calls.NominateData...),
			toBeNominated,
		) {
			t.Errorf(
				"nominated data:\n===actual===\n%+v\n===expected===\n%+v",
				utils.Concat(nomi.Calls.NominateData...),
				toBeNominated,
			)
		}

		ret := try.To(
			scanner.New[tables.DataTimeStamp]().QueryAll(
				ctx, conn,
				`
				select
					"knit_id", "timestamp"
				from "knit_timestamp"
				where "knit_id" = any($1)
				`,
				outputData,
			),
		).OrFatal(t)

		knitIdsWhichHaveTimestamp := utils.Map(
			ret,
			func(r tables.DataTimeStamp) string { return r.KnitId },
		)
		if !cmp.SliceContentEq(knitIdsWhichHaveTimestamp, outputDataWithTimestamp) {
			t.Errorf(
				"unexpectedly timestamped:\n===actual===\n%+v\n===expected===\n%+v",
				knitIdsWhichHaveTimestamp, outputDataWithTimestamp,
			)
		}
	})

	t.Run(fmt.Sprintf("status should NOT be changed from %s when it causes error [PickAndSetStatus]", when.Target.Status), func(t *testing.T) {
		pgpool := poolBroaker.GetPool(ctx, t)
		wpool := proxy.Wrap(pgpool)
		shouldLock(ctx, t, wpool, then)
		nomi := kpgnommock.New(t)
		nomi.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
			return nil
		}
		testee := kpgrun.New(wpool, kpgrun.WithNominator(nomi))
		for _, op := range given {
			op.Apply(ctx, pgpool)
		}

		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		allRuns := try.To(scanner.New[tables.Run]().QueryAll(
			ctx, conn, `table "run"`,
		)).OrFatal(t)

		expectedError := errors.New("fake error")
		nextCursor, statusChanged, err := testee.PickAndSetStatus(
			ctx, when.Cursor,
			func(r kdb.Run) (kdb.KnitRunStatus, error) {
				return then.NewStatus, expectedError
			},
		)
		if statusChanged {
			t.Errorf("unexpectedly status changed")
		}
		if err == nil || !errors.Is(err, expectedError) {
			t.Errorf("unexpected error: %+v", err)
		}
		{
			expected := kdb.RunCursor{
				Status:     when.Cursor.Status,
				Head:       when.Target.RunBody.Id,
				Pseudo:     when.Cursor.Pseudo,
				PseudoOnly: when.Cursor.PseudoOnly,
			}
			if !nextCursor.Equal(expected) {
				t.Errorf(
					"unexpected next cursor:\n===actual===\n%+v\n===expected===\n%+v",
					nextCursor, expected,
				)
			}
		}
		actualRuns := try.To(scanner.New[tables.Run]().QueryAll(
			ctx, conn, `table "run"`,
		)).OrFatal(t)
		if !cmp.SliceContentEqWith(
			actualRuns, allRuns,
			func(acr, alr tables.Run) bool { return acr.Equal(&alr) },
		) {
			t.Errorf(
				"unexpectedly runs changed\n===actual===\n%+v\n===expected===\n%+v",
				actualRuns, allRuns,
			)
		}

		var outputData []string
		var outputDataWithTimestamp []string
		{
			_out := utils.Map(
				when.Target.Outputs,
				func(a kdb.Assignment) kdb.KnitDataBody { return a.KnitDataBody },
			)
			if when.Target.Log != nil && when.Target.Log.KnitDataBody.KnitId != "" {
				_out = append(_out, when.Target.Log.KnitDataBody)
			}
			_outWithTimestamp, _ := utils.Group(
				_out,
				func(a kdb.KnitDataBody) bool {
					_, ok := utils.First(
						a.Tags.SystemTag(),
						func(tag kdb.Tag) bool { return tag.Key == kdb.KeyKnitTimestamp },
					)
					return ok
				},
			)
			outputData = utils.Map(
				_out,
				func(a kdb.KnitDataBody) string { return a.KnitId },
			)
			outputDataWithTimestamp = utils.Map(
				_outWithTimestamp,
				func(a kdb.KnitDataBody) string { return a.KnitId },
			)
		}

		toBeNominated := []string{}
		if !cmp.SliceContentEq(
			utils.Concat(nomi.Calls.NominateData...),
			toBeNominated,
		) {
			t.Errorf(
				"nominated data:\n===actual===\n%+v\n===expected===\n%+v",
				utils.Concat(nomi.Calls.NominateData...),
				toBeNominated,
			)
		}

		ret := try.To(
			scanner.New[tables.DataTimeStamp]().QueryAll(
				ctx, conn,
				`
				select
					"knit_id", "timestamp"
				from "knit_timestamp"
				where "knit_id" = any($1)
				`,
				outputData,
			),
		).OrFatal(t)

		knitIdsWhichHaveTimestamp := utils.Map(
			ret,
			func(r tables.DataTimeStamp) string { return r.KnitId },
		)
		if !cmp.SliceContentEq(knitIdsWhichHaveTimestamp, outputDataWithTimestamp) {
			t.Errorf(
				"unexpectedly timestamped:\n===actual===\n%+v\n===expected===\n%+v",
				knitIdsWhichHaveTimestamp, outputDataWithTimestamp,
			)
		}
	})
}
