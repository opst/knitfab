package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/domain"
	kpggbg "github.com/opst/knitfab/pkg/domain/garbage/db/postgres"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	. "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestGarbage_Pop(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	t.Run("If there is 1 record in garbage, that record will be popped", func(t *testing.T) {
		// [Preparation]
		// Connect to the database
		// Insert 1 record into the knit_id table and the garbage table
		ctx := context.Background()
		pgpool := poolBroaker.GetPool(ctx, t)
		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		expectedGarbage := domain.Garbage{KnitId: Padding36("knit-1"), VolumeRef: "garbage-1"}
		if _, err := conn.Exec(
			ctx,
			`insert into "knit_id" ("knit_id") values ($1)`,
			expectedGarbage.KnitId,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(
			ctx,
			`insert into "garbage" ("knit_id", "volume_ref") values ($1, $2)`,
			expectedGarbage.KnitId,
			expectedGarbage.VolumeRef,
		); err != nil {
			t.Fatal(err)
		}

		testee := kpggbg.New(pgpool)
		pop, err := testee.Pop(
			ctx,
			func(g domain.Garbage) error {
				if g != expectedGarbage {
					t.Errorf(
						"argument of callback function does not match.(KnitId, VolumeRef = %v, %v)",
						g.KnitId, g.VolumeRef,
					)
				}
				return nil
			},
		)
		// [Verification]
		// The record to be popped is passed as an argument to the callback function.
		// The first return value "pop" of the target function is true, and the second return value is nil.
		// The record count of the knit_id table and the garbage table becomes 0.
		if !pop || err != nil {
			t.Errorf(
				"return value of test function does not match. (pop, err = %v, %v)",
				pop, err,
			)
		}
		knitIds := try.To(
			scanner.New[string]().QueryAll(ctx, conn, `table "knit_id"`),
		).OrFatal(t)

		if want := []string{expectedGarbage.KnitId}; !cmp.SliceContentEq(knitIds, want) {
			t.Errorf(
				"record of knit_id table does not match. got: %v, want: %v",
				knitIds, want,
			)
		}
		garbage := try.To(
			scanner.New[domain.Garbage]().QueryAll(ctx, conn, `table "garbage"`),
		).OrFatal(t)

		if want := []domain.Garbage{}; !cmp.SliceContentEq(garbage, want) {
			t.Errorf(
				"record count does not match. got: %v, want: %v",
				garbage, want,
			)
		}
	})

	t.Run("If there is 1 record in garbage and the callback function is nil, that record will be popped", func(t *testing.T) {
		// [Preparation]
		// Connect to the database
		// Insert 1 record into the knit_id table and the garbage table
		ctx := context.Background()
		pgpool := poolBroaker.GetPool(ctx, t)
		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		if _, err := conn.Exec(
			ctx,
			`insert into "knit_id" ("knit_id") values ($1)`,
			testhelpers.Padding36("knit-1"),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(
			ctx,
			`insert into "garbage" ("knit_id", "volume_ref") values ($1, $2)`,
			testhelpers.Padding36("knit-1"), "garbage-1",
		); err != nil {
			t.Fatal(err)
		}

		testee := kpggbg.New(pgpool)
		pop, err := testee.Pop(ctx, nil)
		// [Verification]
		// The first return value "pop" of the target function is true, and the second return value is nil.
		// The record count of the "knit_id" table and the "garbage" table becomes 0.
		if !pop || err != nil {
			t.Errorf(
				"return value of test function does not match. (pop, err = %v, %v)",
				pop, err,
			)
		}

		knitIds := try.To(
			scanner.New[string]().QueryAll(ctx, conn, `table "knit_id"`),
		).OrFatal(t)

		if want := []string{
			testhelpers.Padding36("knit-1"),
		}; !cmp.SliceContentEq(knitIds, want) {
			t.Errorf("record of knit_id table does not match. got: %v, want: %v", knitIds, want)
		}

		garbage := try.To(
			scanner.New[domain.Garbage]().QueryAll(ctx, conn, `table "garbage"`),
		).OrFatal(t)
		if want := []domain.Garbage{}; !cmp.SliceContentEq(garbage, want) {
			t.Errorf(
				"record count does not match. want: %v, got: %v",
				want, garbage,
			)
		}
	})

	t.Run("If there are 0 records in garbage, nothing is popped", func(t *testing.T) {
		// [Preparation]
		// Connect to the database
		ctx := context.Background()
		pgpool := poolBroaker.GetPool(ctx, t)
		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		// Insert record into the knit_id table
		if _, err := conn.Exec(
			ctx,
			`insert into "knit_id" ("knit_id") values ($1)`,
			testhelpers.Padding36("knit-1"),
		); err != nil {
			t.Fatal(err)
		}

		expectedError := fmt.Errorf("callback was used")
		testee := kpggbg.New(pgpool)
		pop, err := testee.Pop(
			ctx,
			func(domain.Garbage) error { return expectedError },
		)
		// [Verification]
		// The first return value "pop" of the target function is false, and the second return value is nil.
		if pop || err != nil {
			t.Errorf(
				"return value of test function does not match. (pop,err) = (%v, %v)",
				pop, err,
			)
		}
		// The callback function is not executed.
		if errors.Is(err, expectedError) {
			t.Error("callback was used")
		}
		// The record count of the knit_id table remains unchanged.
		knitIds := try.To(
			scanner.New[string]().QueryAll(ctx, conn, `table "knit_id"`),
		).OrFatal(t)
		if want := []string{
			testhelpers.Padding36("knit-1"),
		}; !cmp.SliceContentEq(knitIds, want) {
			t.Errorf(
				"record count of knit_id table changes! want: %v, got: %v",
				want, knitIds,
			)
		}
	})
	t.Run("When the callback function returns an error, the entire function becomes an error", func(t *testing.T) {
		//[Preparation]
		//Connect to the database
		//Insert 1 record into the knit_id table and the garbage table
		ctx := context.Background()
		pgpool := poolBroaker.GetPool(ctx, t)
		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		if _, err := conn.Exec(
			ctx,
			`insert into "knit_id" ("knit_id") values ($1)`,
			testhelpers.Padding36("knit-1"),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(
			ctx,
			`insert into "garbage" ("knit_id", "volume_ref") values ($1, $2)`,
			testhelpers.Padding36("knit-1"), "garbage-1",
		); err != nil {
			t.Fatal(err)
		}
		expectedError := fmt.Errorf("callback causes expected error")
		testee := kpggbg.New(pgpool)
		pop, err := testee.Pop(
			ctx,
			func(domain.Garbage) error { return expectedError },
		)
		// [Verification]
		// The first return value "pop" of the target function is false, and the second return value is expectedError.
		if pop || !errors.Is(err, expectedError) {
			t.Errorf(
				"return value of test function does not match. (pop,err) = (%v, %v)",
				pop, err,
			)
		}
		// The record count of the knit_id table and the garbage table remains unchanged (still 1).
		knitIds := try.To(
			scanner.New[string]().QueryAll(ctx, conn, `table "knit_id"`),
		).OrFatal(t)
		if want := []string{
			testhelpers.Padding36("knit-1"),
		}; !cmp.SliceContentEq(knitIds, want) {
			t.Errorf(
				"record count of knit_id table changes! want: %v, got: %v",
				want, knitIds,
			)
		}

		garbage := try.To(
			scanner.New[domain.Garbage]().QueryAll(ctx, conn, `table "garbage";`),
		).OrFatal(t)
		if want := []domain.Garbage{
			{
				KnitId:    testhelpers.Padding36("knit-1"),
				VolumeRef: "garbage-1",
			},
		}; !cmp.SliceContentEq(garbage, want) {
			t.Errorf(
				"record count of garbage table changes! want: %v, got: %v",
				want, garbage,
			)
		}
	})
	t.Run("If all records in the garbage table are locked, nothing is popped", func(t *testing.T) {
		// [Preparation]
		// Connect to the database
		// Insert 1 record into the knit_id table and the garbage table
		// Lock all records in the garbage table
		ctx := context.Background()
		pgpool := poolBroaker.GetPool(ctx, t)
		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		if _, err := conn.Exec(
			ctx,
			`insert into "knit_id" ("knit_id") values ($1)`,
			testhelpers.Padding36("knit-1"),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(
			ctx,
			`insert into "garbage" ("knit_id","volume_ref") values ($1, $2)`,
			testhelpers.Padding36("knit-1"), "garbage-1",
		); err != nil {
			t.Fatal(err)
		}

		locked := try.To(pgpool.Begin(ctx)).OrFatal(t)
		defer locked.Rollback(ctx)
		if _, err := locked.Exec(
			ctx,
			`select * from "garbage" for update`,
		); err != nil {
			t.Fatal(err)
		}

		expectedError := fmt.Errorf("callback was used")
		testee := kpggbg.New(pgpool)
		pop, err := testee.Pop(
			ctx,
			func(domain.Garbage) error { return expectedError },
		)
		// [Verification]
		// The first return value of the test function should be false, and the second return value should be nil.
		if pop || err != nil {
			t.Errorf(
				"return value of test function does not match. (pop,err) = (%v, %v)",
				pop, err,
			)
		}
		// The callback function is not executed
		if errors.Is(err, expectedError) {
			t.Error("callback was used")
		}
		// The record count of the knit_id table and the garbage table remains unchanged (still 1)
		knitIds := try.To(
			scanner.New[string]().QueryAll(ctx, conn, `table "knit_id"`),
		).OrFatal(t)

		if want := []string{
			testhelpers.Padding36("knit-1"),
		}; !cmp.SliceContentEq(knitIds, want) {
			t.Errorf(
				"record count of knit_id table changes! want: %v, got: %v",
				want, knitIds,
			)
		}

		garbage := try.To(
			scanner.New[domain.Garbage]().QueryAll(ctx, conn, `table "garbage"`),
		).OrFatal(t)

		if want := []domain.Garbage{
			{
				KnitId:    testhelpers.Padding36("knit-1"),
				VolumeRef: "garbage-1",
			},
		}; !cmp.SliceContentEq(garbage, want) {
			t.Errorf(
				"record count of garbage table changes! want: %v, got: %v",
				want, garbage,
			)
		}
	})
}
