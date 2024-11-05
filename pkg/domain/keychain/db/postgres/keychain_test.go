package keychain_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/try"

	kpgkc "github.com/opst/knitfab/pkg/domain/keychain/db/postgres"
)

func TestKeychain_Lock(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	t.Run("When there are no records, Lock creates new record and take lock", func(t *testing.T) {
		ctx := context.Background()
		pgpool := poolBroaker.GetPool(ctx, t)

		keychainName := "key-1"

		wrapped := proxy.Wrap(pgpool)
		wrapped.Events().Events().Query.After(func() {
			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			// keychainName = "key-1" is inserted by Lock,
			// so it is invisible before Commit.
			// We tests that the record is not found in unlocked records.
			unlockedNames := try.To(scanner.New[string]().QueryAll(
				ctx, conn,
				`select "name" from "keychain" for update skip locked`,
			)).OrFatal(t)

			if cmp.SliceContains(unlockedNames, []string{keychainName}) {
				t.Errorf("unexpected unlocked names: %v", unlockedNames)
			}

		})

		testee := kpgkc.New(wrapped)
		err := testee.Lock(ctx, keychainName, func(ctx context.Context) error {
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		{
			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			// test the record remains after the critical section, and not locked
			names := try.To(scanner.New[string]().QueryAll(
				ctx, conn, `select "name" from "keychain" for update`,
			)).OrFatal(t)
			if !cmp.SliceEq(names, []string{keychainName}) {
				t.Errorf("unexpected names: %v", names)
			}
		}
	})

	t.Run("When there are no records, Lock return error and not create keychain record", func(t *testing.T) {
		ctx := context.Background()
		pgpool := poolBroaker.GetPool(ctx, t)

		keychainName := "key-1"

		wrapped := proxy.Wrap(pgpool)
		wrapped.Events().Events().Query.After(func() {
			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			// keychainName = "key-1" is inserted by Lock,
			// so it is invisible before Commit.
			// We tests that the record is not found in unlocked records.
			unlockedNames := try.To(scanner.New[string]().QueryAll(
				ctx, conn,
				`select "name" from "keychain" for update skip locked`,
			)).OrFatal(t)

			if cmp.SliceContains(unlockedNames, []string{keychainName}) {
				t.Errorf("unexpected unlocked names: %v", unlockedNames)
			}

		})

		testee := kpgkc.New(wrapped)
		expectedError := errors.New("fake")
		err := testee.Lock(ctx, keychainName, func(ctx context.Context) error {
			return expectedError
		})
		if !errors.Is(err, expectedError) {
			t.Fatalf("unexpected error: %v", err)
		}

		{
			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			names := try.To(scanner.New[string]().QueryAll(
				ctx, conn, `select "name" from "keychain"`,
			)).OrFatal(t)
			if cmp.SliceContains(names, []string{keychainName}) {
				t.Errorf("unexpected names: %v", names)
			}
		}
	})

	t.Run("When there is a record, Lock takes lock", func(t *testing.T) {
		ctx := context.Background()
		pgpool := poolBroaker.GetPool(ctx, t)
		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		keychainName := "key-1"

		// prepare a record
		try.To(conn.Exec(ctx, `insert into "keychain" ("name") values ($1)`, keychainName)).OrFatal(t)

		wrapped := proxy.Wrap(pgpool)
		wrapped.Events().Events().Query.After(func() {
			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			lockedNames := try.To(scanner.New[string]().QueryAll(
				ctx, conn,
				`
				with "all" as (
					select "name" from "keychain"
				),
				"unlocked" as (
					select "name" from "keychain" for update skip locked
				)
				select "name" from "all" except select "name" from "unlocked"
				`,
			)).OrFatal(t)

			if !cmp.SliceEq(lockedNames, []string{keychainName}) {
				t.Errorf("unexpected locked names: %v", lockedNames)
			}

		})

		testee := kpgkc.New(wrapped)
		err := testee.Lock(ctx, keychainName, func(ctx context.Context) error {
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		{
			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			// test the record remains after the critical section, and not locked
			names := try.To(scanner.New[string]().QueryAll(
				ctx, conn, `select "name" from "keychain" for update`,
			)).OrFatal(t)
			if !cmp.SliceEq(names, []string{keychainName}) {
				t.Errorf("unexpected names: %v", names)
			}
		}
	})

}
