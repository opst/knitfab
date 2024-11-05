package keychain

import (
	"context"

	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	kdbkeychain "github.com/opst/knitfab/pkg/domain/keychain/db"
)

type pgKeychain struct {
	pool kpool.Pool
}

func New(pool kpool.Pool) kdbkeychain.KeychainInterface {
	return &pgKeychain{pool: pool}
}

func (kc *pgKeychain) Lock(ctx context.Context, name string, criticalSection func(context.Context) error) error {
	tx, err := kc.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := tx.QueryRow(
		ctx,
		`
		with
		"old" as (
			select "name" from "keychain"
			where "name" = $1 for update
		),
		"new" as (
			insert into "keychain" ("name") values ($1)
			on conflict ("name") do nothing
			returning "name"
		)
		select * from "old"
		union all
		select * from "new"
		`,
		name,
	).Scan(nil); err != nil {
		return err
	}

	if err := criticalSection(ctx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}
