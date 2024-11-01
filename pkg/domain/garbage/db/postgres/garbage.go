package postgres

import (
	"context"

	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	types "github.com/opst/knitfab/pkg/domain"
	kgarbage "github.com/opst/knitfab/pkg/domain/garbage/db"
)

type pgGarbage struct {
	pool kpool.Pool
}

func New(pool kpool.Pool) kgarbage.Interface {
	return &pgGarbage{pool: pool}
}

func (g *pgGarbage) Pop(ctx context.Context, callback func(types.Garbage) error) (bool, error) {
	tx, err := g.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	// pop a record from　garbage table
	rows, err := tx.Query(
		ctx,
		`
		with "del_id" as (
			select "knit_id","volume_ref" from "garbage" limit 1 for update skip locked
		),
		"del_garbage" as (
			delete from "garbage"
			where "knit_id" in (select "knit_id" from "del_id")
		),
		"del_knit" as (
			delete from "knit_id"
			where "knit_id" in (select "knit_id" from "del_id")
		)
		select * from "del_id";
		`,
	)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var KnitId string
	var VolumeRef string
	pop := false
	for rows.Next() {
		err = rows.Scan(&KnitId, &VolumeRef)
		if err != nil {
			return false, err
		}
		pop = true
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	if pop && callback != nil {
		if err := callback(types.Garbage{KnitId: KnitId, VolumeRef: VolumeRef}); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}

	return pop, err
}
