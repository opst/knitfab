package postgres

import (
	"context"
	"time"

	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/slices"
)

func GetDataBody(ctx context.Context, conn kpool.Queryer, knitIds []string) (map[string]domain.KnitDataBody, error) {
	rows, err := conn.Query(
		ctx,
		`
		with "data" as (
			select
				"knit_id", "volume_ref", "run_id"
			from "data"
			where "knit_id" = any($1::varchar[])
		),
		"data_with_timestamp" as (
			select
				"knit_id", "volume_ref", "run_id", "timestamp"
			from "data"
			left join "knit_timestamp" using("knit_id")
		)
		select
			"knit_id",
			"volume_ref",
			"status" = any($2::runStatus[]) as "knit_transient__processing",
			"status" = any($3::runStatus[]) as "knit_transient__failed",
			"timestamp" is not null as "has_timestamp",
			coalesce("timestamp", to_timestamp(0)) as "timestamp"
		from "data_with_timestamp"
		inner join "run" using ("run_id")
		`,
		knitIds,
		slices.Map(domain.ProcessingStatuses(), domain.KnitRunStatus.String),
		slices.Map(domain.FailedStatuses(), domain.KnitRunStatus.String),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	bodies := map[string]domain.KnitDataBody{}
	tags := map[string][]domain.Tag{}
	for rows.Next() {
		b := domain.KnitDataBody{}
		var transientProcessing, transientFailed, hasTimestamp bool
		var timestamp time.Time
		err := rows.Scan(
			&b.KnitId, &b.VolumeRef, &transientProcessing, &transientFailed,
			&hasTimestamp, &timestamp,
		)
		if err != nil {
			return nil, err
		}
		ts := []domain.Tag{
			{Key: domain.KeyKnitId, Value: b.KnitId},
		}
		if transientProcessing {
			ts = append(
				ts,
				domain.Tag{
					Key:   domain.KeyKnitTransient,
					Value: domain.ValueKnitTransientProcessing,
				},
			)
		}
		if transientFailed {
			ts = append(
				ts,
				domain.Tag{
					Key:   domain.KeyKnitTransient,
					Value: domain.ValueKnitTransientFailed,
				},
			)
		}
		if hasTimestamp {
			ts = append(ts, domain.NewTimestampTag(timestamp))
		}
		bodies[b.KnitId] = b
		tags[b.KnitId] = ts
	}

	utags, err := UserTagsOfData(ctx, conn, knitIds)
	if err != nil {
		return nil, err
	}
	for knitId, utag := range utags {
		tags[knitId] = append(tags[knitId], utag...)
	}

	for knitId, ts := range tags {
		b := bodies[knitId]
		b.Tags = domain.NewTagSet(ts)
		bodies[knitId] = b
	}

	return bodies, nil
}

func UserTagsOfData(ctx context.Context, conn kpool.Queryer, knitId []string) (map[string][]domain.Tag, error) {
	rows, err := conn.Query(
		ctx,
		`
		with "tag_ref" as (
			select "knit_id", "tag_id" from "tag_data" where "knit_id" = any($1)
		),
		"tag_val" as (
			select "knit_id", "key_id", "value" from "tag"
			inner join "tag_ref"
				on "tag"."id" = "tag_ref"."tag_id"
		)
		select "knit_id", "key", "value"
		from "tag_key"
		inner join "tag_val"
			on "tag_val"."key_id" = "tag_key"."id"
		`,
		knitId,
	)
	if err != nil {
		return nil, err
	}

	result := map[string][]domain.Tag{}

	for rows.Next() {
		var knitId, key, value string

		if err := rows.Scan(&knitId, &key, &value); err != nil {
			return nil, err
		}

		tag, err := domain.NewTag(key, value)
		if err != nil {
			return nil, err
		}
		result[knitId] = append(result[knitId], tag)
	}

	return result, nil
}
