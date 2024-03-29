package internal

import (
	"context"
	"time"

	kdb "github.com/opst/knitfab/pkg/db"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
	"github.com/opst/knitfab/pkg/utils"
)

func GetDataBody(ctx context.Context, conn kpool.Queryer, knitIds []string) (map[string]kdb.KnitDataBody, error) {
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
		utils.Map(kdb.ProcessingStatuses(), kdb.KnitRunStatus.String),
		utils.Map(kdb.FailedStatuses(), kdb.KnitRunStatus.String),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	bodies := map[string]kdb.KnitDataBody{}
	tags := map[string][]kdb.Tag{}
	for rows.Next() {
		b := kdb.KnitDataBody{}
		var transientProcessing, transientFailed, hasTimestamp bool
		var timestamp time.Time
		err := rows.Scan(
			&b.KnitId, &b.VolumeRef, &transientProcessing, &transientFailed,
			&hasTimestamp, &timestamp,
		)
		if err != nil {
			return nil, err
		}
		ts := []kdb.Tag{
			{Key: kdb.KeyKnitId, Value: b.KnitId},
		}
		if transientProcessing {
			ts = append(
				ts,
				kdb.Tag{
					Key:   kdb.KeyKnitTransient,
					Value: kdb.ValueKnitTransientProcessing,
				},
			)
		}
		if transientFailed {
			ts = append(
				ts,
				kdb.Tag{
					Key:   kdb.KeyKnitTransient,
					Value: kdb.ValueKnitTransientFailed,
				},
			)
		}
		if hasTimestamp {
			ts = append(ts, kdb.NewTimestampTag(timestamp))
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
		b.Tags = kdb.NewTagSet(ts)
		bodies[knitId] = b
	}

	return bodies, nil
}

func UserTagsOfData(ctx context.Context, conn kpool.Queryer, knitId []string) (map[string][]kdb.Tag, error) {
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

	result := map[string][]kdb.Tag{}

	for rows.Next() {
		var knitId, key, value string

		if err := rows.Scan(&knitId, &key, &value); err != nil {
			return nil, err
		}

		tag, err := kdb.NewTag(key, value)
		if err != nil {
			return nil, err
		}
		result[knitId] = append(result[knitId], tag)
	}

	return result, nil
}
