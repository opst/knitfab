package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	kpool "github.com/opst/knitfab/v2/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/v2/pkg/domain"
	kpgerrors "github.com/opst/knitfab/v2/pkg/domain/errors/dberrors/postgres"
	nominator "github.com/opst/knitfab/v2/pkg/domain/nomination/db/postgres"
	"github.com/opst/knitfab/v2/pkg/utils/slices"
)

func GetDataBody(ctx context.Context, conn kpool.Queryer, knitIds []string) (map[string]domain.KnitDataBody, error) {
	rows, err := conn.Query(
		ctx,
		`
		with "_data" as (
			select
				"knit_id", "run_id"
			from "data"
			where "knit_id" = any($1::varchar[])
		),
		"data" as (
			select "knit_id", "volume_ref", "run_id"
			from "_data"
			left join "volume_ref" using ("knit_id")
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
			"timestamp"
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
		var transientProcessing, transientFailed bool
		var timestamp *time.Time
		err := rows.Scan(
			&b.KnitId, &b.VolumeRef, &transientProcessing, &transientFailed,
			&timestamp,
		)
		if err != nil {
			return nil, err
		}
		ts := []domain.Tag{
			{Key: domain.KeyKnitId, Value: b.KnitId},
		}
		if b.VolumeRef == nil {
			ts = append(ts, domain.Tag{Key: domain.KeyKnitTransient, Value: domain.ValueKnitTransientPurged})
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
		if timestamp != nil {
			ts = append(ts, domain.NewTimestampTag(*timestamp))
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

// PurgeData purges VolumeRef from the Data.
//
// It returns an error if the Data is being accessed by DataAgents or Runs.
// It returns nil if the Data is already purged or purged successfully.
//
// Lock:
//
// PurgeData locks Data and VolumeRef for update.
func PurgeData(ctx context.Context, tx kpool.Tx, nom nominator.Nominator, knitId string) error {
	// lock Data
	var status domain.KnitRunStatus
	{
		var volumeRef string
		var _statusStr string
		if err := tx.QueryRow(
			ctx,
			`
			with "data" as (
				select "knit_id", "run_id"
				from "data"
				where "knit_id" = $1
				for update
			),
			"data_and_volume_ref" as (
				select "knit_id", "volume_ref"
				from "data"
				inner join "volume_ref" using ("knit_id")
				for update of "volume_ref"
			),
			"data_and_status" as (
				select "knit_id", "status" from "run"
				inner join "data" using ("run_id")
			)
			select "knit_id", "status", coalesce("volume_ref", '' ) as "volume_ref"
			from "data_and_status"
			left join "data_and_volume_ref" using ("knit_id")
			`,
			knitId,
		).Scan(nil, &_statusStr, &volumeRef); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return &kpgerrors.Missing{Table: "data", Identity: fmt.Sprintf("knit_id=%s", knitId)}
			}
			return err
		}
		if volumeRef == "" {
			return nil // already purged
		}
		_status, err := domain.AsKnitRunStatus(_statusStr)
		if err != nil {
			return err
		}
		status = _status
	}

	// check upstream Run
	switch status {
	case domain.Done, domain.Failed, domain.Invalidated:
		break
	default:
		return fmt.Errorf(
			"%w: Data(knit#id: %s) is being written by Run: %s",
			domain.ErrDataInUse, knitId, status,
		)
	}

	// check downstream Runs and DataAgents
	{
		var num int
		if err := tx.QueryRow(
			ctx,
			`select count(*) as "num" from "data_agent" where "knit_id" = $1`,
			knitId,
		).Scan(&num); err != nil {
			return err
		}

		if 0 < num {
			return fmt.Errorf(
				"%w: Data(knit#id: %s) is being accessed by users.",
				domain.ErrDataInUse, knitId,
			)
		}
	}

	{
		rows, err := tx.Query(
			ctx,
			`
			with "assigned_run" as (
				select distinct "run_id" from "assign" where "knit_id" = $1
			)
			select "run_id" from "run"
			where "run_id" in (select "run_id" from "assigned_run")
				and not ( "status" = any($2::runStatus[]) )
			`,
			knitId,
			[]string{
				domain.Done.String(), domain.Failed.String(), domain.Invalidated.String(),
			},
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		runIds := []string{}
		for rows.Next() {
			var runId string
			if err := rows.Scan(&runId); err != nil {
				return err
			}
			runIds = append(runIds, runId)
		}

		if 0 < len(runIds) {
			return fmt.Errorf(
				"%w: Data(knit#id: %s) is being accessed by Runs: %s",
				domain.ErrDataInUse, knitId, strings.Join(runIds, ", "),
			)
		}
	}

	if status == domain.Done {
		// delete VolumeRef of the Data
		if err := nom.DropData(ctx, tx, []string{knitId}); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(
		ctx,
		`
		with "del" as (
			delete from "volume_ref" where "knit_id" = $1
			returning  "knit_id", "volume_ref"
		)
		insert into "garbage" ("knit_id", "volume_ref")
		select "knit_id", "volume_ref" from "del"
		`,
		knitId,
	); err != nil {
		return err
	}

	return nil
}
