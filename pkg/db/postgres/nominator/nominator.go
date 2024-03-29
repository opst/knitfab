package nominator

import (
	"context"

	kdb "github.com/opst/knitfab/pkg/db"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
)

type Nominator interface {
	// nominate with data.
	//
	// args:
	//    - context.Context
	//    - pgxQueryer: (transactional )connection operating data or data tag.
	//    - string: knit ids to be nominated
	NominateData(context.Context, kpool.Tx, []string) error
	// nominate with Mountpoints.
	//
	// args:
	//    - context.Context
	//    - pgxQueryer: (transactional )connection operating data or data tag.
	//    - []int: mountpointIds
	NominateMountpoints(context.Context, kpool.Tx, []int) error
	// drop nomination for specified data.
	//
	// args:
	//    - context.Context
	//    - pgxQueryer: (transactional )connection operating data.
	//    - []string: knitIds
	DropData(context.Context, kpool.Tx, []string) error
}

type nominator struct {
}

// nominator implements Nominator
var _ Nominator = &nominator{}

func DefaultNominator() Nominator {
	return &nominator{}
}

func (n *nominator) NominateData(ctx context.Context, conn kpool.Tx, knitIds []string) error {

	if _, err := conn.Exec(
		ctx, `lock table "nomination" in EXCLUSIVE mode;`,
	); err != nil {
		return err
	}

	if _, err := conn.Exec(
		ctx,
		`
		with
		"data" as (
			select "knit_id" from "data"
			inner join "run" using("run_id")
			where "knit_id" = any($1) and "status" = $2
		),
		"d_tags" as (
			select
				"knit_id", "tag_id"
			from "tag_data"
			inner join "data" using("knit_id")
		),

		"cardinality_i_tags" as (
			select
				count(*) as "cardinality", "input_id"
			from "tag_input"
			group by "input_id"
		),
		"cardinality_i_and_d_tags" as (
			select
				count(*) as "cardinality",
				"input_id", "knit_id"
			from "tag_input"
			inner join "d_tags" using("tag_id")
			group by "input_id", "knit_id"
		),
		"match_with_usertag" as (
			select
				"input_id", "m"."knit_id" as "knit_id"
			from (
				select
					"input_id", "knit_id"
				from "cardinality_i_tags"
				inner join "cardinality_i_and_d_tags" using("input_id", "cardinality")
			) as "m"
			left join "knit_timestamp" as "dt" using("knit_id")
			left join "knitid_input" as "ki" using("input_id")
			left join "timestamp_input" as "ti" using("input_id")
			where ("ki"."knit_id" is null or "ki"."knit_id" = "m"."knit_id")
				and ("ti"."timestamp" is null or "ti"."timestamp" = "dt"."timestamp")
		),
		"inputs_requires_only_systemtag" as (
			select
				"input_id",
				"timestamp" as "tag_timestamp",
				"knit_id" as "tag_knit_id"
			from (
				select "input_id" from "input"
				where "input_id" not in (select distinct "input_id" from "tag_input")
			) as "i"
			left join "knitid_input" using("input_id")
			left join "timestamp_input" using("input_id")
			where "timestamp" is not null or "knit_id" is not null
		),
		"match_only_systemtag" as (
			select
				"input_id", "knit_id"
			from "inputs_requires_only_systemtag"
			inner join (
				select "input_id" from "inputs_requires_only_systemtag"
				group by "input_id" having count(*) = 1
			) as "i" using("input_id")
			inner join (
				select "knit_id", "timestamp"
				from "data"
				left join "knit_timestamp" using("knit_id")
			) "d" on ("tag_knit_id" is null or "tag_knit_id" = "knit_id")
				 and ("tag_timestamp" is null or "tag_timestamp" = "timestamp")
		),
		"match" as (
			select "input_id", "knit_id" from "match_with_usertag"
			union
			select "input_id", "knit_id" from "match_only_systemtag"
		),
		"remove_unmatch" as (
			delete from "nomination"
			where "knit_id" = any($1::varchar[])
				and ("input_id", "knit_id") not in (
					select "input_id", "knit_id" from "match"
				)
		)
		insert into "nomination" ("knit_id", "input_id", "updated")
		select "knit_id", "input_id", true as "updated" from "match"
		on conflict do nothing
		`,
		knitIds, kdb.Done,
	); err != nil {
		return err
	}
	return nil
}

func (n *nominator) NominateMountpoints(ctx context.Context, conn kpool.Tx, inputIds []int) error {

	if _, err := conn.Exec(ctx, `lock table "nomination" in EXCLUSIVE mode;`); err != nil {
		return err
	}

	_, err := conn.Exec(
		ctx,
		`
		with
		"data" as (
			select "knit_id" from "data"
			inner join "run" using("run_id")
			where "status" = $1
		),
		"d_tags" as (
			select
				"knit_id", "tag_id"
			from "tag_data"
			inner join "data" using("knit_id")
		),

		"i_tags" as (
			select "input_id", "tag_id"
			from "tag_input" where "input_id" = any($2::int[])
		),
		"cardinality_i_tags" as (
			select
				count(*) as "cardinality", "input_id"
			from "i_tags"
			group by "input_id"
		),
		"cardinality_i_and_d_tags" as (
			select
				count(*) as "cardinality",
				"input_id", "knit_id"
			from "i_tags"
			inner join "d_tags" using("tag_id")
			group by "input_id", "knit_id"
		),
		"match_with_usertag" as (
			select
				"input_id", "m"."knit_id" as "knit_id"
			from (
				select
					"input_id", "knit_id"
				from "cardinality_i_tags"
				inner join "cardinality_i_and_d_tags" using("input_id", "cardinality")
			) as "m"
			left join "knit_timestamp" as "dt" using("knit_id")
			left join "knitid_input" as "ki" using("input_id")
			left join "timestamp_input" as "ti" using("input_id")
			where ("ki"."knit_id" is null or "ki"."knit_id" = "m"."knit_id")
				and ("ti"."timestamp" is null or "ti"."timestamp" = "dt"."timestamp")
		),
		"inputs_requires_only_systemtag" as (
			select
				"input_id",
				"timestamp" as "tag_timestamp",
				"knit_id" as "tag_knit_id"
			from (
				select "input_id" from "input"
				where "input_id" not in (select distinct "input_id" from "tag_input")
					and "input_id" = any($2::int[])
			) as "i"
			left join "knitid_input" using("input_id")
			left join "timestamp_input" using("input_id")
			where "timestamp" is not null or "knit_id" is not null
		),
		"match_only_systemtag" as (
			select
				"input_id", "knit_id"
			from "inputs_requires_only_systemtag"
			inner join (
				select "input_id" from "inputs_requires_only_systemtag"
				group by "input_id" having count(*) = 1
			) as "i" using("input_id")
			inner join (
				select "knit_id", "timestamp"
				from "data"
				left join "knit_timestamp" using("knit_id")
			) "d" on ("tag_knit_id" is null or "tag_knit_id" = "knit_id")
				 and ("tag_timestamp" is null or "tag_timestamp" = "timestamp")
		),

		"match" as (
			select "input_id", "knit_id" from "match_with_usertag"
			union
			select "input_id", "knit_id" from "match_only_systemtag"
		),
		"remove_unmatch" as (
			delete from "nomination"
			where "input_id" = any($2::int[])
				and ("input_id", "knit_id") not in (
					select "input_id", "knit_id" from "match"
				)
		)
		insert into "nomination" ("knit_id", "input_id", "updated")
		select "knit_id", "input_id", true as "updated" from "match"
		on conflict do nothing
		`,
		kdb.Done, inputIds,
	)
	if err != nil {
		return err
	}

	return nil
}

func (n *nominator) DropData(ctx context.Context, conn kpool.Tx, knitIds []string) error {

	if _, err := conn.Exec(ctx, `lock table "nomination" in EXCLUSIVE mode;`); err != nil {
		return err
	}

	if _, err := conn.Exec(
		ctx, `delete from "nomination" where "knit_id" = ANY($1::varchar[])`, knitIds,
	); err != nil {
		return err
	}

	return nil
}
