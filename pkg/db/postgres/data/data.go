package data

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	pgerrcode "github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	kdb "github.com/opst/knitfab/pkg/db"
	kpgerr "github.com/opst/knitfab/pkg/db/postgres/errors"
	kpgintr "github.com/opst/knitfab/pkg/db/postgres/internal"
	kpgnom "github.com/opst/knitfab/pkg/db/postgres/nominator"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/tuple"
)

type dataPG struct { // implements kdb.DataInterface

	// connection pool for PostgreSQL
	pool kpool.Pool

	nominator kpgnom.Nominator
}

type Option func(*dataPG) *dataPG

func WithNominator(nominator kpgnom.Nominator) Option {
	return func(d *dataPG) *dataPG {
		d.nominator = nominator
		return d
	}
}

// args:
//   - pool: connection pool used to query/exec SQL
//   - nominator : nominator
func New(pool kpool.Pool, option ...Option) *dataPG {
	d := &dataPG{
		pool:      pool,
		nominator: kpgnom.DefaultNominator(),
	}

	for _, opt := range option {
		d = opt(d)
	}
	return d
}

func (d *dataPG) Get(ctx context.Context, knitIds []string) (map[string]kdb.KnitData, error) {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	return d.get(ctx, conn, knitIds)
}

func (d *dataPG) get(ctx context.Context, conn kpool.Conn, knitIds []string) (map[string]kdb.KnitData, error) {
	if len(knitIds) == 0 {
		return map[string]kdb.KnitData{}, nil
	}

	bodies, err := kpgintr.GetDataBody(ctx, conn, knitIds)
	if err != nil {
		return nil, err
	}

	//                  knit id    ->   (runId, outputId)
	upstreamIds := map[string]tuple.Pair[string, int]{}
	runIds := map[string]struct{}{}
	outputIds := map[int]struct{}{}
	{
		rows, err := conn.Query(
			ctx,
			`
			select
				"knit_id", "run_id", "output_id"
			from "data"
			where "knit_id" = any($1::varchar[])
			`,
			utils.KeysOf(bodies),
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var knitId, runId string
			var outputId int
			if err := rows.Scan(&knitId, &runId, &outputId); err != nil {
				return nil, err
			}
			upstreamIds[knitId] = tuple.PairOf(runId, outputId)
			runIds[runId] = struct{}{}
			outputIds[outputId] = struct{}{}
		}
	}

	if len(upstreamIds) == 0 { // means, no queried knitIds points actual ones.
		return map[string]kdb.KnitData{}, nil
	}

	//                  knit id    ->   (runId, inputId)
	downstreamIds := map[string][]tuple.Pair[string, int]{}
	inputIds := map[int]struct{}{}
	{
		rows, err := conn.Query(
			ctx,
			`select
				"knit_id", "run_id", "input_id"
			from "assign"
			where "knit_id" = any($1)`,
			knitIds,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var knitId, runId string
			var inputId int
			if err := rows.Scan(&knitId, &runId, &inputId); err != nil {
				return nil, err
			}
			downstreamIds[knitId] = append(
				downstreamIds[knitId], tuple.PairOf(runId, inputId),
			)
			runIds[runId] = struct{}{}
			inputIds[inputId] = struct{}{}
		}
	}

	upstreams, err := kpgintr.GetOutputs(ctx, conn, utils.KeysOf(outputIds))
	if err != nil {
		return nil, err
	}

	downstreams, err := kpgintr.GetInputs(ctx, conn, utils.KeysOf(inputIds))
	if err != nil {
		return nil, err
	}

	runBodies, err := kpgintr.GetRunBody(ctx, conn, utils.KeysOf(runIds))
	if err != nil {
		return nil, err
	}

	nominations, err := kpgintr.GetNominationByKnitId(ctx, conn, knitIds)
	if err != nil {
		return nil, err
	}

	// resolve indirect references:
	//
	// knitId -> runId -> run body, for each data, upstream/downstream
	result := map[string]kdb.KnitData{}
	for knitId, b := range bodies {
		runId, outputId := upstreamIds[knitId].Decompose()
		data := kdb.KnitData{
			KnitDataBody: b,
			Upsteram: kdb.Dependency{
				RunBody:    runBodies[runId],
				MountPoint: upstreams[outputId].MountPoint,
			},
			Downstreams: []kdb.Dependency{},
			NominatedBy: nominations[knitId],
		}

		for _, dn := range downstreamIds[knitId] {
			runId, inputId := dn.Decompose()
			dep := kdb.Dependency{
				RunBody:    runBodies[runId],
				MountPoint: downstreams[inputId],
			}
			data.Downstreams = append(data.Downstreams, dep)
		}
		result[knitId] = data
	}

	return result, nil
}

func (d *dataPG) find(ctx context.Context, conn kpool.Queryer, query dataFindQuery) ([]string, error) {

	knitIds := []string{}

	var timestamp *time.Time
	if query.sysKnitTimeStamp != nil {
		t, err := rfctime.ParseRFC3339DateTime(*query.sysKnitTimeStamp)
		if err != nil {
			return nil, err
		}
		_t := t.Time()
		timestamp = &_t
	}

	processingStatus := []string{}
	if query.sysKnitTransientProcessing != nil && *query.sysKnitTransientProcessing {
		processingStatus = utils.Map(kdb.ProcessingStatuses(), kdb.KnitRunStatus.String)
	}

	failedStatus := []string{}
	if query.sysKnitTransientFailed != nil && *query.sysKnitTransientFailed {
		failedStatus = utils.Map(kdb.FailedStatuses(), kdb.KnitRunStatus.String)
	}

	rows, err := conn.Query(
		ctx,
		`
		with
		"__data" as (
			select
				"knit_id" as "knit_id",
				"timestamp" as "raw_timestamp",
				coalesce("timestamp", "run"."updated_at") as "timestamp"
			from "data"
			inner join "run" using("run_id")
			left outer join "knit_timestamp" using("knit_id")
			where
				($1::varchar is null or "knit_id" = $1::varchar)
				and (cardinality($2::runStatus[]) = 0 or "status" = any($2::runStatus[]))
				and (cardinality($3::runStatus[]) = 0 or "status" = any($3::runStatus[]))
		),
		"_data" as (
			select "knit_id", "raw_timestamp", "timestamp" from "__data"
			where
				($4::timestamp with time zone is null or "timestamp" = $4::timestamp with time zone)
				and ($5::timestamp with time zone is null or "timestamp" >= $5::timestamp with time zone)
				and ($6::timestamp with time zone is null or "timestamp" < $6::timestamp with time zone)
		),
		"_query" as (
			select
				unnest("c"[:][1:1]) as "key",
				unnest("c"[:][2:2]) as "value"
			from (select $7::varchar[][]) as "t"("c")
		),
		"_tag_key" as (
			select "key", "id" as "key_id"
			from "tag_key"
			where "key" in (select distinct "key" from "_query")
		),
		"query" as (
			select "id" as "tag_id"
			from "tag"
			inner join "_tag_key" using("key_id")
			where ("key", "value") in (select * from "_query")
		),
		"data" as (
			select "knit_id" from "tag_data"
			inner join "query" using("tag_id")
			inner join "_data" using("knit_id")
			group by "knit_id"
			having count(*) = (select count(*) from "_query")

			union

			select "knit_id" from "_data"
			where (select count(*) from "_query") = 0
		)
		select "knit_id" from "data"
		inner join "_data" using("knit_id")
		order by "raw_timestamp" ASC NULLS LAST, "knit_id"
		`,
		query.sysKnitId, processingStatus, failedStatus, timestamp, query.updatedSince, query.updatedUntil,
		utils.Map(query.userTag, func(t kdb.Tag) [2]string { return [2]string{t.Key, t.Value} }),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var knitId string
		rows.Scan(&knitId)
		knitIds = append(knitIds, knitId)
	}

	return knitIds, nil
}

func makeDataFindQuery(tag []kdb.Tag, since *time.Time, until *time.Time) *dataFindQuery {

	// Remove whitespace, remove duplicates, and extract system tags for incoming tags.
	// If more than one system tag is specified or an undefined system tag is specified,
	// return nil.

	normalizedTags := map[kdb.Tag]struct{}{}

	for _, t := range tag {
		normalized := kdb.Tag{Key: strings.TrimSpace(t.Key), Value: strings.TrimSpace(t.Value)}
		normalizedTags[normalized] = struct{}{}
	}

	result := dataFindQuery{}

	if since != nil {
		result.updatedSince = since
	}

	if until != nil {
		result.updatedUntil = until
	}

	for t := range normalizedTags {
		if !strings.HasPrefix(t.Key, kdb.SystemTagPrefix) {
			result.userTag = append(result.userTag, t)
			continue
		}

		switch t.Key {
		case kdb.KeyKnitId:
			if result.sysKnitId != nil {
				return nil
			}
			val := t.Value
			result.sysKnitId = &val
		case kdb.KeyKnitTransient:
			if t.Value == kdb.ValueKnitTransientProcessing {
				if result.sysKnitTransientProcessing != nil {
					return nil
				}
				t := true
				f := false
				result.sysKnitTransientProcessing = &t
				result.sysKnitTransientFailed = &f
			} else if t.Value == kdb.ValueKnitTransientFailed {
				if result.sysKnitTransientFailed != nil {
					return nil
				}
				t := true
				f := false
				result.sysKnitTransientProcessing = &f
				result.sysKnitTransientFailed = &t
			} else {
				return nil
			}
		case kdb.KeyKnitTimestamp:
			if result.sysKnitTimeStamp != nil {
				return nil
			}
			val := t.Value
			result.sysKnitTimeStamp = &val
		default:
			return nil
		}
	}

	return &result

}

func (d *dataPG) Find(ctx context.Context, tag []kdb.Tag, since *time.Time, until *time.Time) ([]string, error) {
	query := makeDataFindQuery(tag, since, until)
	if query == nil {
		// When nil returns, it returns an empty list
		// because it is known that
		// there is no corresponding data for the specified tag combination.
		return []string{}, nil
	}

	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	return d.find(ctx, conn, *query)
}

type dataFindQuery struct {
	userTag                    []kdb.Tag
	sysKnitId                  *string
	sysKnitTimeStamp           *string
	sysKnitTransientProcessing *bool
	sysKnitTransientFailed     *bool
	updatedSince               *time.Time
	updatedUntil               *time.Time
}

func (d *dataPG) UpdateTag(ctx context.Context, knitId string, delta kdb.TagDelta) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := d.updateTag(ctx, tx, knitId, delta); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func lockData(ctx context.Context, conn kpool.Queryer, knitId string) error {
	rows, err := conn.Query(
		ctx,
		`select "knit_id" from "data" where "knit_id" = $1 for update`,
		knitId,
	)
	if err != nil {
		return err
	}
	rows.Close()

	return nil
}

func (d *dataPG) updateTag(ctx context.Context, tx kpool.Tx, knitId string, delta kdb.TagDelta) error {
	if err := lockData(ctx, tx, knitId); err != nil {
		return err
	}

	if err := addTagsForData(ctx, tx, knitId, delta.Add); err != nil {
		return err
	}

	if err := removeTagsFromData(&ctx, tx, knitId, delta.Remove); err != nil {
		return err
	}

	if err := d.nominator.NominateData(ctx, tx, []string{knitId}); err != nil {
		return err
	}

	return nil
}

func addTagsForData(ctx context.Context, conn kpool.Queryer, knitId string, addTags []kdb.Tag) error {
	for _, tag := range addTags {

		_, err := conn.Exec(
			ctx,
			`
			with "key_insert" as (
				insert into "tag_key" ("key") values ($1)
				on conflict do nothing
				returning "id"
			),
			"key" as (
				select "id" as id from "key_insert"
				union
				select "id" as id from "tag_key" where "key" = $1
				limit 1
			),
			"tag_insert" as (
				insert into "tag" ("key_id", "value")
				select
					"key"."id" as "key_id",
					$2 as value
				from "key"
				on conflict do nothing
				returning "id"
			),
			"tag_in" as (
				select "id" as "tag_id" from "tag_insert"
				union
				select "tag"."id" as "tag_id" from "tag"
					inner join "key" on "key"."id" = "tag"."key_id"
					where "tag"."value" = $2
				limit 1
			)
			insert into "tag_data" ("tag_id", "knit_id")
			select
				"tag_in"."tag_id" as "tag_id",
				$3 as "knit_id"
			from "tag_in"
			on conflict do nothing
			`,
			tag.Key, tag.Value, knitId,
		)

		if err != nil {
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) {
				return err
			} else if pgErr.Code != pgerrcode.ForeignKeyViolation {
				return err
			}

			tableName := pgErr.TableName
			if tableName == "" {
				tableName = "data, tag, tag_key"
			}
			return kpgerr.Missing{
				Table: pgErr.TableName,
				Identity: fmt.Sprintf(
					"knit_id='%s' (constraint: %s)",
					knitId, pgErr.ConstraintName,
				),
			}
		}
	}

	return nil
}

func removeTagsFromData(ctx *context.Context, conn kpool.Queryer, knitId string, remTags []kdb.Tag) error {

	for _, t := range remTags {

		// get tag_key_id
		_, err := conn.Exec(
			*ctx,
			`
			with
			"tag" as (select "id", "key_id" from "tag" where "value" = $1),
			"key" as (select "id" from "tag_key" where "key" = $2),
			"tag_remove" as (
				select "tag"."id" as "id"
				from "tag"
				inner join "key" on "tag"."key_id" = "key"."id"
			)
			delete from "tag_data"
			where "tag_id" in(select "id" from "tag_remove") and "knit_id" = $3
			`,
			t.Value, t.Key, knitId,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *dataPG) NewAgent(ctx context.Context, knitId string, mode kdb.DataAgentMode, lifecycleSuspend time.Duration) (kdb.DataAgent, error) {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return kdb.DataAgent{}, err
	}
	defer tx.Rollback(ctx)

	var daname string
	if err := tx.QueryRow(
		ctx,
		`
		with "data" as (
			select "knit_id" from "data" where "knit_id" = $1 for update
		)
		insert into "data_agent" ("name", "knit_id", "mode", "lifecycle_suspend_until")
		select
			'knitid-' || "knit_id" || '-' || $2 || '-' || substr(md5(random()::text), 0, 6),
			"knit_id",
			$2::dataAgentMode,
			now() + $3
		from "data"
		returning "name"
		`,
		knitId, string(mode), lifecycleSuspend,
	).Scan(&daname); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return kdb.DataAgent{}, kpgerr.Missing{
				Table: "data", Identity: fmt.Sprintf("knit_id='%s'", knitId),
			}
		}
		return kdb.DataAgent{}, err
	}
	body, err := kpgintr.GetDataBody(ctx, tx, []string{knitId})
	if err != nil {
		return kdb.DataAgent{}, err
	}
	da := kdb.DataAgent{
		Name:         daname,
		Mode:         mode,
		KnitDataBody: body[knitId],
	}

	if err := tx.Commit(ctx); err != nil {
		return kdb.DataAgent{}, err
	}
	return da, nil
}

type pgDataAgentMode kdb.DataAgentMode

func (m *pgDataAgentMode) Scan(v any) error {
	var s string
	switch vv := v.(type) {
	case string:
		s = vv
	case []byte:
		s = string(vv)
	default:
		return fmt.Errorf("parse error for DataAgentMode: %#v", v)
	}

	parsed, err := kdb.AsDataAgentMode(s)
	if err != nil {
		return err
	}
	*m = pgDataAgentMode(parsed)
	return nil
}

func (m *dataPG) RemoveAgent(ctx context.Context, name string) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(
		ctx, `delete from "data_agent" where "name" = $1`, name,
	); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

func (m *dataPG) PickAndRemoveAgent(ctx context.Context, cursor kdb.DataAgentCursor, f func(kdb.DataAgent) (bool, error)) (kdb.DataAgentCursor, error) {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return cursor, err
	}
	defer tx.Rollback(ctx)

	var knitId string
	var mode pgDataAgentMode
	var name string
	if err := tx.QueryRow(
		ctx,
		`
		select
			"name", "mode", "knit_id"
		from "data_agent"
		where "lifecycle_suspend_until" < now()
		order by
			"name" <= $1, "name"
		limit 1
		for update skip locked
		`,
		cursor.Head,
	).Scan(&name, &mode, &knitId); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return cursor, nil
		}
		return cursor, err
	}

	body, err := kpgintr.GetDataBody(ctx, tx, []string{knitId})
	if err != nil {
		return cursor, err
	}

	da := kdb.DataAgent{
		Name:         name,
		Mode:         kdb.DataAgentMode(mode),
		KnitDataBody: body[knitId],
	}

	cursor = kdb.DataAgentCursor{Head: da.Name, Debounce: cursor.Debounce}

	ok, err := f(da)

	if !ok || err != nil {
		if _, _err := tx.Exec(
			ctx,
			`
			update "data_agent"
			set "lifecycle_suspend_until" = now() + $1
			where "name" = $2
			`,
			cursor.Debounce, cursor.Head,
		); _err != nil {
			return cursor, _err
		}
		if _err := tx.Commit(ctx); _err != nil {
			return cursor, _err
		}
		return cursor, err
	}

	if _, err := tx.Exec(
		ctx, `delete from "data_agent" where "name" = $1`, da.Name,
	); err != nil {
		return cursor, err
	}

	if err := tx.Commit(ctx); err != nil {
		return cursor, err
	}

	return cursor, nil
}

func (m *dataPG) GetAgentName(ctx context.Context, knitId string, modes []kdb.DataAgentMode) ([]string, error) {
	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	rows, err := conn.Query(
		ctx,
		`
		select 'd' as "type", "knit_id" from "data"
		where "knit_id" = $1
		union
		select 'a' as "type", "name" from "data_agent"
		where "knit_id" = $1 and "mode" = any($2::dataAgentMode[])
		`,
		knitId, utils.Map(modes, kdb.DataAgentMode.String),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dataIsFound := false
	names := []string{}
	for rows.Next() {
		var typeCode string
		var name string
		if err := rows.Scan(&typeCode, &name); err != nil {
			return nil, err
		}
		switch typeCode {
		case "d":
			dataIsFound = true
		case "a":
			names = append(names, name)
		}
	}
	if !dataIsFound {
		return nil, kpgerr.Missing{
			Table: "data", Identity: fmt.Sprintf("knit_id='%s'", knitId),
		}
	}

	return names, nil
}
