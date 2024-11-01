package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	pgerrcode "github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/domain"
	kpgerr "github.com/opst/knitfab/pkg/domain/errors/dberrors/postgres"
	kpgintr "github.com/opst/knitfab/pkg/domain/internal/db/postgres"
	kpgnom "github.com/opst/knitfab/pkg/domain/nomination/db/postgres"
	"github.com/opst/knitfab/pkg/utils/slices"
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

func (d *dataPG) Get(ctx context.Context, knitIds []string) (map[string]domain.KnitData, error) {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	return d.get(ctx, conn, knitIds)
}

func (d *dataPG) get(ctx context.Context, conn kpool.Conn, knitIds []string) (map[string]domain.KnitData, error) {
	if len(knitIds) == 0 {
		return map[string]domain.KnitData{}, nil
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
			slices.KeysOf(bodies),
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
		return map[string]domain.KnitData{}, nil
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

	upstreams, err := kpgintr.GetOutputs(ctx, conn, slices.KeysOf(outputIds))
	if err != nil {
		return nil, err
	}

	downstreams, err := kpgintr.GetInputs(ctx, conn, slices.KeysOf(inputIds))
	if err != nil {
		return nil, err
	}

	runBodies, err := kpgintr.GetRunBody(ctx, conn, slices.KeysOf(runIds))
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
	result := map[string]domain.KnitData{}
	for knitId, b := range bodies {
		runId, outputId := upstreamIds[knitId].Decompose()
		data := domain.KnitData{
			KnitDataBody: b,
			Upsteram: domain.Dependency{
				RunBody:    runBodies[runId],
				MountPoint: upstreams[outputId].MountPoint,
			},
			Downstreams: []domain.Dependency{},
			NominatedBy: nominations[knitId],
		}

		for _, dn := range downstreamIds[knitId] {
			runId, inputId := dn.Decompose()
			dep := domain.Dependency{
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
		processingStatus = slices.Map(domain.ProcessingStatuses(), domain.KnitRunStatus.String)
	}

	failedStatus := []string{}
	if query.sysKnitTransientFailed != nil && *query.sysKnitTransientFailed {
		failedStatus = slices.Map(domain.FailedStatuses(), domain.KnitRunStatus.String)
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
		slices.Map(query.userTag, func(t domain.Tag) [2]string { return [2]string{t.Key, t.Value} }),
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

func makeDataFindQuery(tag []domain.Tag, since *time.Time, until *time.Time) *dataFindQuery {

	// Remove whitespace, remove duplicates, and extract system tags for incoming tags.
	// If more than one system tag is specified or an undefined system tag is specified,
	// return nil.

	normalizedTags := map[domain.Tag]struct{}{}

	for _, t := range tag {
		normalized := domain.Tag{Key: strings.TrimSpace(t.Key), Value: strings.TrimSpace(t.Value)}
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
		if !strings.HasPrefix(t.Key, domain.SystemTagPrefix) {
			result.userTag = append(result.userTag, t)
			continue
		}

		switch t.Key {
		case domain.KeyKnitId:
			if result.sysKnitId != nil {
				return nil
			}
			val := t.Value
			result.sysKnitId = &val
		case domain.KeyKnitTransient:
			if t.Value == domain.ValueKnitTransientProcessing {
				if result.sysKnitTransientProcessing != nil {
					return nil
				}
				t := true
				f := false
				result.sysKnitTransientProcessing = &t
				result.sysKnitTransientFailed = &f
			} else if t.Value == domain.ValueKnitTransientFailed {
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
		case domain.KeyKnitTimestamp:
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

func (d *dataPG) Find(ctx context.Context, tag []domain.Tag, since *time.Time, until *time.Time) ([]string, error) {
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
	userTag                    []domain.Tag
	sysKnitId                  *string
	sysKnitTimeStamp           *string
	sysKnitTransientProcessing *bool
	sysKnitTransientFailed     *bool
	updatedSince               *time.Time
	updatedUntil               *time.Time
}

func (d *dataPG) UpdateTag(ctx context.Context, knitId string, delta domain.TagDelta) error {
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

func (d *dataPG) updateTag(ctx context.Context, tx kpool.Tx, knitId string, delta domain.TagDelta) error {
	if err := lockData(ctx, tx, knitId); err != nil {
		return err
	}

	if err := removeTagsFromData(ctx, tx, knitId, delta.Remove, delta.RemoveKey); err != nil {
		return err
	}

	if err := addTagsForData(ctx, tx, knitId, delta.Add); err != nil {
		return err
	}

	if err := d.nominator.NominateData(ctx, tx, []string{knitId}); err != nil {
		return err
	}

	return nil
}

func addTagsForData(ctx context.Context, conn kpool.Queryer, knitId string, addTags []domain.Tag) error {
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

func removeTagsFromData(ctx context.Context, conn kpool.Queryer, knitId string, remTags []domain.Tag, remKeys []string) error {

	remtagKeys := []string{}
	remtagValues := []string{}

	for _, t := range remTags {
		remtagKeys = append(remtagKeys, t.Key)
		remtagValues = append(remtagValues, t.Value)
	}

	_, err := conn.Exec(
		ctx,
		`
		with
		"request" as (
			select unnest($1::varchar[]) as "key", unnest($2::varchar[]) as "value"
		),
		"tag_data" as (
			select "tag_id", "knit_id" from "tag_data" where "knit_id" = $4
		),
		"tag_value" as (
			select "id" as "tag_id", "key_id", "value" from "tag"
			where "id" in (select "tag_id" from "tag_data")
		),
		"full_tag" as (
			select "tag_id", "key", "value" from "tag_value" as "v"
			inner join "tag_key" as "k" on "v"."key_id" = "k"."id"
		),
		"tags_to_be_removed" as (
			select "tag_id" from "full_tag"
			inner join "request" using ("key", "value")
		),
		"tag_keys_to_be_removed" as (
			select "tag_id" from "full_tag"
			where "key" = any($3::varchar[])
		),
		"target_tag_ids" as (
			select "tag_id" from "tags_to_be_removed"
			union
			select "tag_id" from "tag_keys_to_be_removed"
		)
		delete from "tag_data" where "knit_id" = $4 and "tag_id" in (table "target_tag_ids")
		`,
		remtagKeys, remtagValues, remKeys, knitId,
	)

	if err != nil {
		return err
	}

	return nil
}

func (m *dataPG) NewAgent(ctx context.Context, knitId string, mode domain.DataAgentMode, lifecycleSuspend time.Duration) (domain.DataAgent, error) {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return domain.DataAgent{}, err
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
			return domain.DataAgent{}, kpgerr.Missing{
				Table: "data", Identity: fmt.Sprintf("knit_id='%s'", knitId),
			}
		}
		return domain.DataAgent{}, err
	}
	body, err := kpgintr.GetDataBody(ctx, tx, []string{knitId})
	if err != nil {
		return domain.DataAgent{}, err
	}
	da := domain.DataAgent{
		Name:         daname,
		Mode:         mode,
		KnitDataBody: body[knitId],
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.DataAgent{}, err
	}
	return da, nil
}

type pgDataAgentMode domain.DataAgentMode

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

	parsed, err := domain.AsDataAgentMode(s)
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

func (m *dataPG) PickAndRemoveAgent(ctx context.Context, cursor domain.DataAgentCursor, f func(domain.DataAgent) (bool, error)) (domain.DataAgentCursor, error) {
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

	da := domain.DataAgent{
		Name:         name,
		Mode:         domain.DataAgentMode(mode),
		KnitDataBody: body[knitId],
	}

	cursor = domain.DataAgentCursor{Head: da.Name, Debounce: cursor.Debounce}

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

func (m *dataPG) GetAgentName(ctx context.Context, knitId string, modes []domain.DataAgentMode) ([]string, error) {
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
		knitId, slices.Map(modes, domain.DataAgentMode.String),
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
