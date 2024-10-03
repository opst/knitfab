package plan

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/opst/knitfab-api-types/misc/rfctime"
	kdb "github.com/opst/knitfab/pkg/db"
	kpgerr "github.com/opst/knitfab/pkg/db/postgres/errors"
	kpgintr "github.com/opst/knitfab/pkg/db/postgres/internal"
	"github.com/opst/knitfab/pkg/db/postgres/marshal"
	kpgnom "github.com/opst/knitfab/pkg/db/postgres/nominator"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
	xe "github.com/opst/knitfab/pkg/errors"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/logic"
)

type planPG struct { // implements kdb.PlanInterface
	pool      kpool.Pool
	nominator kpgnom.Nominator
}

type Option func(*planPG) *planPG

func WithNominator(nomi kpgnom.Nominator) Option {
	return func(p *planPG) *planPG {
		p.nominator = nomi
		return p
	}
}

func New(pool kpool.Pool, options ...Option) *planPG {
	p := &planPG{pool: pool, nominator: kpgnom.DefaultNominator()}
	for _, opt := range options {
		p = opt(p)
	}
	return p
}

func (m *planPG) Get(ctx context.Context, planId []string) (map[string]*kdb.Plan, error) {
	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	return kpgintr.GetPlan(ctx, conn, planId)
}

func (m *planPG) Register(ctx context.Context, plan *kdb.PlanSpec) (string, error) {
	if err := plan.Validate(); err != nil {
		return "", err
	}

	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return "", xe.Wrap(err)
	}
	defer conn.Release()

	tx, err := m.pool.BeginTx(
		ctx, pgx.TxOptions{IsoLevel: pgx.Serializable},
		// XXX: does need this IsoLevel?
		//      meybe yes; to make checking of plans conflict/cyclic deps predictive & deterministic
	)
	if err != nil {
		return "", xe.Wrap(err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `lock table "plan" in EXCLUSIVE mode;`); err != nil {
		return "", err
	}

	// step1: make sure that there are no equivarent plans
	planHashConflicted, err := m.getPlansByHash(ctx, tx, plan.Hash())
	if err != nil {
		return "", xe.Wrap(err)
	}

	if found, ok := utils.First(
		planHashConflicted,
		func(p kdb.Plan) bool { return plan.EquivPlan(&p) },
	); ok {
		return "", xe.Wrap(kdb.NewErrEquivPlanExists(found.PlanId))
	}

	// step2: register it
	created, mountpoints, err := registerPlan(ctx, tx, plan)
	if err != nil {
		return "", xe.Wrap(err)
	}

	if err := planDependencyIsCyclic(ctx, tx, created); err != nil {
		return "", err
	}

	// step3. nominate it
	if err := m.nominator.NominateMountpoints(ctx, tx, mountpoints.Inputs); err != nil {
		return "", xe.Wrap(err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return created, nil
}

func (m *planPG) Activate(ctx context.Context, planId string, activenessToBe bool) error {
	tx, err := m.pool.BeginTx(
		ctx, pgx.TxOptions{IsoLevel: pgx.Serializable},
		// Just in case, we have set up 'Serializable'.
		// Not sure if this strictest IsoLevel is necessary.
	)
	if err != nil {
		return xe.Wrap(err)
	}
	defer tx.Rollback(ctx)

	if err := m.activate(ctx, tx, planId, activenessToBe); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

func (m *planPG) activate(ctx context.Context, tx kpool.Tx, planId string, activenessToBe bool) error {

	statusAfterUpdated, statusToBeUpdated := kdb.GetRunStatusesForPlanActivate(activenessToBe)

	// this task will be done in 2 steps:
	// - 1. lock records to be affected
	// - 2. update

	// step1. lock plan, run (if to be updated) and output data (if any).
	//
	// return:
	//
	// - error : if the specified plan is not found, kdb.ErrMissing.
	// also other errors from Tx can cause.
	err := func() error {
		rows, err := tx.Query(
			ctx,
			`
			with
			"plan_image" as (select "plan_id" from "plan_image" where "plan_id" = $1),
			"plan" as (
				select "plan_id" from "plan" where "plan_id" in (table "plan_image")
				for update of "plan"
			),
			"run" as (
				select "run_id"
				from "plan" inner join "run" using("plan_id")
				where "run"."status" = $2 order by "run_id"
				for update of "run"
			),
			"data" as (
				select "knit_id" from "data"
				inner join "run" using("run_id")
				order by "data"."knit_id" for update of "data"
			)
			select 'p'::"char" as "type", count("plan_id") from "plan"
			union all
			select 'r'::"char" as "type", count("run_id") from "run"
			union all
			select 'd'::"char" as "type", count("knit_id") from "data"
			`,
			planId, statusToBeUpdated,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		plans := uint(0)
		for rows.Next() {
			var t rune
			var n uint
			if err := rows.Scan(&t, &n); err != nil {
				return err
			}
			if t != 'p' {
				continue
			}
			plans += n
		}
		if plans <= 0 {
			return kpgerr.Missing{
				Table:    "plan",
				Identity: fmt.Sprintf("plan_id='%s'", planId),
			}
		}
		return nil
	}()
	if err != nil {
		return err
	}

	// step2, update 'em
	if _, err := tx.Exec(
		ctx,
		`
		with "run_update" as (
			update "run" set "status" = $3
			where "plan_id" = $1 and "status" = $4
		)
		update "plan" set "active" = $2
		where "plan_id" = $1 and "active" = not $2;
		`,
		planId, activenessToBe,
		statusAfterUpdated, statusToBeUpdated,
	); err != nil {
		return err
	}

	return err
}

// return Plans which have specified hash.
func (m *planPG) getPlansByHash(ctx context.Context, conn kpool.Queryer, hash string) ([]kdb.Plan, error) {
	retPlan, err := conn.Query(
		ctx, `select "plan_id" from "plan" where "hash" = $1;`, hash,
	)
	if err != nil {
		return nil, xe.Wrap(err)
	}
	defer retPlan.Close()

	planIds := make([]string, 0, retPlan.CommandTag().RowsAffected())
	for retPlan.Next() {
		var planId string
		if err := retPlan.Scan(&planId); err != nil {
			return nil, err
		}
		planIds = append(planIds, planId)
	}

	plans, err := kpgintr.GetPlan(ctx, conn, planIds)
	if err != nil {
		return nil, err
	}
	return utils.DerefOf(utils.ValuesOf(plans)), nil
}

type mountpointIds struct {
	Inputs  []int
	Outputs []int
	Log     *int
}

// Register plan along a specified specification.
//
// # Args
//
// - ctx
//
// - tx
//
// - plan: specification of plan to be created
//
// # Return
//
// - string: planId
//
// - map[MountPointMode][]string: mountpoint ids per usage
//
// - error
func registerPlan(ctx context.Context, tx kpool.Tx, plan *kdb.PlanSpec) (string, mountpointIds, error) {
	if err := plan.Validate(); err != nil {
		return "", mountpointIds{}, err
	}

	// this task will be done in 2 steps:
	// - 1. insert plan
	// - 2. insert input/output

	// step1.
	//
	// args:
	//     - *kdb.PlanSpec: plan to be created.
	// return:
	//    - *kdb.Plan: created plan
	//    - error
	insertPlan := func(plan *kdb.PlanSpec) (planId string, err error) {
		nOnNode := len(plan.OnNode())
		nodeTolModes := make([]string, nOnNode)
		nodeTolKeys := make([]string, nOnNode)
		nodeTolValues := make([]string, nOnNode)
		for i, onNode := range plan.OnNode() {
			nodeTolModes[i] = onNode.Mode.String()
			nodeTolKeys[i] = onNode.Key
			nodeTolValues[i] = onNode.Value
		}

		nResource := len(plan.Resources())
		resourceTypes := make([]string, nResource)
		resourceValues := make([]marshal.ResourceQuantity, nResource)
		{
			i := 0
			for typ, res := range plan.Resources() {
				resourceTypes[i] = typ
				resourceValues[i] = marshal.ResourceQuantity(res)
				i += 1
			}
		}
		if err := tx.QueryRow(
			ctx,
			`
			with
			"new_plan" as (
				insert into "plan" ("active", "hash")
				values ($1, $2)
				returning "plan_id", "active", "hash"
			),
			"new_plan_image" as (
				insert into "plan_image" ("plan_id", "image", "version")
				select "plan_id", $3 as "image", $4 as "version"
				from "new_plan"
				returning "plan_id", "image", "version"
			),
			"on_node" as (
				insert into "plan_on_node" ("plan_id", "mode", "key", "value")
				select
					"plan_id",
					unnest($5::on_node_mode[]) as "mode",
					unnest($6::varchar[]) as "key",
					unnest($7::varchar[]) as "value"
				from "new_plan_image"
			),
			"resource" as (
				insert into "plan_resource" ("plan_id", "type", "value")
				select "plan_id", unnest($8::varchar[]) as "type", unnest($9::varchar[]) as "value"
				from "new_plan"
			)
			select "new_plan"."plan_id"
			from "new_plan" inner join "new_plan_image" using("plan_id")
			;`,
			plan.Active(), plan.Hash(), plan.Image(), plan.Version(),
			nodeTolModes, nodeTolKeys, nodeTolValues,
			resourceTypes, resourceValues,
		).Scan(
			&planId,
		); err != nil {
			return "", xe.Wrap(err)
		}

		if annotations := plan.Annotations(); 0 < len(annotations) {
			annoKeys := make([]string, len(annotations))
			annoValues := make([]string, len(annotations))

			for i, anno := range plan.Annotations() {
				annoKeys[i] = anno.Key
				annoValues[i] = anno.Value
			}

			if _, err := tx.Exec(
				ctx,
				`
				insert into "plan_annotation" ("plan_id", "key", "value")
				select $1 as "plan_id", unnest($2::varchar[]) as "key", unnest($3::varchar[]) as "value"
				on conflict do nothing
				`,
				planId, annoKeys, annoValues,
			); err != nil {
				return "", xe.Wrap(err)
			}
		}

		if serviceAccount := plan.ServiceAccount(); serviceAccount != "" {
			if _, err := tx.Exec(
				ctx,
				`
				insert into "plan_service_account" ("plan_id", "service_account")
				values ($1, $2)
				`,
				planId, serviceAccount,
			); err != nil {
				return "", xe.Wrap(err)
			}
		}
		return
	}

	// step 2. (for input)
	//
	// args:
	//     - planId: plan id owning new mountpoint
	//     - path: where the new input should be mount on
	//     - tags: tags to be put on the new input
	// return:
	//     - *kdb.Mountpoint: new mountpoint
	//     - error
	insertInput := func(planId string, path string, tags *kdb.TagSet) (int, error) {
		var mpid int
		if err := tx.QueryRow(
			ctx,
			`
			insert into "input" ("plan_id", "path") values ($1, $2)
			returning "input_id"
			`,
			planId, path,
		).Scan(&mpid); err != nil {
			return 0, err
		}

		// set tags.
		for _, tag := range tags.Slice() {
			switch {
			case tag.Key == kdb.KeyKnitTransient:
				return 0, kdb.NewErrUnacceptableTag(
					path, `data with "knit#transient" is never used for input tag`,
				)
			case tag.Key == kdb.KeyKnitId:
				if _, err := tx.Exec(
					ctx,
					`insert into "knitid_input" ("knit_id", "input_id") values ($1, $2)`,
					tag.Value, mpid,
				); err != nil {
					return 0, err
				}
				continue
			case tag.Key == kdb.KeyKnitTimestamp:
				t, _err := rfctime.ParseRFC3339DateTime(tag.Value)
				if _err != nil {
					return 0, kdb.NewErrBadFormatKnitTimestamp(tag.Value)
				}
				if _, err := tx.Exec(
					ctx,
					`insert into "timestamp_input" ("timestamp", "input_id") values ($1, $2)`,
					t, mpid,
				); err != nil {
					return 0, err
				}
				continue
			case strings.HasPrefix(tag.Key, kdb.SystemTagPrefix):
				return 0, kdb.NewErrUnknownSystemTag(&tag)
			}

			if _, err := tx.Exec(
				ctx,
				`
				with "new_tag_key" as (
					insert into "tag_key" ("key") values ($1) on conflict do nothing
					returning "id", "key"
				),
				"tag_key" as (
					select "id", "key" from "new_tag_key"
					union
					select "id", "key" from "tag_key" where "key" = $1
					limit 1
				),
				"new_tag" as (
					insert into "tag" ("key_id", "value")
						select "id" as "key_id", $2 as "value" from "tag_key"
					on conflict do nothing
					returning "id", "value"
				),
				"tag" as (
					select "id", "value" from "new_tag"
					union
					select "tag"."id", "tag"."value" from "tag"
					inner join "tag_key" on "tag_key"."id" = "tag"."key_id"
					where "tag"."value" = $2
					limit 1
				)
				insert into "tag_input" ("input_id", "tag_id")
				select $3 as "input_id", "tag"."id" as "tag_id" from "tag"
				on conflict do nothing
				`,
				tag.Key, tag.Value, mpid,
			); err != nil {
				return 0, err
			}
		}
		return mpid, nil
	}

	// step 2. (for output)
	//
	// args:
	//     - planId: plan id owning new mountpoint
	//     - path: where the new output should be mount on
	//     - forLog: set true if this new output is for log
	//     - tags: tags to be put on the new output
	// return:
	//     - *kdb.Mountpoint: new mountpoint
	//     - error
	insertOutput := func(planId string, path string, forLog bool, tags *kdb.TagSet) (int, error) {
		var mpid int
		if err := tx.QueryRow(
			ctx,
			`
			insert into "output" ("plan_id", "path") values ($1, $2)
			returning "output_id"
			`,
			planId, path,
		).Scan(&mpid); err != nil {
			return 0, err
		}

		if forLog {
			if _, err := tx.Exec(
				ctx,
				`insert into "log" ("plan_id", "output_id") values ($1, $2)`,
				planId, mpid,
			); err != nil {
				return 0, err
			}
		}

		// set tags.
		for _, tag := range tags.Slice() {
			if strings.HasPrefix(tag.Key, kdb.SystemTagPrefix) {
				return 0, fmt.Errorf(
					"%s: %w",
					tag, kdb.NewErrUnacceptableTag(path, "cannot put system tags on output"),
				)
			}

			if _, err := tx.Exec(
				ctx,
				`
				with "new_tag_key" as (
					insert into "tag_key" ("key") values ($1) on conflict do nothing
					returning "id", "key"
				),
				"tag_key" as (
					select "id", "key" from "new_tag_key"
					union
					select "id", "key" from "tag_key" where "key" = $1
					limit 1
				),
				"new_tag" as (
					insert into "tag" ("key_id", "value")
						select "id" as "key_id", $2 as "value" from "tag_key"
					on conflict do nothing
					returning "id", "value"
				),
				"tag" as (
					select "id", "value" from "new_tag"
					union
					select "tag"."id", "tag"."value" from "tag"
					inner join "tag_key" on "tag_key"."id" = "tag"."key_id"
					where "tag"."value" = $2
					limit 1
				)
				insert into "tag_output" ("output_id", "tag_id")
				select $3 as "output_id", "tag"."id" as "tag_id" from "tag"
				on conflict do nothing
				`,
				tag.Key, tag.Value, mpid,
			); err != nil {
				return 0, err
			}
		}
		return mpid, nil
	}

	planId, err := insertPlan(plan)
	if err != nil {
		return "", mountpointIds{}, err
	}

	mountpoints := mountpointIds{}
	for _, mp := range plan.Inputs() {
		mpid, err := insertInput(planId, mp.Path, mp.Tags)
		if err != nil {
			return "", mountpointIds{}, err
		}
		mountpoints.Inputs = append(mountpoints.Inputs, mpid)
	}

	for _, mp := range plan.Outputs() {
		mpid, err := insertOutput(planId, mp.Path, false, mp.Tags)
		if err != nil {
			return "", mountpointIds{}, err
		}
		mountpoints.Outputs = append(mountpoints.Outputs, mpid)
	}

	if log := plan.Log(); log != nil {
		mpid, err := insertOutput(planId, "/log", true, log.Tags)
		if err != nil {
			return "", mountpointIds{}, err
		}
		mountpoints.Log = &mpid
	}

	return planId, mountpoints, nil
}

func (m *planPG) SetResourceLimit(ctx context.Context, planId string, resource map[string]resource.Quantity) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	types := []string{}
	quants := []marshal.ResourceQuantity{}
	for typ, val := range resource {
		types = append(types, typ)
		quant := marshal.ResourceQuantity(val)
		quants = append(quants, quant)
	}

	found := 0
	if err := tx.QueryRow(
		ctx,
		`
		with "plan" as (
			select "plan_id" from "plan" where "plan_id" = $1
		),
		"ins" as (
			insert into "plan_resource" ("plan_id", "type", "value")
			select "plan_id", unnest($2::varchar[]) as "type", unnest($3::varchar[]) as "value"
			from "plan"
			on conflict ("plan_id", "type") do update set "value" = excluded."value"
		)
		select count("plan_id") from "plan"
		`,
		planId, types, quants,
	).Scan(&found); err != nil {
		return err
	}

	if found == 0 {
		return kpgerr.Missing{
			Table:    "plan",
			Identity: fmt.Sprintf("plan_id='%s'", planId),
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func (m *planPG) UnsetResourceLimit(ctx context.Context, planId string, resourceType []string) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	found := 0
	if err := tx.QueryRow(
		ctx,
		`
		with "plan" as (
			select "plan_id" from "plan" where "plan_id" = $1
			for update of "plan"
		),
		"del" as (
			delete from "plan_resource"
			where "plan_id" in (select "plan_id" from "plan") and "type" = any($2)
		)
		select count("plan_id") from "plan"
		`,
		planId, resourceType,
	).Scan(&found); err != nil {
		return err
	}
	if found == 0 {
		return kpgerr.Missing{
			Table:    "plan",
			Identity: fmt.Sprintf("plan_id='%s'", planId),
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// detect cyclic depencency of plans. if it is found, error `ErrCyclicPlan` will be returned
//
// args:
//   - context.Context
//   - PgxConn
//   - plan: start point to detect cyclic dependency.
//
// return:
//   - error: caused error.
//     If any cyclic dependencies are found, `ErrCyclicPlan` will be returned.
//     If other errors from postgres are caused, they will be returned.
//     Otherwise, `nil`.
func planDependencyIsCyclic(ctx context.Context, conn kpool.Queryer, plan string) error {
	// XXX: ===== PERFORMANCE NOTICE [youta.takaoka] ======
	//
	// for implimenting this function, there are two options:
	//
	// - A. get whole adjency map, and then traverse on it
	// - B. get a subset of adgency map, and traverse setp-by-step
	//
	// "A." is *memory consuming* (on DB), but sends query just once.
	// "B." is *chatty*, but consumes less memory for each query.
	//
	// I (y.t) have choiced "B." .
	// "slow" is better than "never", I think. "A." is more likely to exhaust RAM.
	// but, I have little confidence in reasonableness of the choice.
	//
	// We should hear voices of users and servers.
	//

	// traverse 1 step on dependency graph of plans
	//
	// args:
	//     - origin: plan id, where we have started cyclic dependency detection at.
	//     - from: plan ids where we traverse from.
	// return:
	//     - []string: plan ids depending on one of `from`.
	//     - error: caused error
	findPlanIdInDependants := func(origin string, from []string) ([]string, error) {
		if len(from) == 0 {
			return []string{}, nil
		}
		ret, err := conn.Query(
			ctx,
			`
			with
			"outputs" as (
				select "output_id" from "output" where "plan_id" = ANY($1)
			),
			"tags_out" as (
				select
					"outputs"."output_id" as "output_id",
					"tag_output"."tag_id" as "tag_id"
				from "tag_output" inner join "outputs" using("output_id")
			),
			"inputs" as (
				select
					"input_id"
				from "input"
				left join "knitid_input" using("input_id")
				left join "timestamp_input" using("input_id")
				where "knitid_input"."knit_id" is null
				  and "timestamp_input"."timestamp" is null
				-- inputs needing system tags cannot make cycle
			),
			"tags_in" as (
				select
					"inputs"."input_id" as "input_id",
					"tag_input"."tag_id" as "tag_id"
				from "tag_input" inner join "inputs" using("input_id")
			),
			"tags_in_and_out" as (
				select
					"input_id", "output_id", "tags_in"."tag_id" as "tag_id"
				from "tags_in"
				inner join "tags_out"
					on "tags_in"."tag_id" = "tags_out"."tag_id"
			),
			"card_tags_in" as (
				select "input_id", count(*) as "cardinarity"
				from "tags_in" group by "input_id"
			),
			"card_tags_in_and_out" as (
				select
					"input_id", "output_id", count(*) as "cardinarity"
				from "tags_in_and_out"
				group by "input_id", "output_id"
			),
			"downstream" as (
				select distinct "card_tags_in"."input_id"
				from "card_tags_in_and_out"
				inner join "card_tags_in"
				on
					"card_tags_in"."input_id" = "card_tags_in_and_out"."input_id"
					and "card_tags_in"."cardinarity" = "card_tags_in_and_out"."cardinarity"
			)
			select distinct "plan_id" from "input"
			inner join "downstream" using("input_id")
			`,
			from,
		)
		if err != nil {
			return nil, xe.Wrap(err)
		}
		defer ret.Close()

		next := make([]string, 0, ret.CommandTag().RowsAffected())
		for ret.Next() {
			var dependant string
			if err := ret.Scan(&dependant); err != nil {
				return nil, xe.Wrap(err)
			}
			if dependant == origin {
				return nil, xe.Wrap(kdb.NewErrCyclicPlan())
			}
			next = append(next, dependant)
		}
		return next, nil
	}

	traverser := []string{plan}
	for {
		if len(traverser) == 0 {
			break
		}

		var err error
		traverser, err = findPlanIdInDependants(plan, traverser)
		if err != nil {
			return xe.Wrap(err)
		}
	}
	return nil
}

func (m *planPG) Find(ctx context.Context, active logic.Ternary, imageVer kdb.ImageIdentifier, inTag []kdb.Tag, outTag []kdb.Tag) ([]string, error) {

	// "in_tag" and "out_tag" are deduplicated by the normalization of TagSet.

	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	tagQuery := m.makeTagQuery(inTag, outTag)
	if tagQuery == nil {
		// When nil returns, it returns an empty list
		// because it is known that
		// there is no corresponding plan for the specified tag combination.
		return []string{}, nil
	}
	return m.find(ctx, conn, active, imageVer, *tagQuery)
}

func (m *planPG) makeTagQuery(inTag []kdb.Tag, outTag []kdb.Tag) *findTagQuery {

	// Remove leading and trailing spaces
	// Remove duplicate tags
	// If there is more than one knit#id in the in_tag, return nil
	// If there is more than one knit#timestamp in the in_tag, return nil
	// If the in_tag has knit#transient, return nil
	// If the out_tag contains a system tag, return nil
	// Returning nil indicates that there is no corresponding
	// plan for the specified tag combination.
	trimSpace := func(tag kdb.Tag) kdb.Tag {
		return kdb.Tag{
			Key:   strings.TrimSpace(tag.Key),
			Value: strings.TrimSpace(tag.Value),
		}
	}

	outTagset := kdb.NewTagSet(utils.Map(outTag, trimSpace))
	if 0 < len(outTagset.SystemTag()) {
		return nil
	}

	inTagset := kdb.NewTagSet(utils.Map(inTag, trimSpace))

	result := findTagQuery{
		inUserTag:  inTagset.UserTag(),
		outUserTag: outTagset.UserTag(),
	}

	for _, p := range inTagset.SystemTag() {
		switch p.Key {
		case kdb.KeyKnitId:
			if result.inSysKnitId != nil {
				if result.inSysKnitId != &p.Value {
					return nil
				}
			}
			sysKnitId := p.Value
			result.inSysKnitId = &sysKnitId
		case kdb.KeyKnitTimestamp:
			sysTimestamp, err := rfctime.ParseRFC3339DateTime(p.Value)
			if err != nil {
				return nil
			}

			if result.inSysTimestamp != nil {
				if !result.inSysTimestamp.Equal(sysTimestamp) {
					return nil
				}
			}
			result.inSysTimestamp = &sysTimestamp
		case kdb.KeyKnitTransient:
			return nil
		default:
			return nil
		}
	}

	return &result
}

type findTagQuery struct {
	inUserTag      []kdb.Tag
	outUserTag     []kdb.Tag
	inSysKnitId    *string
	inSysTimestamp *rfctime.RFC3339
}

func (m *planPG) find(
	ctx context.Context,
	conn kpool.Conn,
	active logic.Ternary,
	imageVer kdb.ImageIdentifier,
	tagQuery findTagQuery,
) ([]string, error) {
	rows, err := conn.Query(
		ctx,
		`
		with
		"required_output_tags" as (
			select distinct "tag"."id" as "tag_id"
			from "tag" inner join "tag_key" on "tag"."key_id" = "tag_key"."id"
			where ("key", "value") in (
				select
					unnest("c"[:][1:1]) as "key",
					unnest("c"[:][2:2]) as "value"
				from (select $1::varchar[][]) as "t"("c")
			)
		),
		"required_input_tags" as (
			select distinct "tag"."id" as "tag_id"
			from "tag" inner join "tag_key" on "tag"."key_id" = "tag_key"."id"
			where ("key", "value") in (
				select
					unnest("c"[:][1:1]) as "key",
					unnest("c"[:][2:2]) as "value"
				from (select $2::varchar[][]) as "t"("c")
			)
		),
		"plan" as (
			select "plan_id" from "plan"
			left join "plan_image" using("plan_id")
			where ($3 = '' or "image"   = $3)
			  and ($4 = '' or "version" = $4)
			  and ($5 or "active" = $6)
		),

		"o" as (
			select "output_id", count("tag_id") as "match_tags" from "tag_output"
			where "tag_id" in (select "tag_id" from "required_output_tags")
			group by "output_id"
		),
		"output_match" as (
			select
				"plan_id"
			from "plan"
			left join "output" using("plan_id")
			left join "o" using("output_id")
			group by "plan_id"
			having coalesce(max("match_tags"), 0) = (select count(*) from "required_output_tags")
		),

		"i" as (
			select
				"input_id", count("tag_id") as "match_tags"
			from "tag_input"
			where "tag_id" in (select "tag_id" from "required_input_tags")
			group by "input_id"
		),
		"input_match_user_tag" as (
			select
				"plan_id"
			from "plan"
			left join "input" using("plan_id")
			left join "i" using("input_id")
			group by "plan_id"
			having coalesce(max("match_tags"), 0) = (select count(*) from "required_input_tags")
		),

		"ii" as (
			select "input_id" from "input"
			left join "knitid_input" using("input_id")
			left join "timestamp_input" using("input_id")
			where
				($7::varchar is null or "knitid_input"."knit_id" = $7)
				and ($8::timestamp with time zone is null or "timestamp_input"."timestamp" = $8)
		),
		"input_match_system_tag" as (
			select
				distinct "plan_id"
			from "plan" left join "input" using("plan_id")
			where
				"input_id" in (select "input_id" from "ii")
				or ($7::varchar is null and $8::timestamp with time zone is null)
		)
		select "plan_id" from "plan"
		intersect
		select "plan_id" from "output_match"
		intersect
		select "plan_id" from "input_match_user_tag"
		intersect
		select "plan_id" from "input_match_system_tag"
		order by "plan_id"
		`,
		utils.Map(tagQuery.outUserTag, func(v kdb.Tag) [2]string { return [2]string{v.Key, v.Value} }),
		utils.Map(tagQuery.inUserTag, func(v kdb.Tag) [2]string { return [2]string{v.Key, v.Value} }),
		imageVer.Image, imageVer.Version,
		(active == logic.Indeterminate), (active == logic.True),
		tagQuery.inSysKnitId, tagQuery.inSysTimestamp,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	planIds := []string{}
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		planIds = append(planIds, p)
	}

	return planIds, nil
}

func (m *planPG) UpdateAnnotations(ctx context.Context, planId string, delta kdb.AnnotationDelta) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	{ // check plan existence
		found := 0
		if err := tx.QueryRow(
			ctx,
			`
with "plan" as (
	select "plan_id" from "plan" where "plan_id" = $1 for key share
)
select count("plan_id") from "plan"
`,
			planId,
		).Scan(&found); err != nil {
			return err
		}

		if found <= 0 {
			return kpgerr.Missing{
				Table:    "plan",
				Identity: fmt.Sprintf("plan_id='%s'", planId),
			}
		}
	}

	if remove := delta.Remove; 0 < len(remove) {
		keys := make([]string, len(remove))
		values := make([]string, len(remove))

		for i, anno := range remove {
			keys[i] = anno.Key
			values[i] = anno.Value
		}

		if _, err := tx.Exec(
			ctx,
			`
with "plan" as (
	select "plan_id" from "plan" where "plan_id" = $1
),
"_rem" as (
	select unnest($2::varchar[]) as "key", unnest($3::varchar[]) as "value"
),
"rem" as (
	select "plan_id", "key", "value"
	from "plan"
	left join (table "_rem") as "t" on true
)
delete from "plan_annotation"
where ("plan_id", "key", "value") = any(select "plan_id", "key", "value" from "rem")
`,
			planId, keys, values,
		); err != nil {
			return err
		}
	}

	if add := delta.Add; 0 < len(add) {
		keys := make([]string, len(add))
		values := make([]string, len(add))

		for i, anno := range add {
			keys[i] = anno.Key
			values[i] = anno.Value
		}

		if _, err := tx.Exec(
			ctx,
			`
with "plan" as (
	select "plan_id" from "plan" where "plan_id" = $1
),
"_add" as (
	select unnest($2::varchar[]) as "key", unnest($3::varchar[]) as "value"
),
"add" as (
	select "plan_id", "key", "value"
	from "plan"
	left join (table "_add") as "t" on true
)
insert into "plan_annotation" ("plan_id", "key", "value")
select distinct "plan_id", "key", "value"
from "add"
on conflict do nothing
`,
			planId, keys, values,
		); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}
