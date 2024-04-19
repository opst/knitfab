package run

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	kpgerr "github.com/opst/knitfab/pkg/db/postgres/errors"
	kpgintr "github.com/opst/knitfab/pkg/db/postgres/internal"
	kpgnom "github.com/opst/knitfab/pkg/db/postgres/nominator"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
	xe "github.com/opst/knitfab/pkg/errors"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/combination"
	"github.com/opst/knitfab/pkg/utils/rfctime"
)

type NamingConvention interface {
	VolumeRef(knitId string) (volumeRef string, err error)
	Worker(runId string) (workeName string, err error)
}

type PrefixNamigConvention struct {
	PrefixVolumeRef string
	PrefixWorker    string
}

func (p PrefixNamigConvention) VolumeRef(knitId string) (string, error) {
	return p.PrefixVolumeRef + knitId, nil
}

func (p PrefixNamigConvention) Worker(runId string) (string, error) {
	return p.PrefixWorker + runId, nil
}

var defaultRunNamingConvention = PrefixNamigConvention{
	PrefixVolumeRef: "data-knitid-",
	PrefixWorker:    "worker-run-",
}

func DefaultNamingConvention() NamingConvention {
	return defaultRunNamingConvention
}

// a struct for DB operations related to Run
type runPG struct { // implements kdb.RunInterface
	// Db connection pool
	pool kpool.Pool
	// Data nominator
	nominator kpgnom.Nominator
	// Naming convention object for volume reference name or worker name
	naming NamingConvention
}

type Option func(*runPG) *runPG

func WithNominator(n kpgnom.Nominator) Option {
	return func(r *runPG) *runPG {
		r.nominator = n
		return r
	}
}

func WithNamingConvention(n NamingConvention) Option {
	return func(r *runPG) *runPG {
		r.naming = n
		return r
	}
}

func New(pool kpool.Pool, options ...func(*runPG) *runPG) *runPG {
	r := &runPG{
		pool:      pool,
		nominator: kpgnom.DefaultNominator(),
		naming:    DefaultNamingConvention(),
	}
	for _, o := range options {
		r = o(r)
	}
	return r
}

// &runPG implements RunInterface
var _ kdb.RunInterface = &runPG{}

func (m *runPG) NewPseudo(
	ctx context.Context, planName kdb.PseudoPlanName, lifecycleSuspend time.Duration,
) (string, error) {

	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var pseudoPlanId string
	if err := tx.QueryRow(
		ctx, `select "plan_id" from "plan_pseudo" where "name" = $1;`, string(planName),
	).Scan(&pseudoPlanId); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", kpgerr.Missing{
				Table:    "plan",
				Identity: fmt.Sprintf("System defined pseudo plan '%s'", planName),
			}
		}
		return "", err
	}

	runId, err := m.register(ctx, tx, pseudoPlanId, nil)
	if err != nil {
		return "", err
	}

	if err := m.setStatus(ctx, tx, runId, kdb.Running, 0); err != nil {
		return "", err
	}

	if _, err := tx.Exec(
		ctx,
		`
		update "run" set "lifecycle_suspend_until" = now() + $1
		where "run_id" = $2
		`,
		lifecycleSuspend, runId,
	); err != nil {
		return "", err
	}

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}

	return runId, nil
}

func (m *runPG) New(ctx context.Context) ([]string, *kdb.ProjectionTrigger, error) {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)

	// step 1. determine plan & lock the plan + nominated data
	var planId string
	var _dn int
	if err := tx.QueryRow(
		ctx,
		`
		with
		"mp" as (
			select distinct "input_id" from "nomination" where "updated"
		),
		"pids" as (
			select "plan_id" from "plan_image"

			intersect

			select "plan_id" from "input"
			inner join "mp" using ("input_id")
		),
		"plan" as (
			select "plan_id" from "plan" inner join "pids" using("plan_id")
			for update of "plan" skip locked
			limit 1
		),
		"rel_mp" as (
			select "input_id" from "input" inner join "plan" using("plan_id")
		),
		"n" as (
			select "knit_id" from "nomination" inner join "rel_mp" using("input_id")
		),
		"d" as (
			select "knit_id" from "data" inner join "n" using("knit_id")
			order by "knit_id" for share of "data"
		),
		"dn" as (select count("knit_id") as "locked_data" from "d")
		select "plan_id", "locked_data" from "plan", "dn"
		`,
		// to lock data, we count (scanning all rows) them.
		//
		// This operation needs preventing update/delete data, so we take lock on data with "for share".
		// But not need to occupy them; only referencing them is Okay, multiple New's can run in parallel.
	).Scan(&planId, &_dn); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, nil // nothing to do
		}
		return nil, nil, err
	}

	// step 2. fetch nominations
	nominations := map[int][]string{}  // mountpointId -> knitIds, nominated
	var trigger *kdb.ProjectionTrigger // mountpontId, knitId nominated
	{
		rows, err := tx.Query(
			ctx,
			`
			with
			"input" as (
				select "input_id" from "input"
				where "plan_id" = $1
			),
			"nom" as (
				select "input_id", "knit_id", "updated" from "nomination"
				inner join "input" using ("input_id")
			),
			"new" as (
				select "input_id", "knit_id", "updated" from "nom"
				where "updated" limit 1
			),
			"known" as (
				select "input_id", "knit_id", "updated" from "nom"
				where not "updated"
			)
			select "input_id", "knit_id", "updated"
			from "input"
			left outer join (table "known" union table "new") as "n" using("input_id")
			`,
			planId,
		)
		if err != nil {
			return nil, nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var mountpointId int
			var knitId *string
			var updated *bool
			if err := rows.Scan(&mountpointId, &knitId, &updated); err != nil {
				return nil, nil, err
			}
			k, ok := nominations[mountpointId]
			if !ok {
				k = []string{}
				nominations[mountpointId] = k
			}
			if knitId == nil {
				continue
			}

			if updated != nil && *updated {
				trigger = &kdb.ProjectionTrigger{
					PlanId: planId, InputId: mountpointId, KnitId: *knitId,
				}
			} else {
				nominations[mountpointId] = append(k, *knitId)
			}
		}
		rows.Close()
	}

	if trigger == nil {
		return nil, nil, nil
	}

	// step 3. perform projection.
	newRunIds, err := m.project(ctx, tx, *trigger, nominations)
	if err != nil {
		return nil, nil, err
	}

	// step final. mark as it has been done.
	if _, err := tx.Exec(
		ctx,
		`
		update "nomination" set "updated" = false
		where "input_id" = $1 and "knit_id" = $2
		`,
		trigger.InputId, trigger.KnitId,
	); err != nil {
		return nil, nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}

	return newRunIds, trigger, nil
}

func (r *runPG) project(
	ctx context.Context, tx kpool.Tx,
	trigger kdb.ProjectionTrigger,
	nominations map[int][]string, // mountpointId -> nominated knit_ids
) ([]string, error) {
	{
		// Drop nomination from triggering mountpoint.
		//
		// Not to be destractive, coping map here.
		// Do not delete(nominations, trigger.MountPointId).
		m := map[int][]string{
			trigger.InputId: {trigger.KnitId},
		}
		for mpid, knitIds := range nominations {
			if mpid == trigger.InputId {
				continue
			}
			if len(knitIds) == 0 {
				// If mountpoint has no data, this projection generates nothing.
				// Return as early as possible.
				return nil, nil
			}
			m[mpid] = knitIds
		}
		nominations = m
	}

	// === fetch already performed runs ===

	type assignment map[int]string // mountpoint_id -> knit_id
	var performed []assignment
	{
		_performed := map[string]assignment{}
		//       (string)     (int)      (string)
		//        run_id -> {input_id -> knit_id}
		//                  ^^^^^^^^^^^^^^^^^^^^^
		//                       assignment
		rows, err := tx.Query(
			ctx,
			`
			select "run_id", "input_id", "knit_id"
			from "assign"
			where "plan_id" = $1 and "input_id" = $2 and "knit_id" = $3
			`,
			trigger.PlanId,
			trigger.InputId,
			trigger.KnitId,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var runId, knitId string
			var mountpointId int

			if err := rows.Scan(&runId, &mountpointId, &knitId); err != nil {
				return nil, err
			}

			var a assignment
			if _a, ok := _performed[runId]; ok {
				a = _a
			} else {
				a = assignment{}
			}

			a[mountpointId] = knitId
			_performed[runId] = a
		}

		performed = utils.ValuesOf(_performed)
	}

	// XXX: this call can cause OOM (MapCartesian can generate huge result).
	// If it causes, rewrite MapCartesian as "func(...) <-chan map[K]T" and
	// make this online algorithm.
	newInputPattern, _ := utils.Group(
		combination.MapCartesian(nominations),
		func(q map[int]string) bool {
			_, found := utils.First(
				performed, func(p assignment) bool {
					return cmp.MapEq(p, q)
				},
			)
			return !found
		},
	)
	if len(newInputPattern) == 0 {
		return nil, nil
	}

	runIds := make([]string, 0, len(newInputPattern))
	for _, pat := range newInputPattern {
		runId, err := r.register(ctx, tx, trigger.PlanId, pat)
		if err != nil {
			return nil, err
		}
		runIds = append(runIds, runId)

		if err := r.setWorker(ctx, tx, runId); err != nil {
			return nil, err
		}
	}

	return runIds, nil
}

// finish specified run.
//
// # Args
//
// - context.Context
//
// - Tx
//
// - runId string: runId of finishing run.
// YOU SHOULD TAKE LOCK OF IT AND ITS INPUT DATA BEFORE CALL.
func (m *runPG) finish(ctx context.Context, tx kpool.Tx, runId string) error {
	var runStatus kpgintr.KnitRunStatus
	if err := tx.QueryRow(
		ctx,
		`select "status" from "run" where "run_id" = $1`,
		runId,
	).Scan(&runStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return kpgerr.Missing{
				Table:    "run",
				Identity: fmt.Sprintf("Run (id='%s')", runId),
			}
		}
		return err
	}

	var runStatusShouldBe kdb.KnitRunStatus = kdb.Failed
	switch d := kdb.KnitRunStatus(runStatus); d {
	case kdb.Completing:
		runStatusShouldBe = kdb.Done
	case kdb.Aborting:
		runStatusShouldBe = kdb.Failed
	default:
		return fmt.Errorf(
			`%w: run (id='%s', status='%s') has not started and stopped`,
			kdb.NewErrInvalidRunStateChanging(d, kdb.Done),
			runId, d,
		)
	}

	knitIds := []string{}
	{
		rows, err := tx.Query(
			ctx,
			`
			insert into "knit_timestamp" ("knit_id", "timestamp")
			select "knit_id", now() from "data" where "run_id" = $1
			returning "knit_id"
			`,
			runId,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var kid string
			if err := rows.Scan(&kid); err != nil {
				return err
			}
			knitIds = append(knitIds, kid)
		}
		if err := rows.Err(); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(
		ctx,
		`
		update "run"
		set
			"updated_at" = now(),
			"lifecycle_suspend_until" = now(),
			"status" = $1
		where "status" = $2 and "run_id" = $3
		`,
		runStatusShouldBe, runStatus, runId,
	); err != nil {
		return err
	}

	if runStatusShouldBe == kdb.Done {
		if err := m.nominator.NominateData(ctx, tx, knitIds); err != nil {
			return err
		}
	}
	return nil
}

func (m *runPG) Finish(ctx context.Context, runId string) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	{
		// take lock.
		var _na int
		if err := tx.QueryRow(
			ctx,
			`
			with
			"run" as (
				select "run_id" from "run"
				where "run_id" = $1 for update
			),
			"assign" as (
				select "knit_id" from "assign"
				where "run_id" in (table "run")
			),
			"data" as (
				select "knit_id" from "data"
				where "knit_id" in (table "assign")
				order by "knit_id"
				for update of "data"
			)
			select count(*) from "data"
			`,
			runId,
		).Scan(&_na); err != nil {
			return err
		}
	}

	if err := m.finish(ctx, tx, runId); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

func (m *runPG) find(
	ctx context.Context, conn kpool.Conn,
	planId []string,
	knitIdIn []string,
	knitIdOut []string,
	status []kdb.KnitRunStatus,
	since *string,
	duration *string,
) ([]string, error) {

	runIds := []string{}

	var updatedSince *time.Time
	if since != nil {
		t, err := rfctime.ParseRFC3339DateTime(*since)
		if err != nil {
			return nil, err
		}
		_t := t.Time()
		updatedSince = &_t
	}
	rows, err := conn.Query(
		ctx,
		`
		with "run" as (
			select "run_id", "updated_at" from "run"
			where
			($1 or "plan_id" = ANY($2::varchar[]))
			and ($3 or "status" = ANY($4::runStatus[]))
		),
		"assign" as (
			select distinct "run_id", "updated_at"
			from "run"
			left join "assign" using ("run_id")
			where $5 or "knit_id" = ANY($6::varchar[])
		),
		"assign_and_data" as (
			select distinct "run_id", "updated_at"
			from "assign"
			left join "data" using("run_id")
			where $7 or "knit_id" = ANY($8::varchar[])
		)
		select "run_id"
		from "assign_and_data"
		where 
			($9::timestamp with time zone is null or "updated_at" >= $9::timestamp with time zone)
			and ($10::interval is null or "updated_at" <= ($9+$10)::timestamp with time zone)
		order by "updated_at", "run_id"
		`,
		len(planId) == 0, planId,
		len(status) == 0, utils.Map(
			status, func(s kdb.KnitRunStatus) string { return string(s) },
		),
		len(knitIdIn) == 0, knitIdIn,
		len(knitIdOut) == 0, knitIdOut,
		updatedSince, duration,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, err
		}
		runIds = append(runIds, r)
	}

	return runIds, nil
}

func (m *runPG) Find(ctx context.Context, q kdb.RunFindQuery) ([]string, error) {

	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	return m.find(ctx, conn, q.PlanId, q.InputKnitId, q.OutputKnitId, q.Status, q.Since, q.Duration)
}

func (m *runPG) Get(ctx context.Context, runId []string) (map[string]kdb.Run, error) {
	if len(runId) == 0 {
		return map[string]kdb.Run{}, nil
	}

	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	return kpgintr.GetRun(ctx, conn, runId)
}

// register a new run.
//
// # Params
//
// - ctx
//
// - conn
//
// - planId: the plan id which the new run should be based
//
// - intpus: {input id: knit id} pairs
//
// # Returns
//
// - string: run id newly registered
//
// - error
func (m *runPG) register(
	ctx context.Context,
	conn kpool.Queryer,
	planId string,
	inputs map[int]string,
) (string, error) {
	// this task will been done with 3 steps.
	//
	//   step 1. insert a run record
	//   step 2. insert assign records
	//   step 3. insert data records which will be desinations of the run

	var runId string

	// step 1. insert a run record.
	if err := conn.QueryRow(
		ctx,
		`
		with "status_select" as (
			select
				"plan_id", $1 as "status", 0 as "priority"
			from "plan_pseudo"
			where "plan_id" = $4
			UNION
			select
				"plan_id",
				case when "active" then $2
					else $3
				end as "status",
				1 as "priority"
			from "plan"
			where "plan_id" = $4
			order by "priority" asc
			limit 1
		)
		insert into "run" ("plan_id", "status")
		select "plan_id", "status"::runStatus from "status_select"
		returning "run_id";
		`,
		kdb.Running,
		kdb.Waiting, kdb.Deactivated,
		planId,
	).Scan(&runId); err != nil {
		return "", xe.Wrap(err)
	}

	// step 2. insert assign records
	for inputId, knitId := range inputs {
		if _, err := conn.Exec(
			ctx,
			`
			insert into "assign" ("run_id", "input_id", "plan_id", "knit_id")
			values ($1, $2, $3, $4)
			`,
			runId, inputId, planId, knitId,
		); err != nil {
			return "", xe.Wrap(err)
		}
	}

	{ // verify: are all inputs filled?
		var emptyInputs int
		if err := conn.QueryRow(
			ctx,
			`
			with
			"plan" as (
				select "plan_id" from "run" where "run_id" = $1
			),
			"input" as (
				select "input_id" from "input"
				inner join "plan" using ("plan_id")
			),
			"assign" as (
				select "input_id", "knit_id" from "assign" where "run_id" = $1
			)
			select count(*)
			from "input" left join "assign" using("input_id")
			where "knit_id" is null
			`,
			runId,
		).Scan(&emptyInputs); err != nil {
			return "", err
		}
		if 0 < emptyInputs {
			return "", fmt.Errorf(
				"cannot create new run: there are empty input: for plan_id = %s (%+v)",
				planId, inputs,
			)
		}
	}

	// step 3. insert data records
	if err := m.complementData(ctx, conn, runId); err != nil {
		return "", err
	}

	return runId, nil
}

func (m *runPG) setWorker(ctx context.Context, conn kpool.Queryer, runId string) error {
	workerName, err := m.naming.Worker(runId)
	if err != nil {
		return err
	}
	if _, err := conn.Exec(
		ctx,
		`
		with
		"_run" as (
			select "run_id", "plan_id" from "run" where "run_id" = $1
		),
		"run" as (
			select "run_id" from "plan_image" inner join "_run" using("plan_id")
		)
		insert into "worker" ("run_id", "name")
		select "run_id", $2 as "name"
		from "run"
		on conflict do nothing
		`,
		runId, workerName,
	); err != nil {
		return err
	}
	return nil
}

func (m *runPG) complementData(ctx context.Context, conn kpool.Queryer, runId string) error {
	type dataSpec struct {
		runId    string
		planId   string
		outputId int
	}
	missingData := []dataSpec{}
	{
		rows, err := conn.Query(
			ctx,
			`
			with
			"run" as (
				select "plan_id", "run_id" from "run" where "run_id" = $1
			),
			"output" as (
				select "run_id", "plan_id", "output_id" from "output"
				inner join "run" using("plan_id")
			)
			select
				"run_id", "plan_id", "output_id"
			from "output"
			left join "data" using("run_id", "plan_id", "output_id")
			where "knit_id" is null
			`,
			runId,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			ds := dataSpec{}
			if err := rows.Scan(&ds.runId, &ds.planId, &ds.outputId); err != nil {
				return err
			}
			// knitId and volumeRef are set later.
			missingData = append(missingData, ds)
		}
	}

	for _, spec := range missingData {
		var knitId string
		if err := conn.QueryRow(
			ctx,
			`
			insert into "knit_id" DEFAULT VALUES
			returning "knit_id"
			`,
		).Scan(&knitId); err != nil {
			return err
		}
		if volumeRef, err := m.naming.VolumeRef(knitId); err != nil {
			return err
		} else {
			_, err := conn.Exec(
				ctx,
				`
				insert into "data" ("knit_id", "volume_ref", "output_id", "run_id", "plan_id")
				values ($1, $2, $3, $4, $5)
				`,
				knitId, volumeRef, spec.outputId, spec.runId, spec.planId,
			)
			if err != nil {
				return err
			}
		}

		if _, err := conn.Exec(
			ctx,
			`
			insert into "tag_data"
			select "tag_id", $1 as "knit_id"
			from "tag_output"
			where "output_id" = $2
			`,
			knitId, spec.outputId,
		); err != nil {
			return err
		}
	}
	return nil
}

func (m *runPG) SetStatus(ctx context.Context, runId string, newRunStatus kdb.KnitRunStatus) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := m.setStatus(ctx, tx, runId, newRunStatus, 0); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

func (m *runPG) setStatus(
	ctx context.Context, tx kpool.Tx, runId string,
	newRunStatus kdb.KnitRunStatus, debounceIfNotChanged time.Duration,
) error {
	var current kdb.KnitRunStatus
	{
		var _current kpgintr.KnitRunStatus
		if err := tx.QueryRow(
			ctx,
			`
			with "run" as (
				select "run_id", "status"
				from "run"
				where "run_id" = $1 for update
			),
			"assign" as (
				select "knit_id" from "assign"
				where "run_id" in (select "run_id" from "run")
			),
			"knit_ids" as (
				select "knit_id" from "data"
				where "knit_id" in (table "assign")
				   or "run_id" in (select "run_id" from "run")
				order by "knit_id"
				for update of "data"
			)
			select "status", "c"
			from "run", (select count(*) as "c" from "knit_ids") as "d"
			`,
			runId,
		).Scan(&_current, nil); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return kpgerr.Missing{
					Table:    "run",
					Identity: fmt.Sprintf("run_id = %s", runId),
				}
			}
			return err
		}
		current = kdb.KnitRunStatus(_current)
	}

	allowed := false
	switch current {
	case kdb.Invalidated, kdb.Done, kdb.Failed:
		allowed = false
	case newRunStatus:
		if _, err := tx.Exec(
			ctx,
			`
			update "run" set
				"lifecycle_suspend_until" = now() + $1
			where "run_id" = $2
			`,
			debounceIfNotChanged, runId,
		); err != nil {
			return err
		}

		return nil
	case kdb.Deactivated:
		switch newRunStatus {
		case kdb.Deactivated, kdb.Waiting, kdb.Aborting:
			allowed = true
		}
	case kdb.Waiting:
		switch newRunStatus {
		case kdb.Deactivated, kdb.Waiting, kdb.Ready, kdb.Aborting:
			allowed = true
		}
	case kdb.Ready:
		switch newRunStatus {
		case kdb.Ready, kdb.Starting, kdb.Running, kdb.Aborting, kdb.Completing:
			allowed = true
		}
	case kdb.Starting:
		switch newRunStatus {
		case kdb.Starting, kdb.Running, kdb.Aborting, kdb.Completing:
			allowed = true
		}
	case kdb.Running:
		switch newRunStatus {
		case kdb.Running, kdb.Aborting, kdb.Completing:
			allowed = true
		}
	case kdb.Aborting:
		if newRunStatus == kdb.Failed {
			err := m.finish(ctx, tx, runId)
			if err != nil {
				return err
			}
			return nil
		}
	case kdb.Completing:
		if newRunStatus == kdb.Done {
			err := m.finish(ctx, tx, runId)
			if err != nil {
				return err
			}
			return nil
		}
	}
	if !allowed {
		return fmt.Errorf(
			"%w: %s -> %s",
			kdb.ErrInvalidRunStateChanging, current, newRunStatus,
		)
	}

	cmd, err := tx.Exec(
		ctx,
		`
		update "run" set
			"status" = $1,
			"updated_at" = now(),
			"lifecycle_suspend_until" = now()
		where run_id = $2
		`,
		newRunStatus, runId,
	)
	if err != nil {
		return xe.Wrap(err)
	}

	if cmd.RowsAffected() == 0 {
		return kpgerr.Missing{
			Table:    "run",
			Identity: fmt.Sprintf("updating run_id='%s'", runId),
		}
	}

	return nil
}

// select the run which satisfies the specified condition, and change its status.
func (m *runPG) PickAndSetStatus(
	ctx context.Context,
	cursor kdb.RunCursor,
	task func(r kdb.Run) (kdb.KnitRunStatus, error),
) (kdb.RunCursor, error) {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return cursor, err
	}
	defer tx.Rollback(ctx)

	var run kdb.Run
	{
		var runId string
		if err := tx.QueryRow(
			ctx,
			`
			with
			"_run" as (
				select
					"run_id", "plan_id"
				from "run"
				where
					"status" = any($1::runStatus[])
					and "lifecycle_suspend_until" < now()
			),
			"run_pseudo" as (
				select "run_id" from "_run"
				inner join "plan_pseudo" on
					"plan_pseudo"."plan_id" = "_run"."plan_id"
					and "name" = any($2::varchar[])
			),
			"run_image" as (
				select "run_id" from "_run"
				inner join "plan_image" on
					"plan_image"."plan_id" = "_run"."plan_id"
					and not $3
			),
			"target_run" as (
				select "run_id" from "run"
				where "run_id" in (
					table "run_image" union table "run_pseudo"
				)
				order by "run_id" <= $4, "run_id"
				limit 1
				for no key update skip locked
			),
			"assign" as (
				select "knit_id" from "assign"
				where "run_id" in (table "target_run")
			),
			"data" as (
				select "knit_id" from "data"
				where "knit_id" in (table "assign")
				   or "run_id" in (table "target_run")
				order by "knit_id"
				for update of "data"
			)
			select
				"run_id", "c"
			from
				"target_run", (select count(*) as "c" from "data") as "d"
			`,
			utils.Map(cursor.Status, kdb.KnitRunStatus.String),
			utils.Map(cursor.Pseudo, kdb.PseudoPlanName.String),
			cursor.PseudoOnly,
			cursor.Head,
		).Scan(&runId, nil); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return cursor, nil
			}
			return cursor, err
		}

		r, err := kpgintr.GetRun(ctx, tx, []string{runId})
		if err != nil {
			return cursor, err
		}
		run = r[runId]

		// cursor is moved!
		cursor = kdb.RunCursor{
			Head:       runId,
			Status:     cursor.Status,
			Pseudo:     cursor.Pseudo,
			PseudoOnly: cursor.PseudoOnly,
			Debounce:   cursor.Debounce,
		}
	}

	// exec task() and get its result.
	newStatus, err := task(run)
	if err != nil {
		return cursor, err
	}
	// according to the result above, reflect the new status to the database.
	if err := m.setStatus(ctx, tx, run.Id, newStatus, cursor.Debounce); err != nil {
		return cursor, err
	}
	// commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return cursor, err
	}
	return cursor, nil
}

func (m *runPG) SetExit(ctx context.Context, runId string, exit kdb.RunExit) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(
		ctx,
		`
		insert into "run_exit" ("run_id", "exit_code", "message")
		values ($1, $2, $3)
		on conflict ("run_id") do update
		set
			"exit_code" = $2,
			"message" = $3
		`,
		runId, exit.Code, exit.Message,
	); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func (m *runPG) delete(ctx context.Context, tx kpool.Tx, runId string) error {
	if err := m.truncateRun(ctx, tx, runId); err != nil {
		return err
	}

	var runStatus kdb.KnitRunStatus
	var upstreams int
	if err := tx.QueryRow(
		ctx,
		`
		with "run_status" as (
			select "status" from "run" where "run_id" = $1
		),
		"assign" as (
			select count(*) as "n_input" from "assign" where "run_id" = $1
		)
		select "status", "n_input" from "run_status", "assign"
		`,
		runId,
	).Scan((*kpgintr.KnitRunStatus)(&runStatus), &upstreams); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return kpgerr.Missing{
				Table:    "run",
				Identity: fmt.Sprintf("Run (id='%s')", runId),
			}
		}
		return err
	}

	switch runStatus {
	case kdb.Waiting, kdb.Deactivated, kdb.Done, kdb.Failed:
		// ok. they can be deleted.
	case kdb.Invalidated:
		//no. they does not exited.
		return kpgerr.Missing{
			Table:    "run",
			Identity: fmt.Sprintf("run_id = %s", runId),
		}
	default:
		return fmt.Errorf(
			"%w: run (id='%s', status='%s') is not stopped",
			kdb.ErrWorkerActive, runId, runStatus,
		)
	}

	if upstreams == 0 || runStatus == kdb.Invalidated {
		if _, err := tx.Exec(
			ctx, `
			with
			"drop_assign" as (
				delete from "assign" where "run_id" = $1
			)
			delete from "run" where "run_id" = $1
			`, runId,
		); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec(
			ctx,
			`
			update "run" set
				"status" = $1,
				"updated_at" = DEFAULT
			where "run_id" = $2
			`,
			kdb.Invalidated, runId,
		); err != nil {
			return err
		}
	}

	return nil
}

func (m *runPG) Delete(ctx context.Context, runId string) error {

	tx, err := m.pool.BeginTx(
		ctx, pgx.TxOptions{IsoLevel: pgx.Serializable},
		// Just in case, we have set up 'Serializable'.
		// Not sure if this strictest IsoLevel is necessary.
	)
	if err != nil {
		return xe.Wrap(err)
	}
	defer tx.Rollback(ctx)

	if err := m.delete(ctx, tx, runId); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

func (m *runPG) DeleteWorker(ctx context.Context, runId string) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(
		ctx,
		`
		delete from "worker" where "run_id" = $1
		`,
		runId); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func (r *runPG) Retry(ctx context.Context, runId string) error {
	tx, err := r.pool.Begin(ctx)

	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := r.truncateRun(ctx, tx, runId); err != nil {
		return err
	}

	if err := r.complementData(ctx, tx, runId); err != nil {
		return err
	}
	if err := r.setWorker(ctx, tx, runId); err != nil {
		return err
	}

	var status kdb.KnitRunStatus
	// okay, verified. it can be restarted.
	if err := tx.QueryRow(
		ctx,
		`
		with "run" as (
			select "status", "plan_id" from "run" where "run_id" = $1
		)
		select "status"
		from "run"
		inner join "plan_image" using ("plan_id")
		`,
		runId,
	).Scan((*kpgintr.KnitRunStatus)(&status)); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// This run DOES exist, because truncateRun goes well.
			// So, this error means that there are no "plan_image" record,
			// that is, this run is not image-based.
			return fmt.Errorf(
				"%w: run (id='%s') is not image-based, cannot be retryed",
				kdb.ErrRunIsProtected, runId,
			)
		}
		return err
	}

	switch status {
	case kdb.Done, kdb.Failed:
		// ok
	case kdb.Invalidated:
		return kpgerr.Missing{
			Table:    "run",
			Identity: fmt.Sprintf("run_id=%s", runId),
		}
	default:
		return fmt.Errorf(
			"%w: runId=%s: cannot be retried (current status: %s)",
			kdb.ErrInvalidRunStateChanging, runId, status,
		)
	}

	if _, err := tx.Exec(
		ctx,
		`
		update "run" set
			"status" = $1,
			"updated_at" = now(),
			"lifecycle_suspend_until" = now()
		where "run_id" = $2
		`,
		kdb.Waiting, runId,
	); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// truncateRun truncates the downward resources of the run.
//
// This removes...
//
// - downstream invalidated runs ("run" table) and data assignments ("assign" table).
//
// - output data ("data" table. records are moved to "garbage")
// and its tags ("tag_data" and "knit_timestamp" table).
//
// And also, This perform dropping nominations of output data.
//
// Note that:
//
// - this DOES NOT remove the run itself.
//
// - this DOES NOT verify thr run's status.
//
// These are upto caller.
//
// # Args
//
// - ctx
//
// - tx
//
// - runId: runId of the run to truncate
//
// # Returns
//
// - error : If the run cannot be truncated, it returns an error.
// kdb.Missing = the run does not exist.;
// kdb.WorkerActive = the run's Worker or output's DataAgent may exist.;
// kdb.ErrRunHasDownstreams = the run has downstream runs.;
// and other errors from Nominator.DropData() or database.
func (m *runPG) truncateRun(
	ctx context.Context,
	tx kpool.Tx,
	runId string,
) error {
	var runStatus kdb.KnitRunStatus
	var worker string
	var downstreams int
	var dataagents int
	if err := tx.QueryRow(
		ctx,
		`
		with
		"target" as (
			select
				"status",
				coalesce("worker"."name", '') as "worker_name",
				"plan_id"
			from "run"
			left join "worker" using ("run_id")
			where "run_id" = $1 for update of "run"
		),
		"drop_run_exit" as (
			delete from "run_exit" where "run_id" = $1
		),
		"data" as (
			select "knit_id" from "data"
			where "run_id" = $1 for update
		),
		"assign" as (
			select distinct "run_id" from "assign"
			where "knit_id" in (select "knit_id" from "data")
		),
		"invalidated_downstreams" as (
			select "run_id" from "run"
			where "run_id" in (select "run_id" from "assign")
			and "status" = 'invalidated'
		),
		"drop_invalidated_assignment" as (
			delete from "assign"
			where "run_id" in (select "run_id" from "invalidated_downstreams")
			returning "run_id"
		),
		"drop_invalidated_run_exit" as (
			delete from "run_exit"
			where "run_id" in (select "run_id" from "drop_invalidated_assignment")
		),
		"drop_invalidated_run" as (
			delete from "run"
			where "run_id" in (select "run_id" from "drop_invalidated_assignment")
		),
		"alive_downstream" as (
			select count("run_id") as "n_downstream" from "run"
			where "run_id" in (select distinct "run_id" from "assign")
			and "status" != 'invalidated'
		),
		"alive_dataagents" as (
			select count("name") as "n_dataagents" from "data_agent"
			where "knit_id" in (select "knit_id" from "data")
		)
		select
			"status",
			"worker_name",
			"n_downstream",
			"n_dataagents",
			"c"."n_data"
		from "target"
		cross join "alive_downstream"
		cross join "alive_dataagents"
		cross join (select count("knit_id") as "n_data" from "data") as "c"
		`,
		// drop invalidated downstreams here.
		// transaction rollbacks them if the run shouldn't be truncated.
		runId,
	).Scan(
		(*kpgintr.KnitRunStatus)(&runStatus),
		&worker,
		&downstreams,
		&dataagents,
		nil, // discard "n_data". It is needed to lock "data" table sure.
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return kpgerr.Missing{
				Table:    "run",
				Identity: fmt.Sprintf("run_id=%s", runId),
			}
		}
		return err
	}

	if 0 < dataagents || worker != "" {
		return fmt.Errorf(
			"%w: runId = %s: data reading or writing is ongoing",
			kdb.ErrWorkerActive, runId,
		)
	}

	if 0 < downstreams {
		return fmt.Errorf("%w: runId = %s", kdb.ErrRunHasDownstreams, runId)
	}

	knitIds := []string{}
	rows, err := tx.Query(
		ctx,
		`
		with
		"outputs" as (
			select "knit_id" from "data" where "run_id" = $1
		),
		"drop_tags" as (
			delete from "tag_data" where "knit_id" in (select "knit_id" from "outputs")
		),
		"drop_timestamps" as (
			delete from "knit_timestamp" where "knit_id" in (select "knit_id" from "outputs")
		)
		select "knit_id" from "outputs"
		`,
		runId,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var knitId string
		if err := rows.Scan(&knitId); err != nil {
			return err
		}
		knitIds = append(knitIds, knitId)
	}

	if err := m.nominator.DropData(ctx, tx, knitIds); err != nil {
		return err
	}

	if _, err := tx.Exec(
		ctx,
		`
		with "data" as (
			delete from "data" where "knit_id" = any($1)
			returning "knit_id", "volume_ref"
		)
		insert into "garbage" ("knit_id", "volume_ref")
		select "knit_id", "volume_ref" from "data"
		`,
		knitIds,
	); err != nil {
		return err
	}
	return nil
}
