// manupirate records to postgres.
package tables

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/jackc/pgconn"

	kdb "github.com/opst/knitfab/pkg/db"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
)

func withCause(v any, reason error) error {
	return fmt.Errorf("error caused inserting record %+v: %w", v, reason)
}

// table-level operations for PostgreSQL.
//
// Note: this package DOES NOT verify/guarantee consistencies of records.
//
// naming convention:
//
//	method of Tables are named according convention below:
//
//	- `Insert...` : insert a record into a table
//	    for example, `InsertData` inserts a record into `"data"` table only.
//	    (So, it will cause error when no `"knit_id"` record for that does not exist. Baware.)
//	- `Register...` : insert record*s* into tables if needed.
//	    for example, `RegisterData` inserts a record into `"data"` table (main effect)
//	    and `"knit_id"` table (side effect; if needed to insert into `"data"`).
type Tables struct {
	ctx  context.Context
	pool kpool.Pool
}

func New(ctx context.Context, pool kpool.Pool) *Tables {
	return &Tables{
		ctx: ctx, pool: pool,
	}
}

func (f *Tables) acquire() (kpool.Conn, error) {
	return f.pool.Acquire(f.ctx)
}

func shouldEffect(ctag pgconn.CommandTag, require int) error {
	aff := ctag.RowsAffected()
	if int64(require) <= aff {
		return nil
	}
	_, file, line, ok := runtime.Caller(1)
	if ok {
		return fmt.Errorf("added rows are not enough @ %s:%d", file, line)
	} else {
		return errors.New("added rows are not enough")
	}
}

func (f *Tables) InsertKnitId(knitId string) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`insert into "knit_id" ("knit_id") values ($1)`,
		knitId,
	)
	if err != nil {
		return withCause(struct{ KnitId string }{KnitId: knitId}, err)
	}
	return shouldEffect(ctag, 1)
}

// insert a record into `data` table only.
//
// When you want to insert `data` and `knit_id` table, use `RegisterData` method.
func (f *Tables) InsertData(d *Data) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`
		insert into "data" ("knit_id", "volume_ref", "plan_id", "run_id", "output_id")
		values ($1, $2, $3, $4, $5)
		`,
		d.KnitId, d.VolumeRef, d.PlanId, d.RunId, d.OutputId,
	)
	if err != nil {
		return withCause(d, err)
	}

	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertDataTimestamp(dt *DataTimeStamp) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`insert into "knit_timestamp" ("knit_id", "timestamp") values ($1, $2);`,
		dt.KnitId, dt.Timestamp,
	)

	if err != nil {
		return withCause(dt, err)
	}

	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertTagKey(tk *TagKey) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`insert into "tag_key" ("id", "key") values ($1, $2);`,
		tk.Id, tk.Key,
	)
	if err != nil {
		return withCause(tk, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertTag(tag *TagValue) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`insert into "tag" ("key_id", "id", "value") values ($1, $2, $3);`,
		tag.KeyId, tag.Id, tag.Value,
	)
	if err != nil {
		return withCause(tag, err)
	}

	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertTagData(knitId string, tagId int) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ret, err := conn.Exec(
		f.ctx,
		`insert into "tag_data" ("tag_id", "knit_id") values ($1, $2);`,
		tagId, knitId,
	)
	if err != nil {
		return withCause(struct {
			KnitId string
			TagId  int
		}{KnitId: knitId, TagId: tagId}, err)
	}
	return shouldEffect(ret, 1)
}

func (f *Tables) InsertGarbage(garbage *Garbage) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`insert into "garbage" ("knit_id", "volume_ref") values ($1, $2);`,
		garbage.KnitId, garbage.VolumeRef,
	)
	if err != nil {
		return withCause(garbage, err)
	}

	return shouldEffect(ctag, 1)
}

// put tags for data.
//
// this function inserts into tables of
//   - "tag" (if needed)
//   - "tag_key" (if needed)
//   - "tag_data"
//
// NOTE: This function DO NOT VALIDATION for tag keys.
//
// args:
//   - knitId: knit id to be put tag on
//   - usertags: tags to be put on data.
func (f *Tables) RegisterUserTagsForData(knitId string, usertags []kdb.Tag) error {

	tagIds := map[int]struct{}{}
	for _, ut := range usertags {
		tid, err := f.RegisterTag(&ut)
		if err != nil {
			return err
		}
		tagIds[tid] = struct{}{}
	}

	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	for tid := range tagIds {
		if err := f.InsertTagData(knitId, tid); err != nil {
			return err
		}
	}

	return nil
}

// insert tag key & tag pair if needed, and retreive tag id.
//
// returns:
//   - int : tag id corresponds given `*kdb.Tag`.
//     if such tag does not exist, it insert the tag silenly, and returns its id.
//     otherwise, find such tag and retreive its id.
func (f *Tables) RegisterTag(t *kdb.Tag) (int, error) {
	conn, err := f.acquire()
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	var tagId int
	err = conn.QueryRow(
		f.ctx,
		`
		with
		"new_tag_key" as (
			insert into "tag_key" ("key") values ($1)
			on conflict do nothing
			returning "id"
		),
		"tag_key" as (
			select "id" from "tag_key" where "key" = $1
			union
			select "id" from "new_tag_key"
			limit 1
		),
		"new_tag" as (
			insert into "tag" ("key_id", "value")
			select "id" as "key_id", $2 as "value" from "tag_key"
			on conflict do nothing
			returning "id"
		)
		select "id" from "new_tag"
		union
		select "tag"."id" as "id" from "tag"
			inner join "tag_key" on "tag_key"."id" = "tag"."key_id"
		where "tag"."value" = $2
		limit 1
		`,
		t.Key, t.Value,
	).Scan(&tagId)

	if err != nil {
		return 0, withCause(t, err)
	}

	return tagId, nil
}

func (f *Tables) InsertPlan(p *Plan) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`
		insert into "plan" ("plan_id", "active", "hash")
		values ($1, $2, $3);
		`,
		p.PlanId, p.Active, p.Hash,
	)
	if err != nil {
		return withCause(p, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertPlanResource(res PlanResource) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`
		insert into "plan_resource" ("plan_id", "type", "value")
		values ($1, $2, $3);
		`,
		res.PlanId, res.Type, res.Value,
	)
	if err != nil {
		return withCause(res, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertPlanOnNode(p *PlanOnNode) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`
		insert into "plan_on_node" ("plan_id", "mode", "key", "value")
		values ($1, $2, $3, $4);
		`,
		p.PlanId, p.Mode, p.Key, p.Value,
	)
	if err != nil {
		return withCause(p, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertPlanImage(pi *PlanImage) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`
		insert into "plan_image" ("plan_id", "image", "version")
		values ($1, $2, $3)
		`,
		pi.PlanId, pi.Image, pi.Version,
	)
	if err != nil {
		return withCause(pi, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertPlanPseudo(pp *PlanPseudo) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`insert into "plan_pseudo" ("plan_id", "name") values ($1, $2);`,
		pp.PlanId, pp.Name,
	)
	if err != nil {
		return err
	}

	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertInput(in *Input) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`insert into "input" ("input_id", "plan_id", "path") values ($1, $2, $3)`,
		in.InputId, in.PlanId, in.Path,
	)
	if err != nil {
		return withCause(in, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertOutput(out *Output) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`insert into "output" ("output_id", "plan_id", "path") values ($1, $2, $3)`,
		out.OutputId, out.PlanId, out.Path,
	)
	if err != nil {
		return withCause(out, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) SetAsLog(out *Output) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`insert into "log" ("output_id", "plan_id") values ($1, $2);`,
		out.OutputId, out.PlanId,
	)
	if err != nil {
		return withCause(out, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertTagInput(inputId int, tagId int) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ret, err := conn.Exec(
		f.ctx,
		`insert into "tag_input" ("tag_id", "input_id") values ($1, $2);`,
		tagId, inputId,
	)
	if err != nil {
		return withCause(struct {
			InputId int
			TagId   int
		}{InputId: inputId, TagId: tagId}, err)
	}
	return shouldEffect(ret, 1)
}
func (f *Tables) InsertTagOutput(outputId int, tagId int) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ret, err := conn.Exec(
		f.ctx,
		`insert into "tag_output" ("tag_id", "output_id") values ($1, $2);`,
		tagId, outputId,
	)
	if err != nil {
		return withCause(struct {
			OutputId int
			TagId    int
		}{OutputId: outputId, TagId: tagId}, err)
	}
	return shouldEffect(ret, 1)
}

func (f *Tables) RegisterUserTagsForInput(inputId int, usertag []kdb.Tag) error {

	tagIds := map[int]struct{}{}
	for _, ut := range usertag {
		tid, err := f.RegisterTag(&ut)
		if err != nil {
			return err
		}
		tagIds[tid] = struct{}{}
	}

	for tid := range tagIds {
		if err := f.InsertTagInput(inputId, tid); err != nil {
			return err
		}
	}

	return nil
}

func (f *Tables) RegisterUserTagsForOutput(outputId int, usertag []kdb.Tag) error {

	tagIds := map[int]struct{}{}
	for _, ut := range usertag {
		tid, err := f.RegisterTag(&ut)
		if err != nil {
			return err
		}
		tagIds[tid] = struct{}{}
	}

	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	for tid := range tagIds {
		if err := f.InsertTagOutput(outputId, tid); err != nil {
			return err
		}
	}

	return nil
}

func (f *Tables) InsertKnitIdInput(inputId int, knitId string) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`insert into "knitid_input" ("knit_id", "input_id") values ($1, $2)`,
		knitId, inputId,
	)
	if err != nil {
		return withCause(struct {
			InputId int
			KnitId  string
		}{InputId: inputId, KnitId: knitId}, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertTimestampInput(inputId int, timestamp time.Time) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`insert into "timestamp_input" ("timestamp", "input_id") values ($1, $2);`,
		timestamp, inputId,
	)
	if err != nil {
		return withCause(struct {
			InputId   int
			Timestamp time.Time
		}{InputId: inputId, Timestamp: timestamp}, err)
	}

	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertRun(r *Run) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`
		insert into "run"
		("run_id", "plan_id", "status", "lifecycle_suspend_until", "updated_at")
		values ($1, $2, $3, $4, $5);
		`,
		r.RunId, r.PlanId, r.Status, r.LifecycleSuspendUntil, r.UpdatedAt,
	)
	if err != nil {
		return withCause(r, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertRunExit(re *RunExit) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`
		insert into "run_exit" ("run_id", "exit_code", "message")
		values ($1, $2, $3);
		`,
		re.RunId, re.ExitCode, re.Message,
	)
	if err != nil {
		return withCause(re, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertAssign(assign *Assign) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`
		insert into "assign" ("run_id", "input_id", "knit_id", "plan_id")
		values ($1, $2, $3, $4);
		`,
		assign.RunId, assign.InputId, assign.KnitId, assign.PlanId,
	)

	if err != nil {
		return withCause(assign, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertWorker(w *Worker) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`insert into "worker" ("run_id", "name") values ($1, $2)`,
		w.RunId, w.Name,
	)

	if err != nil {
		return withCause(w, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertNomination(n *Nomination) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`insert into "nomination" ("knit_id", "input_id", "updated") values ($1, $2, $3)`,
		n.KnitId, n.InputId, n.Updated,
	)

	if err != nil {
		return withCause(n, err)
	}
	return shouldEffect(ctag, 1)
}

func (f *Tables) InsertDataAgent(da *DataAgent) error {
	conn, err := f.acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	ctag, err := conn.Exec(
		f.ctx,
		`
		insert into "data_agent"
		("name", "knit_id", "mode", "lifecycle_suspend_until") values
		($1, $2, $3, $4)
		`,
		da.Name, da.KnitId, da.Mode, da.LifecycleSuspendUntil,
	)

	if err != nil {
		return withCause(da, err)
	}
	return shouldEffect(ctag, 1)
}
