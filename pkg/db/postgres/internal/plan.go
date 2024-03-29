package internal

import (
	"context"
	"fmt"
	"time"

	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/db/postgres/marshal"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/tuple"
	"k8s.io/apimachinery/pkg/api/resource"
)

type pgPlanOnMode kdb.OnNodeMode

func (m pgPlanOnMode) String() string {
	return string(m)
}

func (m pgPlanOnMode) Value() (interface{}, error) {
	return string(m), nil
}

func (m *pgPlanOnMode) Scan(src interface{}) error {
	expr, ok := src.(string)
	if !ok {
		return fmt.Errorf("OnNodeMode: unexpected type: %T", src)
	}
	switch n := kdb.OnNodeMode(expr); n {
	case kdb.MayOnNode, kdb.MustOnNode, kdb.PreferOnNode:
		*m = pgPlanOnMode(n)
		return nil
	default:
		return fmt.Errorf("OnNodeMode: unexpected value: %s", expr)
	}
}

func GetPlanBody(ctx context.Context, conn kpool.Queryer, planIds []string) (map[string]kdb.PlanBody, error) {
	rows, err := conn.Query(
		ctx,
		`
		with
		"plan" as (
			select "plan_id", "active", "hash" from "plan" where "plan_id" = any($1)
		),
		"plan_image" as (
			select "plan_id", "image", "version" from "plan_image" where "plan_id" = any($1)
		),
		"plan_pseudo" as (
			select "plan_id", "name" from "plan_pseudo" where "plan_id" = any($1)
		)
		select
			"plan_id", "active", "hash",
			"image" is not null as "is_image", coalesce("image", ''), coalesce("version", ''),
			"name" is not null as "is_pseudo", coalesce("name", '')
		from "plan"
		left outer join "plan_image" using ("plan_id")
		left outer join "plan_pseudo" using ("plan_id")
		`,
		planIds,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[string]kdb.PlanBody{}
	for rows.Next() {
		var isImage, isPseudo bool
		plan := kdb.PlanBody{
			Resources: map[string]resource.Quantity{},
		}
		image := kdb.ImageIdentifier{}
		pseudoDetail := kdb.PseudoPlanDetail{}
		if err := rows.Scan(
			&plan.PlanId, &plan.Active, &plan.Hash,
			&isImage, &image.Image, &image.Version,
			&isPseudo, &pseudoDetail.Name,
		); err != nil {
			return nil, err
		}
		if isImage {
			plan.Image = &image
		}
		if isPseudo {
			plan.Pseudo = &pseudoDetail
		}

		result[plan.PlanId] = plan
	}

	on_node_rows, err := conn.Query(
		ctx,
		`
		select "plan_id", "mode", "key", "value" from "plan_on_node"
		where "plan_id" = any($1)
		`,
		planIds,
	)
	if err != nil {
		return nil, err
	}
	defer on_node_rows.Close()
	for on_node_rows.Next() {
		var planId, key, value string
		var mode *pgPlanOnMode
		if err := on_node_rows.Scan(&planId, &mode, &key, &value); err != nil {
			return nil, err
		}
		plan := result[planId]
		plan.OnNode = append(result[planId].OnNode, kdb.OnNode{
			Mode:  kdb.OnNodeMode(*mode),
			Key:   key,
			Value: value,
		})
		result[planId] = plan
	}

	resources_rows, err := conn.Query(
		ctx,
		`
		select "plan_id", "type", "value" from "plan_resource"
		where "plan_id" = any($1)
		`,
		planIds,
	)
	if err != nil {
		return nil, err
	}
	defer resources_rows.Close()

	for resources_rows.Next() {
		var planId, typ string
		var value marshal.ResourceQuantity
		if err := resources_rows.Scan(&planId, &typ, &value); err != nil {
			return nil, err
		}
		plan := result[planId]
		plan.Resources[typ] = resource.Quantity(value)
		result[planId] = plan
	}

	return result, nil
}

func GetPlan(ctx context.Context, conn kpool.Queryer, planId []string) (map[string]*kdb.Plan, error) {

	bodies, err := GetPlanBody(ctx, conn, planId)
	if err != nil {
		return nil, err
	}

	ins := map[string][]kdb.MountPoint{}
	{
		_ins, err := GetInputsForPlan(ctx, conn, planId)
		if err != nil {
			return nil, err
		}

		for planId, mps := range _ins {
			ins[planId] = utils.ValuesOf(mps)
		}
	}

	outs := map[string][]kdb.MountPoint{}
	logs := map[string]kdb.LogPoint{}
	{
		_outs, err := GetOutputsForPlan(ctx, conn, planId)
		if err != nil {
			return nil, err
		}
		for planId, ops := range _outs {
			for _, op := range ops {
				if op.ForLog {
					logs[planId] = kdb.LogPoint{
						Id: op.Id, Tags: op.Tags,
					}
					continue
				}

				outs[planId] = append(outs[planId], op.MountPoint)
			}
		}
	}

	result := map[string]*kdb.Plan{}
	for plid, body := range bodies {
		var log *kdb.LogPoint
		if l, ok := logs[plid]; ok {
			log = &l
		}
		result[plid] = &kdb.Plan{
			PlanBody: body,
			Inputs:   ins[plid],
			Outputs:  outs[plid],
			Log:      log,
		}
	}

	return result, nil
}

func GetInputs(
	ctx context.Context, conn kpool.Queryer, inputIds []int,
) (map[int]kdb.MountPoint, error) {
	bodies := map[int]kdb.MountPoint{}
	{
		rows, err := conn.Query(
			ctx,
			`
			select
				"input_id", "path"
			from "input"
			where "input_id" = any($1)
			`,
			inputIds,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			mp := kdb.MountPoint{}
			if err := rows.Scan(&mp.Id, &mp.Path); err != nil {
				return nil, err
			}
			bodies[mp.Id] = mp
		}
	}

	if len(bodies) == 0 {
		return bodies, nil
	}

	var tags map[int]*kdb.TagSet
	{
		_tags, err := getTagsOnInput(ctx, conn, inputIds)
		if err != nil {
			return nil, err
		}
		tags = _tags
	}

	for runId := range bodies {
		b := bodies[runId]
		b.Tags = tags[runId]
		bodies[runId] = b
	}

	return bodies, nil
}

type OutputPoint struct {
	kdb.MountPoint
	ForLog bool
}

func GetOutputs(ctx context.Context, conn kpool.Queryer, outputIds []int) (map[int]OutputPoint, error) {
	mps := map[int]OutputPoint{}
	{
		rows, err := conn.Query(
			ctx,
			`select
				"output_id", "path"::varchar, "log"."output_id" is not null
			from "output"
			left join "log" using("plan_id", "output_id")
			where "output_id" = any($1)`,
			outputIds,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var forLog bool
			mp := kdb.MountPoint{}
			if err := rows.Scan(&mp.Id, &mp.Path, &forLog); err != nil {
				return nil, err
			}
			mps[mp.Id] = OutputPoint{MountPoint: mp, ForLog: forLog}
		}
	}

	tags, err := getTagsOnOutput(ctx, conn, outputIds)
	if err != nil {
		return nil, err
	}
	for outputId := range tags {
		mp := mps[outputId]
		mp.Tags = tags[outputId]
		mps[outputId] = mp
	}

	return mps, nil
}

func getTagsOnInput(ctx context.Context, conn kpool.Queryer, inputIds []int) (map[int]*kdb.TagSet, error) {
	tags := map[int][]kdb.Tag{}
	{ // user tags
		rows, err := conn.Query(
			ctx,
			`
			with "tag_input" as (
				select "input_id", "tag_id" from "tag_input"
				where "input_id" = any($1)
			),
			"tagv" as (
				select "input_id", "tag"."value" as "tag", "key_id"
				from "tag"
				inner join "tag_input" on "tag_input"."tag_id" = "tag"."id"
			)
			select "input_id", "tag_key"."key", "tag"
			from "tag_key"
			inner join "tagv" on "tagv"."key_id" = "tag_key"."id"
			`,
			inputIds,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var iid int
			var key, tag string
			if err := rows.Scan(&iid, &key, &tag); err != nil {
				return nil, err
			}
			if t, err := kdb.NewTag(key, tag); err != nil {
				return nil, err
			} else {
				tags[iid] = append(tags[iid], t)
			}
		}
	}
	{ // knit#id
		rows, err := conn.Query(
			ctx,
			`
			select "input_id", "knit_id" from "knitid_input"
			where "input_id" = any($1)
			`,
			inputIds,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var iid int
			var knitId string
			if err := rows.Scan(&iid, &knitId); err != nil {
				return nil, err
			}
			if t, err := kdb.NewTag(kdb.KeyKnitId, knitId); err != nil {
				return nil, err
			} else {
				tags[iid] = append(tags[iid], t)
			}
		}
	}
	{ // knit#timestamp
		rows, err := conn.Query(
			ctx,
			`
			select "input_id", "timestamp" from "timestamp_input"
			where "input_id" = any($1)
			`,
			inputIds,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var iid int
			var timestamp time.Time
			if err := rows.Scan(&iid, &timestamp); err != nil {
				return nil, err
			}
			tags[iid] = append(tags[iid], kdb.NewTimestampTag(timestamp))
		}
	}

	ret := map[int]*kdb.TagSet{}
	for inputId := range tags {
		ret[inputId] = kdb.NewTagSet(tags[inputId])
	}

	return ret, nil
}

func getTagsOnOutput(ctx context.Context, conn kpool.Queryer, outputIds []int) (map[int]*kdb.TagSet, error) {
	rows, err := conn.Query(
		ctx,
		`
		with "tag_output" as (
			select "output_id", "tag_id" from "tag_output"
			where "output_id" = any($1)
		),
		"tagv" as (
			select "output_id", "tag"."value" as "tag", "key_id"
			from "tag"
			inner join "tag_output" on "tag_output"."tag_id" = "tag"."id"
		)
		select "output_id", "tag_key"."key", "tag"
		from "tag_key"
		inner join "tagv" on "tagv"."key_id" = "tag_key"."id"
		`,
		outputIds,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tags := map[int][]kdb.Tag{}
	for rows.Next() {
		var oid int
		var key, tag string
		if err := rows.Scan(&oid, &key, &tag); err != nil {
			return nil, err
		}
		if t, err := kdb.NewTag(key, tag); err != nil {
			return nil, err
		} else {
			tags[oid] = append(tags[oid], t)
		}
	}

	tss := map[int]*kdb.TagSet{}
	for outputId, ts := range tags {
		tss[outputId] = kdb.NewTagSet(ts)
	}

	return tss, nil
}

func GetInputsForPlan(
	ctx context.Context, conn kpool.Queryer, planIds []string,
) (map[string]map[int]kdb.MountPoint, error) {
	bodies := map[int]tuple.Pair[string, kdb.MountPoint]{}
	{
		rows, err := conn.Query(
			ctx,
			`
			select "plan_id", "input_id", "path" from "input"
			where "plan_id" = any($1::varchar[])
			`,
			planIds,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var planId string
			mp := kdb.MountPoint{}
			if err := rows.Scan(&planId, &mp.Id, &mp.Path); err != nil {
				return nil, err
			}
			bodies[mp.Id] = tuple.PairOf(planId, mp)
		}
	}

	if len(bodies) == 0 {
		// no plans has any inputs. return empty for each plan.
		rows, err := conn.Query(
			ctx,
			`select "plan_id" from "plan" where "plan_id" = any($1::varchar[])`,
			planIds,
		)
		if err != nil {
			return nil, err
		}
		ret := map[string]map[int]kdb.MountPoint{}
		defer rows.Close()
		for rows.Next() {
			var pid string
			if err := rows.Scan(&pid); err != nil {
				return nil, err
			}
			ret[pid] = map[int]kdb.MountPoint{}
		}
		return ret, nil
	}
	inputIds := utils.KeysOf(bodies)

	var tags map[int]*kdb.TagSet
	{
		_tags, err := getTagsOnInput(ctx, conn, inputIds)
		if err != nil {
			return nil, err
		}
		tags = _tags
	}

	// transpose index
	ret := map[string]map[int]kdb.MountPoint{}
	for runId := range bodies {
		planId, mp := bodies[runId].Decompose()
		mp.Tags = tags[runId]
		if _, ok := ret[planId]; !ok {
			ret[planId] = map[int]kdb.MountPoint{}
		}
		ret[planId][runId] = mp
	}

	return ret, nil
}

func GetOutputsForPlan(
	ctx context.Context, conn kpool.Queryer, planIds []string,
) (map[string]map[int]OutputPoint, error) {

	if len(planIds) == 0 {
		return map[string]map[int]OutputPoint{}, nil
	}

	planToOutputs := map[string][]int{}
	{
		// no plans has any outputs. return empty for each plan.
		rows, err := conn.Query(
			ctx,
			`select
				"plan_id", "output_id"
			from "output"
			where "plan_id" = any($1)`,
			planIds,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var pid string
			var oid int
			if err := rows.Scan(&pid, &oid); err != nil {
				return nil, err
			}
			planToOutputs[pid] = append(planToOutputs[pid], oid)
		}
	}

	outputs, err := GetOutputs(
		ctx, conn,
		utils.Concat(utils.ValuesOf(planToOutputs)...),
	)
	if err != nil {
		return nil, err
	}

	ret := map[string]map[int]OutputPoint{}

	for planId := range planToOutputs {
		os := map[int]OutputPoint{}

		for _, outputId := range planToOutputs[planId] {
			os[outputId] = outputs[outputId]
		}

		ret[planId] = os
	}

	return ret, nil
}
