package postgres

import (
	"context"
	"fmt"
	"time"

	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/slices"
	"github.com/opst/knitfab/pkg/utils/tuple"
	"k8s.io/apimachinery/pkg/api/resource"
)

type pgPlanOnMode domain.OnNodeMode

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
	switch n := domain.OnNodeMode(expr); n {
	case domain.MayOnNode, domain.MustOnNode, domain.PreferOnNode:
		*m = pgPlanOnMode(n)
		return nil
	default:
		return fmt.Errorf("OnNodeMode: unexpected value: %s", expr)
	}
}

func GetPlanBody(ctx context.Context, conn kpool.Queryer, planIds []string) (map[string]domain.PlanBody, error) {
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
		),
		"plan_entrypoint" as (
			select "plan_id", "entrypoint" from "plan_entrypoint" where "plan_id" = any($1)
		),
		"plan_args" as (
			select "plan_id", "args" from "plan_args" where "plan_id" = any($1)
		)
		select
			"plan_id", "active", "hash", "entrypoint", "args",
			"image" is not null as "is_image", coalesce("image", ''), coalesce("version", ''),
			"name" is not null as "is_pseudo", coalesce("name", ''), coalesce("service_account", '')
		from "plan"
		left outer join "plan_image" using ("plan_id")
		left outer join "plan_pseudo" using ("plan_id")
		left outer join "plan_service_account" using ("plan_id")
		left outer join "plan_entrypoint" using ("plan_id")
		left outer join "plan_args" using ("plan_id")
		`,
		planIds,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[string]domain.PlanBody{}
	for rows.Next() {
		var isImage, isPseudo bool
		plan := domain.PlanBody{
			Resources: map[string]resource.Quantity{},
		}
		image := domain.ImageIdentifier{}
		pseudoDetail := domain.PseudoPlanDetail{}
		if err := rows.Scan(
			&plan.PlanId, &plan.Active, &plan.Hash, &plan.Entrypoint, &plan.Args,
			&isImage, &image.Image, &image.Version,
			&isPseudo, &pseudoDetail.Name, &plan.ServiceAccount,
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
		plan.OnNode = append(result[planId].OnNode, domain.OnNode{
			Mode:  domain.OnNodeMode(*mode),
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
		var value ResourceQuantity
		if err := resources_rows.Scan(&planId, &typ, &value); err != nil {
			return nil, err
		}
		plan := result[planId]
		plan.Resources[typ] = resource.Quantity(value)
		result[planId] = plan
	}

	annotations_rows, err := conn.Query(
		ctx,
		`
		select "plan_id", "key", "value" from "plan_annotation"
		where "plan_id" = any($1)
		`,
		planIds,
	)
	if err != nil {
		return nil, err
	}
	defer annotations_rows.Close()

	for annotations_rows.Next() {
		var planId, key, value string
		if err := annotations_rows.Scan(&planId, &key, &value); err != nil {
			return nil, err
		}
		plan := result[planId]
		plan.Annotations = append(plan.Annotations, domain.Annotation{Key: key, Value: value})
		result[planId] = plan
	}

	return result, nil
}

func GetPlan(ctx context.Context, conn kpool.Queryer, planId []string) (map[string]*domain.Plan, error) {

	bodies, err := GetPlanBody(ctx, conn, planId)
	if err != nil {
		return nil, err
	}

	// inputs: planId -> []Input
	inputs := map[string][]domain.Input{}
	{
		_inputMps, err := GetInputMountpointsForPlan(ctx, conn, planId)
		if err != nil {
			return nil, err
		}

		upstreams := map[int][]domain.PlanUpstream{}

		{
			_inpitIds := []int{}
			for _, mps := range _inputMps {
				for _, mp := range mps {
					_inpitIds = append(_inpitIds, mp.Id)
				}
			}

			// upstreams: inputId -> [](planId, outputId)
			upstreamIds := map[int][]tuple.Pair[string, int]{}

			outputIds := map[int]struct{}{}

			ret, err := conn.Query(
				ctx,
				`
				with "inputs" as (
					select distinct unnest($1::int[]) as "input_id"
				),
				"i_tags" as (
					select "input_id", "tag_id" from "tag_input"
					where
							"input_id" in (select "input_id" from "inputs")
						-- output has no system tag. exclude them.
						and "input_id" not in (select "input_id" from "knitid_input")
						and "input_id" not in (select "input_id" from "timestamp_input")
				),
				"i_tags_count" as (
					select "input_id", count(*) as "count" from "i_tags" group by "input_id"
				),
				"o_tags" as (
					select "output_id", "tag_id" from "tag_output"
					where "tag_id" in (select distinct "tag_id" from "i_tags")
				),
				"shared_tags_count" as (
					select "input_id", "output_id", count(*) as "count"
					from "i_tags"
					inner join "o_tags" using ("tag_id")
					group by "input_id", "output_id"
				),
				"match" as (
					select distinct
						"i_tags_count"."input_id" as "input_id",
						"output_id"
					from "shared_tags_count"
					inner join "i_tags_count" on
						"i_tags_count"."input_id" = "shared_tags_count"."input_id"
					and "i_tags_count"."count"    = "shared_tags_count"."count"
				)
				select
					"input_id",
					"plan_id" as "output_plan_id",
					"output_id" as "upstream_id"
				from "match"
				inner join "output" using ("output_id")
				`,
				_inpitIds,
			)
			if err != nil {
				return nil, err
			}

			defer ret.Close()
			for ret.Next() {
				var inputId, upstreamId int
				var planId string
				if err := ret.Scan(&inputId, &planId, &upstreamId); err != nil {
					return nil, err
				}
				upstreamIds[inputId] = append(upstreamIds[inputId], tuple.PairOf(planId, upstreamId))
				outputIds[upstreamId] = struct{}{}
			}

			_outputs, err := GetOutputs(ctx, conn, slices.KeysOf(outputIds))
			if err != nil {
				return nil, err
			}

			for inputId, planIdAndUpstreamIds := range upstreamIds {
				upstreams[inputId] = slices.Map(
					planIdAndUpstreamIds,
					func(planIdAndUpstreamId tuple.Pair[string, int]) domain.PlanUpstream {
						planId, upstreamId := planIdAndUpstreamId.Decompose()
						output := _outputs[upstreamId]

						u := domain.PlanUpstream{PlanId: planId}

						if output.ForLog {
							u.Log = &domain.LogPoint{
								Id:   output.MountPoint.Id,
								Tags: output.MountPoint.Tags,
							}
						} else {
							u.Mountpoint = &output.MountPoint
						}

						return u
					},
				)
			}

		}

		for planId, mps := range _inputMps {
			inputs[planId] = slices.Map(
				slices.ValuesOf(mps),
				func(mp domain.MountPoint) domain.Input {
					return domain.Input{
						MountPoint: mp,
						Upstreams:  upstreams[mp.Id],
					}
				},
			)
		}
	}

	// outputs: planId -> []Output
	outputs := map[string][]domain.Output{}

	// logs: planId -> Log
	logs := map[string]domain.LogPoint{}

	{
		_outputMps, err := GetOutputMountpointsForPlan(ctx, conn, planId)
		if err != nil {
			return nil, err
		}

		downstreams := map[int][]domain.PlanDownstream{}

		{
			_outputIds := []int{}
			for _, mps := range _outputMps {
				for _, mp := range mps {
					_outputIds = append(_outputIds, mp.Id)
				}
			}

			// downstreams: outputId -> [](planId, inputId)
			downstreamIds := map[int][]tuple.Pair[string, int]{}

			inputIds := map[int]struct{}{}

			ret, err := conn.Query(
				ctx,
				`
				with "query" as (
					select distinct unnest($1::int[]) as "output_id"
				),
				"o_tags" as (
					select "output_id", "tag_id" from "tag_output"
					where "output_id" in (select "output_id" from "query")
				),
				"_i_tags" as (
					-- filter out inputs have no tags in common with the outputs
					select "input_id", "tag_id" from "tag_input"
					where "input_id" in (
						select distinct "input_id" from "tag_input"
						where "tag_id" in (select distinct "tag_id" from "o_tags")
					)
				),
				"i_tags" as (
					select "input_id", "tag_id" from "_i_tags"
					where "input_id" not in (select "input_id" from "knitid_input")
					and "input_id" not in (select "input_id" from "timestamp_input")
				),
				"i_tags_count" as (
					select "input_id", count(*) as "count" from "i_tags" group by "input_id"
				),
				"shared_tags_count" as (
					select "output_id", "input_id", count(*) as "count"
					from "o_tags"
					inner join "i_tags" using ("tag_id")
					group by "output_id", "input_id"
				),
				"match" as (
					select distinct
						"i_tags_count"."input_id" as "input_id",
						"output_id"
					from "shared_tags_count"
					inner join "i_tags_count" on
							"i_tags_count"."input_id" = "shared_tags_count"."input_id"
						and "i_tags_count"."count"    = "shared_tags_count"."count"
				)
				select
					"output_id",
					"plan_id" as "input_plan_id",
					"input_id" as "downstream_id"
				from "match"
				inner join "input" using ("input_id")
				`,
				_outputIds,
			)
			if err != nil {
				return nil, err
			}
			defer ret.Close()

			for ret.Next() {
				var outputId, downstreamId int
				var planId string
				if err := ret.Scan(&outputId, &planId, &downstreamId); err != nil {
					return nil, err
				}
				downstreamIds[outputId] = append(downstreamIds[outputId], tuple.PairOf(planId, downstreamId))
				inputIds[downstreamId] = struct{}{}
			}

			_inputs, err := GetInputs(ctx, conn, slices.KeysOf(inputIds))
			if err != nil {
				return nil, err
			}

			for outputId, planIdAndInputIds := range downstreamIds {
				downstreams[outputId] = slices.Map(
					planIdAndInputIds,
					func(planIdAndDownstreamId tuple.Pair[string, int]) domain.PlanDownstream {
						planId, downstreamId := planIdAndDownstreamId.Decompose()
						input := _inputs[downstreamId]
						return domain.PlanDownstream{
							PlanId:     planId,
							Mountpoint: input,
						}
					},
				)
			}
		}

		for planId, ops := range _outputMps {
			for _, op := range ops {
				dss := downstreams[op.Id]

				if op.ForLog {
					logs[planId] = domain.LogPoint{
						Id: op.Id, Tags: op.Tags,
						Downstreams: dss,
					}
					continue
				}

				outputs[planId] = append(
					outputs[planId], domain.Output{
						MountPoint:  op.MountPoint,
						Downstreams: dss,
					},
				)
			}
		}
	}

	result := map[string]*domain.Plan{}
	for plid, body := range bodies {
		var log *domain.LogPoint
		if l, ok := logs[plid]; ok {
			log = &l
		}
		result[plid] = &domain.Plan{
			PlanBody: body,
			Inputs:   inputs[plid],
			Outputs:  outputs[plid],
			Log:      log,
		}
	}

	return result, nil
}

func GetInputs(
	ctx context.Context, conn kpool.Queryer, inputIds []int,
) (map[int]domain.MountPoint, error) {
	bodies := map[int]domain.MountPoint{}
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
			mp := domain.MountPoint{}
			if err := rows.Scan(&mp.Id, &mp.Path); err != nil {
				return nil, err
			}
			bodies[mp.Id] = mp
		}
	}

	if len(bodies) == 0 {
		return bodies, nil
	}

	var tags map[int]*domain.TagSet
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
	domain.MountPoint
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
			mp := domain.MountPoint{}
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

func getTagsOnInput(ctx context.Context, conn kpool.Queryer, inputIds []int) (map[int]*domain.TagSet, error) {
	tags := map[int][]domain.Tag{}
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
			if t, err := domain.NewTag(key, tag); err != nil {
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
			if t, err := domain.NewTag(domain.KeyKnitId, knitId); err != nil {
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
			tags[iid] = append(tags[iid], domain.NewTimestampTag(timestamp))
		}
	}

	ret := map[int]*domain.TagSet{}
	for inputId := range tags {
		ret[inputId] = domain.NewTagSet(tags[inputId])
	}

	return ret, nil
}

func getTagsOnOutput(ctx context.Context, conn kpool.Queryer, outputIds []int) (map[int]*domain.TagSet, error) {
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

	tags := map[int][]domain.Tag{}
	for rows.Next() {
		var oid int
		var key, tag string
		if err := rows.Scan(&oid, &key, &tag); err != nil {
			return nil, err
		}
		if t, err := domain.NewTag(key, tag); err != nil {
			return nil, err
		} else {
			tags[oid] = append(tags[oid], t)
		}
	}

	tss := map[int]*domain.TagSet{}
	for outputId, ts := range tags {
		tss[outputId] = domain.NewTagSet(ts)
	}

	return tss, nil
}

func GetInputMountpointsForPlan(
	ctx context.Context, conn kpool.Queryer, planIds []string,
) (map[string]map[int]domain.MountPoint, error) {
	bodies := map[int]tuple.Pair[string, domain.MountPoint]{}
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
			mp := domain.MountPoint{}
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
		ret := map[string]map[int]domain.MountPoint{}
		defer rows.Close()
		for rows.Next() {
			var pid string
			if err := rows.Scan(&pid); err != nil {
				return nil, err
			}
			ret[pid] = map[int]domain.MountPoint{}
		}
		return ret, nil
	}
	inputIds := slices.KeysOf(bodies)

	var tags map[int]*domain.TagSet
	{
		_tags, err := getTagsOnInput(ctx, conn, inputIds)
		if err != nil {
			return nil, err
		}
		tags = _tags
	}

	// transpose index
	ret := map[string]map[int]domain.MountPoint{}
	for runId := range bodies {
		planId, mp := bodies[runId].Decompose()
		mp.Tags = tags[runId]
		if _, ok := ret[planId]; !ok {
			ret[planId] = map[int]domain.MountPoint{}
		}
		ret[planId][runId] = mp
	}

	return ret, nil
}

func GetOutputMountpointsForPlan(
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
		slices.Concat(slices.ValuesOf(planToOutputs)...),
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
