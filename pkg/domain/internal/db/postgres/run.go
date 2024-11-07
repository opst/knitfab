package postgres

import (
	"context"
	"fmt"
	"time"

	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/slices"
)

type KnitRunStatus domain.KnitRunStatus

// implement sql.Scanner
func (krs *KnitRunStatus) Scan(v any) error {

	var s string
	switch vv := v.(type) {
	case string:
		s = vv
	case []byte:
		s = string(vv)
	default:
		return fmt.Errorf("parse error for KnitRunStatus: %#v", v)
	}

	parsed, err := domain.AsKnitRunStatus(s)
	if err != nil {
		return err
	}
	*krs = KnitRunStatus(parsed)
	return nil
}

// runDescriptor is half-baked RunBody.
//
// It is used for building Run or RunBody.
type runDescriptor struct {
	Id         string
	Status     KnitRunStatus
	WorkerName string
	UpdatedAt  time.Time
	PlanId     string
}

func getRunDescriptors(ctx context.Context, conn kpool.Queryer, runIds []string) (map[string]runDescriptor, error) {
	result := map[string]runDescriptor{}
	rows, err := conn.Query(
		ctx,
		`
		select
			"run_id", "status", "updated_at",
			"plan_id", coalesce("worker"."name", '')
		from "run"
		left outer join "worker" using ("run_id")
		where "run_id" = ANY($1)
		`,
		runIds,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var rd runDescriptor
		if err := rows.Scan(
			&rd.Id, &rd.Status, &rd.UpdatedAt, &rd.PlanId, &rd.WorkerName,
		); err != nil {
			return nil, err
		}
		result[rd.Id] = rd
	}

	return result, nil
}

// get RunBody by runId
//
// # Args
//
// - context.Context
//
// - Queryer
//
// - []string : runIds to query
//
// # Return
//
// - map[string]kdb.RunBody : mapping from runId to kdb.RunBody
//
// - error
func GetRunBody(ctx context.Context, conn kpool.Queryer, runIds []string) (map[string]domain.RunBody, error) {

	runDescriptors, err := getRunDescriptors(ctx, conn, runIds)
	if err != nil {
		return nil, err
	}
	var planIds []string
	{
		pids := map[string]struct{}{}
		for _, rd := range runDescriptors {
			pids[rd.PlanId] = struct{}{}
		}
		planIds = slices.KeysOf(pids)
	}
	planBodies, err := GetPlanBody(ctx, conn, planIds)
	if err != nil {
		return nil, err
	}

	rows, err := conn.Query(
		ctx,
		`
		select "run_id", "exit_code", "message" from "run_exit"
		where "run_id" = any($1)
		`,
		runIds,
	)
	if err != nil {
		return nil, err
	}

	runExits := map[string]domain.RunExit{}

	defer rows.Close()
	for rows.Next() {
		var runId string
		exit := domain.RunExit{}
		if err := rows.Scan(&runId, &exit.Code, &exit.Message); err != nil {
			return nil, err
		}
		runExits[runId] = exit
	}

	result := map[string]domain.RunBody{}
	for _, rd := range runDescriptors {
		var exit *domain.RunExit
		if e, ok := runExits[rd.Id]; ok {
			exit = &e
		}
		result[rd.Id] = domain.RunBody{
			Id:         rd.Id,
			Status:     domain.KnitRunStatus(rd.Status),
			Exit:       exit,
			WorkerName: rd.WorkerName,
			UpdatedAt:  rd.UpdatedAt,
			PlanBody:   planBodies[rd.PlanId],
		}
	}
	return result, nil
}

func GetRun(ctx context.Context, conn kpool.Queryer, runIds []string) (map[string]domain.Run, error) {

	runBodies, err := GetRunBody(ctx, conn, runIds)
	if err != nil {
		return nil, err
	}
	planIds := map[string]struct{}{}
	for _, rd := range runBodies {
		planIds[rd.PlanId] = struct{}{}
	}

	inputMPs, err := GetInputMountpointsForPlan(ctx, conn, slices.KeysOf(planIds))
	if err != nil {
		return nil, err
	}

	outputMPs, err := GetOutputMountpointsForPlan(ctx, conn, slices.KeysOf(planIds))
	if err != nil {
		return nil, err
	}

	// runId -> inputId -> knit id
	assignment_in := map[string]map[int]string{}
	// runId -> outputId -> knit id
	assignment_out := map[string]map[int]string{}
	// runId -> knit id
	assignment_log := map[string]string{}
	// knit id -> knit data body
	var dataBodies map[string]domain.KnitDataBody
	{
		knitIds := map[string]struct{}{}

		rows_in, err := conn.Query(
			ctx,
			`
			select "run_id", "input_id", "knit_id" from "assign"
			where "run_id" = any($1)
			`,
			runIds,
		)
		if err != nil {
			return nil, err
		}
		defer rows_in.Close()
		for rows_in.Next() {
			var runId, knitId string
			var inputId int

			if err := rows_in.Scan(&runId, &inputId, &knitId); err != nil {
				return nil, err
			}
			knitIds[knitId] = struct{}{}
			run, ok := assignment_in[runId]
			if !ok {
				run = map[int]string{}
			}
			run[inputId] = knitId
			assignment_in[runId] = run
		}

		rows_out, err := conn.Query(
			ctx,
			`
			select
				"run_id",
				"output_id",
				"knit_id",
				"log"."output_id" is not null
			from "data"
			left join "log" using ("plan_id", "output_id")
			where "run_id" = any($1)
			`,
			runIds,
		)
		if err != nil {
			return nil, err
		}
		defer rows_out.Close()

		for rows_out.Next() {
			var runId, knitId string
			var outputId int
			var isLog bool
			if err := rows_out.Scan(&runId, &outputId, &knitId, &isLog); err != nil {
				return nil, err
			}
			knitIds[knitId] = struct{}{}

			if isLog {
				assignment_log[runId] = knitId
			} else {
				outputs, ok := assignment_out[runId]
				if !ok {
					outputs = map[int]string{}
				}
				outputs[outputId] = knitId
				assignment_out[runId] = outputs
			}
		}

		b, err := GetDataBody(ctx, conn, slices.KeysOf(knitIds))
		if err != nil {
			return nil, err
		}
		dataBodies = b
	}

	// zip-up all!
	result := map[string]domain.Run{}
	for runId := range runBodies {
		rb := runBodies[runId]

		in := []domain.Assignment{}
		out := []domain.Assignment{}
		var log *domain.Log

		for _, mp := range inputMPs[rb.PlanId] {
			knitId, ok := assignment_in[rb.Id][mp.Id]
			if !ok {
				in = append(in, domain.Assignment{MountPoint: mp})
			} else {
				in = append(in, domain.Assignment{
					MountPoint:   mp,
					KnitDataBody: dataBodies[knitId],
				})
			}
		}

		for _, mp := range outputMPs[rb.PlanId] {

			if mp.ForLog {
				knitId, ok := assignment_log[rb.Id]
				log = &domain.Log{Id: mp.Id, Tags: mp.Tags}
				if ok {
					log.KnitDataBody = dataBodies[knitId]
				}
				continue
			}

			knitId, ok := assignment_out[rb.Id][mp.Id]
			a := domain.Assignment{MountPoint: mp.MountPoint}
			if ok {
				a.KnitDataBody = dataBodies[knitId]
			}
			out = append(out, a)
		}

		result[rb.Id] = domain.Run{
			Inputs: in, Outputs: out, Log: log,
			RunBody: rb,
		}
	}

	return result, nil
}
