package postgres

import (
	"context"

	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/tuple"
)

// Args
//
// - context.Context
//
// - Queryer
//
// - []string : interested knitIds
//
// # Return
//
// - map[string][]kdb.Nomnation:
// mapping knitId -> its nominations.
//
// - error
func GetNominationByKnitId(
	ctx context.Context, conn kpool.Queryer, knitId []string,
) (map[string][]domain.Nomination, error) {

	rows, err := conn.Query(
		ctx,
		`
		with "nom" as (
			select "knit_id", "input_id" from "nomination" where "knit_id" = ANY($1)
		)
		select "knit_id", "plan_id", "input_id" from "input"
		inner join "nom" using("input_id")
		`,
		knitId,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	mountpointIds := map[int]struct{}{}
	planIds := map[string]struct{}{}

	// knitId -> (planId, mountpoindId)
	index := map[string]tuple.Pair[string, int]{}

	for rows.Next() {
		var knitId, planId string
		var mpid int
		if err := rows.Scan(&knitId, &planId, &mpid); err != nil {
			return nil, err
		}
		planIds[planId] = struct{}{}
		mountpointIds[mpid] = struct{}{}
		index[knitId] = tuple.PairOf(planId, mpid)
	}

	plans, err := GetPlanBody(ctx, conn, utils.KeysOf(planIds))
	if err != nil {
		return nil, err
	}

	mps, err := GetInputs(ctx, conn, utils.KeysOf(mountpointIds))
	if err != nil {
		return nil, err
	}

	result := map[string][]domain.Nomination{}

	for knitId, p := range index {
		planId, mpid := p.Decompose()
		result[knitId] = append(
			result[knitId],
			domain.Nomination{
				MountPoint: mps[mpid],
				PlanBody:   plans[planId],
			},
		)
	}

	return result, nil
}
