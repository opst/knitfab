package tables

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	kdb "github.com/opst/knitfab/pkg/db"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
)

type InputAttr struct {
	UserTag   []kdb.Tag
	Timestamp []time.Time
	KnitId    []string
}

type OutputAttr struct {
	IsLog   bool
	UserTag []kdb.Tag
}

type DataAttibutes struct {
	UserTag   []kdb.Tag
	Timestamp *time.Time
	Agent     []DataAgent
}

// one Step of Lineage
type Step struct {
	Run      Run
	Assign   []Assign
	Outcomes map[Data]DataAttibutes
	Worker   string
	Exit     *RunExit
}

func (step *Step) Apply(ctx context.Context, pool kpool.Pool) error {
	tbls := New(ctx, pool)
	return step.apply(tbls)
}

func (step *Step) apply(tbls *Tables) error {
	if err := tbls.InsertRun(&step.Run); err != nil {
		return err
	}
	for _, a := range step.Assign {
		if err := tbls.InsertAssign(&a); err != nil {
			return err
		}
	}
	if step.Worker != "" {
		if err := tbls.InsertWorker(
			&Worker{RunId: step.Run.RunId, Name: step.Worker},
		); err != nil {
			return err
		}
	}

	for d, tags := range step.Outcomes {
		if err := tbls.InsertKnitId(d.KnitId); err != nil {
			return err
		}
		if err := tbls.InsertData(&d); err != nil {
			return err
		}
		if err := tbls.RegisterUserTagsForData(d.KnitId, tags.UserTag); err != nil {
			return err
		}
		if tags.Timestamp != nil {
			if err := tbls.InsertDataTimestamp(
				&DataTimeStamp{KnitId: d.KnitId, Timestamp: *tags.Timestamp},
			); err != nil {
				return err
			}
		}
		for _, agent := range tags.Agent {
			if err := tbls.InsertDataAgent(&agent); err != nil {
				return err
			}
		}
	}

	if step.Exit != nil {
		if err := tbls.InsertRunExit(step.Exit); err != nil {
			return err
		}
	}

	return nil
}

// Declare premise of test.
type Operation struct {
	Plan          []Plan
	PlanResources []PlanResource
	OnNode        []PlanOnNode
	PlanImage     []PlanImage
	PlanPseudo    []PlanPseudo
	Inputs        map[Input]InputAttr
	Outputs       map[Output]OutputAttr

	Steps []Step

	Nomination []Nomination
	Garbage    []Garbage

	// insert tags
	//
	// If tags do not appear here, tags in Steps, Inputs, Outputs are also inserted.
	//
	// Use this field to insert tags existing but not used.
	Tags []kdb.Tag
}

func (prem *Operation) Apply(ctx context.Context, pool kpool.Pool) error {
	tbls := New(ctx, pool)

	for _, p := range prem.Plan {
		if err := tbls.InsertPlan(&p); err != nil {
			return err
		}
	}

	for _, res := range prem.PlanResources {
		if err := tbls.InsertPlanResource(res); err != nil {
			return err
		}
	}

	for _, im := range prem.PlanImage {
		if err := tbls.InsertPlanImage(&im); err != nil {
			return err
		}
	}

	for _, ps := range prem.PlanPseudo {
		if err := tbls.InsertPlanPseudo(&ps); err != nil {
			return err
		}
	}

	for _, on := range prem.OnNode {
		if err := tbls.InsertPlanOnNode(&on); err != nil {
			return err
		}
	}

	for in, tags := range prem.Inputs {
		if err := tbls.InsertInput(&in); err != nil {
			return err
		}
		if err := tbls.RegisterUserTagsForInput(in.InputId, tags.UserTag); err != nil {
			return err
		}
		for _, kid := range tags.KnitId {
			if err := tbls.InsertKnitIdInput(in.InputId, kid); err != nil {
				return err
			}
		}
		for _, timestamp := range tags.Timestamp {
			if err := tbls.InsertTimestampInput(in.InputId, timestamp); err != nil {
				return err
			}
		}
	}

	for out, attr := range prem.Outputs {
		if err := tbls.InsertOutput(&out); err != nil {
			return err
		}
		if err := tbls.RegisterUserTagsForOutput(out.OutputId, attr.UserTag); err != nil {
			return err
		}
		if attr.IsLog {
			if err := tbls.SetAsLog(&out); err != nil {
				return err
			}
		}
	}

	for nth, step := range prem.Steps {
		if err := step.apply(tbls); err != nil {
			return fmt.Errorf("@ step#%d: %w", nth, err)
		}
	}

	for _, nom := range prem.Nomination {
		if err := tbls.InsertNomination(&nom); err != nil {
			return err
		}
	}

	for _, gab := range prem.Garbage {
		if err := tbls.InsertKnitId(gab.KnitId); err != nil {
			if pgerr := new(pgconn.PgError); !errors.As(err, &pgerr) || pgerr.Code != pgerrcode.UniqueViolation {
				return err
			}
		}
		if err := tbls.InsertGarbage(&gab); err != nil {
			return err
		}
	}

	for _, tag := range prem.Tags {
		if _, err := tbls.RegisterTag(&tag); err != nil {
			return err
		}
	}

	return nil
}
