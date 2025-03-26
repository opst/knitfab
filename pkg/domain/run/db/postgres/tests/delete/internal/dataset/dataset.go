package dataset

import (
	"time"

	"github.com/opst/knitfab-api-types/v2/misc/rfctime"
	"github.com/opst/knitfab/v2/pkg/domain"
	"github.com/opst/knitfab/v2/pkg/domain/internal/db/postgres/tables"
	th "github.com/opst/knitfab/v2/pkg/domain/internal/db/postgres/testhelpers"
)

var UPLOADED_AT time.Time

func init() {
	_UPLOADED_AT, err := rfctime.ParseRFC3339DateTime(
		"2022-11-12T13:14:15.789+09:00",
	)
	if err != nil {
		panic(err)
	}
	UPLOADED_AT = _UPLOADED_AT.Time()
}

var GivenDatabase = tables.Operation{
	Plan: []tables.Plan{
		{PlanId: th.Padding36("plan-pseudo"), Active: true, Hash: th.Padding64("#plan-pseudo")},
		{PlanId: th.Padding36("plan-gen2"), Active: true, Hash: th.Padding64("#plan-gen2")},
		{PlanId: th.Padding36("plan-gen3"), Active: true, Hash: th.Padding64("#plan-gen3")},
	},
	PlanPseudo: []tables.PlanPseudo{
		{PlanId: th.Padding36("plan-pseudo"), Name: "knit#uploaded"},
	},
	PlanImage: []tables.PlanImage{
		{PlanId: th.Padding36("plan-gen2"), Image: "repo.invalid/gen2", Version: "v1.1"},
		{PlanId: th.Padding36("plan-gen3"), Image: "repo.invalid/gen3", Version: "v1.1"},
	},
	Inputs: map[tables.Input]tables.InputAttr{
		{InputId: 2_100, PlanId: th.Padding36("plan-gen2"), Path: "/in/1"}: {},
		{InputId: 3_100, PlanId: th.Padding36("plan-gen3"), Path: "/in/1"}: {},
	},
	Outputs: map[tables.Output]tables.OutputAttr{
		{OutputId: 1_010, PlanId: th.Padding36("plan-pseudo"), Path: "/out/1"}: {},
		{OutputId: 2_010, PlanId: th.Padding36("plan-gen2"), Path: "/out/1"}:   {},
		{OutputId: 3_010, PlanId: th.Padding36("plan-gen3"), Path: "/out/1"}:   {},
	},
	Steps: []tables.Step{
		// These Runs are predefiend and should not be deleted.
		{
			Run: tables.Run{
				RunId:     th.Padding36("gen1/predefined-1"),
				PlanId:    th.Padding36("plan-pseudo"),
				Status:    domain.Done,
				UpdatedAt: UPLOADED_AT,
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("gen1/predefined-1/:out/1"),
					OutputId: 1_010,
					RunId:    th.Padding36("gen1/predefined-1"),
					PlanId:   th.Padding36("plan-pseudo"),
				}: {
					VolumeRef: "pvc-gen1-predefined-1-out-1",
				},
			},
		},

		{
			Run: tables.Run{
				RunId:     th.Padding36("gen1/predefined-2"),
				PlanId:    th.Padding36("plan-pseudo"),
				Status:    domain.Done,
				UpdatedAt: UPLOADED_AT.Add(time.Hour),
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("gen1/predefined-2/:out/1"),
					OutputId: 1_010,
					RunId:    th.Padding36("gen1/predefined-2"),
					PlanId:   th.Padding36("plan-pseudo"),
				}: {
					VolumeRef: "pvc-gen1-predefined-2-out-1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("gen2/predefined-2"),
				PlanId:    th.Padding36("plan-gen2"),
				Status:    domain.Done,
				UpdatedAt: UPLOADED_AT.Add(2 * time.Hour),
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("gen1/predefined-2/:out/1"),
					InputId: 2_100,
					RunId:   th.Padding36("gen2/predefined-2"),
					PlanId:  th.Padding36("plan-gen2"),
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("gen2/predefined-2/:out/1"),
					OutputId: 2_010,
					RunId:    th.Padding36("gen2/predefined-2"),
					PlanId:   th.Padding36("plan-gen2"),
				}: {
					VolumeRef: "pvc-gen2-predefined-2-out-1",
				},
			},
		},

		{
			Run: tables.Run{
				RunId:     th.Padding36("gen1/predefined-3"),
				PlanId:    th.Padding36("plan-pseudo"),
				Status:    domain.Done,
				UpdatedAt: UPLOADED_AT.Add(1 * time.Hour),
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("gen1/predefined-3/:out/1"),
					OutputId: 1_010,
					RunId:    th.Padding36("gen1/predefined-3"),
					PlanId:   th.Padding36("plan-pseudo"),
				}: {
					VolumeRef: "pvc-gen1-predefined-3-out-1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("gen2/predefined-3"),
				PlanId:    th.Padding36("plan-gen2"),
				Status:    domain.Done,
				UpdatedAt: UPLOADED_AT.Add(2 * time.Hour),
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("gen1/predefined-3/:out/1"),
					InputId: 2_100,
					RunId:   th.Padding36("gen2/predefined-3"),
					PlanId:  th.Padding36("plan-gen2"),
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("gen2/predefined-3/:out/1"),
					OutputId: 2_010,
					RunId:    th.Padding36("gen2/predefined-3"),
					PlanId:   th.Padding36("plan-gen2"),
				}: {
					VolumeRef: "pvc-gen2-predefined-3-out-1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("gen3/predefined-3"),
				PlanId:    th.Padding36("plan-gen3"),
				Status:    domain.Done,
				UpdatedAt: UPLOADED_AT.Add(3 * time.Hour),
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("gen2/predefined-3/:out/1"),
					InputId: 3_100,
					RunId:   th.Padding36("gen3/predefined-3"),
					PlanId:  th.Padding36("plan-gen3"),
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("gen3/predefined-3/:out/1"),
					OutputId: 3_010,
					RunId:    th.Padding36("gen3/predefined-3"),
					PlanId:   th.Padding36("plan-gen3"),
				}: {
					VolumeRef: "pvc-gen3-predefined-3-out-1",
				},
			},
		},
	},
}
