package dataset

import (
	"time"

	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils/slices"
)

var (
	KNITID_1 = th.Padding36("knitid-1")
	KNITID_2 = th.Padding36("knitid-2")

	DAY_1 = time.Date(2022, time.August, 1, 12, 13, 24, 0, time.UTC)
	DAY_2 = time.Date(2022, time.August, 2, 12, 13, 24, 0, time.UTC)

	TAGSET_1 = []domain.Tag{
		{Key: "input#tagset-1", Value: "1 in 3"},
		{Key: "input#tagset-1", Value: "2 in 3"},
		{Key: "input#tagset-1", Value: "3 in 3"},
	}
	TAGSET_2 = []domain.Tag{
		{Key: "input#tagset-2", Value: "1 in 3"},
		{Key: "input#tagset-2", Value: "2 in 3"},
		{Key: "input#tagset-2", Value: "3 in 3"},
	}
	GivenDatabase = tables.Operation{
		Plan: []tables.Plan{
			{PlanId: th.Padding36("plan-pseudo"), Active: true, Hash: th.Padding64("hash-pseudo")},
			{PlanId: th.Padding36("plan1"), Active: true, Hash: th.Padding64("hash1")},
			{PlanId: th.Padding36("plan-no-tags-in-inputs"), Active: true, Hash: th.Padding64("hash-no-inputs")},
			{PlanId: th.Padding36("plan-impossible-inputs"), Active: true, Hash: th.Padding64("hash-impossible-inputs")},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: th.Padding36("plan-pseudo"), Name: "knit#uploaded"},
		},
		PlanImage: []tables.PlanImage{
			{PlanId: th.Padding36("plan1"), Image: "repo.invalid/image", Version: "v0.1"},
			{PlanId: th.Padding36("plan-no-tags-in-inputs"), Image: "repo.invalid/image", Version: "v0.1"},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			//          ,---------- knit id
			//          |   ,------ timestamp
			//          |   |  ,--- tag set
			//          v   v  v
			{InputId: 1_00_00_01, PlanId: th.Padding36("plan1"), Path: "/in/00/00/01"}: {
				UserTag: TAGSET_1,
			},
			{InputId: 1_00_00_02, PlanId: th.Padding36("plan1"), Path: "/in/00/00/02"}: {
				UserTag: TAGSET_2,
			},

			{InputId: 1_00_01_00, PlanId: th.Padding36("plan1"), Path: "/in/00/01/00"}: {
				Timestamp: []time.Time{DAY_1},
			},
			{InputId: 1_00_02_00, PlanId: th.Padding36("plan1"), Path: "/in/00/02/00"}: {
				Timestamp: []time.Time{DAY_2},
			},

			{InputId: 1_01_00_00, PlanId: th.Padding36("plan1"), Path: "/in/01/00/00"}: {
				KnitId: []string{KNITID_1},
			},
			{InputId: 1_02_00_00, PlanId: th.Padding36("plan1"), Path: "/in/02/00/00"}: {
				KnitId: []string{KNITID_2},
			},

			{InputId: 1_00_01_01, PlanId: th.Padding36("plan1"), Path: "/in/00/01/01"}: {
				Timestamp: []time.Time{DAY_1},
				UserTag:   TAGSET_1,
			},
			{InputId: 1_00_02_02, PlanId: th.Padding36("plan1"), Path: "/in/00/02/02"}: {
				Timestamp: []time.Time{DAY_2},
				UserTag:   TAGSET_2,
			},

			{InputId: 1_01_00_01, PlanId: th.Padding36("plan1"), Path: "/in/01/00/01"}: {
				KnitId:  []string{KNITID_1},
				UserTag: TAGSET_1,
			},
			{InputId: 1_02_00_02, PlanId: th.Padding36("plan1"), Path: "/in/02/00/02"}: {
				KnitId:  []string{KNITID_2},
				UserTag: TAGSET_2,
			},

			{InputId: 1_01_01_00, PlanId: th.Padding36("plan1"), Path: "/in/01/01/00"}: {
				KnitId:    []string{KNITID_1},
				Timestamp: []time.Time{DAY_1},
			},
			{InputId: 1_02_02_00, PlanId: th.Padding36("plan1"), Path: "/in/02/02/00"}: {
				KnitId:    []string{KNITID_2},
				Timestamp: []time.Time{DAY_2},
			},

			{InputId: 1_01_01_01, PlanId: th.Padding36("plan1"), Path: "/in/01/01/01"}: {
				KnitId:    []string{KNITID_1},
				Timestamp: []time.Time{DAY_1},
				UserTag:   TAGSET_1,
			},
			{InputId: 1_02_02_02, PlanId: th.Padding36("plan1"), Path: "/in/02/02/02"}: {
				KnitId:    []string{KNITID_2},
				Timestamp: []time.Time{DAY_2},
				UserTag:   TAGSET_2,
			},

			{InputId: 1_00_00_00, PlanId: th.Padding36("plan1"), Path: "/in/00/00/00"}: {},

			{InputId: 2_00_00_00, PlanId: th.Padding36("plan-no-tags-in-inputs"), Path: "/in/00/00/00"}: {},

			{InputId: 3_00_00_12, PlanId: th.Padding36("plan-impossible-inputs"), Path: "/in/00/00/12"}: {
				UserTag: slices.Concat(TAGSET_1, TAGSET_2),
			},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 1, PlanId: th.Padding36("plan-pseudo"), Path: "/out"}: {},
		},

		Steps: []tables.Step{
			{
				Run: tables.Run{
					RunId:     th.Padding36("run-pseudo/pre-1"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    domain.Done,
					UpdatedAt: DAY_1.Add(-24 * 7 * time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:   th.Padding36("knit-pre1"),
						OutputId: 1,
						RunId:    th.Padding36("run-pseudo/pre-1"),
						PlanId:   th.Padding36("plan-pseudo"),
					}: {
						VolumeRef: "should not be nominated unless nomination performs",
						UserTag:   TAGSET_1,
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("run-pseudo/pre-2"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    domain.Done,
					UpdatedAt: DAY_1.Add(-24 * 7 * time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:   th.Padding36("knit-pre2"),
						OutputId: 1,
						RunId:    th.Padding36("run-pseudo/pre-2"),
						PlanId:   th.Padding36("plan-pseudo"),
					}: {
						VolumeRef: "should not be denominated unless nomination performs",
						Timestamp: &DAY_1,
						UserTag:   TAGSET_1,
					},
				},
			},
		},

		Nomination: []tables.Nomination{
			{KnitId: th.Padding36("knit-pre2"), InputId: 1_02_02_02, Updated: false},
		},
	}
)
