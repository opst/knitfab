package nominator_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	pgerrcode "github.com/jackc/pgerrcode"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	. "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	kpgnom "github.com/opst/knitfab/pkg/domain/nomination/db/postgres"
	"github.com/opst/knitfab/pkg/utils/cmp"
	fn "github.com/opst/knitfab/pkg/utils/function"
	"github.com/opst/knitfab/pkg/utils/slices"
	kstr "github.com/opst/knitfab/pkg/utils/strings"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestNominator_NominateData(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	KNITID_1 := Padding36("knitid-1")
	KNITID_2 := Padding36("knitid-2")

	DAY_1 := time.Date(2022, time.August, 1, 12, 13, 24, 0, time.UTC)
	DAY_2 := time.Date(2022, time.August, 2, 12, 13, 24, 0, time.UTC)

	TAGSET_1 := []domain.Tag{
		{Key: "input#tagset-1", Value: "1 in 3"},
		{Key: "input#tagset-1", Value: "2 in 3"},
		{Key: "input#tagset-1", Value: "3 in 3"},
	}
	TAGSET_2 := []domain.Tag{
		{Key: "input#tagset-2", Value: "1 in 3"},
		{Key: "input#tagset-2", Value: "2 in 3"},
		{Key: "input#tagset-2", Value: "3 in 3"},
	}

	plans := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: Padding36("plan-pseudo"), Active: true, Hash: Padding64("hash-pseudo")},
			{PlanId: Padding36("plan1"), Active: true, Hash: Padding64("hash1")},
			{PlanId: Padding36("plan-no-tags-in-inputs"), Active: true, Hash: Padding64("hash-no-inputs")},
			{PlanId: Padding36("plan-impossible-inputs"), Active: true, Hash: Padding64("hash-impossible-inputs")},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: Padding36("plan-pseudo"), Name: "knit#uploaded"},
		},
		PlanImage: []tables.PlanImage{
			{PlanId: Padding36("plan1"), Image: "repo.invalid/image", Version: "v0.1"},
			{PlanId: Padding36("plan-no-tags-in-inputs"), Image: "repo.invalid/image", Version: "v0.1"},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			//          ,---------- knit id
			//          |   ,------ timestamp
			//          |   |  ,--- tag set
			//          v   v  v
			{InputId: 1_00_00_01, PlanId: Padding36("plan1"), Path: "/in/00/00/01"}: {
				UserTag: TAGSET_1,
			},
			{InputId: 1_00_00_02, PlanId: Padding36("plan1"), Path: "/in/00/00/02"}: {
				UserTag: TAGSET_2,
			},

			{InputId: 1_00_01_00, PlanId: Padding36("plan1"), Path: "/in/00/01/00"}: {
				Timestamp: []time.Time{DAY_1},
			},
			{InputId: 1_00_02_00, PlanId: Padding36("plan1"), Path: "/in/00/02/00"}: {
				Timestamp: []time.Time{DAY_2},
			},

			{InputId: 1_01_00_00, PlanId: Padding36("plan1"), Path: "/in/01/00/00"}: {
				KnitId: []string{KNITID_1},
			},
			{InputId: 1_02_00_00, PlanId: Padding36("plan1"), Path: "/in/02/00/00"}: {
				KnitId: []string{KNITID_2},
			},

			{InputId: 1_00_01_01, PlanId: Padding36("plan1"), Path: "/in/00/01/01"}: {
				Timestamp: []time.Time{DAY_1},
				UserTag:   TAGSET_1,
			},
			{InputId: 1_00_02_02, PlanId: Padding36("plan1"), Path: "/in/00/02/02"}: {
				Timestamp: []time.Time{DAY_2},
				UserTag:   TAGSET_2,
			},

			{InputId: 1_01_00_01, PlanId: Padding36("plan1"), Path: "/in/01/00/01"}: {
				KnitId:  []string{KNITID_1},
				UserTag: TAGSET_1,
			},
			{InputId: 1_02_00_02, PlanId: Padding36("plan1"), Path: "/in/02/00/02"}: {
				KnitId:  []string{KNITID_2},
				UserTag: TAGSET_2,
			},

			{InputId: 1_01_01_00, PlanId: Padding36("plan1"), Path: "/in/01/01/00"}: {
				KnitId:    []string{KNITID_1},
				Timestamp: []time.Time{DAY_1},
			},
			{InputId: 1_02_02_00, PlanId: Padding36("plan1"), Path: "/in/02/02/00"}: {
				KnitId:    []string{KNITID_2},
				Timestamp: []time.Time{DAY_2},
			},

			{InputId: 1_01_01_01, PlanId: Padding36("plan1"), Path: "/in/01/01/01"}: {
				KnitId:    []string{KNITID_1},
				Timestamp: []time.Time{DAY_1},
				UserTag:   TAGSET_1,
			},
			{InputId: 1_02_02_02, PlanId: Padding36("plan1"), Path: "/in/02/02/02"}: {
				KnitId:    []string{KNITID_2},
				Timestamp: []time.Time{DAY_2},
				UserTag:   TAGSET_2,
			},

			{InputId: 1_00_00_00, PlanId: Padding36("plan1"), Path: "/in/00/00/00"}: {},

			{InputId: 2_00_00_00, PlanId: Padding36("plan-no-tags-in-inputs"), Path: "/in/00/00/00"}: {},

			{InputId: 3_00_00_12, PlanId: Padding36("plan-impossible-inputs"), Path: "/in/00/00/12"}: {
				UserTag: slices.Concat(TAGSET_1, TAGSET_2),
			},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 1, PlanId: Padding36("plan-pseudo"), Path: "/out"}: {},
		},

		Steps: []tables.Step{
			{
				Run: tables.Run{
					RunId:     Padding36("run-pseudo/pre-1"),
					PlanId:    Padding36("plan-pseudo"),
					Status:    domain.Done,
					UpdatedAt: DAY_1.Add(-24 * 7 * time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    Padding36("knit-pre1"),
						VolumeRef: "should not be nominated unless nomination performs",
						OutputId:  1,
						RunId:     Padding36("run-pseudo/pre-1"),
						PlanId:    Padding36("plan-pseudo"),
					}: {
						UserTag: TAGSET_1,
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     Padding36("run-pseudo/pre-2"),
					PlanId:    Padding36("plan-pseudo"),
					Status:    domain.Done,
					UpdatedAt: DAY_1.Add(-24 * 7 * time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    Padding36("knit-pre2"),
						VolumeRef: "should not be denominated unless nomination performs",
						OutputId:  1,
						RunId:     Padding36("run-pseudo/pre-1"),
						PlanId:    Padding36("plan-pseudo"),
					}: {
						Timestamp: &DAY_1,
						UserTag:   TAGSET_1,
					},
				},
			},
		},

		Nomination: []tables.Nomination{
			{KnitId: Padding36("knit-pre2"), InputId: 1_02_02_02, Updated: false},
		},
	}

	for name, testcase := range map[string]struct {
		given tables.Operation
		when  string // knit ids to be nominated
		then  []tables.Nomination
	}{
		"it nominates data with {TAGSET_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: Padding36("knit-target"), VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								UserTag: TAGSET_1,
							},
						},
					},
				},
			},
			when: Padding36("knit-target"),
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: Padding36("knit-target"), InputId: 1_00_00_01, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it renominates data with {TAGSET_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: Padding36("knit-target"), VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								UserTag: TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: Padding36("knit-target"), InputId: 1_00_00_01, Updated: false},
					{KnitId: Padding36("knit-target"), InputId: 1_02_02_02, Updated: false},
					{KnitId: Padding36("knit-target"), InputId: 1_02_00_02, Updated: true},
				},
			},
			when: Padding36("knit-target"),
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: Padding36("knit-target"), InputId: 1_00_00_01, Updated: false},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it nominates data with {DAY_2, TAGSET_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: Padding36("knit-target"), VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								Timestamp: &DAY_2, UserTag: TAGSET_1,
							},
						},
					},
				},
			},
			when: Padding36("knit-target"),
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: Padding36("knit-target"), InputId: 1_00_00_01, Updated: true},
					{KnitId: Padding36("knit-target"), InputId: 1_00_02_00, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it renominates data with {DAY_2, TAGSET_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: Padding36("knit-target"), VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								Timestamp: &DAY_2, UserTag: TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: Padding36("knit-target"), InputId: 1_00_00_01, Updated: false},
					{KnitId: Padding36("knit-target"), InputId: 1_02_02_02, Updated: false},
					{KnitId: Padding36("knit-target"), InputId: 1_02_00_02, Updated: true},
				},
			},
			when: Padding36("knit-target"),
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: Padding36("knit-target"), InputId: 1_00_00_01, Updated: false},
					{KnitId: Padding36("knit-target"), InputId: 1_00_02_00, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it nominates data with {DAY_1, TAGSET_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: Padding36("knit-target"), VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								Timestamp: &DAY_1, UserTag: TAGSET_1,
							},
						},
					},
				},
			},
			when: Padding36("knit-target"),
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: Padding36("knit-target"), InputId: 1_00_00_01, Updated: true},
					{KnitId: Padding36("knit-target"), InputId: 1_00_01_00, Updated: true},
					{KnitId: Padding36("knit-target"), InputId: 1_00_01_01, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it renominates data with {DAY_1, TAGSET_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: Padding36("knit-target"), VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								Timestamp: &DAY_1, UserTag: TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: Padding36("knit-target"), InputId: 1_00_01_00, Updated: false},
					{KnitId: Padding36("knit-target"), InputId: 1_02_02_02, Updated: true},
					{KnitId: Padding36("knit-target"), InputId: 1_02_00_02, Updated: false},
				},
			},
			when: Padding36("knit-target"),
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: Padding36("knit-target"), InputId: 1_00_00_01, Updated: true},
					{KnitId: Padding36("knit-target"), InputId: 1_00_01_00, Updated: false},
					{KnitId: Padding36("knit-target"), InputId: 1_00_01_01, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it nominates data with {KNITID_2, DAY_1, TAGSET_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: KNITID_2, VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								Timestamp: &DAY_1, UserTag: TAGSET_1,
							},
						},
					},
				},
			},
			when: KNITID_2,
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: KNITID_2, InputId: 1_00_00_01, Updated: true},
					{KnitId: KNITID_2, InputId: 1_00_01_00, Updated: true},
					{KnitId: KNITID_2, InputId: 1_00_01_01, Updated: true},
					{KnitId: KNITID_2, InputId: 1_02_00_00, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it renominates data with {KNITID_2, DAY_1, TAGSET_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: KNITID_2, VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								Timestamp: &DAY_1, UserTag: TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: KNITID_2, InputId: 1_00_00_01, Updated: false},
					{KnitId: KNITID_2, InputId: 1_02_02_02, Updated: true},
					{KnitId: KNITID_2, InputId: 1_02_00_02, Updated: false},
				},
			},
			when: KNITID_2,
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: KNITID_2, InputId: 1_00_00_01, Updated: false},
					{KnitId: KNITID_2, InputId: 1_00_01_00, Updated: true},
					{KnitId: KNITID_2, InputId: 1_00_01_01, Updated: true},
					{KnitId: KNITID_2, InputId: 1_02_00_00, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it nominates data with {KNITID_1, DAY_1, TAGSET_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: KNITID_1, VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								Timestamp: &DAY_1, UserTag: TAGSET_1,
							},
						},
					},
				},
			},
			when: KNITID_1,
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: KNITID_1, InputId: 1_00_00_01, Updated: true},
					{KnitId: KNITID_1, InputId: 1_00_01_00, Updated: true},
					{KnitId: KNITID_1, InputId: 1_01_00_00, Updated: true},
					{KnitId: KNITID_1, InputId: 1_00_01_01, Updated: true},
					{KnitId: KNITID_1, InputId: 1_01_00_01, Updated: true},
					{KnitId: KNITID_1, InputId: 1_01_01_00, Updated: true},
					{KnitId: KNITID_1, InputId: 1_01_01_01, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it renominates data with {KNITID_1, DAY_1, TAGSET_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: KNITID_1, VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								Timestamp: &DAY_1, UserTag: TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: KNITID_1, InputId: 1_00_01_00, Updated: false},
					{KnitId: KNITID_1, InputId: 1_01_00_00, Updated: false},
					{KnitId: KNITID_1, InputId: 1_00_01_01, Updated: true},
					{KnitId: KNITID_1, InputId: 1_01_00_01, Updated: true},
					{KnitId: KNITID_1, InputId: 1_02_02_02, Updated: false},
					{KnitId: KNITID_1, InputId: 1_02_00_02, Updated: true},
				},
			},
			when: KNITID_1,
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: KNITID_1, InputId: 1_00_00_01, Updated: true},
					{KnitId: KNITID_1, InputId: 1_00_01_00, Updated: false},
					{KnitId: KNITID_1, InputId: 1_01_00_00, Updated: false},
					{KnitId: KNITID_1, InputId: 1_00_01_01, Updated: true},
					{KnitId: KNITID_1, InputId: 1_01_00_01, Updated: true},
					{KnitId: KNITID_1, InputId: 1_01_01_00, Updated: true},
					{KnitId: KNITID_1, InputId: 1_01_01_01, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it nominates data with {KNITID_1, DAY_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: KNITID_1, VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								Timestamp: &DAY_1, UserTag: []domain.Tag{},
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: KNITID_1, InputId: 1_01_00_00, Updated: true},
					{KnitId: KNITID_1, InputId: 1_02_02_00, Updated: true},
				},
			},
			when: KNITID_1,
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: KNITID_1, InputId: 1_01_00_00, Updated: true},
					{KnitId: KNITID_1, InputId: 1_00_01_00, Updated: true},
					{KnitId: KNITID_1, InputId: 1_01_01_00, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it renominates data with {KNITID_1, DAY_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: KNITID_1, VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								Timestamp: &DAY_1, UserTag: []domain.Tag{},
							},
						},
					},
				},
			},
			when: KNITID_1,
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: KNITID_1, InputId: 1_01_00_00, Updated: true},
					{KnitId: KNITID_1, InputId: 1_00_01_00, Updated: true},
					{KnitId: KNITID_1, InputId: 1_01_01_00, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it re-renominates data with {KNITID_1, DAY_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: KNITID_1, VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								Timestamp: &DAY_1, UserTag: []domain.Tag{},
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: KNITID_1, InputId: 1_00_01_00, Updated: false},
					{KnitId: KNITID_1, InputId: 1_00_02_00, Updated: false},
					{KnitId: KNITID_1, InputId: 1_02_02_00, Updated: true},
				},
			},
			when: KNITID_1,
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: KNITID_1, InputId: 1_01_00_00, Updated: true},
					{KnitId: KNITID_1, InputId: 1_00_01_00, Updated: false},
					{KnitId: KNITID_1, InputId: 1_01_01_00, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it nominates data with {KNITID_1, TAGSET_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: KNITID_1, VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								UserTag: TAGSET_1,
							},
						},
					},
				},
			},
			when: KNITID_1,
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: KNITID_1, InputId: 1_01_00_00, Updated: true},
					{KnitId: KNITID_1, InputId: 1_00_00_01, Updated: true},
					{KnitId: KNITID_1, InputId: 1_01_00_01, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
		"it renominates data with {KNITID_1, TAGSET_1} comes from Done run": {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: domain.Done,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: KNITID_1, VolumeRef: "#vol",
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								UserTag: TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: KNITID_1, InputId: 1_00_00_01, Updated: false},
					{KnitId: KNITID_1, InputId: 1_02_00_02, Updated: false},
				},
			},
			when: KNITID_1,
			then: slices.Concat(
				[]tables.Nomination{
					{KnitId: KNITID_1, InputId: 1_01_00_00, Updated: true},
					{KnitId: KNITID_1, InputId: 1_00_00_01, Updated: false},
					{KnitId: KNITID_1, InputId: 1_01_00_01, Updated: true},
				},
				plans.Nomination, // should not be changed
			),
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pool := poolBroaker.GetPool(ctx, t)

			if err := plans.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if err := testcase.given.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			wpool := proxy.Wrap(pool)
			wpool.Events().Query.After(func() {
				BeginFuncToRollback(ctx, pool, fn.Void[error](func(tx kpool.Tx) {
					if _, err := tx.Exec(ctx, `lock table "nomination" in ROW EXCLUSIVE mode nowait`); err == nil {
						t.Errorf("nomination is not locked")
					} else if pgerr := new(pgconn.PgError); !errors.As(err, &pgerr) || pgerr.Code != pgerrcode.LockNotAvailable {
						t.Errorf(
							"unexpected error: expected error code is %s, but %s",
							pgerrcode.LockNotAvailable, err,
						)
					}
				}))
			})
			tx := try.To(wpool.Begin(ctx)).OrFatal(t)
			defer tx.Rollback(ctx)

			testee := kpgnom.DefaultNominator()
			if err := testee.NominateData(ctx, tx, []string{testcase.when}); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}

			conn := try.To(pool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			actual := try.To(scanner.New[tables.Nomination]().QueryAll(
				ctx, conn, `table "nomination"`,
			)).OrFatal(t)

			if !cmp.SliceContentEq(actual, testcase.then) {
				t.Errorf("unmatch:\n===actual===\n%+v\n===expeted===\n%+v", actual, testcase.then)
			}
		})
	}

	for _, nonDone := range []domain.KnitRunStatus{
		domain.Deactivated, domain.Waiting, domain.Ready,
		domain.Starting, domain.Running, domain.Aborting, domain.Completing,
		domain.Failed, domain.Invalidated,
	} {
		for name, testcase := range map[string]struct {
			given tables.Operation
			when  string // knit ids to be nominated
			then  []tables.Nomination
		}{
			"it should not nominate data with {TAGSET_1} comes from run ": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: Padding36("knit-target"), VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									UserTag: TAGSET_1,
								},
							},
						},
					},
				},
				when: Padding36("knit-target"),
				then: plans.Nomination,
			},
			"it renominates data with {TAGSET_1} comes run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: Padding36("knit-target"), VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									UserTag: TAGSET_1,
								},
							},
						},
					},
					Nomination: []tables.Nomination{
						{KnitId: Padding36("knit-target"), InputId: 1_00_00_01, Updated: false},
						{KnitId: Padding36("knit-target"), InputId: 1_02_02_02, Updated: false},
						{KnitId: Padding36("knit-target"), InputId: 1_02_00_02, Updated: true},
					},
				},
				when: Padding36("knit-target"),
				then: plans.Nomination,
			},
			"it should not nominate data with {DAY_2, TAGSET_1} comes from run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: Padding36("knit-target"), VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									Timestamp: &DAY_2, UserTag: TAGSET_1,
								},
							},
						},
					},
				},
				when: Padding36("knit-target"),
				then: plans.Nomination,
			},
			"it renominates data with {DAY_2, TAGSET_1} comes from run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: Padding36("knit-target"), VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									Timestamp: &DAY_2, UserTag: TAGSET_1,
								},
							},
						},
					},
					Nomination: []tables.Nomination{
						{KnitId: Padding36("knit-target"), InputId: 1_00_00_01, Updated: false},
						{KnitId: Padding36("knit-target"), InputId: 1_02_02_02, Updated: false},
						{KnitId: Padding36("knit-target"), InputId: 1_02_00_02, Updated: true},
					},
				},
				when: Padding36("knit-target"),
				then: plans.Nomination,
			},
			"it should not nominate data with {DAY_1, TAGSET_1} comes from run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: Padding36("knit-target"), VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									Timestamp: &DAY_1, UserTag: TAGSET_1,
								},
							},
						},
					},
				},
				when: Padding36("knit-target"),
				then: plans.Nomination,
			},
			"it renominates data with {DAY_1, TAGSET_1} comes from run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: Padding36("knit-target"), VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									Timestamp: &DAY_1, UserTag: TAGSET_1,
								},
							},
						},
					},
					Nomination: []tables.Nomination{
						{KnitId: Padding36("knit-target"), InputId: 1_00_01_00, Updated: false},
						{KnitId: Padding36("knit-target"), InputId: 1_02_02_02, Updated: true},
						{KnitId: Padding36("knit-target"), InputId: 1_02_00_02, Updated: false},
					},
				},
				when: Padding36("knit-target"),
				then: plans.Nomination,
			},
			"it should not nominate data with {KNITID_2, DAY_1, TAGSET_1} comes from run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: KNITID_2, VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									Timestamp: &DAY_1, UserTag: TAGSET_1,
								},
							},
						},
					},
				},
				when: KNITID_2,
				then: plans.Nomination,
			},
			"it renominates data with {KNITID_2, DAY_1, TAGSET_1} comes from run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: KNITID_2, VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									Timestamp: &DAY_1, UserTag: TAGSET_1,
								},
							},
						},
					},
					Nomination: []tables.Nomination{
						{KnitId: KNITID_2, InputId: 1_00_00_01, Updated: false},
						{KnitId: KNITID_2, InputId: 1_02_02_02, Updated: true},
						{KnitId: KNITID_2, InputId: 1_02_00_02, Updated: false},
					},
				},
				when: KNITID_2,
				then: plans.Nomination,
			},
			"it should not nominate data with {KNITID_1, DAY_1, TAGSET_1} comes from run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: KNITID_1, VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									Timestamp: &DAY_1, UserTag: TAGSET_1,
								},
							},
						},
					},
				},
				when: KNITID_1,
				then: plans.Nomination,
			},
			"it renominates data with {KNITID_1, DAY_1, TAGSET_1} comes from run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: KNITID_1, VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									Timestamp: &DAY_1, UserTag: TAGSET_1,
								},
							},
						},
					},
					Nomination: []tables.Nomination{
						{KnitId: KNITID_1, InputId: 1_00_01_00, Updated: false},
						{KnitId: KNITID_1, InputId: 1_01_00_00, Updated: false},
						{KnitId: KNITID_1, InputId: 1_00_01_01, Updated: true},
						{KnitId: KNITID_1, InputId: 1_01_00_01, Updated: true},
						{KnitId: KNITID_1, InputId: 1_02_02_02, Updated: false},
						{KnitId: KNITID_1, InputId: 1_02_00_02, Updated: true},
					},
				},
				when: KNITID_1,
				then: plans.Nomination,
			},
			"it should not nominate data with {KNITID_1, DAY_1} comes from run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: KNITID_1, VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									Timestamp: &DAY_1, UserTag: TAGSET_1,
								},
							},
						},
					},
					Nomination: []tables.Nomination{
						{KnitId: KNITID_1, InputId: 1_01_00_00, Updated: true},
						{KnitId: KNITID_1, InputId: 1_02_02_00, Updated: true},
					},
				},
				when: KNITID_1,
				then: plans.Nomination,
			},
			"it renominates data with {KNITID_1, DAY_1} comes from run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: KNITID_1, VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									Timestamp: &DAY_1, UserTag: TAGSET_1,
								},
							},
						},
					},
				},
				when: KNITID_1,
				then: plans.Nomination,
			},
			"it rerenominates data with {KNITID_1, DAY_1} comes from run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: KNITID_1, VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									Timestamp: &DAY_1, UserTag: TAGSET_1,
								},
							},
						},
					},
					Nomination: []tables.Nomination{
						{KnitId: KNITID_1, InputId: 1_00_01_00, Updated: false},
						{KnitId: KNITID_1, InputId: 1_00_02_00, Updated: false},
						{KnitId: KNITID_1, InputId: 1_02_02_00, Updated: true},
					},
				},
				when: KNITID_1,
				then: plans.Nomination,
			},
			"it nominates data with {KNITID_1, TAGSET_1} comes from run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: KNITID_1, VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									UserTag: TAGSET_1,
								},
							},
						},
					},
				},
				when: KNITID_1,
				then: plans.Nomination,
			},
			"it renominates data with {KNITID_1, TAGSET_1} comes from run": {
				given: tables.Operation{
					Steps: []tables.Step{
						{
							Run: tables.Run{
								Status: nonDone,
								RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								UpdatedAt: DAY_1.Add(-24 * time.Hour),
							},
							Outcomes: map[tables.Data]tables.DataAttibutes{
								{
									KnitId: KNITID_1, VolumeRef: "#vol",
									OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
								}: {
									UserTag: TAGSET_1,
								},
							},
						},
					},
					Nomination: []tables.Nomination{
						{KnitId: KNITID_1, InputId: 1_00_00_01, Updated: false},
						{KnitId: KNITID_1, InputId: 1_02_00_02, Updated: false},
					},
				},
				when: KNITID_1,
				then: plans.Nomination,
			},
		} {
			t.Run(fmt.Sprintf("%s in state '%s'", name, nonDone), func(t *testing.T) {
				ctx := context.Background()
				pool := poolBroaker.GetPool(ctx, t)

				if err := plans.Apply(ctx, pool); err != nil {
					t.Fatal(err)
				}
				if err := testcase.given.Apply(ctx, pool); err != nil {
					t.Fatal(err)
				}

				wpool := proxy.Wrap(pool)
				wpool.Events().Query.After(func() {
					BeginFuncToRollback(ctx, pool, fn.Void[error](func(tx kpool.Tx) {
						if _, err := tx.Exec(ctx, `lock table "nomination" in ROW EXCLUSIVE mode nowait`); err == nil {
							t.Errorf("nomination is not locked")
						} else if pgerr := new(pgconn.PgError); !errors.As(err, &pgerr) || pgerr.Code != pgerrcode.LockNotAvailable {
							t.Errorf(
								"unexpected error: expected error code is %s, but %s",
								pgerrcode.LockNotAvailable, err,
							)
						}
					}))
				})
				tx := try.To(wpool.Begin(ctx)).OrFatal(t)
				defer tx.Rollback(ctx)

				testee := kpgnom.DefaultNominator()
				if err := testee.NominateData(ctx, tx, []string{testcase.when}); err != nil {
					t.Fatal(err)
				}

				if err := tx.Commit(ctx); err != nil {
					t.Fatal(err)
				}

				conn := try.To(pool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()
				actual := try.To(scanner.New[tables.Nomination]().QueryAll(
					ctx, conn, `table "nomination"`,
				)).OrFatal(t)

				if !cmp.SliceContentEq(actual, testcase.then) {
					t.Errorf("unmatch:\n===actual===\n%+v\n===expeted===\n%+v", actual, testcase.then)
				}
			})
		}
	}

	t.Run("if no inputs are in DB, it does not cause error", func(t *testing.T) {

		plans := tables.Operation{
			Plan: []tables.Plan{
				{PlanId: Padding36("pseudo"), Active: true, Hash: Padding64("hash-pseudo")},
			},
			PlanPseudo: []tables.PlanPseudo{
				{PlanId: Padding36("pseudo"), Name: "knit#uploaded"},
			},
			Outputs: map[tables.Output]tables.OutputAttr{
				{OutputId: 1, PlanId: Padding36("pseudo"), Path: "/out"}: {},
			},
			Steps: []tables.Step{
				{
					Run: tables.Run{
						RunId:     Padding36("upload"),
						PlanId:    Padding36("pseudo"),
						Status:    domain.Done,
						UpdatedAt: DAY_1,
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId: Padding36("knit-1"), VolumeRef: "vol",
							OutputId: 1,
							RunId:    Padding36("upload"),
							PlanId:   Padding36("pseudo"),
						}: {Timestamp: &DAY_2, UserTag: TAGSET_1},
					},
				},
			},
		}

		ctx := context.Background()
		pool := poolBroaker.GetPool(ctx, t)

		if err := plans.Apply(ctx, pool); err != nil {
			t.Fatal(err)
		}

		wpool := proxy.Wrap(pool)
		wpool.Events().Query.After(func() {
			BeginFuncToRollback(ctx, pool, fn.Void[error](func(tx kpool.Tx) {
				if _, err := tx.Exec(ctx, `lock table "nomination" in ROW EXCLUSIVE mode nowait`); err == nil {
					t.Errorf("nomination is not locked")
				} else if pgerr := new(pgconn.PgError); !errors.As(err, &pgerr) || pgerr.Code != pgerrcode.LockNotAvailable {
					t.Errorf(
						"unexpected error: expected error code is %s, but %s",
						pgerrcode.LockNotAvailable, err,
					)
				}
			}))
		})

		tx := try.To(wpool.Begin(ctx)).OrFatal(t)
		defer tx.Rollback(ctx)
		testee := kpgnom.DefaultNominator()
		if err := testee.NominateData(ctx, tx, []string{Padding36("knit-1")}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		actual := try.To(scanner.New[tables.Nomination]().QueryAll(
			ctx, conn, `table "nomination"`,
		)).OrFatal(t)

		if len(actual) != 0 {
			t.Error("unexpected nominations are found:", actual)
		}
	})
}

func TestNominator_NominateInput(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	oldTimestamp := try.To(rfctime.ParseRFC3339DateTime(
		"2022-11-12T13:14:15.678+09:00",
	)).OrFatal(t).Time()

	newTimestamp := try.To(rfctime.ParseRFC3339DateTime(
		"2022-11-13T14:15:16.678+09:00",
	)).OrFatal(t).Time()

	tagsetAll := []domain.Tag{
		{Key: "tag-a", Value: "a-value"},
		{Key: "tag-b", Value: "b-value"},
		{Key: "tag-c", Value: "c-value"},
	}
	tagsetAandB := tagsetAll[:2]
	tagsetBandC := tagsetAll[1:]
	tagsetAandC := []domain.Tag{tagsetAll[0], tagsetAll[2]}

	plan := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: Padding36("pseudo"), Active: true, Hash: Padding64("#plan-pseudo")},
			{PlanId: Padding36("plan-pre"), Active: true, Hash: Padding64("#plan-pre")},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: Padding36("pseudo"), Name: "pseudo"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 1010, PlanId: Padding36("pseudo"), Path: "/out"}: {},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			{InputId: 1, PlanId: Padding36("plan-pre"), Path: "/in/tags_a-only"}: {
				UserTag: tagsetAll[:1],
			},
			{InputId: 2, PlanId: Padding36("plan-pre"), Path: "/in/timestamp_old"}: {
				Timestamp: []time.Time{oldTimestamp},
			},
			{InputId: 3, PlanId: Padding36("plan-pre"), Path: "/in/knitid_x"}: {
				KnitId: []string{Padding36("knitid_x")},
			},
		},
		Nomination: []tables.Nomination{
			{InputId: 1, KnitId: Padding36("knit_a-and-b_old_done"), Updated: true},
			{InputId: 1, KnitId: Padding36("knit_a-and-b_new_done"), Updated: true},
			{InputId: 2, KnitId: Padding36("knit_a-and-b_old_done"), Updated: true},
			{InputId: 2, KnitId: Padding36("knit_a-and-b_new_done"), Updated: true},
		},
	}

	// generating data and its upstream
	for tagcode, tag := range map[string][]domain.Tag{
		"a-and-b": tagsetAandB,
		"b-and-c": tagsetBandC,
		// a-and-c: no such data.
		"no-tags": {},
	} {
		for timecode, timestamp := range map[string]*time.Time{
			"old":     &oldTimestamp,
			"new":     &newTimestamp,
			"no-time": nil,
		} {
			for _, status := range []domain.KnitRunStatus{
				// knit#transient: processing
				domain.Deactivated, domain.Waiting, domain.Ready, domain.Starting, domain.Running, domain.Aborting, domain.Completing,

				// no knit#transient
				domain.Done,

				// knit#transient: failed
				domain.Failed, domain.Invalidated,
			} {
				// status in id are cut off after the 4th letter,
				// to maintain ids shorter than 36 chars.
				runid := Padding36(fmt.Sprintf(
					"run_%s_%s_%s", tagcode, timecode, status[:4],
				))
				knitid := Padding36(fmt.Sprintf(
					"knit_%s_%s_%s", tagcode, timecode, status[:4],
				))
				step := tables.Step{
					Run: tables.Run{
						RunId: runid, Status: status, PlanId: Padding36("pseudo"),
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime("2022-10-11T12:13:14.567+09:00")).OrFatal(t).Time(),
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId: knitid, VolumeRef: Padding64("#" + knitid),
							RunId: runid, OutputId: 1010, PlanId: Padding36("pseudo"),
						}: {
							UserTag: tag, Timestamp: timestamp,
						},
					},
				}
				plan.Steps = append(plan.Steps, step)
			}
		}
	}

	for name, testcase := range map[string]struct {
		given tables.Operation
		when  []int // input id
		then  []tables.Nomination
	}{
		"if input has no tags, it should not nominate the input": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input has user tags but not match anything, it should not nominate the input": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag: tagsetAandC,
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input has user tags and knit id but not match anything, it should not nominate the input": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag: tagsetAandB,
						KnitId:  []string{Padding36("knit_b-and-c_new_done")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input has knit id but not match anything, it should not nominate the input": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("no-such-knit-id")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input has timestamp but not match anything, it should not nominate the input": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						Timestamp: []time.Time{newTimestamp.Add(240 * time.Hour)},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input has user tags but no data have such, it should not nominate the input": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag: []domain.Tag{{Key: "unexpected", Value: "tag!"}},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Deactivated data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_deac")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Waiting data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_wait")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Ready data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_read")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Starting data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_star")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Running data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_runn")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Aborting data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_abor")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Completing data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_comp")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Failed data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_fail")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Invalidated data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_inva")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input matches in user tag, it should nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag: tagsetAll[:1], // a only
					},
				},
			},
			when: []int{1_00_00_00},
			then: slices.Concat(
				slices.Map(
					kstr.SprintMany(
						"knit_a-and-b_%s_done",
						[]any{"old", "new", "no-time"},
					),
					func(knitId string) tables.Nomination {
						return tables.Nomination{InputId: 1_00_00_00, KnitId: Padding36(knitId), Updated: true}
					},
				),
				plan.Nomination,
			),
		},
		"if input matches in timestamp, it should nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						Timestamp: []time.Time{newTimestamp},
					},
				},
			},
			when: []int{1_00_00_00},
			then: slices.Concat(
				slices.Map(
					kstr.SprintMany(
						"knit_%s_new_done",
						[]any{"a-and-b", "b-and-c", "no-tags"},
					),
					func(knitId string) tables.Nomination {
						return tables.Nomination{InputId: 1_00_00_00, KnitId: Padding36(knitId), Updated: true}
					},
				),
				plan.Nomination,
			),
		},
		"if input matches in knitid, it should nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_no-time_done")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: slices.Concat(
				[]tables.Nomination{
					{InputId: 1_00_00_00, KnitId: Padding36("knit_a-and-b_no-time_done"), Updated: true},
				},
				plan.Nomination,
			),
		},
		"if input matches in knitid and timestamp, it should nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						Timestamp: []time.Time{newTimestamp},
						KnitId:    []string{Padding36("knit_b-and-c_new_done")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: slices.Concat(
				[]tables.Nomination{{InputId: 1_00_00_00, KnitId: Padding36("knit_b-and-c_new_done"), Updated: true}},
				plan.Nomination,
			),
		},
		"if input matches in knitid and user tag, it should nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag: tagsetAll[1:2], // b
						KnitId:  []string{Padding36("knit_b-and-c_new_done")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: slices.Concat(
				[]tables.Nomination{{InputId: 1_00_00_00, KnitId: Padding36("knit_b-and-c_new_done"), Updated: true}},
				plan.Nomination,
			),
		},
		"if input matches in timestamp and user tag, it should nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag:   tagsetAll[1:2], // b
						Timestamp: []time.Time{newTimestamp},
					},
				},
			},
			when: []int{1_00_00_00},
			then: slices.Concat(
				slices.Map(
					kstr.SprintMany(
						"knit_%s_new_done",
						[]any{"a-and-b", "b-and-c"},
					),
					func(knitId string) tables.Nomination {
						return tables.Nomination{InputId: 1_00_00_00, KnitId: Padding36(knitId), Updated: true}
					},
				),
				plan.Nomination,
			),
		},
		"if inputs are passed, it should perform nomination for each of them": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag:   tagsetAll[:1], // a
						Timestamp: []time.Time{newTimestamp},
					},
					{InputId: 1_01_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag:   tagsetAll[2:], // c
						Timestamp: []time.Time{oldTimestamp},
					},
					{InputId: 1_02_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag: tagsetAll[1:2], // b
					},
				},
			},
			when: []int{1_00_00_00, 1_01_00_00}, // not nominating 1_02_00_00
			then: slices.Concat(
				[]tables.Nomination{
					{InputId: 1_00_00_00, KnitId: Padding36("knit_a-and-b_new_done"), Updated: true},
					{InputId: 1_01_00_00, KnitId: Padding36("knit_b-and-c_old_done"), Updated: true},
					// not nominated input does not appear here.
				},
				plan.Nomination,
			),
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pool := poolBroaker.GetPool(ctx, t)
			if err := plan.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if err := testcase.given.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			wpool := proxy.Wrap(pool)
			wpool.Events().Events().Query.After(func() {
				BeginFuncToRollback(ctx, pool, fn.Void[error](func(tx kpool.Tx) {
					if _, err := tx.Exec(ctx, `lock table "nomination" in ROW EXCLUSIVE mode nowait;`); err == nil {
						t.Errorf("nomination is not locked")
					} else if pgerr := new(pgconn.PgError); !errors.As(err, &pgerr) || pgerr.Code != pgerrcode.LockNotAvailable {
						t.Errorf(
							"unexpected error: expected error code is %s, but %s",
							pgerrcode.LockNotAvailable, err,
						)
					}
				}))
			})

			tx := try.To(wpool.Begin(ctx)).OrFatal(t)
			defer tx.Rollback(ctx)
			testee := kpgnom.DefaultNominator()
			if err := testee.NominateMountpoints(ctx, tx, testcase.when); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}

			conn := try.To(pool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			actual := try.To(scanner.New[tables.Nomination]().QueryAll(
				ctx, conn, `table "nomination"`,
			)).OrFatal(t)

			if !cmp.SliceContentEq(actual, testcase.then) {
				t.Errorf("unmatch\n===actual===\n%+v\n===expected===\n%+v",
					actual, testcase.then,
				)
			}

		})
	}

}

func TestDropData(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	TIMESTAMP := try.To(rfctime.ParseRFC3339DateTime(
		"2022-11-13T14:15:16.678+09:00",
	)).OrFatal(t).Time()

	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: Padding36("pseudo"), Active: true, Hash: "hash"},
			{PlanId: Padding36("plan-1"), Active: true, Hash: "hash"},
			{PlanId: Padding36("plan-2"), Active: true, Hash: "hash"},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: Padding36("pseudo"), Name: "upload"},
		},
		PlanImage: []tables.PlanImage{
			{PlanId: Padding36("plan-1"), Image: "repo.invalid/image", Version: "v0.1"},
			{PlanId: Padding36("plan-2"), Image: "repo.invalid/image", Version: "v0.1"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 1, PlanId: Padding36("pseudo"), Path: "/out"}: {},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			{InputId: 100, PlanId: Padding36("plan-1"), Path: "/in/1"}: {
				KnitId: []string{Padding36("knit-1")},
			},
			{InputId: 101, PlanId: Padding36("plan-1"), Path: "/in/2"}: {
				UserTag: []domain.Tag{{Key: "tagkey", Value: "tagval"}},
			},
			{InputId: 102, PlanId: Padding36("plan-1"), Path: "/in/3"}: {
				Timestamp: []time.Time{TIMESTAMP},
			},

			{InputId: 200, PlanId: Padding36("plan-2"), Path: "/in/1"}: {
				KnitId: []string{Padding36("knit-1")},
			},
			{InputId: 201, PlanId: Padding36("plan-2"), Path: "/in/2"}: {
				UserTag: []domain.Tag{{Key: "tagkey", Value: "tagval"}},
			},
			{InputId: 202, PlanId: Padding36("plan-2"), Path: "/in/3"}: {
				Timestamp: []time.Time{TIMESTAMP},
			},
		},
		Steps: []tables.Step{
			{
				Run: tables.Run{
					RunId: Padding36("run-1"), PlanId: Padding36("pseudo"), Status: domain.Done, UpdatedAt: TIMESTAMP,
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("knit-1"), VolumeRef: "vol-1",
						OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("pseudo"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId: Padding36("run-2"), PlanId: Padding36("pseudo"), Status: domain.Done, UpdatedAt: TIMESTAMP,
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("knit-2"), VolumeRef: "vol-2",
						OutputId: 1, RunId: Padding36("run-2"), PlanId: Padding36("pseudo"),
					}: {
						Timestamp: &TIMESTAMP,
					},
				},
			},
			{
				Run: tables.Run{
					RunId: Padding36("run-3"), PlanId: Padding36("pseudo"), Status: domain.Done, UpdatedAt: TIMESTAMP,
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("knit-3"), VolumeRef: "vol-3",
						OutputId: 1, RunId: Padding36("run-3"), PlanId: Padding36("pseudo"),
					}: {
						UserTag: []domain.Tag{{Key: "tagkey", Value: "tagval"}},
					},
				},
			},
			{
				Run: tables.Run{
					RunId: Padding36("run-4"), PlanId: Padding36("pseudo"), Status: domain.Done, UpdatedAt: TIMESTAMP,
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("knit-4"), VolumeRef: "vol-4",
						OutputId: 1, RunId: Padding36("run-4"), PlanId: Padding36("pseudo"),
					}: {
						Timestamp: &TIMESTAMP,
						UserTag:   []domain.Tag{{Key: "tagkey", Value: "tagval"}},
					},
				},
			},
		},
		Nomination: []tables.Nomination{
			{KnitId: Padding36("knit-1"), InputId: 102, Updated: true},
			{KnitId: Padding36("knit-1"), InputId: 200, Updated: true},
			{KnitId: Padding36("knit-2"), InputId: 100, Updated: false},
			{KnitId: Padding36("knit-2"), InputId: 201, Updated: false},
			{KnitId: Padding36("knit-3"), InputId: 101, Updated: true},
			{KnitId: Padding36("knit-3"), InputId: 202, Updated: true},
		},
	}

	for name, testcase := range map[string]struct {
		when []string
		then []tables.Nomination
	}{
		`when dropping not existing knit id, it does nothing`: {
			when: []string{Padding36("no-such-knit-id")},
			then: given.Nomination,
		},
		`when dropping zero knit ids, it does nothing`: {
			when: []string{},
			then: given.Nomination,
		},
		`when dropping knit id which is not nominated, it does nothing`: {
			when: []string{Padding36("knit-4")},
			then: given.Nomination,
		},
		`when dropping single knit id, it remove nominations related that`: {
			when: []string{Padding36("knit-1")},
			then: []tables.Nomination{
				{KnitId: Padding36("knit-2"), InputId: 100, Updated: false},
				{KnitId: Padding36("knit-2"), InputId: 201, Updated: false},
				{KnitId: Padding36("knit-3"), InputId: 101, Updated: true},
				{KnitId: Padding36("knit-3"), InputId: 202, Updated: true},
			},
		},
		`when dropping knit ids, it remove nominations related that`: {
			when: []string{Padding36("knit-1"), Padding36("knit-3")},
			then: []tables.Nomination{
				{KnitId: Padding36("knit-2"), InputId: 100, Updated: false},
				{KnitId: Padding36("knit-2"), InputId: 201, Updated: false},
			},
		},
		`when dropping knit ids containing non-existing, it remove nominations related that`: {
			when: []string{Padding36("knit-1"), Padding36("no-such-knit-id"), Padding36("knit-3")},
			then: []tables.Nomination{
				{KnitId: Padding36("knit-2"), InputId: 100, Updated: false},
				{KnitId: Padding36("knit-2"), InputId: 201, Updated: false},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pgpool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			wpool := proxy.Wrap(pgpool)
			wpool.Events().Query.After(func() {
				BeginFuncToRollback(ctx, pgpool, fn.Void[error](func(tx kpool.Tx) {
					if _, err := tx.Exec(ctx, `lock table "nomination" in ROW EXCLUSIVE mode nowait;`); err == nil {
						t.Errorf("nomination is not locked")
					} else if pgerr := new(pgconn.PgError); !errors.As(err, &pgerr) || pgerr.Code != pgerrcode.LockNotAvailable {
						t.Errorf(
							"unexpected error: expected error code is %s, but %s",
							pgerrcode.LockNotAvailable, err,
						)
					}
				}))
			})

			tx := try.To(wpool.Begin(ctx)).OrFatal(t)
			defer tx.Rollback(ctx)

			testee := kpgnom.DefaultNominator()

			if err := testee.DropData(ctx, tx, testcase.when); err != nil {
				t.Fatal(err)
			}

			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}

			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			actual := try.To(scanner.New[tables.Nomination]().QueryAll(
				ctx, conn, `table "nomination"`,
			)).OrFatal(t)
			if !cmp.SliceContentEq(actual, testcase.then) {
				t.Errorf(
					"unmatch\n===actual===\n%+v\n===expected===\n%+v",
					actual, testcase.then,
				)
			}

		})
	}
}
