package tests_test

import (
	"context"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	kpgrun "github.com/opst/knitfab/pkg/db/postgres/run"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestRun_Find(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	type then struct {
		run []string
	}
	type testcase struct {
		when kdb.RunFindQuery
		then
	}

	// Dimensions to be tested
	// - Matching plan ID
	// - Matching input knit ID
	// - Matching output knit ID
	// - Matching state
	// 4 bits = 16 combinations

	TIMESTAMP := try.To(rfctime.ParseRFC3339DateTime(
		"2023-04-05T13:34:45+00:00",
	)).OrFatal(t).Time()

	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: th.Padding36("plan-1-pseudo"), Hash: "#pseudo", Active: true},
			{PlanId: th.Padding36("plan-2"), Hash: "#2", Active: true},
			{PlanId: th.Padding36("plan-3"), Hash: "#3", Active: true},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: th.Padding36("plan-1-pseudo"), Name: "uploaded"},
		},
		PlanImage: []tables.PlanImage{
			{PlanId: th.Padding36("plan-2"), Image: "repo.invalid/image", Version: "v1.2"},
			{PlanId: th.Padding36("plan-3"), Image: "repo.invalid/image", Version: "v1.3"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 1_010, PlanId: th.Padding36("plan-1-pseudo"), Path: "/1/out/1"}: {},
			{OutputId: 2_010, PlanId: th.Padding36("plan-2"), Path: "/2/out/1"}:        {},
			{OutputId: 2_020, PlanId: th.Padding36("plan-2"), Path: "/2/out/2"}:        {},
			{OutputId: 2_001, PlanId: th.Padding36("plan-2"), Path: "/2/log/"}:         {IsLog: true},
			{OutputId: 3_010, PlanId: th.Padding36("plan-3"), Path: "/3/out/1"}:        {},
			{OutputId: 3_001, PlanId: th.Padding36("plan-3"), Path: "/3/log/"}:         {IsLog: true},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			{InputId: 2_100, PlanId: th.Padding36("plan-2"), Path: "/2/in/1"}: {},
			{InputId: 2_200, PlanId: th.Padding36("plan-2"), Path: "/2/in/2"}: {},
			{InputId: 3_100, PlanId: th.Padding36("plan-3"), Path: "/3/in/1"}: {},
			{InputId: 3_200, PlanId: th.Padding36("plan-3"), Path: "/3/in/2"}: {},
		},
		Steps: []tables.Step{
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-1-pseudo/running"),
					PlanId:    th.Padding36("plan-1-pseudo"),
					Status:    kdb.Running,
					UpdatedAt: TIMESTAMP,
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/running/out/1"),
						VolumeRef: "#plan-1-pseudo/running/out/1",
						RunId:     th.Padding36("plan-1-pseudo/running"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-1-pseudo/done-a"),
					PlanId:    th.Padding36("plan-1-pseudo"),
					Status:    kdb.Done,
					UpdatedAt: TIMESTAMP,
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/done-a/out/1"),
						VolumeRef: "#plan-1-pseudo/done-a/out/1",
						RunId:     th.Padding36("plan-1-pseudo/done-a"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-1-pseudo/done-b"),
					PlanId:    th.Padding36("plan-1-pseudo"),
					Status:    kdb.Done,
					UpdatedAt: TIMESTAMP.Add(1 * time.Second),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/done-b/out/1"),
						VolumeRef: "#plan-1-pseudo/done-b/out/1",
						RunId:     th.Padding36("plan-1-pseudo/done-b"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-1-pseudo/done-c"),
					PlanId:    th.Padding36("plan-1-pseudo"),
					Status:    kdb.Done,
					UpdatedAt: TIMESTAMP.Add(1 * time.Second),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/done-c/out/1"),
						VolumeRef: "#plan-1-pseudo/done-c/out/1",
						RunId:     th.Padding36("plan-1-pseudo/done-c"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-1-pseudo/failed"),
					PlanId:    th.Padding36("plan-1-pseudo"),
					Status:    kdb.Failed,
					UpdatedAt: TIMESTAMP.Add(2 * time.Second),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/failed/out/1"),
						VolumeRef: "#plan-1-pseudo/failed/out/1",
						RunId:     th.Padding36("plan-1-pseudo/failed"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
					}: {},
				},
			},

			// plan 2; it has runs from deactiated to running
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2/deactivated"),
					PlanId:    th.Padding36("plan-2"),
					Status:    kdb.Deactivated,
					UpdatedAt: TIMESTAMP.Add(1 * time.Minute),
				},
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36("plan-2/deactivated"),
						PlanId:  th.Padding36("plan-2"),
						InputId: 2_100,
						KnitId:  th.Padding36("plan-1-pseudo/done-a/out/1"),
					},
					{
						RunId:   th.Padding36("plan-2/deactivated"),
						PlanId:  th.Padding36("plan-2"),
						InputId: 2_200,
						KnitId:  th.Padding36("plan-1-pseudo/done-a/out/1"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-2/deactivated/out/1"),
						VolumeRef: "#plan-2/deactivated/out/1",
						RunId:     th.Padding36("plan-2/deactivated"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_010,
					}: {},
					{
						KnitId:    th.Padding36("plan-2/deactivated/out/2"),
						VolumeRef: "#plan-2/deactivated/out/2",
						RunId:     th.Padding36("plan-2/deactivated"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_020,
					}: {},
					{
						KnitId:    th.Padding36("plan-2/deactivated/log"),
						VolumeRef: "#plan-2/deactivated/log",
						RunId:     th.Padding36("plan-2/deactivated"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_001,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2/waiting"),
					PlanId:    th.Padding36("plan-2"),
					Status:    kdb.Waiting,
					UpdatedAt: TIMESTAMP.Add(1 * time.Minute),
				},
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36("plan-2/waiting"),
						PlanId:  th.Padding36("plan-2"),
						InputId: 2_100,
						KnitId:  th.Padding36("plan-1-pseudo/done-a/out/1"),
					},
					{
						RunId:   th.Padding36("plan-2/waiting"),
						PlanId:  th.Padding36("plan-2"),
						InputId: 2_200,
						KnitId:  th.Padding36("plan-1-pseudo/done-b/out/1"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-2/waiting/out/1"),
						VolumeRef: "#plan-2/waiting/out/1",
						RunId:     th.Padding36("plan-2/waiting"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_010,
					}: {},
					{
						KnitId:    th.Padding36("plan-2/waiting/out/2"),
						VolumeRef: "#plan-2/waiting/out/2",
						RunId:     th.Padding36("plan-2/waiting"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_020,
					}: {},
					{
						KnitId:    th.Padding36("plan-2/waiting/log"),
						VolumeRef: "#plan-2/waiting/log",
						RunId:     th.Padding36("plan-2/waiting"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_001,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2/ready"),
					PlanId:    th.Padding36("plan-2"),
					Status:    kdb.Ready,
					UpdatedAt: TIMESTAMP.Add(1*time.Minute + 1*time.Second),
				},
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36("plan-2/ready"),
						PlanId:  th.Padding36("plan-2"),
						InputId: 2_100,
						KnitId:  th.Padding36("plan-1-pseudo/done-b/out/1"),
					},
					{
						RunId:   th.Padding36("plan-2/ready"),
						PlanId:  th.Padding36("plan-2"),
						InputId: 2_200,
						KnitId:  th.Padding36("plan-1-pseudo/done-b/out/1"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-2/ready/out/1"),
						VolumeRef: "#plan-2/ready/out/1",
						RunId:     th.Padding36("plan-2/ready"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_010,
					}: {},
					{
						KnitId:    th.Padding36("plan-2/ready/out/2"),
						VolumeRef: "#plan-2/ready/out/2",
						RunId:     th.Padding36("plan-2/ready"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_020,
					}: {},
					{
						KnitId:    th.Padding36("plan-2/ready/log"),
						VolumeRef: "#plan-2/ready/log",
						RunId:     th.Padding36("plan-2/ready"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_001,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2/starting"),
					PlanId:    th.Padding36("plan-2"),
					Status:    kdb.Starting,
					UpdatedAt: TIMESTAMP.Add(1*time.Minute + 1*time.Second),
				},
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36("plan-2/starting"),
						PlanId:  th.Padding36("plan-2"),
						InputId: 2_100,
						KnitId:  th.Padding36("plan-1-pseudo/done-b/out/1"),
					},
					{
						RunId:   th.Padding36("plan-2/starting"),
						PlanId:  th.Padding36("plan-2"),
						InputId: 2_200,
						KnitId:  th.Padding36("plan-1-pseudo/done-a/out/1"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-2/starting/out/1"),
						VolumeRef: "#plan-2/starting/out/1",
						RunId:     th.Padding36("plan-2/starting"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_010,
					}: {},
					{
						KnitId:    th.Padding36("plan-2/starting/out/2"),
						VolumeRef: "#plan-2/starting/out/2",
						RunId:     th.Padding36("plan-2/starting"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_020,
					}: {},
					{
						KnitId:    th.Padding36("plan-2/starting/log"),
						VolumeRef: "#plan-2/starting/log",
						RunId:     th.Padding36("plan-2/starting"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_001,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-2/running"),
					PlanId:    th.Padding36("plan-2"),
					Status:    kdb.Running,
					UpdatedAt: TIMESTAMP.Add(1*time.Minute + 1*time.Second),
				},
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36("plan-2/running"),
						PlanId:  th.Padding36("plan-2"),
						InputId: 2_100,
						KnitId:  th.Padding36("plan-1-pseudo/done-a/out/1"),
					},
					{
						RunId:   th.Padding36("plan-2/running"),
						PlanId:  th.Padding36("plan-2"),
						InputId: 2_200,
						KnitId:  th.Padding36("plan-1-pseudo/done-a/out/1"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-2/running/out/1"),
						VolumeRef: "#plan-2/running/out/1",
						RunId:     th.Padding36("plan-2/running"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_010,
					}: {},
					{
						KnitId:    th.Padding36("plan-2/running/out/2"),
						VolumeRef: "#plan-2/running/out/2",
						RunId:     th.Padding36("plan-2/running"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_020,
					}: {},
					{
						KnitId:    th.Padding36("plan-2/running/log"),
						VolumeRef: "#plan-2/running/log",
						RunId:     th.Padding36("plan-2/running"),
						PlanId:    th.Padding36("plan-2"),
						OutputId:  2_001,
					}: {},
				},
			},

			// plan 3; it has runs from running to invalidated
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-3/running"),
					PlanId:    th.Padding36("plan-3"),
					Status:    kdb.Running,
					UpdatedAt: TIMESTAMP.Add(2 * time.Minute),
				},
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36("plan-3/running"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3_100,
						KnitId:  th.Padding36("plan-1-pseudo/done-c/out/1"),
					},
					{
						RunId:   th.Padding36("plan-3/running"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3_200,
						KnitId:  th.Padding36("plan-1-pseudo/done-c/out/1"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-3/running/out/1"),
						VolumeRef: "#plan-3/running/out/1",
						RunId:     th.Padding36("plan-3/running"),
						PlanId:    th.Padding36("plan-3"),
						OutputId:  3_010,
					}: {},
					{
						KnitId:    th.Padding36("plan-3/running/log"),
						VolumeRef: "#plan-3/running/log",
						RunId:     th.Padding36("plan-3/running"),
						PlanId:    th.Padding36("plan-3"),
						OutputId:  3_001,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-3/completing"),
					PlanId:    th.Padding36("plan-3"),
					Status:    kdb.Completing,
					UpdatedAt: TIMESTAMP.Add(2 * time.Minute),
				},
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36("plan-3/completing"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3_100,
						KnitId:  th.Padding36("plan-1-pseudo/done-b/out/1"),
					},
					{
						RunId:   th.Padding36("plan-3/completing"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3_200,
						KnitId:  th.Padding36("plan-1-pseudo/done-b/out/1"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-3/completing/out/1"),
						VolumeRef: "#plan-3/completing/out/1",
						RunId:     th.Padding36("plan-3/completing"),
						PlanId:    th.Padding36("plan-3"),
						OutputId:  3_010,
					}: {},
					{
						KnitId:    th.Padding36("plan-3/completing/log"),
						VolumeRef: "#plan-3/completing/log",
						RunId:     th.Padding36("plan-3/completing"),
						PlanId:    th.Padding36("plan-3"),
						OutputId:  3_001,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-3/aborting"),
					PlanId:    th.Padding36("plan-3"),
					Status:    kdb.Aborting,
					UpdatedAt: TIMESTAMP.Add(2 * time.Minute),
				},
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36("plan-3/aborting"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3_100,
						KnitId:  th.Padding36("plan-1-pseudo/done-b/out/1"),
					},
					{
						RunId:   th.Padding36("plan-3/aborting"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3_200,
						KnitId:  th.Padding36("plan-1-pseudo/done-c/out/1"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-3/aborting/out/1"),
						VolumeRef: "#plan-3/aborting/out/1",
						RunId:     th.Padding36("plan-3/aborting"),
						PlanId:    th.Padding36("plan-3"),
						OutputId:  3_010,
					}: {},
					{
						KnitId:    th.Padding36("plan-3/aborting/log"),
						VolumeRef: "#plan-3/aborting/log",
						RunId:     th.Padding36("plan-3/aborting"),
						PlanId:    th.Padding36("plan-3"),
						OutputId:  3_001,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-3/done"),
					PlanId:    th.Padding36("plan-3"),
					Status:    kdb.Done,
					UpdatedAt: TIMESTAMP.Add(2 * time.Minute),
				},
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36("plan-3/done"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3_100,
						KnitId:  th.Padding36("plan-1-pseudo/done-c/out/1"),
					},
					{
						RunId:   th.Padding36("plan-3/done"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3_200,
						KnitId:  th.Padding36("plan-1-pseudo/done-b/out/1"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-3/done/out/1"),
						VolumeRef: "#plan-3/done/out/1",
						RunId:     th.Padding36("plan-3/done"),
						PlanId:    th.Padding36("plan-3"),
						OutputId:  3_010,
					}: {},
					{
						KnitId:    th.Padding36("plan-3/done/log"),
						VolumeRef: "#plan-3/done/log",
						RunId:     th.Padding36("plan-3/done"),
						PlanId:    th.Padding36("plan-3"),
						OutputId:  3_001,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-3/failed"),
					PlanId:    th.Padding36("plan-3"),
					Status:    kdb.Failed,
					UpdatedAt: TIMESTAMP.Add(2*time.Minute + 1*time.Second),
				},
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36("plan-3/failed"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3_100,
						KnitId:  th.Padding36("plan-1-pseudo/done-c/out/1"),
					},
					{
						RunId:   th.Padding36("plan-3/failed"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3_200,
						KnitId:  th.Padding36("plan-1-pseudo/done-c/out/1"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-3/failed/out/1"),
						VolumeRef: "#plan-3/failed/out/1",
						RunId:     th.Padding36("plan-3/failed"),
						PlanId:    th.Padding36("plan-3"),
						OutputId:  3_010,
					}: {},
					{
						KnitId:    th.Padding36("plan-3/failed/log"),
						VolumeRef: "#plan-3/failed/log",
						RunId:     th.Padding36("plan-3/failed"),
						PlanId:    th.Padding36("plan-3"),
						OutputId:  3_001,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-3/invalidated"),
					PlanId:    th.Padding36("plan-3"),
					Status:    kdb.Invalidated,
					UpdatedAt: TIMESTAMP.Add(2*time.Minute + 1*time.Second),
				},
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36("plan-3/invalidated"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3_100,
						KnitId:  th.Padding36("plan-1-pseudo/done-b/out/1"),
					},
					{
						RunId:   th.Padding36("plan-3/invalidated"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3_200,
						KnitId:  th.Padding36("plan-1-pseudo/done-b/out/1"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-3/invalidated/out/1"),
						VolumeRef: "#plan-3/invalidated/out/1",
						RunId:     th.Padding36("plan-3/invalidated"),
						PlanId:    th.Padding36("plan-3"),
						OutputId:  3_010,
					}: {},
					{
						KnitId:    th.Padding36("plan-3/invalidated/log"),
						VolumeRef: "#plan-3/invalidated/log",
						RunId:     th.Padding36("plan-3/invalidated"),
						PlanId:    th.Padding36("plan-3"),
						OutputId:  3_001,
					}: {},
				},
			},
		},
	}

	for name, data := range map[string]testcase{
		// zero dimensions
		"when querying no restrictions, it should find all runs": {
			when: kdb.RunFindQuery{},
			then: then{
				run: []string{
					th.Padding36("plan-1-pseudo/done-a"),
					th.Padding36("plan-1-pseudo/running"),
					th.Padding36("plan-1-pseudo/done-b"),
					th.Padding36("plan-1-pseudo/done-c"),
					th.Padding36("plan-1-pseudo/failed"),

					th.Padding36("plan-2/deactivated"),
					th.Padding36("plan-2/waiting"),
					th.Padding36("plan-2/ready"),
					th.Padding36("plan-2/running"),
					th.Padding36("plan-2/starting"),

					th.Padding36("plan-3/aborting"),
					th.Padding36("plan-3/completing"),
					th.Padding36("plan-3/done"),
					th.Padding36("plan-3/running"),
					th.Padding36("plan-3/failed"),
					th.Padding36("plan-3/invalidated"),
				},
			},
		},

		// single dimension
		"when querying by statuses, it should find runIds mathcing with the query": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Failed, kdb.Done},
			},
			then: then{
				run: []string{
					th.Padding36("plan-1-pseudo/done-a"),
					th.Padding36("plan-1-pseudo/done-b"),
					th.Padding36("plan-1-pseudo/done-c"),
					th.Padding36("plan-1-pseudo/failed"),
					th.Padding36("plan-3/done"),
					th.Padding36("plan-3/failed"),
				},
			},
		},
		"when querying by a status, it should find runIds mathcing with the query": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Deactivated},
			},
			then: then{
				run: []string{
					th.Padding36("plan-2/deactivated"),
				},
			},
		},
		"when querying by single Input KnitId, it should find runIds mathcing with the query": {
			when: kdb.RunFindQuery{
				InputKnitId: []string{th.Padding36("plan-1-pseudo/done-a/out/1")},
			},
			then: then{
				run: []string{
					th.Padding36("plan-2/deactivated"),
					th.Padding36("plan-2/waiting"),
					th.Padding36("plan-2/running"),
					th.Padding36("plan-2/starting"),
				},
			},
		},
		"when querying by nonexisting Input KnitId, it should find nothing": {
			when: kdb.RunFindQuery{
				InputKnitId: []string{th.Padding36("no-such-one")},
			},
			then: then{
				run: []string{}, // empty.
			},
		},
		"when querying by Input KnitIds, it should find runIds mathcing with the query": {
			when: kdb.RunFindQuery{
				InputKnitId: []string{
					th.Padding36("plan-1-pseudo/done-b/out/1"),
					th.Padding36("plan-1-pseudo/done-c/out/1"),
				},
			},
			then: then{
				run: []string{
					th.Padding36("plan-2/waiting"),
					th.Padding36("plan-2/ready"),
					th.Padding36("plan-2/starting"),

					th.Padding36("plan-3/aborting"),
					th.Padding36("plan-3/completing"),
					th.Padding36("plan-3/done"),
					th.Padding36("plan-3/running"),
					th.Padding36("plan-3/failed"),
					th.Padding36("plan-3/invalidated"),
				},
			},
		},
		"when querying by single Output KnitId, it should find runIds mathcing with the query": {
			when: kdb.RunFindQuery{
				OutputKnitId: []string{
					th.Padding36("plan-2/deactivated/log"),
				},
			},
			then: then{
				run: []string{
					th.Padding36("plan-2/deactivated"),
				},
			},
		},
		"when querying by Output KnitIds, it should find runIds mathcing with the query": {
			when: kdb.RunFindQuery{
				OutputKnitId: []string{
					th.Padding36("plan-2/running/out/2"),
					th.Padding36("plan-1-pseudo/done-a/out/1"),
				},
			},
			then: then{
				run: []string{
					th.Padding36("plan-1-pseudo/done-a"),
					th.Padding36("plan-2/running"),
				},
			},
		},
		"when querying by nonexisting Output KnitId, it should find nothing": {
			when: kdb.RunFindQuery{
				OutputKnitId: []string{th.Padding36("no-such-one")},
			},
			then: then{
				run: []string{}, // empty
			},
		},

		"when querying by a PlanId, it should find runIds mathcing with the query": {
			when: kdb.RunFindQuery{
				PlanId: []string{
					th.Padding36("plan-1-pseudo"),
				},
			},
			then: then{
				run: []string{
					th.Padding36("plan-1-pseudo/done-a"),
					th.Padding36("plan-1-pseudo/running"),
					th.Padding36("plan-1-pseudo/done-b"),
					th.Padding36("plan-1-pseudo/done-c"),
					th.Padding36("plan-1-pseudo/failed"),
				},
			},
		},
		"when querying by PlanIds, it should find runIds mathcing with the query": {
			when: kdb.RunFindQuery{
				PlanId: []string{
					th.Padding36("plan-1-pseudo"),
					th.Padding36("plan-3"),
				},
			},
			then: then{
				run: []string{
					th.Padding36("plan-1-pseudo/done-a"),
					th.Padding36("plan-1-pseudo/running"),
					th.Padding36("plan-1-pseudo/done-b"),
					th.Padding36("plan-1-pseudo/done-c"),
					th.Padding36("plan-1-pseudo/failed"),
					th.Padding36("plan-3/aborting"),
					th.Padding36("plan-3/completing"),
					th.Padding36("plan-3/done"),
					th.Padding36("plan-3/running"),
					th.Padding36("plan-3/failed"),
					th.Padding36("plan-3/invalidated"),
				},
			},
		},
		"when querying by nonexisting PlanId, it should find nothing": {
			when: kdb.RunFindQuery{
				PlanId: []string{th.Padding36("no-such-one")},
			},
			then: then{
				run: []string{}, // empty.
			},
		},

		// 2 dimensions
		"when querying by Status+InputKnitId, it should find runIds mathcing with the query": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Done, kdb.Completing},
				InputKnitId: []string{
					th.Padding36("plan-1-pseudo/done-c/out/1"),
				},
			},
			then: then{
				run: []string{
					th.Padding36("plan-3/done"),
				},
			},
		},
		"when querying by Status+InputKnitId but there are no runs match such query, it should find nothing": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Done},
				InputKnitId: []string{
					th.Padding36("plan-1-pseudo/done-a/out/1"),
				},
			},
			then: then{
				run: []string{}, // empty.
			},
		},

		"when querying by Status+OutputKnitId, it should find runIds mathcing with the query": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Failed},
				OutputKnitId: []string{
					th.Padding36("plan-3/failed/out/1"),
				},
			},
			then: then{
				run: []string{
					th.Padding36("plan-3/failed"),
				},
			},
		},
		"when querying by Status+OutputKnitId but no runs match that, it should find nothing": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Running},
				OutputKnitId: []string{
					th.Padding36("plan-3/failed/out/1"),
				},
			},
			then: then{
				run: []string{}, // nothing
			},
		},

		"when querying by Status+PlanId, it should find runIds matching with the query": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Running},
				PlanId: []string{
					th.Padding36("plan-2"),
				},
			},
			then: then{
				run: []string{
					th.Padding36("plan-2/running"),
				},
			},
		},
		"when querying by Status+PlanId but no runs match that, it should find nothing": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Completing},
				PlanId: []string{
					th.Padding36("plan-2"),
				},
			},
			then: then{
				run: []string{}, // empty.
			},
		},

		"when querying by InputKnitId+OutputKnitId, it should find runIds matching with the query": {
			when: kdb.RunFindQuery{
				InputKnitId:  []string{th.Padding36("plan-1-pseudo/done-a/out/1")},
				OutputKnitId: []string{th.Padding36("plan-2/deactivated/out/2")},
			},
			then: then{
				run: []string{
					th.Padding36("plan-2/deactivated"),
				},
			},
		},
		"when querying by InputKnitId+OutputKnitId but no runs match that, it should find nothing": {
			when: kdb.RunFindQuery{
				InputKnitId:  []string{th.Padding36("plan-1-pseudo/done-a/out/1")},
				OutputKnitId: []string{th.Padding36("plan-3/completing/out/1")},
			},
			then: then{
				run: []string{}, // empty.
			},
		},

		"when querying by InputKnitId+PlanId, it should find runIds matching with the query": {
			when: kdb.RunFindQuery{
				InputKnitId: []string{th.Padding36("plan-1-pseudo/done-a/out/1")},
				PlanId:      []string{th.Padding36("plan-2")},
			},
			then: then{
				run: []string{
					th.Padding36("plan-2/deactivated"),
					th.Padding36("plan-2/waiting"),
					th.Padding36("plan-2/running"),
					th.Padding36("plan-2/starting"),
				},
			},
		},
		"when querying by InputKnitId+PlanId but no runs match that, it should find nothing": {
			when: kdb.RunFindQuery{
				InputKnitId: []string{th.Padding36("plan-1-pseudo/done-a/out/1")},
				PlanId:      []string{th.Padding36("plan-3")},
			},
			then: then{
				run: []string{}, // empty.
			},
		},

		"when querying by OutputKnitId+PlanId, it should find runIds matching with the query": {
			when: kdb.RunFindQuery{
				OutputKnitId: []string{th.Padding36("plan-2/deactivated/out/2")},
				PlanId:       []string{th.Padding36("plan-2")},
			},
			then: then{
				run: []string{
					th.Padding36("plan-2/deactivated"),
				},
			},
		},
		"when querying by OutputKnitId+PlanId but no runs match that, it should find nothing": {
			when: kdb.RunFindQuery{
				OutputKnitId: []string{th.Padding36("plan-2/deactivated/out/2")},
				PlanId:       []string{th.Padding36("plan-3")},
			},
			then: then{
				run: []string{}, // empty.
			},
		},

		// 3 dimensions
		"when querying by Status+InputKnitId+OutputKnitId, it should find runIds matching with the query": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Ready, kdb.Completing},
				InputKnitId: []string{
					th.Padding36("plan-1-pseudo/done-b/out/1"),
				},
				OutputKnitId: []string{
					th.Padding36("plan-2/ready/out/2"),
					th.Padding36("plan-3/completing/out/1"),
				},
			},
			then: then{
				run: []string{
					th.Padding36("plan-2/ready"),
					th.Padding36("plan-3/completing"),
				},
			},
		},
		"when querying by Status+InputKnitId+OutputKnitId but there are no runs match such query, it should find nothing": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Running},
				InputKnitId: []string{
					th.Padding36("plan-1-pseudo/done-b/out/1"),
				},
				OutputKnitId: []string{
					th.Padding36("plan-2/waiting/out/1"),
					th.Padding36("plan-3/done/out/1"),
				},
			},
			then: then{
				run: []string{}, // empty.
			},
		},

		"when querying by Status+InputKnitId+PlanId, it should find runIds matching with the query": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Running},
				InputKnitId: []string{
					th.Padding36("plan-1-pseudo/done-a/out/1"),
				},
				PlanId: []string{
					th.Padding36("plan-2"),
				},
			},
			then: then{
				run: []string{
					th.Padding36("plan-2/running"),
				},
			},
		},
		"when querying by Status+InputKnitId+PlanId but there are no runs match such query, it should find nothing": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Waiting},
				InputKnitId: []string{
					th.Padding36("plan-1-pseudo/done-b/out/1"),
				},
				PlanId: []string{
					th.Padding36("plan-3"),
				},
			},
			then: then{
				run: []string{}, // empty.
			},
		},

		"when querying by Status+OutputKnitId+PlanId, it should find runIds matching with the query": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Running},
				OutputKnitId: []string{
					th.Padding36("plan-2/running/out/1"),
				},
				PlanId: []string{
					th.Padding36("plan-2"),
				},
			},
			then: then{
				run: []string{
					th.Padding36("plan-2/running"),
				},
			},
		},
		"when querying by Status+OutputKnitId+PlanId but there are no runs match such query, it should find nothing": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{kdb.Running},
				OutputKnitId: []string{
					th.Padding36("plan-3/running/out/1"),
				},
				PlanId: []string{
					th.Padding36("plan-2"),
				},
			},
			then: then{
				run: []string{}, // empty.
			},
		},

		"when querying by InputKnitId+OutputKnitId+PlanId, it should find runIds matching with the query": {
			when: kdb.RunFindQuery{
				InputKnitId: []string{
					th.Padding36("plan-1-pseudo/done-b/out/1"),
				},
				OutputKnitId: []string{
					th.Padding36("plan-3/done/out/1"),
				},
				PlanId: []string{
					th.Padding36("plan-3"),
				},
			},
			then: then{
				run: []string{
					th.Padding36("plan-3/done"),
				},
			},
		},
		"when querying by InputKnitId+OutputKnitId+PlanId but there are no runs match such query, it should find nothing": {
			when: kdb.RunFindQuery{
				InputKnitId: []string{
					th.Padding36("plan-1-pseudo/done-b/out/1"),
				},
				OutputKnitId: []string{
					th.Padding36("plan-2/ready/out/1"),
				},
				PlanId: []string{
					th.Padding36("plan-3"),
				},
			},
			then: then{
				run: []string{}, // empty.
			},
		},

		// 4 dimensions
		"when querying by Status+InputKnitId+OutputKnitId+PlanId, it should find runIds matching with the query": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{
					kdb.Starting, kdb.Running, kdb.Completing, kdb.Aborting,
				},
				InputKnitId: []string{
					th.Padding36("plan-1-pseudo/done-b/out/1"),
					th.Padding36("plan-1-pseudo/done-c/out/1"),
				},
				OutputKnitId: []string{
					th.Padding36("plan-3/running/out/1"),
				},
				PlanId: []string{
					th.Padding36("plan-3"),
				},
			},
			then: then{
				run: []string{
					th.Padding36("plan-3/running"),
				},
			},
		},
		"when querying by Status+InputKnitId+OutputKnitId+PlanId but there are no runs match such query, it should find nothing": {
			when: kdb.RunFindQuery{
				Status: []kdb.KnitRunStatus{
					kdb.Deactivated, kdb.Waiting,
				},
				InputKnitId: []string{
					th.Padding36("plan-1-pseudo/done-c/out/1"),
				},
				OutputKnitId: []string{
					th.Padding36("plan-2/waiting/out/1"),
				},
				PlanId: []string{
					th.Padding36("plan-2"),
				},
			},
			then: then{
				run: []string{}, // empty.
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pgpool := poolBroaker.GetPool(ctx, t)
			if err := given.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			testee := kpgrun.New(pgpool) // only finding. no new items.
			actual := try.To(testee.Find(ctx, data.when)).OrFatal(t)

			if !cmp.SliceEq(actual, data.then.run) {
				t.Errorf(
					"runs does not match. (actual, expected) = \n(%+v, \n%+v)",
					actual, data.then.run,
				)
			}
		})
	}

}

// Test to search using the new fields, "Since" and "Duration", added to RunFindQuery
func TestRun_Find_Add(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	type then struct {
		run []string
	}
	type testcase struct {
		when kdb.RunFindQuery
		then
	}

	dummyUpdatedSince := try.To(rfctime.ParseRFC3339DateTime(
		"2023-04-01T13:34:45+00:00",
	)).OrFatal(t).Time()

	//  time.Duration type.
	dummyUpdatedUntil_1 := try.To(rfctime.ParseRFC3339DateTime(
		"2023-04-01T13:34:45+00:00",
	)).OrFatal(t).Time().Add(30 * time.Minute)
	dummyUpdatedUntil_2 := try.To(rfctime.ParseRFC3339DateTime(
		"2023-04-01T13:34:45+00:00",
	)).OrFatal(t).Time().Add(2*time.Hour + 10*time.Minute)

	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: th.Padding36("plan-1-pseudo"), Hash: "#pseudo", Active: true},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: th.Padding36("plan-1-pseudo"), Name: "uploaded"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 1_010, PlanId: th.Padding36("plan-1-pseudo"), Path: "/1/out/1"}: {},
		},
		Inputs: map[tables.Input]tables.InputAttr{},
		Steps: []tables.Step{
			// four run based on plan-1-pseudo
			// one of UpdatedAt is basedtime - 1hour
			// the other is basedtime
			// the other is basedtime + 30s
			// the other is basedtime + 1minute + 1hour
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-1-pseudo/-1hour"),
					PlanId:    th.Padding36("plan-1-pseudo"),
					Status:    kdb.Done,
					UpdatedAt: dummyUpdatedSince.Add(-1 * time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/-1hour/out/1"),
						VolumeRef: "#plan-1-pseudo/-1hour/out/1",
						RunId:     th.Padding36("plan-1-pseudo/-1hour"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-1-pseudo/basedtime"),
					PlanId:    th.Padding36("plan-1-pseudo"),
					Status:    kdb.Done,
					UpdatedAt: dummyUpdatedSince,
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/basedtime/out/1"),
						VolumeRef: "#plan-1-pseudo/basedtime/out/1",
						RunId:     th.Padding36("plan-1-pseudo/basedtime"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
					}: {},
				},
			},

			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-1-pseudo/30s"),
					PlanId:    th.Padding36("plan-1-pseudo"),
					Status:    kdb.Failed,
					UpdatedAt: dummyUpdatedSince.Add(30 * time.Second),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/30s/out/2"),
						VolumeRef: "#plan-1-pseudo/30s/out/2",
						RunId:     th.Padding36("plan-1-pseudo/30s"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
					}: {},
				},
			},

			{
				Run: tables.Run{
					RunId:     th.Padding36("plan-1-pseudo/1minute/1hour"),
					PlanId:    th.Padding36("plan-1-pseudo"),
					Status:    kdb.Failed,
					UpdatedAt: dummyUpdatedSince.Add(1*time.Minute + 1*time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("plan-1-pseudo/1minute/1hour/out/2"),
						VolumeRef: "#plan-1-pseudo/1minute/1hour/out/2",
						RunId:     th.Padding36("plan-1-pseudo/1minute/1hour"),
						PlanId:    th.Padding36("plan-1-pseudo"),
						OutputId:  1_010,
					}: {},
				},
			},
		},
	}

	for name, data := range map[string]testcase{
		"when no querying, it should find all runIds": {
			when: kdb.RunFindQuery{
				UpdatedSince: nil,
				UpdatedUntil: nil,
			},
			then: then{
				run: []string{
					th.Padding36("plan-1-pseudo/-1hour"),
					th.Padding36("plan-1-pseudo/basedtime"),
					th.Padding36("plan-1-pseudo/30s"),
					th.Padding36("plan-1-pseudo/1minute/1hour"),
				},
			},
		},
		"when querying by UpdatedAt, it should find runIds matching with the query": {
			when: kdb.RunFindQuery{
				UpdatedSince: &dummyUpdatedSince,
				UpdatedUntil: nil,
			},
			then: then{
				run: []string{

					th.Padding36("plan-1-pseudo/basedtime"),
					th.Padding36("plan-1-pseudo/30s"),
					th.Padding36("plan-1-pseudo/1minute/1hour"),
				},
			},
		},
		"when querying by UpdatedAt and Duration specified minutes, it should find runIds matching with the query": {
			when: kdb.RunFindQuery{
				UpdatedSince: &dummyUpdatedSince,
				UpdatedUntil: &dummyUpdatedUntil_1,
			},
			then: then{
				run: []string{
					th.Padding36("plan-1-pseudo/basedtime"),
					th.Padding36("plan-1-pseudo/30s"),
				},
			},
		},

		"when querying by UpdatedAt and Duration specified days and weeks, it should find runIds matching with the query": {
			when: kdb.RunFindQuery{
				UpdatedSince: &dummyUpdatedSince,
				UpdatedUntil: &dummyUpdatedUntil_2,
			},
			then: then{
				run: []string{
					th.Padding36("plan-1-pseudo/basedtime"),
					th.Padding36("plan-1-pseudo/30s"),
					th.Padding36("plan-1-pseudo/1minute/1hour"),
				},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pgpool := poolBroaker.GetPool(ctx, t)
			if err := given.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			testee := kpgrun.New(pgpool) // only finding. no new items.
			actual := try.To(testee.Find(ctx, data.when)).OrFatal(t)

			if !cmp.SliceEq(actual, data.then.run) {
				t.Errorf(
					"runs does not match. (actual, expected) = \n(%+v, \n%+v)",
					actual, data.then.run,
				)
			}
		})
	}

}
