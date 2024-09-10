package get_agent_name_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	kpgdata "github.com/opst/knitfab/pkg/db/postgres/data"
	testenv "github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	. "github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestGetAgentName(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: "test-plan", Active: true, Hash: "#test-plan"},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: "test-plan", Name: "pseudo"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{PlanId: "test-plan", OutputId: 1_010, Path: "/out"}: {},
		},
		Steps: []tables.Step{
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-running-1", Status: kdb.Running,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("test-knit-running-1"), VolumeRef: "#test-knit-running-1",
						PlanId: "test-plan", RunId: "test-run-running-1", OutputId: 1_010,
					}: {
						Agent: []tables.DataAgent{
							{
								Name: "test-agent-1", Mode: kdb.DataAgentRead.String(),
								KnitId: Padding36("test-knit-running-1"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2022-10-11T12:13:14.567+09:00",
								)).OrFatal(t).Time(),
							},
							{
								Name: "test-agent-2", Mode: kdb.DataAgentRead.String(),
								KnitId: Padding36("test-knit-running-1"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2022-10-11T12:13:15.567+09:00",
								)).OrFatal(t).Time(),
							},
							{
								Name: "test-agent-3", Mode: kdb.DataAgentRead.String(),
								KnitId: Padding36("test-knit-running-1"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2022-10-11T12:13:16.567+09:00",
								)).OrFatal(t).Time(),
							},
							{
								Name: "test-agent-4", Mode: kdb.DataAgentWrite.String(),
								KnitId: Padding36("test-knit-running-1"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2022-10-11T12:13:17.567+09:00",
								)).OrFatal(t).Time(),
							},
						},
					},
				},
			},
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-running-2", Status: kdb.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("test-knit-running-2"), VolumeRef: "#test-knit-running-2",
						PlanId: "test-plan", RunId: "test-run-running-2", OutputId: 1_010,
					}: {},
				},
			},
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-running-3", Status: kdb.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("test-knit-running-3"), VolumeRef: "#test-knit-running-3",
						PlanId: "test-plan", RunId: "test-run-running-3", OutputId: 1_010,
					}: {
						Agent: []tables.DataAgent{
							{
								Name: "test-agent-5", Mode: kdb.DataAgentWrite.String(),
								KnitId: Padding36("test-knit-running-3"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2022-10-11T12:13:17.567+09:00",
								)).OrFatal(t).Time(),
							},
						},
					},
				},
			},
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-running-4", Status: kdb.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("test-knit-running-4"), VolumeRef: "#test-knit-running-4",
						PlanId: "test-plan", RunId: "test-run-running-4", OutputId: 1_010,
					}: {
						Agent: []tables.DataAgent{
							{
								Name: "test-agent-6", Mode: kdb.DataAgentRead.String(),
								KnitId: Padding36("test-knit-running-4"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2022-10-11T12:13:18.567+09:00",
								)).OrFatal(t).Time(),
							},
						},
					},
				},
			},
		},
	}

	type When struct {
		KnitId string
		Mode   []kdb.DataAgentMode
	}

	type Then struct {
		Names []string
		Error error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			testee := kpgdata.New(pool)

			actual, err := testee.GetAgentName(ctx, when.KnitId, when.Mode)

			if then.Error != nil {
				if !errors.Is(err, then.Error) {
					t.Errorf("expected error %v but got %v", then.Error, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !cmp.SliceContentEq(actual, then.Names) {
				t.Errorf(
					"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
					actual, then.Names,
				)
			}
		}
	}

	t.Run("when there is no such data, it returns Missing error", theory(
		When{
			KnitId: Padding36("no-such-knit"),
			Mode:   []kdb.DataAgentMode{kdb.DataAgentRead, kdb.DataAgentWrite},
		},
		Then{
			Names: []string{},
			Error: kdb.ErrMissing,
		},
	))

	t.Run("when there is no such agent, it returns empty", theory(
		When{
			KnitId: Padding36("test-knit-running-2"),
			Mode:   []kdb.DataAgentMode{kdb.DataAgentRead, kdb.DataAgentWrite},
		},
		Then{
			Names: []string{},
			Error: nil,
		},
	))

	t.Run("when there is an agent with specified mode (read), it returns the name", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Mode:   []kdb.DataAgentMode{kdb.DataAgentRead},
		},
		Then{
			Names: []string{"test-agent-1", "test-agent-2", "test-agent-3"},
			Error: nil,
		},
	))

	t.Run("when there is an agent with specified mode (write), it returns the name", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Mode:   []kdb.DataAgentMode{kdb.DataAgentWrite},
		},
		Then{
			Names: []string{"test-agent-4"},
			Error: nil,
		},
	))
}
