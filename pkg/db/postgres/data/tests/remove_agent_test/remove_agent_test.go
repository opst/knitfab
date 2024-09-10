package remove_agent_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	kpgdata "github.com/opst/knitfab/pkg/db/postgres/data"
	testenv "github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	. "github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestRemoveAgent(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	knitId := Padding36("test-knit-done")
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
					PlanId: "test-plan", RunId: "test-run-done", Status: kdb.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: knitId, VolumeRef: "#test-knit-done",
						PlanId: "test-plan", RunId: "test-run-done", OutputId: 1_010,
					}: {
						UserTag: []kdb.Tag{
							{Key: "tag-a", Value: "a-value"},
							{Key: "tag-b", Value: "b-value"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-10-11T12:13:14.567+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
		},
	}

	type Record struct {
		Name                  string
		KnitId                string
		Mode                  string
		LifecycleSuspendUntil time.Time
	}

	type When struct {
		Name string
	}
	type Then struct {
		Names []string
		Error error
	}

	type Testcase struct {
		Given []Record
		When  When
		Then  Then
	}

	theory := func(testcase Testcase) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			conn := try.To(pool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			for _, item := range testcase.Given {
				try.To(conn.Exec(
					ctx,
					`
						insert into "data_agent"
						("name", "mode", "knit_id", "lifecycle_suspend_until")
						values ($1, $2, $3, $4)
						`,
					item.Name, item.Mode, item.KnitId, item.LifecycleSuspendUntil,
				)).OrFatal(t)
			}

			testee := kpgdata.New(pool)

			err := testee.RemoveAgent(ctx, testcase.When.Name)
			if testcase.Then.Error != nil {
				if errors.Is(err, testcase.Then.Error) {
					t.Errorf(
						"unexpected error:\n===actual===\n%+v\n===expected===\n%+v",
						err, testcase.Then.Error,
					)
				}
			} else if err != nil {
				t.Fatal(err)
			}

			actual := try.To(scanner.New[string]().QueryAll(
				ctx, conn,
				`select "name" from "data_agent"`,
			)).OrFatal(t)

			if !cmp.SliceContentEq(
				actual, testcase.Then.Names,
			) {
				t.Errorf(
					"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
					actual, testcase.Then.Names,
				)
			}
		}
	}

	t.Run("when there is no such agent, it does nothing", theory(Testcase{
		Given: []Record{
			{
				Name:                  "agent-a",
				KnitId:                knitId,
				Mode:                  string(kdb.DataAgentRead),
				LifecycleSuspendUntil: time.Now().Add(30 * time.Second),
			},
			{
				Name:                  "agent-b",
				KnitId:                knitId,
				Mode:                  string(kdb.DataAgentRead),
				LifecycleSuspendUntil: time.Now().Add(30 * time.Second),
			},
		},
		When: When{Name: "no-such-agent"},
		Then: Then{
			Names: []string{"agent-a", "agent-b"},
		},
	}))

	t.Run("when there are an agent with specified name, it removes the record", theory(Testcase{
		Given: []Record{
			{
				Name:                  "agent-a",
				KnitId:                knitId,
				Mode:                  string(kdb.DataAgentRead),
				LifecycleSuspendUntil: time.Now().Add(30 * time.Second),
			},
			{
				Name:                  "agent-b",
				KnitId:                knitId,
				Mode:                  string(kdb.DataAgentRead),
				LifecycleSuspendUntil: time.Now().Add(30 * time.Second),
			},
		},
		When: When{Name: "agent-a"},
		Then: Then{
			Names: []string{"agent-b"},
		},
	}))

}
