package pick_and_remove_agent_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	kpgdata "github.com/opst/knitfab/pkg/db/postgres/data"
	"github.com/opst/knitfab/pkg/db/postgres/pool/proxy"
	testenv "github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	"github.com/opst/knitfab/pkg/db/postgres/tables/matcher"
	. "github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestPickAndRemoveAgent(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	knitId := Padding36("test-knit-done")
	fixture := tables.Operation{
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
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-10-11T12:13:15.567+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
		},
	}

	type RecordSpec struct {
		Name     string
		Mode     kdb.DataAgentMode
		KnitId   string
		Debounce time.Duration
	}

	type Given struct {
		Agents []RecordSpec
		Locked []string
	}

	type When struct {
		Cursor            kdb.DataAgentCursor
		DoRemove          bool
		ErrorFromCallback error
	}

	type Then struct {
		LockedNames  []string
		DataAgent    kdb.DataAgent
		WantCallback bool
		Cursor       kdb.DataAgentCursor
		Error        error
		Records      []RecordSpec
	}

	theory := func(given Given, when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pool := poolBroaker.GetPool(ctx, t)

			if err := fixture.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			wpool := proxy.Wrap(pool)
			wpool.Events().Query.After(func() {
				conn := try.To(pool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()

				unlockedNames := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "name" from "data_agent" for update skip locked`,
					),
				).OrFatal(t)

				for _, nameActual := range unlockedNames {
					for _, nameNotExpected := range then.LockedNames {
						if nameActual != nameNotExpected {
							continue
						}
						t.Errorf(
							"lock is not enough:\n===not locked===\n%+v\n===should be locked===\n%+v",
							unlockedNames, then.LockedNames,
						)
						return
					}
				}
			})
			testee := kpgdata.New(wpool)

			conn := try.To(pool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			before := try.To(PGNow(ctx, conn)).OrFatal(t)
			for _, item := range given.Agents {
				_, err := conn.Exec(
					ctx,
					`
					insert into "data_agent"
					("name", "mode", "knit_id", "lifecycle_suspend_until")
					values ($1, $2, $3, now() + $4)
					`,
					item.Name, item.Mode, item.KnitId, item.Debounce,
				)
				if err != nil {
					t.Fatal(err)
				}
			}
			if 0 < len(given.Locked) {
				tx := try.To(pool.Begin(ctx)).OrFatal(t)
				defer tx.Rollback(ctx)
				locked := try.To(scanner.New[string]().QueryAll(
					ctx, tx,
					`
					select "name" from "data_agent"
					where "name" = any($1::varchar[])
					for update
					`,
					given.Locked,
				)).OrFatal(t)
				if !cmp.SliceContentEq(locked, given.Locked) {
					t.Fatalf("failed to lock: got %+v, want %+v", locked, given.Locked)
				}
			}

			called := false
			actualCursor, err := testee.PickAndRemoveAgent(
				ctx, when.Cursor,
				func(da kdb.DataAgent) (bool, error) {
					called = true
					if !then.DataAgent.Equal(&da) {
						t.Errorf(
							"picked DataAgent:\n===actual===\n%+v\n===expected===\n%+v",
							da, then.DataAgent,
						)
					}
					return when.DoRemove, when.ErrorFromCallback
				},
			)
			after := try.To(PGNow(ctx, conn)).OrFatal(t)

			if then.Error != nil {
				if err == nil {
					t.Errorf("expected error but got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}

			if actualCursor != then.Cursor {
				t.Errorf(
					"Cursor:\n===actual===\n%+v\n===expected===\n%+v",
					actualCursor, then.Cursor,
				)
			}

			if called != then.WantCallback {
				t.Errorf(
					"calling callback (true=want, false=not want): actual=%v, expected=%v",
					called, then.WantCallback,
				)
			}

			expectedRecords := utils.Map(
				then.Records,
				func(r RecordSpec) matcher.DataAgentMatcher {
					return matcher.DataAgentMatcher{
						Name:   matcher.EqEq(r.Name),
						Mode:   matcher.EqEq(string(r.Mode)),
						KnitId: matcher.EqEq(r.KnitId),
						LifecycleSuspendUntil: matcher.Between(
							before.Add(r.Debounce), after.Add(r.Debounce),
						),
					}
				},
			)

			records := try.To(scanner.New[tables.DataAgent]().QueryAll(
				ctx, conn, `select * from "data_agent"`,
			)).OrFatal(t)

			if !cmp.SliceContentEqWith(
				expectedRecords, records, matcher.DataAgentMatcher.Match,
			) {
				t.Errorf(
					"Records:\n===actual===\n%+v\n===expected===\n%+v",
					records, expectedRecords,
				)
			}
		}
	}

	t.Run("Let no DataAgents are given, it do nothing", theory(
		Given{},
		When{
			Cursor: kdb.DataAgentCursor{},
		},
		Then{
			WantCallback: false,
			Cursor:       kdb.DataAgentCursor{},
		},
	))

	t.Run("Let DataAgents are given, it picks a unsuspended DataAgent named next of Cursor Head, and remove", theory(
		Given{
			Agents: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: -20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
		When{
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-12",
				Debounce: time.Hour,
			},
			DoRemove:          true,
			ErrorFromCallback: nil,
		},
		Then{
			LockedNames:  []string{"knitid-test-knit-done-read-14"},
			WantCallback: true,
			DataAgent: kdb.DataAgent{
				Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead,
				KnitDataBody: kdb.KnitDataBody{
					KnitId: knitId, VolumeRef: "#test-knit-done",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "tag-a", Value: "a-value"},
						{Key: kdb.KeyKnitId, Value: knitId},
						{Key: kdb.KeyKnitTimestamp, Value: "2022-10-11T12:13:15.567+09:00"},
					}),
				},
			},
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-14",
				Debounce: time.Hour,
			},
			Error: nil,
			Records: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				// {KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, LifecycleSuspendUntilDelta: -20 * time.Second},  // removed
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
	))

	t.Run("Let DataAgents are given and some records are locked, it picks a unsuspended & unlocked DataAgent named next of Cursor Head, and remove", theory(
		Given{
			Agents: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: -20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
			Locked: []string{"knitid-test-knit-done-read-14"},
		},
		When{
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-12",
				Debounce: time.Hour,
			},
			DoRemove:          true,
			ErrorFromCallback: nil,
		},
		Then{
			LockedNames: []string{
				"knitid-test-knit-done-read-14", // by given precondition
				"knitid-test-knit-done-read-16", // by testee
			},
			WantCallback: true,
			DataAgent: kdb.DataAgent{
				Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead,
				KnitDataBody: kdb.KnitDataBody{
					KnitId: knitId, VolumeRef: "#test-knit-done",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "tag-a", Value: "a-value"},
						{Key: kdb.KeyKnitId, Value: knitId},
						{Key: kdb.KeyKnitTimestamp, Value: "2022-10-11T12:13:15.567+09:00"},
					}),
				},
			},
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-16",
				Debounce: time.Hour,
			},
			Error: nil,
			Records: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: -20 * time.Second},
				// {KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, LifecycleSuspendUntilDelta: -10 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
	))

	t.Run("Let DataAgents are given but all of them are suspended, it does not pick", theory(
		Given{
			Agents: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
		When{
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-12",
				Debounce: time.Hour,
			},
			DoRemove:          true,
			ErrorFromCallback: nil,
		},
		Then{
			WantCallback: false,
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-12",
				Debounce: time.Hour,
			},
			Error: nil,
			Records: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
	))

	t.Run("Let DataAgents are given, it picks a unsuspended DataAgent named next of Cursor Head, and suspend when it is not deleted", theory(
		Given{
			Agents: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: -20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
		When{
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-12",
				Debounce: time.Hour,
			},
			DoRemove:          false,
			ErrorFromCallback: nil,
		},
		Then{
			LockedNames:  []string{"knitid-test-knit-done-read-14"},
			WantCallback: true,
			DataAgent: kdb.DataAgent{
				Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead,
				KnitDataBody: kdb.KnitDataBody{
					KnitId: knitId, VolumeRef: "#test-knit-done",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "tag-a", Value: "a-value"},
						{Key: kdb.KeyKnitId, Value: knitId},
						{Key: kdb.KeyKnitTimestamp, Value: "2022-10-11T12:13:15.567+09:00"},
					}),
				},
			},
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-14",
				Debounce: time.Hour,
			},
			Error: nil,
			Records: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: time.Hour},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
	))

	expectedError := errors.New("fake error")
	t.Run("Let DataAgents are given, it picks a unsuspended DataAgent named next of Cursor Head, and debounce it when callabck errors", theory(
		Given{
			Agents: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: -20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
		When{
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-12",
				Debounce: time.Hour,
			},
			DoRemove:          true,
			ErrorFromCallback: expectedError,
		},
		Then{
			LockedNames:  []string{"knitid-test-knit-done-read-14"},
			WantCallback: true,
			DataAgent: kdb.DataAgent{
				Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead,
				KnitDataBody: kdb.KnitDataBody{
					KnitId: knitId, VolumeRef: "#test-knit-done",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "tag-a", Value: "a-value"},
						{Key: kdb.KeyKnitId, Value: knitId},
						{Key: kdb.KeyKnitTimestamp, Value: "2022-10-11T12:13:15.567+09:00"},
					}),
				},
			},
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-14",
				Debounce: time.Hour,
			},
			Error: expectedError,
			Records: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: time.Hour},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
	))
}
