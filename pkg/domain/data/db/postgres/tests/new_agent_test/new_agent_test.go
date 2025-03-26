package new_agent_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/opst/knitfab-api-types/v2/misc/rfctime"
	testenv "github.com/opst/knitfab/v2/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/v2/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/v2/pkg/domain"
	kpgdata "github.com/opst/knitfab/v2/pkg/domain/data/db/postgres"
	kerr "github.com/opst/knitfab/v2/pkg/domain/errors"
	"github.com/opst/knitfab/v2/pkg/domain/internal/db/postgres/tables"
	"github.com/opst/knitfab/v2/pkg/domain/internal/db/postgres/tables/matcher"
	. "github.com/opst/knitfab/v2/pkg/domain/internal/db/postgres/testhelpers"
	"github.com/opst/knitfab/v2/pkg/utils/cmp"
	"github.com/opst/knitfab/v2/pkg/utils/pointer"
	"github.com/opst/knitfab/v2/pkg/utils/try"
)

func TestNewAgent(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	knitIdDone := Padding36("test-knit-done")
	knitIdPurged := Padding36("test-knit-purged")
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
					PlanId: "test-plan", RunId: "test-run-done", Status: domain.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: knitIdDone,
						PlanId: "test-plan", RunId: "test-run-done", OutputId: 1_010,
					}: {
						VolumeRef: "#test-knit-done",
						UserTag: []domain.Tag{
							{Key: "tag-a", Value: "a-value"},
							{Key: "tag-b", Value: "b-value"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-10-11T12:13:14.567+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-output-is-purged", Status: domain.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: knitIdPurged,
						PlanId: "test-plan", RunId: "test-run-output-is-purged", OutputId: 1_010,
					}: {
						// no volume ref
						UserTag: []domain.Tag{
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

	type When struct {
		Mode             domain.DataAgentMode
		LifecycleSuspend time.Duration
	}
	{
		theorySingleAgent := func(when When) func(*testing.T) {
			return func(t *testing.T) {
				ctx := context.Background()
				pool := poolBroaker.GetPool(ctx, t)

				if err := given.Apply(ctx, pool); err != nil {
					t.Fatal(err)
				}

				testee := kpgdata.New(pool)

				conn := try.To(pool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()
				before := try.To(PGNow(ctx, conn)).OrFatal(t)
				agent, err := testee.NewAgent(ctx, knitIdDone, when.Mode, when.LifecycleSuspend)
				if err != nil {
					t.Fatal(err)
				}
				after := try.To(PGNow(ctx, conn)).OrFatal(t)

				expectedAgentNamePrefix := fmt.Sprintf("knitid-%s-%s-", knitIdDone, when.Mode)

				{
					expected := struct {
						NamePrefix   string
						Mode         domain.DataAgentMode
						KnitDataBody domain.KnitDataBody
					}{
						NamePrefix: expectedAgentNamePrefix,
						Mode:       when.Mode,
						KnitDataBody: domain.KnitDataBody{
							KnitId:    knitIdDone,
							VolumeRef: pointer.Ref("#test-knit-done"),
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "tag-a", Value: "a-value"},
								{Key: "tag-b", Value: "b-value"},
								{Key: domain.KeyKnitId, Value: knitIdDone},
								{Key: domain.KeyKnitTimestamp, Value: "2022-10-11T12:13:14.567+09:00"},
							}),
						},
					}
					if !strings.HasPrefix(agent.Name, expected.NamePrefix) ||
						agent.Mode != expected.Mode ||
						!agent.KnitDataBody.Equal(&expected.KnitDataBody) {
						t.Errorf(
							"unmatch DataAgent:\n===actual===\n%+v\n===expected===\n%+v",
							agent, expected,
						)
					}
				}

				{
					actual := try.To(scanner.New[tables.DataAgent]().QueryAll(
						ctx, conn,
						`select * from "data_agent" where "knit_id" = $1`,
						knitIdDone,
					)).OrFatal(t)
					expected := []matcher.DataAgentMatcher{
						{
							Name:   matcher.Prefix(expectedAgentNamePrefix),
							KnitId: matcher.EqEq(knitIdDone),
							Mode:   matcher.EqEq(when.Mode.String()),
							LifecycleSuspendUntil: matcher.Between(
								before, after.Add(when.LifecycleSuspend),
							),
						},
					}

					if !cmp.SliceContentEqWith(expected, actual, matcher.DataAgentMatcher.Match) {
						t.Errorf(
							"unmatch DataAgent:\n===actual===\n%+v\n===expected===\n%+v",
							actual, expected,
						)
					}
				}
			}
		}

		t.Run("when mode is 'read'", theorySingleAgent(When{
			Mode:             domain.DataAgentRead,
			LifecycleSuspend: 30 * time.Second,
		}))

		t.Run("when mode is 'write'", theorySingleAgent(When{
			Mode:             domain.DataAgentWrite,
			LifecycleSuspend: 30 * time.Minute,
		}))

	}

	t.Run("when create new agent for not-existing Data, it should error ErrMissing", func(t *testing.T) {
		ctx := context.Background()
		pool := poolBroaker.GetPool(ctx, t)

		testee := kpgdata.New(pool)

		_, err := testee.NewAgent(ctx, Padding36("not-existing-knit"), domain.DataAgentRead, 30*time.Second)
		if !errors.Is(err, kerr.ErrMissing) {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("when create new agent for purged Data, it should error ErrDataIsPurged", func(t *testing.T) {
		ctx := context.Background()
		pool := poolBroaker.GetPool(ctx, t)
		if err := given.Apply(ctx, pool); err != nil {
			t.Fatal(err)
		}

		testee := kpgdata.New(pool)

		_, err := testee.NewAgent(ctx, knitIdPurged, domain.DataAgentRead, 30*time.Second)
		if !errors.Is(err, domain.ErrDataIsPurged) {
			t.Errorf("unexpected error: %v", err)
		}
	})

	{
		type Condition struct {
			when      When
			wantError bool
		}
		theoryMultipleAgents := func(testcase []Condition) func(*testing.T) {
			return func(t *testing.T) {
				ctx := context.Background()
				pool := poolBroaker.GetPool(ctx, t)

				if err := given.Apply(ctx, pool); err != nil {
					t.Fatal(err)
				}

				testee := kpgdata.New(pool)

				conn := try.To(pool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()

				for nth, item := range testcase {
					_, err := testee.NewAgent(
						ctx, knitIdDone, item.when.Mode, item.when.LifecycleSuspend,
					)
					if item.wantError {
						if err == nil {
							t.Errorf("#%d: expected error but got nil", nth+1)
						}
					} else {
						if err != nil {
							t.Errorf("#%d: unexpected error: %v", nth+1, err)
						}
					}
				}
			}
		}

		t.Run("when create many 'read' agents, it should success", theoryMultipleAgents([]Condition{
			{when: When{Mode: domain.DataAgentRead, LifecycleSuspend: 30 * time.Second}},
			{when: When{Mode: domain.DataAgentRead, LifecycleSuspend: 30 * time.Second}},
			{when: When{Mode: domain.DataAgentRead, LifecycleSuspend: 30 * time.Second}},
		}))

		t.Run("when create many 'write' agents, it should error", theoryMultipleAgents([]Condition{
			{when: When{Mode: domain.DataAgentWrite, LifecycleSuspend: 30 * time.Second}},
			{when: When{Mode: domain.DataAgentWrite, LifecycleSuspend: 30 * time.Second}, wantError: true},
		}))

		t.Run("when create many 'write' and 'read' agents, it should error", theoryMultipleAgents([]Condition{
			{when: When{Mode: domain.DataAgentRead, LifecycleSuspend: 30 * time.Second}},
			{when: When{Mode: domain.DataAgentWrite, LifecycleSuspend: 30 * time.Second}},
			{when: When{Mode: domain.DataAgentRead, LifecycleSuspend: 30 * time.Second}},
			{when: When{Mode: domain.DataAgentWrite, LifecycleSuspend: 30 * time.Second}, wantError: true},
		}))
	}

}
