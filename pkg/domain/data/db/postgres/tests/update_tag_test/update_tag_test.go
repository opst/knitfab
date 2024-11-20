package update_tag_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab-api-types/misc/rfctime"
	testenv "github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/domain"
	kpgdata "github.com/opst/knitfab/pkg/domain/data/db/postgres"
	kerr "github.com/opst/knitfab/pkg/domain/errors"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	. "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestUpdateTag(t *testing.T) {
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
			{PlanId: "test-plan", OutputId: 1_020, Path: "/out"}: {},
		},
		Steps: []tables.Step{
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-running-1", Status: domain.Running,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("test-knit-running-1"), VolumeRef: "#test-knit-running-1",
						PlanId: "test-plan", RunId: "test-run-running-1", OutputId: 1_010,
					}: {
						UserTag: []domain.Tag{
							{Key: "tag-a", Value: "value 1"},
							{Key: "tag-a", Value: "value 2"},
							{Key: "tag-b", Value: "value 1"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-10-11T12:13:15.567+09:00",
						)).OrFatal(t).Time()),
					},
					{
						KnitId: Padding36("test-knit-running-2"), VolumeRef: "#test-knit-running-2",
						PlanId: "test-plan", RunId: "test-run-running-1", OutputId: 1_020,
					}: {
						UserTag: []domain.Tag{
							{Key: "tag-a", Value: "value 1"},
							{Key: "tag-a", Value: "value 2"},
							{Key: "tag-b", Value: "value 1"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-10-11T12:13:15.567+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-running-2", Status: domain.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("test-knit-running-3"), VolumeRef: "#test-knit-running-3",
						PlanId: "test-plan", RunId: "test-run-running-2", OutputId: 1_010,
					}: {},
				},
			},
		},
	}

	type When struct {
		KnitId string
		Delta  domain.TagDelta
	}

	type Then struct {
		KnownTags []domain.Tag
		Tagging   map[string][]domain.Tag // knitId -> []Tag
		Error     error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			testee := kpgdata.New(pool)

			err := testee.UpdateTag(ctx, when.KnitId, when.Delta)

			if then.Error != nil {
				if !errors.Is(err, then.Error) {
					t.Errorf("expected error %v but got %v", then.Error, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			conn := try.To(pool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			{
				actual := try.To(scanner.New[domain.Tag]().QueryAll(
					ctx, conn,
					`
					select
						"tag_key"."key" as "key",
						"tag"."value" as "value"
					from "tag"
					inner join "tag_key"
						on "tag"."key_id" = "tag_key"."id"
					`,
				)).OrFatal(t)

				if !cmp.SliceContentEqWith(
					actual, then.KnownTags,
					func(a, b domain.Tag) bool { return a.Equal(&b) },
				) {
					t.Errorf(
						"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
						actual, then.KnownTags,
					)
				}
			}

			{
				actual := try.To(scanner.New[struct {
					KnitId string
					Key    string
					Value  string
				}]().QueryAll(
					ctx, conn,
					`
					select
						"tag_data"."knit_id" as "knit_id",
						"tag_key"."key" as "key",
						"tag"."value" as "value"
					from "tag_data"
					inner join "tag"
						on "tag_data"."tag_id" = "tag"."id"
					inner join "tag_key"
						on "tag"."key_id" = "tag_key"."id"
					`,
				)).OrFatal(t)

				tagging := map[string][]domain.Tag{}
				for _, r := range actual {
					tagging[r.KnitId] = append(
						tagging[r.KnitId],
						domain.Tag{Key: r.Key, Value: r.Value},
					)
				}

				if !cmp.MapEqWith(
					tagging, then.Tagging,
					func(a, b []domain.Tag) bool {
						return cmp.SliceContentEqWith(
							a, b,
							func(a, b domain.Tag) bool { return a.Equal(&b) },
						)
					},
				) {
					t.Errorf(
						"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
						tagging, then.Tagging,
					)
				}
			}
		}
	}

	t.Run("when there is no such data, it returns Missing error", theory(
		When{
			KnitId: Padding36("no-such-knit"),
			Delta:  domain.TagDelta{Add: []domain.Tag{{Key: "tag-a", Value: "value 1"}}},
		},
		Then{
			KnownTags: []domain.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]domain.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
				Padding36("test-knit-running-2"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: kerr.ErrMissing,
		},
	))

	t.Run("when adding new tag, it adds the tag", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta:  domain.TagDelta{Add: []domain.Tag{{Key: "tag-c", Value: "value 1"}}},
		},
		Then{
			KnownTags: []domain.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
				{Key: "tag-c", Value: "value 1"},
			},
			Tagging: map[string][]domain.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
					{Key: "tag-c", Value: "value 1"},
				},
				Padding36("test-knit-running-2"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when adding existing tag, it does nothing", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta:  domain.TagDelta{Add: []domain.Tag{{Key: "tag-a", Value: "value 1"}}},
		},
		Then{
			KnownTags: []domain.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]domain.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
				Padding36("test-knit-running-2"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when removing existing tag, it removes the tag", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta:  domain.TagDelta{Remove: []domain.Tag{{Key: "tag-a", Value: "value 1"}}},
		},
		Then{
			KnownTags: []domain.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]domain.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
				Padding36("test-knit-running-2"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when removing existing tag by key, it removes the tag", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta:  domain.TagDelta{RemoveKey: []string{"tag-a"}},
		},
		Then{
			KnownTags: []domain.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]domain.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-b", Value: "value 1"},
				},
				Padding36("test-knit-running-2"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when removing non-existing tag, it does nothing", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta:  domain.TagDelta{Remove: []domain.Tag{{Key: "tag-c", Value: "value 1"}}},
		},
		Then{
			KnownTags: []domain.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]domain.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
				Padding36("test-knit-running-2"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when removing non-existing tag by key, it does nothing", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta:  domain.TagDelta{RemoveKey: []string{"tag-c"}},
		},
		Then{
			KnownTags: []domain.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]domain.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
				Padding36("test-knit-running-2"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when adding and removing tags, it removes and then add the tags", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta: domain.TagDelta{
				Add:       []domain.Tag{{Key: "tag-c", Value: "value 1"}},
				Remove:    []domain.Tag{{Key: "tag-a", Value: "value 1"}},
				RemoveKey: []string{"tag-b"},
			},
		},
		Then{
			KnownTags: []domain.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
				{Key: "tag-c", Value: "value 1"},
			},
			Tagging: map[string][]domain.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-c", Value: "value 1"},
				},
				Padding36("test-knit-running-2"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when adding and removing same new tag, the new tag is added", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta: domain.TagDelta{
				Add:    []domain.Tag{{Key: "tag-a", Value: "value 3"}},
				Remove: []domain.Tag{{Key: "tag-a", Value: "value 3"}},
			},
		},
		Then{
			KnownTags: []domain.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-a", Value: "value 3"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]domain.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-a", Value: "value 3"},
					{Key: "tag-b", Value: "value 1"},
				},
				Padding36("test-knit-running-2"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when adding and removing same existing tag, the tags are remained", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta: domain.TagDelta{
				Add:    []domain.Tag{{Key: "tag-a", Value: "value 2"}},
				Remove: []domain.Tag{{Key: "tag-a", Value: "value 2"}},
			},
		},
		Then{
			KnownTags: []domain.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]domain.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
				Padding36("test-knit-running-2"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

}
