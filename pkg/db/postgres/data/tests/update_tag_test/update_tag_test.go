package update_tag_test

import (
	"context"
	"errors"
	"testing"

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
						UserTag: []kdb.Tag{
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
		},
	}

	type When struct {
		KnitId string
		Delta  kdb.TagDelta
	}

	type Then struct {
		KnownTags []kdb.Tag
		Tagging   map[string][]kdb.Tag // knitId -> []Tag
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
				actual := try.To(scanner.New[kdb.Tag]().QueryAll(
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
					func(a, b kdb.Tag) bool { return a.Equal(&b) },
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

				tagging := map[string][]kdb.Tag{}
				for _, r := range actual {
					tagging[r.KnitId] = append(
						tagging[r.KnitId],
						kdb.Tag{Key: r.Key, Value: r.Value},
					)
				}

				if !cmp.MapEqWith(
					tagging, then.Tagging,
					func(a, b []kdb.Tag) bool {
						return cmp.SliceContentEqWith(
							a, b,
							func(a, b kdb.Tag) bool { return a.Equal(&b) },
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
			Delta:  kdb.TagDelta{Add: []kdb.Tag{{Key: "tag-a", Value: "value 1"}}},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: kdb.ErrMissing,
		},
	))

	t.Run("when adding new tag, it adds the tag", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta:  kdb.TagDelta{Add: []kdb.Tag{{Key: "tag-c", Value: "value 1"}}},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
				{Key: "tag-c", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
					{Key: "tag-c", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when adding existing tag, it does nothing", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta:  kdb.TagDelta{Add: []kdb.Tag{{Key: "tag-a", Value: "value 1"}}},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
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
			Delta:  kdb.TagDelta{Remove: []kdb.Tag{{Key: "tag-a", Value: "value 1"}}},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
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
			Delta:  kdb.TagDelta{Remove: []kdb.Tag{{Key: "tag-c", Value: "value 1"}}},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when adding and removing tags, it adds and removes the tags", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta: kdb.TagDelta{
				Add:    []kdb.Tag{{Key: "tag-c", Value: "value 1"}},
				Remove: []kdb.Tag{{Key: "tag-a", Value: "value 1"}},
			},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
				{Key: "tag-c", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
					{Key: "tag-c", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when adding and removing same new tag, it results as it was", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta: kdb.TagDelta{
				Add:    []kdb.Tag{{Key: "tag-a", Value: "value 3"}},
				Remove: []kdb.Tag{{Key: "tag-a", Value: "value 3"}},
			},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-a", Value: "value 3"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when adding and removing same existing tag, it removes the tags", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta: kdb.TagDelta{
				Add:    []kdb.Tag{{Key: "tag-a", Value: "value 2"}},
				Remove: []kdb.Tag{{Key: "tag-a", Value: "value 2"}},
			},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

}
