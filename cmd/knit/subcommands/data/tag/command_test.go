package tag_test

import (
	"context"
	"errors"
	"testing"

	dara "github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	apitag "github.com/opst/knitfab-api-types/tags"
	rmock "github.com/opst/knitfab/cmd/knit/rest/mock"
	data_tag "github.com/opst/knitfab/cmd/knit/subcommands/data/tag"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestDataTag(t *testing.T) {

	t.Run("when client does not cause any error, updating (add and remove) tags of data is done successfully.", func(t *testing.T) {
		// prepare for test
		mock := rmock.New(t)

		logger := logger.Null()

		// setting data for test
		addTags := []apitag.UserTag{
			{Key: "key1", Value: "val1"},
			{Key: "key2", Value: "val2"},
		}
		removeTags := []apitag.UserTag{
			{Key: "remkey1", Value: "remval1"},
			{Key: "remkey2", Value: "remval2"},
		}
		expectedPutHist := []rmock.PutTagsForDataArgs{
			{
				KnitId: "1234",
				Tags: apitag.Change{
					AddTags: []apitag.UserTag{
						{Key: "key1", Value: "val1"},
						{Key: "key2", Value: "val2"},
					},
					RemoveTags: []apitag.UserTag{
						{Key: "remkey1", Value: "remval1"},
						{Key: "remkey2", Value: "remval2"},
					},
				},
			},
		}

		// setting function for test
		mock.Impl.PutTagsForData = func(knitId string, argtags apitag.Change) (*dara.Detail, error) {
			res := dara.Detail{
				KnitId: knitId,
				Tags: []apitag.Tag{
					{Key: "key1", Value: "val1"},
					{Key: "key2", Value: "val2"},
					{Key: "another-tag", Value: "value-x"},
					{Key: apitag.KeyKnitId, Value: knitId},
					{Key: apitag.KeyKnitTimestamp, Value: ""},
				},
				Upstream: dara.AssignedTo{
					Run: runs.Summary{
						RunId: "run#1", Status: "done",
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-10-11T13:14:15+00:00",
						)).OrFatal(t),
						Plan: plans.Summary{
							PlanId: "uploaded", Name: "knit#uploaded",
						},
					},
				},
				Downstreams: []dara.AssignedTo{
					{
						Run: runs.Summary{
							RunId: "run#2", Status: "failed",
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T13:14:15+00:00",
							)).OrFatal(t),
							Plan: plans.Summary{
								PlanId: "plan#1",
								Image:  &plans.Image{Repository: "repo.invalid/image1", Tag: "v1"},
							},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/in",
							Tags: []apitag.Tag{
								{Key: "remkey1", Value: "remval1"},
								{Key: "another-tag", Value: "value-x"},
							},
						},
					},
					{
						Run: runs.Summary{
							RunId: "run#3", Status: "running",
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T13:16:15+00:00",
							)).OrFatal(t),
							Plan: plans.Summary{
								PlanId: "plan#2",
								Image:  &plans.Image{Repository: "repo.invalid/image2", Tag: "v1"},
							},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/in",
							Tags: []apitag.Tag{
								{Key: apitag.KeyKnitId, Value: knitId},
							},
						},
					},
				},
				Nomination: []dara.NominatedBy{
					{
						Plan: plans.Summary{
							PlanId: "plan#2",
							Image:  &plans.Image{Repository: "repo.invalid/image2", Tag: "v1"},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/in",
							Tags: []apitag.Tag{
								{Key: apitag.KeyKnitId, Value: knitId},
							},
						},
					},
					{
						Plan: plans.Summary{
							PlanId: "plan#3",
							Image:  &plans.Image{Repository: "repo.invalid/image3", Tag: "v0.1"},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/trigger",
							Tags: []apitag.Tag{
								{Key: "key2", Value: "val2"},
								{Key: "another-tag", Value: "value-x"},
							},
						},
					},
				},
			}
			return &res, nil
		}

		// test start

		ctx := context.Background()
		err := data_tag.UpdateTag(ctx, logger, mock, "1234", addTags, removeTags)

		if err != nil {
			t.Errorf("unexpected error occured:%v\n", err)
		}
		if len(mock.Calls.PutTagsForData) != 1 {
			t.Error("tags put function was not called correctly.")
		}
		if !comparePutHist(mock.Calls.PutTagsForData, expectedPutHist) {
			t.Error("data post function was not called with incorrect args.")
		}
	})

	t.Run("when client does not cause any error, adding tag to data is done successfully.", func(t *testing.T) {
		// prepare for test
		mock := rmock.New(t)

		logger := logger.Null()

		// setting data for test
		addTags := []apitag.UserTag{
			{Key: "key1", Value: "val1"},
			{Key: "key2", Value: "val2"},
		}
		removeTags := []apitag.UserTag{}
		expectedPutHist := []rmock.PutTagsForDataArgs{
			{
				KnitId: "1234",
				Tags: apitag.Change{
					AddTags: []apitag.UserTag{
						{Key: "key1", Value: "val1"}, {Key: "key2", Value: "val2"},
					},
					RemoveTags: []apitag.UserTag{},
				},
			},
		}

		// setting function for test
		mock.Impl.PutTagsForData = func(knitId string, argtags apitag.Change) (*dara.Detail, error) {
			res := dara.Detail{
				KnitId: knitId,
				Tags: []apitag.Tag{
					{Key: "key1", Value: "val1"},
					{Key: "key2", Value: "val2"},
					{Key: "another-tag", Value: "value-x"},
					{Key: apitag.KeyKnitId, Value: knitId},
					{Key: apitag.KeyKnitTimestamp, Value: ""},
				},
				Upstream: dara.AssignedTo{
					Run: runs.Summary{
						RunId: "run#1", Status: "done",
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-10-11T13:14:15+00:00",
						)).OrFatal(t),
						Plan: plans.Summary{
							PlanId: "uploaded", Name: "knit#uploaded",
						},
					},
				},
				Downstreams: []dara.AssignedTo{
					{
						Run: runs.Summary{
							RunId: "run#2", Status: "failed",
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T13:14:15+00:00",
							)).OrFatal(t),
							Plan: plans.Summary{
								PlanId: "plan#1",
								Image:  &plans.Image{Repository: "repo.invalid/image1", Tag: "v1"},
							},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/in",
							Tags: []apitag.Tag{
								{Key: "remkey1", Value: "remval1"},
								{Key: "another-tag", Value: "value-x"},
							},
						},
					},
					{
						Run: runs.Summary{
							RunId: "run#3", Status: "running",
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T13:16:15+00:00",
							)).OrFatal(t),
							Plan: plans.Summary{
								PlanId: "plan#2",
								Image:  &plans.Image{Repository: "repo.invalid/image2", Tag: "v1"},
							},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/in",
							Tags: []apitag.Tag{
								{Key: apitag.KeyKnitId, Value: knitId},
							},
						},
					},
				},
				Nomination: []dara.NominatedBy{
					{
						Plan: plans.Summary{
							PlanId: "plan#2",
							Image:  &plans.Image{Repository: "repo.invalid/image2", Tag: "v1"},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/in",
							Tags: []apitag.Tag{
								{Key: apitag.KeyKnitId, Value: knitId},
							},
						},
					},
					{
						Plan: plans.Summary{
							PlanId: "plan#3",
							Image:  &plans.Image{Repository: "repo.invalid/image3", Tag: "v0.1"},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/trigger",
							Tags: []apitag.Tag{
								{Key: "key2", Value: "val2"},
								{Key: "another-tag", Value: "value-x"},
							},
						},
					},
				},
			}
			return &res, nil
		}

		// test start
		ctx := context.Background()
		err := data_tag.UpdateTag(ctx, logger, mock, "1234", addTags, removeTags)

		if err != nil {
			t.Errorf("unexpected error occured:%v\n", err)
		}
		if len(mock.Calls.PutTagsForData) != 1 {
			t.Error("tags put function was not called correctly.")
		}
		if !comparePutHist(mock.Calls.PutTagsForData, expectedPutHist) {
			t.Error("data post function was not called with incorrect args.")
		}
	})

	t.Run("when client does not cause any error, removing tags from the data is done successfully.", func(t *testing.T) {
		// prepare for test
		mock := rmock.New(t)

		logger := logger.Null()

		// setting data for test
		addTags := []apitag.UserTag{}
		removeTags := []apitag.UserTag{
			{Key: "remkey1", Value: "remval1"},
			{Key: "remkey2", Value: "remval2"},
		}
		expectedPutHist := []rmock.PutTagsForDataArgs{
			{
				KnitId: "1234",
				Tags: apitag.Change{
					AddTags:    addTags,
					RemoveTags: removeTags,
				},
			},
		}

		// setting function for test
		mock.Impl.PutTagsForData = func(knitId string, argtags apitag.Change) (*dara.Detail, error) {
			res := dara.Detail{
				KnitId: knitId,
				Tags: []apitag.Tag{
					{Key: "key1", Value: "val1"},
					{Key: "key2", Value: "val2"},
					{Key: "another-tag", Value: "value-x"},
					{Key: apitag.KeyKnitId, Value: knitId},
					{Key: apitag.KeyKnitTimestamp, Value: ""},
				},
				Upstream: dara.AssignedTo{
					Run: runs.Summary{
						RunId: "run#1", Status: "done",
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-10-11T13:14:15+00:00",
						)).OrFatal(t),
						Plan: plans.Summary{
							PlanId: "uploaded", Name: "knit#uploaded",
						},
					},
				},
				Downstreams: []dara.AssignedTo{
					{
						Run: runs.Summary{
							RunId: "run#2", Status: "failed",
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T13:14:15+00:00",
							)).OrFatal(t),
							Plan: plans.Summary{
								PlanId: "plan#1",
								Image:  &plans.Image{Repository: "repo.invalid/image1", Tag: "v1"},
							},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/in",
							Tags: []apitag.Tag{
								{Key: "remkey1", Value: "remval1"},
								{Key: "another-tag", Value: "value-x"},
							},
						},
					},
					{
						Run: runs.Summary{
							RunId: "run#3", Status: "running",
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T13:16:15+00:00",
							)).OrFatal(t),
							Plan: plans.Summary{
								PlanId: "plan#2",
								Image:  &plans.Image{Repository: "repo.invalid/image2", Tag: "v1"},
							},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/in",
							Tags: []apitag.Tag{
								{Key: apitag.KeyKnitId, Value: knitId},
							},
						},
					},
				},
				Nomination: []dara.NominatedBy{
					{
						Plan: plans.Summary{
							PlanId: "plan#2",
							Image:  &plans.Image{Repository: "repo.invalid/image2", Tag: "v1"},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/in",
							Tags: []apitag.Tag{
								{Key: apitag.KeyKnitId, Value: knitId},
							},
						},
					},
					{
						Plan: plans.Summary{
							PlanId: "plan#3",
							Image:  &plans.Image{Repository: "repo.invalid/image3", Tag: "v0.1"},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/trigger",
							Tags: []apitag.Tag{
								{Key: "key2", Value: "val2"},
								{Key: "another-tag", Value: "value-x"},
							},
						},
					},
				},
			}
			return &res, nil
		}

		// test start
		ctx := context.Background()
		err := data_tag.UpdateTag(ctx, logger, mock, "1234", addTags, removeTags)

		if err != nil {
			t.Errorf("unexpected error occured:%v\n", err)
		}
		if len(mock.Calls.PutTagsForData) != 1 {
			t.Error("tags put function was not called correctly.")
		}

		if !comparePutHist(mock.Calls.PutTagsForData, expectedPutHist) {
			t.Error("data post function was not called with incorrect args.")
		}
	})

	t.Run("it returns error when client.PutTagsForData returns error.", func(t *testing.T) {
		mock := rmock.New(t)

		logger := logger.Null()

		// setting data for test
		addTags := []apitag.UserTag{{Key: "key1", Value: "val1"}, {Key: "key2", Value: "val2"}}
		removeTags := []apitag.UserTag{{Key: "remkey1", Value: "remval1"}, {Key: "remkey2", Value: "remval2"}}

		// setting function for test
		mock.Impl.PutTagsForData = func(knitId string, argtags apitag.Change) (*dara.Detail, error) {
			return nil, errors.New("Internel server error 500")
		}

		// test start
		ctx := context.Background()
		err := data_tag.UpdateTag(ctx, logger, mock, "1234", addTags, removeTags)

		if err == nil {
			t.Errorf("unmatch error")
		}
		if len(mock.Calls.PutTagsForData) != 1 {
			t.Error("tags put function was not called correctly.")
		}
	})
}

func comparePutHist(actualHist, expectedHist []rmock.PutTagsForDataArgs) bool {

	if len(actualHist) != len(expectedHist) {
		return false
	}

	for i, a := range actualHist {
		if !cmp.SliceContentEq(a.Tags.AddTags, expectedHist[i].Tags.AddTags) ||
			!cmp.SliceContentEq(a.Tags.RemoveTags, expectedHist[i].Tags.RemoveTags) {
			return false
		}
	}
	return true
}
