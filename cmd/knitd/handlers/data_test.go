package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	httptestutil "github.com/opst/knitfab/internal/testutils/http"
	kdb "github.com/opst/knitfab/pkg/db"
	dbmock "github.com/opst/knitfab/pkg/db/mocks"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/try"

	"github.com/opst/knitfab/cmd/knitd/handlers"
)

func TestGetDataForDataHandler(t *testing.T) {

	t.Run("When data is received from the database, it should be converted to JSON format", func(t *testing.T) {
		mckdbdata := dbmock.NewDataInterface()
		mckdbdata.Impl.Find = func(ctx context.Context, tags []kdb.Tag, since *time.Time, until *time.Time) ([]string, error) {
			return []string{"knit-1", "knit-2"}, nil
		}
		mckdbdata.Impl.Get = func(ctx context.Context, knitId []string) (map[string]kdb.KnitData, error) {
			d := map[string]kdb.KnitData{
				"knit-1": {
					KnitDataBody: kdb.KnitDataBody{
						KnitId: "knit-1", VolumeRef: "pvc-knit-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "project", Value: "test-project"},
							{Key: "series", Value: "42"},
							{Key: "type", Value: "training-dataset"},
							{Key: "format", Value: "semantic-segmentation"},
							{Key: kdb.KeyKnitId, Value: "knit-1"},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-07-29T01:10:25.100+09:00"},
						}),
					},
					Upsteram: kdb.Dependency{
						RunBody: kdb.RunBody{
							Id: "run-1", Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-07-29T01:10:25.666+09:00",
							)).OrFatal(t).Time(),
							PlanBody: kdb.PlanBody{
								PlanId: "plan-1", Active: true, Hash: "#plan-1",
								Pseudo: &kdb.PseudoPlanDetail{Name: "knit#uploaded"},
							},
						},
						MountPoint: kdb.MountPoint{Id: 1010, Path: "/out"},
					},
					Downstreams: []kdb.Dependency{
						{
							RunBody: kdb.RunBody{
								Id: "run-2", Status: kdb.Running,
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
									"2022-07-30T01:10:25.222+09:00",
								)).OrFatal(t).Time(),
								PlanBody: kdb.PlanBody{
									PlanId: "plan-2", Active: true, Hash: "hash-2",
									Image: &kdb.ImageIdentifier{Image: "repo.invalid/trainer", Version: "v1"},
								},
							},
							MountPoint: kdb.MountPoint{
								Id: 2100, Path: "/in",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "test-project"},
									{Key: "type", Value: "training-dataset"},
									{Key: "format", Value: "semantic-segmentation"},
								}),
							},
						},
						{
							RunBody: kdb.RunBody{
								Id: "run-x", Status: kdb.Invalidated,
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
									"2022-10-11T12:13:14+00:00",
								)).OrFatal(t).Time(),
							},
							MountPoint: kdb.MountPoint{
								Id: 10100, Path: "/in",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "test-project"},
									{Key: "type", Value: "training-dataset"},
									{Key: "format", Value: "semantic-segmentation"},
								}),
							},
						},
					},
					NominatedBy: []kdb.Nomination{
						{
							PlanBody: kdb.PlanBody{
								PlanId: "plan-2", Active: true, Hash: "hash-2",
								Image: &kdb.ImageIdentifier{Image: "repo.invalid/trainer", Version: "v1"},
							},
							MountPoint: kdb.MountPoint{
								Id: 2100, Path: "/in",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "training-dataset"},
									{Key: "format", Value: "semantic-segmentation"},
								}),
							},
						},
					},
				},
				"knit-2": {
					KnitDataBody: kdb.KnitDataBody{
						KnitId: "knit-2", VolumeRef: "pvc-knit-2",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "model-parameter"},
							{Key: "framework", Value: "pytorch"},
							{Key: "task", Value: "semantic-segmentation"},
							{Key: kdb.KeyKnitId, Value: "knit-2"},
							{Key: kdb.KeyKnitTransient, Value: "processing"},
						}),
					},
					Upsteram: kdb.Dependency{
						RunBody: kdb.RunBody{
							Id: "run-2", Status: kdb.Running,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-07-30T01:10:25.222+09:00",
							)).OrFatal(t).Time(),
							PlanBody: kdb.PlanBody{
								PlanId: "plan-2", Active: true, Hash: "hash-2",
								Image: &kdb.ImageIdentifier{Image: "repo.invalid/trainer", Version: "v1"},
							},
						},
						MountPoint: kdb.MountPoint{
							Id: 2100, Path: "/out",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "type", Value: "model-parameter"},
								{Key: "framework", Value: "pytorch"},
								{Key: "task", Value: "semantic-segmentation"},
							}),
						},
					},
				},
			}
			return d, nil
		}

		e := echo.New()
		c, respRec := httptestutil.Get(e, "/api/data/?tag=Key-1:Value-2&tag=Key-2:Value-5&tag=knit#transient:processing&since=2024-04-01T12%3A00%3A00%2B00%3A00&duration=2h30m45s")

		testee := handlers.GetDataForDataHandler(mckdbdata)
		if err := testee(c); err != nil {
			t.Fatal(err)
		}

		expectTag := []kdb.Tag{
			{Key: "Key-1", Value: "Value-2"},
			{Key: "Key-2", Value: "Value-5"},
			{Key: "knit#transient", Value: "processing"},
		}

		expectedTime := "2024-04-01T12:00:00+00:00"
		expectedSince := try.To(rfctime.ParseRFC3339DateTime(expectedTime)).OrFatal(t).Time()
		expectedUntil := try.To(rfctime.ParseRFC3339DateTime(expectedTime)).OrFatal(t).Time().Add(2*time.Hour + 30*time.Minute + 45*time.Second)

		if !cmp.SliceEqWith(
			mckdbdata.Calls.Find,
			[]struct {
				Tags  []kdb.Tag
				Since *time.Time
				Until *time.Time
			}{
				{Tags: expectTag, Since: &expectedSince, Until: &expectedUntil},
			},
			func(
				a struct {
					Tags  []kdb.Tag
					Since *time.Time
					Until *time.Time
				},
				b struct {
					Tags  []kdb.Tag
					Since *time.Time
					Until *time.Time
				}) bool {
				return cmp.SliceContentEqWith(utils.RefOf(a.Tags), utils.RefOf(b.Tags), (*kdb.Tag).Equal) &&
					a.Since.Equal(*b.Since) && a.Until.Equal(*b.Until)
			},
		) {
			t.Error("DataInterface.Find did not call with correct userTag args.")
		}

		expected := []data.Detail{
			{
				KnitId: "knit-1",
				Tags: []tags.Tag{
					{Key: "project", Value: "test-project"},
					{Key: "series", Value: "42"},
					{Key: "type", Value: "training-dataset"},
					{Key: "format", Value: "semantic-segmentation"},
					{Key: kdb.KeyKnitId, Value: "knit-1"},
					{Key: kdb.KeyKnitTimestamp, Value: "2022-07-29T01:10:25.100+09:00"},
				},
				Upstream: data.AssignedTo{
					Run: runs.Summary{
						RunId: "run-1", Status: string(kdb.Done),
						Plan: plans.Summary{PlanId: "plan-1", Name: "knit#uploaded"},
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-07-29T01:10:25.666+09:00",
						)).OrFatal(t),
					},
					Mountpoint: plans.Mountpoint{Path: "/out"},
				},
				Downstreams: []data.AssignedTo{
					{
						Run: runs.Summary{
							RunId: "run-2", Status: string(kdb.Running),
							Plan: plans.Summary{
								PlanId: "plan-2",
								Image:  &plans.Image{Repository: "repo.invalid/trainer", Tag: "v1"},
							},
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-07-30T01:10:25.222+09:00",
							)).OrFatal(t),
						},
						Mountpoint: plans.Mountpoint{
							Path: "/in",
							Tags: []tags.Tag{
								{Key: "project", Value: "test-project"},
								{Key: "type", Value: "training-dataset"},
								{Key: "format", Value: "semantic-segmentation"},
							},
						},
					},
					// invalidated run should not appear
				},
				Nomination: []data.NominatedBy{
					{
						Plan: plans.Summary{
							PlanId: "plan-2",
							Image:  &plans.Image{Repository: "repo.invalid/trainer", Tag: "v1"},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/in",
							Tags: []tags.Tag{
								{Key: "type", Value: "training-dataset"},
								{Key: "format", Value: "semantic-segmentation"},
							},
						},
					},
				},
			},
			{
				KnitId: "knit-2",
				Tags: []tags.Tag{
					{Key: "type", Value: "model-parameter"},
					{Key: "framework", Value: "pytorch"},
					{Key: "task", Value: "semantic-segmentation"},
					{Key: kdb.KeyKnitId, Value: "knit-2"},
					{Key: kdb.KeyKnitTransient, Value: "processing"},
				},
				Upstream: data.AssignedTo{
					Run: runs.Summary{
						RunId: "run-2", Status: string(kdb.Running),
						Plan: plans.Summary{
							PlanId: "plan-2",
							Image:  &plans.Image{Repository: "repo.invalid/trainer", Tag: "v1"},
						},
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-07-30T01:10:25.222+09:00",
						)).OrFatal(t),
					},
					Mountpoint: plans.Mountpoint{
						Path: "/out",
						Tags: []tags.Tag{
							{Key: "type", Value: "model-parameter"},
							{Key: "framework", Value: "pytorch"},
							{Key: "task", Value: "semantic-segmentation"},
						},
					},
				},
				Downstreams: []data.AssignedTo{},
				Nomination:  []data.NominatedBy{},
			},
		}

		actualResponse := []data.Detail{}
		if err := json.Unmarshal(respRec.Body.Bytes(), &actualResponse); err != nil {
			t.Errorf("response is not illegal. error = %v", err)
		}

		if !cmp.SliceEqWith(actualResponse, expected, data.Detail.Equal) {
			t.Errorf(
				"data does not match. (actual, expected) = \n(%+v, \n%+v)",
				actualResponse, expected,
			)
		}
	})

	t.Run("When no such knitId exists, return an empty response without error", func(t *testing.T) {
		knitId := []string{}

		mckdbdata := dbmock.NewDataInterface()
		mckdbdata.Impl.Find = func(ctx context.Context, tags []kdb.Tag, since *time.Time, until *time.Time) ([]string, error) {
			d := knitId
			return d, nil
		}

		e := echo.New()
		c, respRec := httptestutil.Get(e, "/api/data/?tag=knit#transient:processing")

		testee := handlers.GetDataForDataHandler(mckdbdata)
		err := testee(c)
		if err != nil {
			t.Errorf("response is not illegal. error = %v", err)
		}

		expectedStatusCode := 200
		statusCode := respRec.Result().StatusCode
		if respRec.Result().StatusCode != expectedStatusCode {
			t.Errorf("status code %d != %d", statusCode, expectedStatusCode)
		}
		expectedContentType := "application/json"
		contentTypeBody := strings.Split(respRec.Result().Header.Get("Content-Type"), ";")[0]
		if contentTypeBody != expectedContentType {
			t.Errorf("Content-Type: %s != %s", expectedContentType, contentTypeBody)
		}

		expected := []data.Detail{}
		actualResponse := []data.Detail{}
		err = json.Unmarshal(respRec.Body.Bytes(), &actualResponse)
		if err != nil {
			t.Errorf("response is not illegal. error = %v", err)
		}

		if !cmp.SliceContentEqWith(actualResponse, expected, data.Detail.Equal) {
			t.Errorf(
				"data does not match. (actual, expected) = \n(%v, \n%v)",
				actualResponse, expected,
			)
		}
	})

	t.Run("When tags in query parameter can not be parsed as a key:value pair, status code should be 400", func(t *testing.T) {
		mckdbdata := dbmock.NewDataInterface()

		e := echo.New()
		c, _ := httptestutil.Get(e, "/api/data/?tag=knit#transientprocessing")

		testee := handlers.GetDataForDataHandler(mckdbdata)
		err := testee(c)

		var echoErr *echo.HTTPError
		if !errors.As(err, &echoErr) {
			t.Fatalf("error is not echo.HTTPError. acutal = %#v", err)
		}
		if echoErr.Code != http.StatusBadRequest {
			t.Errorf("unmatch error code:%d, expeced:%d", echoErr.Code, http.StatusBadRequest)
		}
	})

	t.Run("When Process of obtaining knitId from specified tag encounters an internal error, status code should be 500", func(t *testing.T) {
		mckdbdata := dbmock.NewDataInterface()
		mckdbdata.Impl.Find = func(ctx context.Context, tags []kdb.Tag, since *time.Time, until *time.Time) ([]string, error) {
			return nil, errors.New("Test Internal Error")
		}

		e := echo.New()
		c, _ := httptestutil.Get(e, "/api/data/?tag=knit#transient:processing")

		testee := handlers.GetDataForDataHandler(mckdbdata)
		err := testee(c)

		var echoErr *echo.HTTPError
		if !errors.As(err, &echoErr) {
			t.Fatalf("error is not echo.HTTPError. acutal = %#v", err)
		}
		if echoErr.Code != http.StatusInternalServerError {
			t.Errorf("unmatch error code:%d, expeced:%d", echoErr.Code, http.StatusInternalServerError)
		}
	})

	t.Run("When Process of obtaining a data from a specified knitId encounters an internal error, status code should be 500", func(t *testing.T) {
		knitId := []string{"knit-1"}

		mckdbdata := dbmock.NewDataInterface()
		mckdbdata.Impl.Find = func(ctx context.Context, tags []kdb.Tag, since *time.Time, until *time.Time) ([]string, error) {
			d := knitId
			return d, nil
		}
		mckdbdata.Impl.Get = func(ctx context.Context, knitId []string) (map[string]kdb.KnitData, error) {
			return nil, errors.New("Test Internal Error")
		}

		e := echo.New()
		c, _ := httptestutil.Get(e, "/api/data/?tag=knit#transient:processing")

		testee := handlers.GetDataForDataHandler(mckdbdata)
		err := testee(c)

		var echoErr *echo.HTTPError
		if !errors.As(err, &echoErr) {
			t.Fatalf("error is not echo.HTTPError. acutal = %#v", err)
		}
		if echoErr.Code != http.StatusInternalServerError {
			t.Errorf("unmatch error code:%d, expeced:%d", echoErr.Code, http.StatusInternalServerError)
		}
	})

}

func TestPutTagsForDataHandler(t *testing.T) {

	t.Run("update tags and return response", func(t *testing.T) {
		knitId := "knidittest"

		body := []byte(
			`{
	"add":[
		{"key":"addTag1","value":"addVal1"},
		{"key":"addTag2","value":"addVal2"}
	],
	"remove":[
		{"key":"remTag1","value":"remVal1"},
		{"key":"remTag2","value":"remVal2"}
	]
}`,
		)

		dbdata := dbmock.NewDataInterface()
		dbdata.Impl.UpdateTag = func(context.Context, string, kdb.TagDelta) error {
			return nil
		}
		dbdata.Impl.Get = func(ctx context.Context, s []string) (map[string]kdb.KnitData, error) {
			return map[string]kdb.KnitData{
				knitId: {
					KnitDataBody: kdb.KnitDataBody{
						KnitId: knitId, VolumeRef: "#volume-ref",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "model-parameter"},
							{Key: "project", Value: "testing"},
							{Key: "addtag1", Value: "addVal1"},
							{Key: "addtag2", Value: "addVal2"},
							{Key: tags.KeyKnitId, Value: knitId},
						}),
					},
					Upsteram: kdb.Dependency{
						RunBody: kdb.RunBody{
							Id: "run#1", Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime("2022-10-11T12:34:56+09:00")).OrFatal(t).Time(),
							PlanBody: kdb.PlanBody{
								PlanId: "plan#1", Hash: "#plan1", Active: true,
								Image: &kdb.ImageIdentifier{Image: "repo.invalid/image", Version: "v1"},
							},
						},
						MountPoint: kdb.MountPoint{
							Id: 1010, Path: "/out/1",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "type", Value: "model-parameter"},
								{Key: "project", Value: "testing"},
							}),
						},
					},
					Downstreams: []kdb.Dependency{
						{
							RunBody: kdb.RunBody{
								Id: "run#2", Status: kdb.Running,
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime("2022-10-12T12:34:56+09:00")).OrFatal(t).Time(),
								PlanBody: kdb.PlanBody{
									PlanId: "plan#2", Hash: "#plan2", Active: true,
									Image: &kdb.ImageIdentifier{Image: "repo.invalid/image", Version: "v2"},
								},
							},
							MountPoint: kdb.MountPoint{
								Id: 2100, Path: "/in",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "model-parameter"},
									{Key: "project", Value: "testing"},
									{Key: "remTag1", Value: "remVal1"},
								}),
							},
						},
						{
							RunBody: kdb.RunBody{
								Id: "run#3", Status: kdb.Starting,
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime("2022-10-12T13:34:56+09:00")).OrFatal(t).Time(),
								PlanBody: kdb.PlanBody{
									PlanId: "plan#3", Hash: "#plan3", Active: true,
									Image: &kdb.ImageIdentifier{Image: "repo.invalid/image", Version: "v3"},
								},
							},
							MountPoint: kdb.MountPoint{
								Id: 3100, Path: "/in",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "model-parameter"},
									{Key: "project", Value: "testing"},
									{Key: "remTag2", Value: "remVal2"},
								}),
							},
						},
					},
					NominatedBy: []kdb.Nomination{
						{
							PlanBody: kdb.PlanBody{
								PlanId: "plan#4", Hash: "#plan3", Active: true,
								Image: &kdb.ImageIdentifier{Image: "repo.invalid/image", Version: "v3"},
							},
							MountPoint: kdb.MountPoint{
								Id: 4100, Path: "/in",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "model-parameter"},
									{Key: "project", Value: "testing"},
									{Key: "addTag1", Value: "addVal1"},
								}),
							},
						},
					},
				},
			}, nil
		}

		expectedDelta := kdb.TagDelta{
			Add: []kdb.Tag{
				{Key: "addTag1", Value: "addVal1"},
				{Key: "addTag2", Value: "addVal2"},
			},
			Remove: []kdb.Tag{
				{Key: "remTag1", Value: "remVal1"},
				{Key: "remTag2", Value: "remVal2"},
			},
		}

		expectedResponse := data.Detail{
			KnitId: knitId,
			Tags: []tags.Tag{
				{Key: "type", Value: "model-parameter"},
				{Key: "project", Value: "testing"},
				{Key: "addtag1", Value: "addVal1"},
				{Key: "addtag2", Value: "addVal2"},
				{Key: tags.KeyKnitId, Value: knitId},
			},
			Upstream: data.AssignedTo{
				Run: runs.Summary{
					RunId: "run#1", Status: string(kdb.Done),
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime("2022-10-11T12:34:56+09:00")).OrFatal(t),
					Plan: plans.Summary{
						PlanId: "plan#1",
						Image:  &plans.Image{Repository: "repo.invalid/image", Tag: "v1"},
					},
				},
				Mountpoint: plans.Mountpoint{
					Path: "/out/1",
					Tags: []tags.Tag{
						{Key: "type", Value: "model-parameter"},
						{Key: "project", Value: "testing"},
					},
				},
			},
			Downstreams: []data.AssignedTo{
				{
					Run: runs.Summary{
						RunId: "run#2", Status: string(kdb.Running),
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime("2022-10-12T12:34:56+09:00")).OrFatal(t),
						Plan: plans.Summary{
							PlanId: "plan#2",
							Image:  &plans.Image{Repository: "repo.invalid/image", Tag: "v2"},
						},
					},
					Mountpoint: plans.Mountpoint{
						Path: "/in",
						Tags: []tags.Tag{
							{Key: "type", Value: "model-parameter"},
							{Key: "project", Value: "testing"},
							{Key: "remTag1", Value: "remVal1"},
						},
					},
				},
				{
					Run: runs.Summary{
						RunId: "run#3", Status: string(kdb.Starting),
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime("2022-10-12T13:34:56+09:00")).OrFatal(t),
						Plan: plans.Summary{
							PlanId: "plan#3",
							Image:  &plans.Image{Repository: "repo.invalid/image", Tag: "v3"},
						},
					},
					Mountpoint: plans.Mountpoint{
						Path: "/in",
						Tags: []tags.Tag{
							{Key: "type", Value: "model-parameter"},
							{Key: "project", Value: "testing"},
							{Key: "remTag2", Value: "remVal2"},
						},
					},
				},
			},
			Nomination: []data.NominatedBy{
				{
					Plan: plans.Summary{
						PlanId: "plan#4",
						Image:  &plans.Image{Repository: "repo.invalid/image", Tag: "v3"},
					},
					Mountpoint: plans.Mountpoint{
						Path: "/in",
						Tags: []tags.Tag{
							{Key: "type", Value: "model-parameter"},
							{Key: "project", Value: "testing"},
							{Key: "addTag1", Value: "addVal1"},
						},
					},
				},
			},
		}

		e := echo.New()
		c, respRec := httptestutil.Put(e, "/data/"+knitId, bytes.NewReader(body))
		c.SetPath("/data/:knitid")
		c.SetParamNames("knitid")
		c.SetParamValues(knitId)

		testee := handlers.PutTagForDataHandler(dbdata, "knitid")
		err := testee(c)

		if err != nil {
			t.Error("unexpected error occures")
		}

		if !cmp.SliceContentEqWith(
			dbdata.Calls.Updatetag,
			[]struct {
				KnitId string
				Delta  kdb.TagDelta
			}{
				{KnitId: knitId, Delta: expectedDelta},
			},
			func(a, b struct {
				KnitId string
				Delta  kdb.TagDelta
			}) bool {
				return a.KnitId == b.KnitId && a.Delta.Equal(&b.Delta)
			},
		) {
			t.Error("UpdateTagofData did not call with correct args.")
		}

		actualResponse := data.Detail{}
		if err := json.Unmarshal(respRec.Body.Bytes(), &actualResponse); err != nil {
			t.Errorf("response is not DataChangeResult. error = %v", err)
		} else if !expectedResponse.Equal(actualResponse) {
			t.Errorf(
				"unmatch body:\n===actual===\n%s\n===expected===\n%s",
				try.To(json.MarshalIndent(actualResponse, "", "  ")).OrFatal(t),
				try.To(json.MarshalIndent(expectedResponse, "", "  ")).OrFatal(t),
			)
		}
	})

	t.Run("request body is invalid. it should be return bad request error", func(t *testing.T) {
		e := echo.New()
		c, _ := httptestutil.Put(
			e, "/data/test-knit-id",
			bytes.NewReader([]byte("it is not a json")),
		)

		c.SetPath("/data/:knitid")
		c.SetParamNames("knitid")
		c.SetParamValues("test-knit-id")

		dbtag := dbmock.NewDataInterface()
		testee := handlers.PutTagForDataHandler(dbtag, "knitid")
		err := testee(c)

		var echoErr *echo.HTTPError
		if !errors.As(err, &echoErr) {
			t.Fatalf("error is not echo.HTTPError. acutal = %#v", err)
		}
		if echoErr.Code != http.StatusBadRequest {
			t.Errorf("unmatch error code:%d, expeced:%d", echoErr.Code, http.StatusBadRequest)
		}
	})

	for name, testcase := range map[string]struct {
		requestContent []byte
	}{
		"when add has tag with key starts with 'knit#', it should response Bad Request": {
			requestContent: []byte(`{
	"add":[
		{"key":"knit#addTag1", "value":"addVal1"},
		{"key":"addTag2", "value":"addVal2"}
	],
	"remove":[
		{"key":"remTag1", "value":"remVal1"},
		{"key":"remTag2", "value":"remVal2"}
	]
}`),
		},
		"when remove has tag with key starts with 'knit#', it should response Bad Request": {
			requestContent: []byte(`{
	"add":[
		{"key":"addTag1", "value":"addVal1"},
		{"key":"addTag2", "value":"addVal2"}
	],
	"remove":[
		{"key":"remTag1", "value":"remVal1"},
		{"key":"knit#remTag2", "value":"remVal2"}
	]
}`),
		},
	} {
		t.Run(name, func(t *testing.T) {
			dbData := dbmock.NewDataInterface()

			e := echo.New()
			c, _ := httptestutil.Put(e, "/data/test-knit-id", bytes.NewReader(testcase.requestContent))
			c.SetPath("/data/:knitid")
			c.SetParamNames("knitid")
			c.SetParamValues("test-knit-id")
			testee := handlers.PutTagForDataHandler(dbData, "knitid")

			err := testee(c)

			var echoErr *echo.HTTPError
			if !errors.As(err, &echoErr) {
				t.Fatalf("error is not echo.HTTPError: actual = %+v", err)
			}
			if echoErr.Code != http.StatusBadRequest {
				t.Errorf("unmatch error code:%d, expeced:%d", echoErr.Code, http.StatusBadRequest)
			}
		})

	}

	t.Run("If target data is not found, it should response Not Found", func(t *testing.T) {
		dbtag := dbmock.NewDataInterface()
		dbtag.Impl.UpdateTag = func(ctx context.Context, knitId string, delta kdb.TagDelta) error {
			return kdb.ErrMissing
		}

		body := []byte(`{
	"add": [
		{"key":"addTag1", "value":"addVal1"},
		{"key":"addTag2","value":"addVal2"}
	],
	"remove": [
		{"key":"remTag1","value":"remVal1"},
		{"key":"remTag2","value":"remVal2"}
	]
}`)

		e := echo.New()
		c, _ := httptestutil.Put(e, "/data/example-knit-id", bytes.NewReader(body))
		c.SetPath("/data/:knitid")
		c.SetParamNames("knitid")
		c.SetParamValues("example-knit-id")
		testee := handlers.PutTagForDataHandler(dbtag, "knitid")
		err := testee(c)

		var echoErr *echo.HTTPError
		if !errors.As(err, &echoErr) {
			t.Fatalf("error is not echo.HTTPError. actual = %+v", err)
		}
		if echoErr.Code != http.StatusNotFound {
			t.Errorf("unmatch error code:%d, expeced:%d", echoErr.Code, http.StatusBadRequest)
		}
	})

	t.Run("update operation did not completed with unexpected error. it should be retur internal server error", func(t *testing.T) {
		dbtag := dbmock.NewDataInterface()
		dbtag.Impl.UpdateTag = func(context.Context, string, kdb.TagDelta) error {
			return errors.New("some kind of error happen")
		}
		body := []byte(`{
	"add": [
		{"key": "addTag1", "value": "addVal1"},
		{"key": "addTag2", "value": "addVal2"}
	],
	"remove": [
		{"key": "remTag1", "value": "remVal1"},
		{"key": "remTag2", "value": "remVal2"}
	]
}`)

		e := echo.New()
		c, _ := httptestutil.Put(e, "/data/example-knit-id", bytes.NewReader(body))
		c.SetPath("/data/:knitid")
		c.SetParamNames("knitid")
		c.SetParamValues("example-knit-id")
		testee := handlers.PutTagForDataHandler(dbtag, "knitid")
		err := testee(c)

		var echoErr *echo.HTTPError
		if !errors.As(err, &echoErr) {
			t.Fatal("unmatch error type")
		}
		if echoErr.Code != http.StatusInternalServerError {
			t.Errorf("unmatch error code:%d, expeced:%d", echoErr.Code, http.StatusBadRequest)
		}
	})
}
