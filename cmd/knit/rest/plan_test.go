package rest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"k8s.io/apimachinery/pkg/api/resource"

	apierr "github.com/opst/knitfab-api-types/errors"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/tags"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/logic"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestGetPlans(t *testing.T) {
	t.Run("when server returns data, it returns that as is", func(t *testing.T) {
		hadelerFactory := func(t *testing.T, resp plans.Detail) (http.Handler, func() *http.Request) {
			var request *http.Request
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request = r

				w.Header().Add("Content-Type", "application/json")

				body, err := json.Marshal(resp)
				if err != nil {
					t.Fatal(err.Error())
				}

				w.WriteHeader(http.StatusOK)
				w.Write(body)
			})
			return h, func() *http.Request { return request }
		}

		expectedSummary := plans.Summary{
			PlanId: "test-Id",
			Image: &plans.Image{
				Repository: "test-image", Tag: "test-version",
			},
			Name: "test-Name",
		}
		expectedInputs := []plans.Mountpoint{
			{
				Path: "/in/1",
				Tags: []tags.Tag{
					{Key: "type", Value: "raw data"},
					{Key: "format", Value: "rgb image"},
				},
			},
		}
		expectedOutputs := []plans.Mountpoint{
			{
				Path: "/out/2",
				Tags: []tags.Tag{
					{Key: "type", Value: "training data"},
					{Key: "format", Value: "mask"},
				},
			},
		}
		expectedLog := &plans.LogPoint{
			Tags: []tags.Tag{
				{Key: "type", Value: "log"},
				{Key: "format", Value: "jsonl"},
			},
		}
		expectedResponse := plans.Detail{
			Summary: expectedSummary,
			Inputs:  expectedInputs,
			Outputs: expectedOutputs,
			Log:     expectedLog,
			Active:  true,
		}

		handler, _ := hadelerFactory(t, expectedResponse)
		server := httptest.NewServer(handler)
		defer server.Close()

		profile := kprof.KnitProfile{ApiRoot: server.URL}

		testee := try.To(krst.NewClient(&profile)).OrFatal(t)
		planId := "test-Id"
		actualResponse := try.To(testee.GetPlans(context.Background(), planId)).OrFatal(t)
		if !actualResponse.Equal(expectedResponse) {
			t.Errorf("response is not equal (actual,expected): %v,%v", actualResponse, expectedResponse)
		}
	})

	t.Run("a server responding with error is given", func(t *testing.T) {
		handlerFactory := func(t *testing.T, status int, message string) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Helper()
				w.WriteHeader(status)
				w.Header().Set("Content-Type", "application/json")

				buf, err := json.Marshal(apierr.ErrorMessage{
					Reason: message,
				})
				if err != nil {
					t.Fatal(err)
				}
				w.Write(buf)
			})
		}
		for _, status := range []int{http.StatusNotFound, http.StatusInternalServerError} {
			t.Run(fmt.Sprintf("when server responding with %d, it returns error", status), func(t *testing.T) {
				ctx := context.Background()
				handler := handlerFactory(t, status, "something wrong")

				server := httptest.NewServer(handler)
				defer server.Close()

				profile := kprof.KnitProfile{ApiRoot: server.URL}

				testee, err := krst.NewClient(&profile)
				if err != nil {
					t.Fatal(err.Error())
				}
				planId := "test-Id"
				if _, err := testee.GetPlans(ctx, planId); err == nil {
					t.Errorf("no error occured")
				}
			})
		}
	})
}

func TestPutPlanForActivate(t *testing.T) {
	type testcase struct {
		isActive         bool
		expectedResponse plans.Detail
	}

	hadelerFactory := func(t *testing.T, resp plans.Detail, mode bool) (http.Handler, func() *http.Request) {
		var request *http.Request
		planId := "somePlanId"

		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			method := http.MethodPut
			if !mode {
				method = http.MethodDelete
			}

			if r.Method != method {
				t.Errorf("request is not %s actual method = %s", r.Method, method)
			}
			if !strings.HasSuffix(r.URL.Path, "/plans/"+planId+"/active") {
				t.Errorf("request is not plans/:planId/active. actual path = %s", r.URL.Path)
			}

			w.Header().Add("Content-Type", "application/json")
			body, err := json.Marshal(resp)
			if err != nil {
				t.Fatal(err.Error())
			}
			w.WriteHeader(http.StatusOK)
			w.Write(body)
		})
		return h, func() *http.Request { return request }
	}
	for name, tesecase := range map[string]testcase{
		"when  argument isActive is ture and server returns activate plan, it reruns that as is": {
			isActive:         true,
			expectedResponse: dummyplan(true),
		},
		"when  argument isActive is false and server returns deactivate plan, it reruns that as is": {
			isActive:         false,
			expectedResponse: dummyplan(false),
		},
	} {
		t.Run(name, func(t *testing.T) {
			isActive := tesecase.isActive
			expectedResponse := tesecase.expectedResponse

			handler, _ := hadelerFactory(t, expectedResponse, isActive)
			server := httptest.NewServer(handler)
			defer server.Close()

			profile := kprof.KnitProfile{ApiRoot: server.URL}
			testee := try.To(krst.NewClient(&profile)).OrFatal(t)

			actualResponse := try.To(testee.PutPlanForActivate(context.Background(), "somePlanId", isActive)).OrFatal(t)
			if !actualResponse.Equal(expectedResponse) {
				t.Errorf("response is not equal (actual,expected): %v,%v", actualResponse, expectedResponse)
			}
		})
	}

	t.Run("a server responding with error is given", func(t *testing.T) {
		handlerFactory := func(t *testing.T, status int, message string) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Helper()
				w.WriteHeader(status)
				w.Header().Set("Content-Type", "application/json")

				if buf, err := json.Marshal(apierr.ErrorMessage{
					Reason: message,
				}); err != nil {
					t.Fatal(err)
				} else {
					w.Write(buf)
				}
			})
		}
		for _, status := range []int{http.StatusNotFound, http.StatusInternalServerError} {
			t.Run(fmt.Sprintf("when server responding with %d, it returns error", status), func(t *testing.T) {
				handler := handlerFactory(t, status, "something wrong")

				server := httptest.NewServer(handler)
				defer server.Close()

				profile := kprof.KnitProfile{ApiRoot: server.URL}

				testee, err := krst.NewClient(&profile)
				if err != nil {
					t.Fatal(err.Error())
				}
				planId := "somePlanId"
				if _, err := testee.PutPlanForActivate(context.Background(), planId, true); err == nil {
					t.Errorf("no error occured")
				}
			})
		}
	})
}

func dummyplan(isActivate bool) plans.Detail {
	return plans.Detail{
		Summary: plans.Summary{
			PlanId: "test-Id",
			Image: &plans.Image{
				Repository: "test-image", Tag: "test-version",
			},
			Name: "test-Name",
		},
		Inputs: []plans.Mountpoint{
			{
				Path: "/in/1",
				Tags: []tags.Tag{
					{Key: "type", Value: "raw data"},
					{Key: "format", Value: "rgb image"},
				},
			},
		},
		Outputs: []plans.Mountpoint{
			{
				Path: "/out/2",
				Tags: []tags.Tag{
					{Key: "type", Value: "training data"},
					{Key: "format", Value: "mask"},
				},
			},
		},
		Log: &plans.LogPoint{
			Tags: []tags.Tag{
				{Key: "type", Value: "log"},
				{Key: "format", Value: "jsonl"},
			},
		},
		Active: isActivate,
	}
}

func TestRegisterPlan(t *testing.T) {
	{
		theory := func(spec plans.PlanSpec, response plans.Detail) func(t *testing.T) {
			return func(t *testing.T) {
				hadelerFactory := func(t *testing.T, resp plans.Detail) http.Handler {
					h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						{
							b := bytes.NewBuffer(nil)
							if _, err := b.ReadFrom(r.Body); err != nil {
								t.Fatal(err)
							}
							actual := new(plans.PlanSpec)
							if err := json.Unmarshal(b.Bytes(), actual); err != nil {
								t.Fatal(err)
							}
							if !actual.Equal(spec) {
								t.Errorf(
									"request body:\n=== actual ===\n+%v\n=== expected ===\n+%v\n",
									actual, spec,
								)
							}
						}
						w.Header().Add("Content-Type", "application/json")

						body := try.To(json.Marshal(resp)).OrFatal(t)
						w.WriteHeader(http.StatusOK)
						w.Write(body)
					})
					return h
				}

				handler := hadelerFactory(t, response)
				server := httptest.NewServer(handler)
				defer server.Close()

				profile := kprof.KnitProfile{ApiRoot: server.URL}

				testee := try.To(krst.NewClient(&profile)).OrFatal(t)
				ctx := context.Background()
				actualResponse := try.To(testee.RegisterPlan(ctx, spec)).OrFatal(t)
				if !actualResponse.Equal(response) {
					t.Errorf(
						"response:\n=== actual ===\n+%v\n=== expected ===\n+%v\n",
						actualResponse, response,
					)
				}
			}
		}
		t.Run("when server returns data, it returns that as is", theory(
			plans.PlanSpec{
				Image: plans.Image{
					Repository: "test-image", Tag: "test-version",
				},
				Inputs: []plans.Mountpoint{
					{
						Path: "/in/1",
						Tags: []tags.Tag{
							{Key: "type", Value: "raw data"},
							{Key: "format", Value: "rgb image"},
						},
					},
				},
				Outputs: []plans.Mountpoint{
					{
						Path: "/out/2",
						Tags: []tags.Tag{
							{Key: "type", Value: "training data"},
							{Key: "format", Value: "mask"},
						},
					},
				},
				Log: &plans.LogPoint{
					Tags: []tags.Tag{
						{Key: "type", Value: "log"},
						{Key: "format", Value: "jsonl"},
					},
				},
				Active: ref(true),
			},
			plans.Detail{
				Summary: plans.Summary{
					PlanId: "test-Id",
					Image: &plans.Image{
						Repository: "test-image", Tag: "test-version",
					},
					Name: "test-Name",
				},
				Inputs: []plans.Mountpoint{
					{
						Path: "/in/1",
						Tags: []tags.Tag{
							{Key: "type", Value: "raw data"},
							{Key: "format", Value: "rgb image"},
						},
					},
				},
				Outputs: []plans.Mountpoint{
					{
						Path: "/out/2",
						Tags: []tags.Tag{
							{Key: "type", Value: "training data"},
							{Key: "format", Value: "mask"},
						},
					},
				},
				Log: &plans.LogPoint{
					Tags: []tags.Tag{
						{Key: "type", Value: "log"},
						{Key: "format", Value: "jsonl"},
					},
				},
				Active: true,
			},
		))
	}

	{
		theory := func(spec plans.PlanSpec, status int, message string) func(t *testing.T) {
			return func(t *testing.T) {
				handlerFactory := func(t *testing.T, status int, message string) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						t.Helper()
						w.WriteHeader(status)
						w.Header().Set("Content-Type", "application/json")

						if buf, err := json.Marshal(
							apierr.ErrorMessage{Reason: message},
						); err != nil {
							t.Fatal(err)
						} else {
							w.Write(buf)
						}
					})
				}
				ctx := context.Background()
				handler := handlerFactory(t, status, message)

				server := httptest.NewServer(handler)
				defer server.Close()

				profile := kprof.KnitProfile{ApiRoot: server.URL}

				testee := try.To(krst.NewClient(&profile)).OrFatal(t)
				if _, err := testee.RegisterPlan(ctx, spec); err == nil {
					t.Errorf("no error occured")
				}
			}
		}

		t.Run("when server returns 4xx error, it returns error", theory(
			plans.PlanSpec{
				Image: plans.Image{
					Repository: "test-image", Tag: "test-version",
				},
				Inputs: []plans.Mountpoint{
					{
						Path: "/in/1",
						Tags: []tags.Tag{
							{Key: "type", Value: "raw data"},
						},
					},
				},
				Outputs: []plans.Mountpoint{},
				Log:     nil,
				Active:  ref(true),
			},
			http.StatusBadRequest,
			`{"message": "invalid request"}`,
		))
		t.Run("when server returns 5xx error, it returns error", theory(
			plans.PlanSpec{
				Image: plans.Image{
					Repository: "test-image", Tag: "test-version",
				},
				Inputs: []plans.Mountpoint{
					{
						Path: "/in/1",
						Tags: []tags.Tag{
							{Key: "type", Value: "raw data"},
						},
					},
				},
				Outputs: []plans.Mountpoint{},
				Log:     nil,
				Active:  ref(true),
			},
			http.StatusServiceUnavailable,
			`{"message": "invalid request"}`,
		))
	}
}

func ref[T any](v T) *T {
	return &v
}

func TestFindPlan(t *testing.T) {
	t.Run("a server responding successfully	is given", func(t *testing.T) {
		handlerFactory := func(t *testing.T, resp []plans.Detail) (http.Handler, func() *http.Request) {
			var request *http.Request
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Helper()

				request = r

				w.Header().Add("Content-Type", "application/json")

				buf, err := json.Marshal(resp)
				if err != nil {
					t.Fatal(err)
				}
				w.Write(buf)
			})
			return h, func() *http.Request { return request }
		}

		type when struct {
			active   logic.Ternary
			imageVer kdb.ImageIdentifier
			inTags   []tags.Tag
			outTags  []tags.Tag
		}
		type then struct {
			activeInQuery   []string
			imageVerInQuery []string
			inTagsInQuery   []string
			outTagsInQuery  []string
		}

		type testcase struct {
			when when
			then then
		}

		for name, testcase := range map[string]testcase{
			"when query with active:Indeterminate nothing else, server receives empty query string": {
				when: when{
					active:   logic.Indeterminate,
					imageVer: kdb.ImageIdentifier{},
					inTags:   []tags.Tag{},
					outTags:  []tags.Tag{},
				},
				then: then{
					activeInQuery:   []string{},
					imageVerInQuery: []string{},
					inTagsInQuery:   []string{},
					outTagsInQuery:  []string{},
				},
			},
			"when query with active:true, server receives this": {
				when: when{
					active:   logic.True,
					imageVer: kdb.ImageIdentifier{},
					inTags:   []tags.Tag{},
					outTags:  []tags.Tag{},
				},
				then: then{
					activeInQuery:   []string{"true"},
					imageVerInQuery: []string{},
					inTagsInQuery:   []string{},
					outTagsInQuery:  []string{},
				},
			},
			"when query with active:false, server receives this": {
				when: when{
					active:   logic.False,
					imageVer: kdb.ImageIdentifier{},
					inTags:   []tags.Tag{},
					outTags:  []tags.Tag{},
				},
				then: then{
					activeInQuery:   []string{"false"},
					imageVerInQuery: []string{},
					inTagsInQuery:   []string{},
					outTagsInQuery:  []string{},
				},
			},
			"when query with imageVer, server receives this": {
				when: when{
					active: logic.Indeterminate,
					imageVer: kdb.ImageIdentifier{
						Image:   "image-test",
						Version: "v0.0.1",
					},
					inTags:  []tags.Tag{},
					outTags: []tags.Tag{},
				},
				then: then{
					activeInQuery:   []string{},
					imageVerInQuery: []string{"image-test:v0.0.1"},
					inTagsInQuery:   []string{},
					outTagsInQuery:  []string{},
				},
			},
			"when query with tags, server receives them": {
				when: when{
					active:   logic.Indeterminate,
					imageVer: kdb.ImageIdentifier{},
					inTags: []tags.Tag{
						{Key: "key-a", Value: "value/a"},
						{Key: "type", Value: "unknown?"},
					},
					outTags: []tags.Tag{
						{Key: "knit#id", Value: "some-knit-id"},
						{Key: "owner", Value: "100% our-team&client, of cource!"},
					},
				},
				then: then{
					activeInQuery:   []string{},
					imageVerInQuery: []string{},
					inTagsInQuery: []string{
						"key-a:value/a",
						"type:unknown?",
					},
					outTagsInQuery: []string{
						"knit#id:some-knit-id",
						"owner:100% our-team&client, of cource!",
					},
				},
			},
		} {
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				response := []plans.Detail{} //empty. this is out of scope of these testcases.

				handler, getLastRequest := handlerFactory(t, response)
				ts := httptest.NewServer(handler)
				defer ts.Close()

				// prepare for the tests
				profile := kprof.KnitProfile{ApiRoot: ts.URL}

				when := testcase.when
				then := testcase.then

				//test start
				testee := try.To(krst.NewClient(&profile)).OrFatal(t)
				result := try.To(testee.FindPlan(
					ctx, when.active, when.imageVer, when.inTags, when.outTags,
				)).OrFatal(t)

				// check response
				if !cmp.SliceContentEqWith(result, response, plans.Detail.Equal) {
					t.Errorf(
						"response is wrong:\n- actual:\n%#v\n- expected:\n%#v",
						result, response,
					)
				}

				// check method
				actualMethod := getLastRequest().Method
				if actualMethod != http.MethodGet {
					t.Errorf("wrong HTTP method: %s (!= %s )", actualMethod, http.MethodGet)
				}

				// check query
				actualActive := getLastRequest().URL.Query()["active"]
				actualImage := getLastRequest().URL.Query()["image"]
				actualInTags := getLastRequest().URL.Query()["in_tag"]
				actualOutTags := getLastRequest().URL.Query()["out_tag"]

				if !cmp.SliceContentEq(actualActive, then.activeInQuery) {
					t.Errorf(
						"query active is wrong:\n- actual  : %s\n- expected: %s",
						actualActive, then.activeInQuery,
					)
				}

				if !cmp.SliceContentEq(actualImage, then.imageVerInQuery) {
					t.Errorf(
						"query image is wrong:\n- actual  : %s\n- expected: %s",
						actualImage, then.imageVerInQuery,
					)
				}

				if !cmp.SliceContentEq(actualInTags, then.inTagsInQuery) {
					t.Errorf(
						"query input tag is wrong:\n- actual  : %s\n- expected: %s",
						actualInTags, then.inTagsInQuery,
					)
				}

				if !cmp.SliceContentEq(actualOutTags, then.outTagsInQuery) {
					t.Errorf(
						"query output tag is wrong:\n- actual  : %s\n- expected: %s",
						actualOutTags, then.outTagsInQuery,
					)
				}

			})
		}

		t.Run("when server returns data, it returns that as is", func(t *testing.T) {
			ctx := context.Background()

			expectedResponse := []plans.Detail{}

			handler, _ := handlerFactory(t, expectedResponse)

			ts := httptest.NewServer(handler)
			defer ts.Close()

			// prepare for the tests
			profile := kprof.KnitProfile{ApiRoot: ts.URL}
			queryActive := logic.Indeterminate
			queryImagever := kdb.ImageIdentifier{
				Image: "test-image", Version: "test-version",
			}
			queryInTags := []tags.Tag{
				{Key: "tag-a", Value: "value-a"},
			}
			queryOutTags := []tags.Tag{
				{Key: "tag-b", Value: "value-b"},
			}
			//test start
			testee := try.To(krst.NewClient(&profile)).OrFatal(t)
			actualResponse := try.To(testee.FindPlan(ctx, queryActive, queryImagever, queryInTags, queryOutTags)).OrFatal(t)

			if !cmp.SliceContentEqWith(actualResponse, expectedResponse, plans.Detail.Equal) {
				t.Errorf(
					"response is in unexpected form:\n===actual===\n%+v\n===expected===\n%+v",
					actualResponse, expectedResponse,
				)
			}
		})
	})

	t.Run("a server responding with error is given", func(t *testing.T) {
		handerFactory := func(t *testing.T, status int, message string) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Helper()
				w.Header().Add("Content-Type", "application/json")
				w.WriteHeader(status)

				buf, err := json.Marshal(apierr.ErrorMessage{
					Reason: message,
				})
				if err != nil {
					t.Fatal(err)
				}
				w.Write(buf)
			})
		}

		for _, status := range []int{http.StatusBadRequest, http.StatusInternalServerError} {
			t.Run(fmt.Sprintf("when server responding with %d, it returns error", status), func(t *testing.T) {
				ctx := context.Background()
				handler := handerFactory(t, status, "something wrong")

				ts := httptest.NewServer(handler)
				defer ts.Close()

				// prepare for the tests
				profile := kprof.KnitProfile{ApiRoot: ts.URL}
				testee, err := krst.NewClient(&profile)
				if err != nil {
					t.Fatal(err.Error())
				}
				queryActive := logic.Indeterminate
				queryImagever := kdb.ImageIdentifier{
					Image: "test-image", Version: "test-version",
				}
				queryInTags := []tags.Tag{
					{Key: "tag-a", Value: "value-a"},
				}
				queryOutTags := []tags.Tag{
					{Key: "tag-b", Value: "value-b"},
				}

				if _, err := testee.FindPlan(ctx, queryActive, queryImagever, queryInTags, queryOutTags); err == nil {
					t.Errorf("no error occured")
				}
			})
		}
	})
}

func TestUpdateResources(t *testing.T) {
	t.Run("when server returns data, it returns that as is", func(t *testing.T) {
		hadelerFactory := func(t *testing.T, want plans.ResourceLimitChange, resp plans.Detail) http.Handler {
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut {
					t.Errorf("request is not PUT actual method = %s", r.Method)
				}

				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("request is not application/json. actual content-type = %s", r.Header.Get("Content-Type"))
				}

				if !strings.HasSuffix(r.URL.Path, "/plans/"+resp.Summary.PlanId+"/resources") {
					t.Errorf("request is not /plans/:planId/resources. actual path = %s", r.URL.Path)
				}

				got := new(plans.ResourceLimitChange)
				if err := json.NewDecoder(r.Body).Decode(got); err != nil {
					t.Fatal(err)
				}

				if !cmp.SliceContentEq(got.Unset, want.Unset) {
					t.Errorf("unset is wrong: actual = %v, expected = %v", got.Unset, want.Unset)
				}

				if !cmp.MapEqWith(got.Set, want.Set, resource.Quantity.Equal) {
					t.Errorf("set is wrong: actual = %v, expected = %v", got.Set, want.Set)
				}

				w.Header().Add("Content-Type", "application/json")
				body := try.To(json.Marshal(resp)).OrFatal(t)
				w.WriteHeader(http.StatusOK)
				w.Write(body)
			})
			return h
		}

		expectedResponse := plans.Detail{
			Summary: plans.Summary{
				PlanId: "test-Id",
				Image: &plans.Image{
					Repository: "test-image", Tag: "test-version",
				},
				Name: "test-Name",
			},
			Inputs: []plans.Mountpoint{
				{
					Path: "/in/1",
					Tags: []tags.Tag{
						{Key: "type", Value: "raw data"},
						{Key: "format", Value: "rgb image"},
					},
				},
			},
			Outputs: []plans.Mountpoint{
				{
					Path: "/out/2",
					Tags: []tags.Tag{
						{Key: "type", Value: "training data"},
						{Key: "format", Value: "mask"},
					},
				},
			},
			Log: &plans.LogPoint{
				Tags: []tags.Tag{
					{Key: "type", Value: "log"},
					{Key: "format", Value: "jsonl"},
				},
			},
			Active: true,
			Resources: map[string]resource.Quantity{
				"memory": resource.MustParse("2Gi"),
			},
		}

		wantedResourceChange := plans.ResourceLimitChange{
			Unset: []string{"cpu"},
			Set: map[string]resource.Quantity{
				"memory": resource.MustParse("2Gi"),
			},
		}

		handler := hadelerFactory(t, wantedResourceChange, expectedResponse)
		server := httptest.NewServer(handler)
		defer server.Close()

		profile := kprof.KnitProfile{ApiRoot: server.URL}

		testee := try.To(krst.NewClient(&profile)).OrFatal(t)
		ctx := context.Background()
		planId := "test-Id"
		actualResponse := try.To(testee.UpdateResources(ctx, planId, wantedResourceChange)).OrFatal(t)
		if !actualResponse.Equal(expectedResponse) {
			t.Errorf("response is not equal (actual,expected): %v,%v", actualResponse, expectedResponse)
		}
	})

	t.Run("a server responding with error is given", func(t *testing.T) {
		handlerFactory := func(t *testing.T, status int, message string) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Helper()
				w.WriteHeader(status)
				w.Header().Set("Content-Type", "application/json")

				buf := try.To(json.Marshal(apierr.ErrorMessage{
					Reason: message,
				})).OrFatal(t)

				w.Write(buf)
			})
		}
		for _, status := range []int{http.StatusNotFound, http.StatusInternalServerError} {
			t.Run(fmt.Sprintf("when server responding with %d, it returns error", status), func(t *testing.T) {
				handler := handlerFactory(t, status, "something wrong")

				server := httptest.NewServer(handler)
				defer server.Close()

				profile := kprof.KnitProfile{ApiRoot: server.URL}

				testee := try.To(krst.NewClient(&profile)).OrFatal(t)
				planId := "test-Id"
				if _, err := testee.UpdateResources(context.Background(), planId, plans.ResourceLimitChange{}); err == nil {
					t.Errorf("no error occured")
				}
			})
		}
	})
}

func TestUpdateAnnotations(t *testing.T) {
	t.Run("success case", func(t *testing.T) {
		type When struct {
			planId   string
			change   plans.AnnotationChange
			response plans.Detail
		}

		theory := func(when When) func(t *testing.T) {
			return func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("Content-Type") != "application/json" {
						t.Errorf("request is not application/json. actual content-type = %s", r.Header.Get("Content-Type"))
					}

					got := new(plans.AnnotationChange)
					if err := json.NewDecoder(r.Body).Decode(got); err != nil {
						t.Fatal(err)
					}

					if !cmp.SliceContentEq(got.Add, when.change.Add) {
						t.Errorf("add is wrong: actual = %v, expected = %v", got.Add, when.change.Add)
					}

					if !cmp.SliceContentEq(got.Remove, when.change.Remove) {
						t.Errorf("remove is wrong: actual = %v, expected = %v", got.Remove, when.change.Remove)
					}

					w.Header().Add("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					body := try.To(json.Marshal(when.response)).OrFatal(t)
					w.Write(body)
				}))

				defer server.Close()

				profile := kprof.KnitProfile{ApiRoot: server.URL}

				testee := try.To(krst.NewClient(&profile)).OrFatal(t)

				ctx := context.Background()
				actualResponse := try.To(testee.UpdateAnnotations(ctx, when.planId, when.change)).OrFatal(t)

				if !actualResponse.Equal(when.response) {
					t.Errorf("response is not equal (actual,expected): %v,%v", actualResponse, when.response)
				}
			}
		}

		t.Run("when server returns data, it returns that as is", theory(When{
			planId: "test-Id",
			change: plans.AnnotationChange{
				Add:    plans.Annotations{{Key: "key-a", Value: "value-a"}},
				Remove: plans.Annotations{{Key: "key-b", Value: "value-b"}},
			},
			response: plans.Detail{
				Summary: plans.Summary{
					PlanId: "test-Id",
					Image: &plans.Image{
						Repository: "test-image", Tag: "test-version",
					},
					Annotations: plans.Annotations{
						{Key: "key-a", Value: "value-a"},
						{Key: "key-c", Value: "value-c"},
					},
				},
				Inputs: []plans.Mountpoint{
					{
						Path: "/in/1",
						Tags: []tags.Tag{
							{Key: "type", Value: "raw data"},
							{Key: "format", Value: "rgb image"},
						},
					},
				},
				Outputs: []plans.Mountpoint{
					{
						Path: "/out/2",
						Tags: []tags.Tag{
							{Key: "type", Value: "training data"},
							{Key: "format", Value: "mask"},
						},
					},
				},
				Log: &plans.LogPoint{
					Tags: []tags.Tag{
						{Key: "type", Value: "log"},
						{Key: "format", Value: "jsonl"},
					},
				},
				Active: true,
			},
		}))
	})

	t.Run("a server responding with error", func(t *testing.T) {
		type When struct {
			planId string
			change plans.AnnotationChange

			statusCode int
		}

		theory := func(when When) func(t *testing.T) {
			return func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("Content-Type") != "application/json" {
						t.Errorf("request is not application/json. actual content-type = %s", r.Header.Get("Content-Type"))
					}

					got := new(plans.AnnotationChange)
					if err := json.NewDecoder(r.Body).Decode(got); err != nil {
						t.Fatal(err)
					}

					if !cmp.SliceContentEq(got.Add, when.change.Add) {
						t.Errorf("add is wrong: actual = %v, expected = %v", got.Add, when.change.Add)
					}

					if !cmp.SliceContentEq(got.Remove, when.change.Remove) {
						t.Errorf("remove is wrong: actual = %v, expected = %v", got.Remove, when.change.Remove)
					}

					w.Header().Add("Content-Type", "application/json")
					w.WriteHeader(when.statusCode)
				}))

				defer server.Close()

				profile := kprof.KnitProfile{ApiRoot: server.URL}

				testee := try.To(krst.NewClient(&profile)).OrFatal(t)

				ctx := context.Background()
				_, err := testee.UpdateAnnotations(ctx, when.planId, when.change)

				if err == nil {
					t.Error("no error occured")
				}
			}
		}

		t.Run(": 4xx error", theory(When{
			planId: "test-Id",
			change: plans.AnnotationChange{
				Add:    plans.Annotations{{Key: "key-a", Value: "value-a"}},
				Remove: plans.Annotations{{Key: "key-b", Value: "value-b"}},
			},
			statusCode: http.StatusBadRequest,
		}))

		t.Run(": 5xx error", theory(When{
			planId: "test-Id",
			change: plans.AnnotationChange{
				Add:    plans.Annotations{{Key: "key-a", Value: "value-a"}},
				Remove: plans.Annotations{{Key: "key-b", Value: "value-b"}},
			},
			statusCode: http.StatusInternalServerError,
		}))
	})
}

func TestSetServiceAccount_success_case(t *testing.T) {
	type When struct {
		planId             string
		sa                 plans.SetServiceAccount
		requestContentType string

		response plans.Detail
	}

	theory := func(when When) func(t *testing.T) {
		return func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Content-Type") != when.requestContentType {
					t.Errorf("request is not %s. actual content-type = %s", when.requestContentType, r.Header.Get("Content-Type"))
				}

				got := new(plans.SetServiceAccount)
				if err := json.NewDecoder(r.Body).Decode(got); err != nil {
					t.Fatal(err)
				}

				if *got != when.sa {
					t.Errorf("request is wrong: actual = %v, expected = %v", got, when.sa)
				}

				w.Header().Add("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				body := try.To(json.Marshal(when.response)).OrFatal(t)
				w.Write(body)
			}))

			defer server.Close()

			profile := kprof.KnitProfile{ApiRoot: server.URL}

			testee := try.To(krst.NewClient(&profile)).OrFatal(t)

			ctx := context.Background()
			actualResponse := try.To(testee.SetServiceAccount(ctx, when.planId, when.sa)).OrFatal(t)

			if !actualResponse.Equal(when.response) {
				t.Errorf("response is not equal (actual,expected): %+v\n%+v", actualResponse, when.response)
			}
		}
	}

	t.Run("when server returns data, it returns that as is", theory(When{
		planId: "test-Id",
		sa: plans.SetServiceAccount{
			ServiceAccount: "test-service-account",
		},
		requestContentType: "application/json",
		response: plans.Detail{
			Summary: plans.Summary{
				PlanId: "test-Id",
				Image: &plans.Image{
					Repository: "test-image", Tag: "test-version",
				},
				Name: "test-Name",
			},
			Inputs: []plans.Mountpoint{
				{
					Path: "/in/1",
					Tags: []tags.Tag{
						{Key: "type", Value: "raw data"},
						{Key: "format", Value: "rgb image"},
					},
				},
			},
			Outputs: []plans.Mountpoint{
				{
					Path: "/out/2",
					Tags: []tags.Tag{
						{Key: "type", Value: "training data"},
						{Key: "format", Value: "mask"},
					},
				},
			},
			Log: &plans.LogPoint{
				Tags: []tags.Tag{
					{Key: "type", Value: "log"},
					{Key: "format", Value: "jsonl"},
				},
			},
			Active:         true,
			ServiceAccount: "test-service-account",
		},
	}))
}

func TestSetServiceAccount_error_case(t *testing.T) {
	type When struct {
		planId string
		sa     plans.SetServiceAccount

		statusCode int
	}

	theory := func(when When) func(t *testing.T) {
		return func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("request is not application/json. actual content-type = %s", r.Header.Get("Content-Type"))
				}

				got := new(plans.SetServiceAccount)
				if err := json.NewDecoder(r.Body).Decode(got); err != nil {
					t.Fatal(err)
				}

				if *got != when.sa {
					t.Errorf("request is wrong: actual = %v, expected = %v", got, when.sa)
				}

				w.Header().Add("Content-Type", "application/json")
				w.WriteHeader(when.statusCode)
			}))

			defer server.Close()

			profile := kprof.KnitProfile{ApiRoot: server.URL}

			testee := try.To(krst.NewClient(&profile)).OrFatal(t)

			ctx := context.Background()
			_, err := testee.SetServiceAccount(ctx, when.planId, when.sa)

			if err == nil {
				t.Error("no error occured")
			}
		}
	}

	t.Run(": 4xx error", theory(When{
		planId: "test-Id",
		sa: plans.SetServiceAccount{
			ServiceAccount: "test-service-account",
		},
		statusCode: http.StatusBadRequest,
	}))

	t.Run(": 5xx error", theory(When{
		planId: "test-Id",
		sa: plans.SetServiceAccount{
			ServiceAccount: "test-service-account",
		},
		statusCode: http.StatusInternalServerError,
	}))
}

func TestUnsetServiceAccount_success_case(t *testing.T) {
	type When struct {
		planId string

		response plans.Detail
	}

	theory := func(when When) func(t *testing.T) {
		return func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete {
					t.Errorf(
						"request is not DELETE: actual method = %s",
						r.Method,
					)
				}

				if !strings.HasSuffix(r.URL.Path, "/plans/"+when.planId+"/serviceaccount") {
					t.Errorf(
						"request is not /plans/:planId/service_account. actual path = %s",
						r.URL.Path,
					)
				}

				w.Header().Add("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				body := try.To(json.Marshal(when.response)).OrFatal(t)
				w.Write(body)
			}))

			defer server.Close()

			profile := kprof.KnitProfile{ApiRoot: server.URL}

			testee := try.To(krst.NewClient(&profile)).OrFatal(t)

			ctx := context.Background()
			got, err := testee.UnsetServiceAccount(ctx, when.planId)
			if err != nil {
				t.Fatal(err)
			}

			if !got.Equal(when.response) {
				t.Errorf(
					"response is not equal (actual,expected): %+v\n%+v",
					got, when.response,
				)
			}
		}
	}

	t.Run("when server returns data, it returns that as is", theory(When{
		planId: "test-Id",
		response: plans.Detail{
			Summary: plans.Summary{
				PlanId: "test-id",
				Image: &plans.Image{
					Repository: "test-image", Tag: "test-version",
				},
				Name: "test-Name",
			},
			Inputs: []plans.Mountpoint{
				{
					Path: "/in/1",
					Tags: []tags.Tag{
						{Key: "type", Value: "raw data"},
						{Key: "format", Value: "rgb image"},
					},
				},
			},
			Outputs: []plans.Mountpoint{
				{
					Path: "/out/2",
					Tags: []tags.Tag{
						{Key: "type", Value: "training data"},
						{Key: "format", Value: "mask"},
					},
				},
			},
			Log: &plans.LogPoint{
				Tags: []tags.Tag{
					{Key: "type", Value: "log"},
					{Key: "format", Value: "jsonl"},
				},
			},
			Active: true,
		},
	}))
}

func TestUnsetServiceAccount_error_case(t *testing.T) {
	type When struct {
		planId string

		statusCode int
	}

	theory := func(when When) func(t *testing.T) {
		return func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete {
					t.Errorf(
						"request is not DELETE: actual method = %s",
						r.Method,
					)
				}

				if !strings.HasSuffix(r.URL.Path, "/plans/"+when.planId+"/serviceaccount") {
					t.Errorf(
						"request is not /plans/:planId/service_account. actual path = %s",
						r.URL.Path,
					)
				}

				w.Header().Add("Content-Type", "application/json")
				w.WriteHeader(when.statusCode)
			}))

			defer server.Close()

			profile := kprof.KnitProfile{ApiRoot: server.URL}

			testee := try.To(krst.NewClient(&profile)).OrFatal(t)

			ctx := context.Background()
			_, err := testee.UnsetServiceAccount(ctx, when.planId)

			if err == nil {
				t.Error("no error occured")
			}
		}
	}

	t.Run(": 4xx error", theory(When{
		planId:     "test-Id",
		statusCode: http.StatusBadRequest,
	}))

	t.Run(": 5xx error", theory(When{
		planId:     "test-Id",
		statusCode: http.StatusInternalServerError,
	}))
}
