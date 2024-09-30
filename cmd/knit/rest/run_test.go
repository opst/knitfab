package rest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	apierr "github.com/opst/knitfab-api-types/errors"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestGetRun(t *testing.T) {
	t.Run("when server returns data, it returns that as is", func(t *testing.T) {
		hadelerFactory := func(t *testing.T, resp runs.Detail) (http.Handler, func() *http.Request) {
			var request *http.Request
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

				if r.Method != http.MethodGet {
					t.Errorf("request is not GET /api/runs/:runid (actual method = %s)", r.Method)
				}

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
		expectedResponse := runs.Detail{
			Summary: runs.Summary{
				RunId:  "test-runId",
				Status: "done",
				Plan: plans.Summary{
					PlanId: "test-Id",
					Image: &plans.Image{
						Repository: "test-image",
						Tag:        "test-version",
					},
					Name: "test-Name",
				},
				UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
					"2022-04-02T12:00:00+00:00",
				)).OrFatal(t),
			},
			Inputs: []runs.Assignment{
				{
					Mountpoint: plans.Mountpoint{
						Path: "/in/1",
						Tags: []tags.Tag{
							{Key: "type", Value: "raw data"},
							{Key: "format", Value: "rgb image"},
						},
					},
					KnitId: "test-knitId-a",
				},
			},
			Outputs: []runs.Assignment{
				{
					Mountpoint: plans.Mountpoint{
						Path: "/out/2",
						Tags: []tags.Tag{
							{Key: "type", Value: "training data"},
							{Key: "format", Value: "mask"},
						},
					},
					KnitId: "test-knitId-b",
				}},
			Log: &runs.LogSummary{
				LogPoint: plans.LogPoint{
					Tags: []tags.Tag{
						{Key: "type", Value: "log"},
						{Key: "format", Value: "jsonl"},
					},
				},
				KnitId: "test-knitId",
			},
		}

		handler, _ := hadelerFactory(t, expectedResponse)
		server := httptest.NewServer(handler)
		defer server.Close()

		profile := kprof.KnitProfile{ApiRoot: server.URL}

		testee := try.To(krst.NewClient(&profile)).OrFatal(t)
		runId := "test-runId"
		actualResponse := try.To(testee.GetRun(context.Background(), runId)).OrFatal(t)
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

				buf := try.To(json.Marshal(
					apierr.ErrorMessage{Reason: message},
				)).OrFatal(t)
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

				testee := try.To(krst.NewClient(&profile)).OrFatal(t)
				runId := "test-Id"
				if _, err := testee.GetRun(ctx, runId); err == nil {
					t.Errorf("no error occured")
				}
			})
		}
	})
}

func TestGetRunLog(t *testing.T) {
	t.Run("when server response with 200 in chunked, it returns the stream in response (non-follow)", func(t *testing.T) {
		expectedContent := []byte("streaming payload...")
		runId := "someRunId"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("request is not GET /api/runs/:runid/log (actual method = %s)", r.Method)
			}
			if !strings.HasSuffix(r.URL.Path, "/runs/"+runId+"/log") {
				t.Errorf("request is not GET /api/runs/:runid/log (actual path = %s)", r.URL.Path)
			}
			if r.URL.Query().Has("follow") {
				t.Errorf("request has follow query")
			}

			w.Header().Add("Transfer-Encoding", "chunked")
			w.WriteHeader(http.StatusOK)

			io.Copy(w, bytes.NewBuffer(expectedContent))
		}),
		)
		defer server.Close()

		prof := kprof.KnitProfile{ApiRoot: server.URL}

		testee := try.To(krst.NewClient(&prof)).OrFatal(t)

		ctx := context.Background()
		payload := try.To(testee.GetRunLog(ctx, runId, false)).OrFatal(t)

		defer payload.Close()

		actualContent := bytes.NewBuffer(nil)
		try.To(io.Copy(actualContent, payload)).OrFatal(t)
		if !bytes.Equal(actualContent.Bytes(), expectedContent) {
			t.Errorf(
				"content is wrong: (actual, expected) = (%s, %s)",
				actualContent.Bytes(), expectedContent,
			)
		}
	})

	t.Run("when server response with 200 in chunked, it returns the stream in response (follow)", func(t *testing.T) {
		expectedContent := []byte("streaming payload...")
		runId := "someRunId"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("request is not GET /api/runs/:runid/log (actual method = %s)", r.Method)
			}
			if !strings.HasSuffix(r.URL.Path, "/runs/"+runId+"/log") {
				t.Errorf("request is not GET /api/runs/:runid/log (actual path = %s)", r.URL.Path)
			}
			if !r.URL.Query().Has("follow") {
				t.Errorf("request has follow query")
			}

			w.Header().Add("Transfer-Encoding", "chunked")
			w.WriteHeader(http.StatusOK)

			io.Copy(w, bytes.NewBuffer(expectedContent))
		}),
		)
		defer server.Close()

		prof := kprof.KnitProfile{ApiRoot: server.URL}

		testee := try.To(krst.NewClient(&prof)).OrFatal(t)

		ctx := context.Background()
		payload := try.To(testee.GetRunLog(ctx, runId, true)).OrFatal(t)

		defer payload.Close()

		actualContent := bytes.NewBuffer(nil)
		try.To(io.Copy(actualContent, payload)).OrFatal(t)
		if !bytes.Equal(actualContent.Bytes(), expectedContent) {
			t.Errorf(
				"content is wrong: (actual, expected) = (%s, %s)",
				actualContent.Bytes(), expectedContent,
			)
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

				testee := try.To(krst.NewClient(&profile)).OrFatal(t)
				runId := "test-Id"
				if _, err := testee.GetRunLog(ctx, runId, false); err == nil {
					t.Errorf("no error occured")
				}
			})
		}
	})

	t.Run("When send request to invalid host, it returns error", func(t *testing.T) {
		runId := "someRunId"
		ctx := context.Background()

		prof := kprof.KnitProfile{ApiRoot: "http://test.invalid"}

		testee := try.To(krst.NewClient(&prof)).OrFatal(t)

		payload, err := testee.GetRunLog(ctx, runId, false)
		if payload != nil {
			payload.Close()
			t.Error("payload is not nil")
		}

		if err == nil {
			t.Fatalf("GetRunLog does not return error")
		} else if urlerr := new(url.Error); !errors.As(err, &urlerr) {
			t.Errorf(
				"GetRunLog does not return expected error (url.Error), but actual = %s (%#v)",
				err, err,
			)
		}
	})
}

func TestFindRun(t *testing.T) {
	t.Run("a server responding successfully	is given", func(t *testing.T) {
		handlerFactory := func(t *testing.T, resp []runs.Detail) (http.Handler, func() *http.Request) {
			var request *http.Request
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Helper()

				if r.Method != http.MethodGet {
					t.Errorf("request is not GET /api/runs (actual method = %s)", r.Method)
				}

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

		type then struct {
			planIdInQuery    []string
			knitIdInInQuery  []string
			knitIdOutInQuery []string
			statusInQuery    []string
			sinceQuery       string
			durationQuery    string
		}

		type testcase struct {
			when krst.FindRunParameter
			then then
		}

		timeStamp := "2024-04-22T12:34:56.987654321+07:00"
		since := try.To(rfctime.ParseRFC3339DateTime(timeStamp)).OrFatal(t).Time()
		duration := time.Duration(2 * time.Hour)

		for name, testcase := range map[string]testcase{
			"when query with nothing, server receives empty query": {
				when: krst.FindRunParameter{},
				then: then{
					planIdInQuery:    []string{},
					knitIdInInQuery:  []string{},
					knitIdOutInQuery: []string{},
					statusInQuery:    []string{},
					sinceQuery:       "",
					durationQuery:    "",
				},
			},
			"when query with each item, server receives all": {
				when: krst.FindRunParameter{
					PlanId:    []string{"test-a", "test-b"},
					KnitIdIn:  []string{"in-a", "in-b"},
					KnitIdOut: []string{"out-a", "out-b"},
					Status:    []string{"wating", "running"},
					Since:     &since,
					Duration:  &duration,
				},
				then: then{
					planIdInQuery:    []string{"test-a,test-b"},
					knitIdInInQuery:  []string{"in-a,in-b"},
					knitIdOutInQuery: []string{"out-a,out-b"},
					statusInQuery:    []string{"wating,running"},
					sinceQuery:       timeStamp,
					durationQuery:    "2h0m0s",
				},
			},
		} {
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				response := []runs.Detail{} //empty. this is out of scope of these testcases.

				handler, getLastRequest := handlerFactory(t, response)
				ts := httptest.NewServer(handler)
				defer ts.Close()

				// prepare for the tests
				profile := kprof.KnitProfile{ApiRoot: ts.URL}

				when := testcase.when
				then := testcase.then

				//test start
				testee := try.To(krst.NewClient(&profile)).OrFatal(t)
				result := try.To(testee.FindRun(
					ctx, when,
				)).OrFatal(t)

				// check response
				if !cmp.SliceContentEqWith(result, response, runs.Detail.Equal) {
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

				//Check the content of the query received by the server
				actualPlan := getLastRequest().URL.Query()["plan"]
				actualKnitIdIn := getLastRequest().URL.Query()["knitIdInput"]
				actualKnitIdOut := getLastRequest().URL.Query()["knitIdOutput"]
				actualStatus := getLastRequest().URL.Query()["status"]
				actualSince := getLastRequest().URL.Query().Get("since")
				actualDuration := getLastRequest().URL.Query().Get("duration")

				checkSliceContentEquality(t, "active", actualPlan, then.planIdInQuery)
				checkSliceContentEquality(t, "image", actualKnitIdIn, then.knitIdInInQuery)
				checkSliceContentEquality(t, "input tag", actualKnitIdOut, then.knitIdOutInQuery)
				checkSliceContentEquality(t, "output tag", actualStatus, then.statusInQuery)
				if actualSince != then.sinceQuery {
					t.Errorf("query since is wrong: actual=%s, then=%s)", actualSince, then.sinceQuery)
				}
				if actualDuration != then.durationQuery {
					t.Errorf("query duration is wrong: actual=%s,then=%s)", actualDuration, then.durationQuery)
				}
			})
		}

		t.Run("when server returns data, it returns that as is", func(t *testing.T) {
			ctx := context.Background()

			expectedResponse := []runs.Detail{}

			handler, _ := handlerFactory(t, expectedResponse)

			ts := httptest.NewServer(handler)
			defer ts.Close()

			// prepare for the tests
			profile := kprof.KnitProfile{ApiRoot: ts.URL}
			testee, err := krst.NewClient(&profile)
			if err != nil {
				t.Fatal(err.Error())
			}

			// argements set up
			since := try.To(rfctime.ParseRFC3339DateTime("2024-04-22T12:34:56.987654321+07:00")).OrFatal(t).Time()
			duration := time.Duration(2 * time.Hour)

			findRunParameter := krst.FindRunParameter{
				PlanId:    []string{"test-planId"},
				KnitIdIn:  []string{"test-inputKnitId"},
				KnitIdOut: []string{"test-outputKnitId"},
				Status:    []string{"test-status"},
				Since:     &since,
				Duration:  &duration,
			}

			//test start
			actualResponse := try.To(
				testee.FindRun(
					ctx, findRunParameter,
				),
			).OrFatal(t)

			if !cmp.SliceContentEqWith(actualResponse, expectedResponse, runs.Detail.Equal) {
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

				since := try.To(rfctime.ParseRFC3339DateTime("2024-04-22T12:34:56.987654321+07:00")).OrFatal(t).Time()
				duration := time.Duration(2 * time.Hour)

				// arguments set up
				findRunParameter := krst.FindRunParameter{
					PlanId:    []string{"test-planId"},
					KnitIdIn:  []string{"test-inputKnitId"},
					KnitIdOut: []string{"test-outputKnitId"},
					Status:    []string{"test-status"},
					Since:     &since,
					Duration:  &duration,
				}
				if _, err := testee.FindRun(
					ctx, findRunParameter,
				); err == nil {
					t.Errorf("no error occured")
				}
			})
		}
	})
}

func checkSliceContentEquality(t *testing.T, name string, actual, expected []string) {
	if !cmp.SliceContentEq(actual, expected) {
		t.Errorf(
			"query %s is wrong:\n- actual  : %s\n- expected: %s",
			name, actual, expected,
		)
	}
}

func TestRunAbort(t *testing.T) {
	type When struct {
		statusCode    int
		responseOk    runs.Detail
		responseError apierr.ErrorMessage
	}
	type Then struct {
		wantError bool
	}
	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			runId := "someRunId"

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut {
					t.Errorf("request is not PUT /api/runs/:runid/abort (actual method = %s)", r.Method)
				}
				if !strings.HasSuffix(r.URL.Path, fmt.Sprintf("/runs/%s/abort", runId)) {
					t.Errorf("request is not PUT /api/runs/:runid/abort (actual path = %s)", r.URL.Path)
				}

				w.Header().Add("Transfer-Encoding", "chunked")
				w.WriteHeader(when.statusCode)

				var buf []byte
				if when.statusCode == http.StatusOK {
					buf = try.To(json.Marshal(when.responseOk)).OrFatal(t)
				} else {
					buf = try.To(json.Marshal(when.responseError)).OrFatal(t)
				}
				w.Write(buf)
			}),
			)
			defer server.Close()

			prof := kprof.KnitProfile{ApiRoot: server.URL}

			testee := try.To(krst.NewClient(&prof)).OrFatal(t)

			ctx := context.Background()
			payload, err := testee.Abort(ctx, runId)

			if then.wantError {
				if err == nil {
					t.Error("Abort does not return error")
				}
				return
			}

			if err != nil {
				t.Fatalf("Abort returns error: %s", err)
			}

			if !payload.Equal(when.responseOk) {
				t.Errorf(
					"Abort returns wrong payload (actual, expected) = (%v, %v)",
					payload, when.responseOk,
				)
			}
		}
	}

	t.Run("when server response with 200, it returns the run detail", theory(
		When{
			statusCode: http.StatusOK,
			responseOk: runs.Detail{
				Summary: runs.Summary{
					RunId:  "test-runId",
					Status: "done",
					Plan: plans.Summary{
						PlanId: "test-Id",
						Image: &plans.Image{
							Repository: "test-image",
							Tag:        "test-version",
						},
						Name: "test-Name",
					},

					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-04-02T12:00:00+00:00",
					)).OrFatal(t),
				},
				Inputs: []runs.Assignment{
					{
						Mountpoint: plans.Mountpoint{
							Path: "/in/1",
							Tags: []tags.Tag{
								{Key: "type", Value: "raw data"},
								{Key: "format", Value: "rgb image"},
							},
						},
						KnitId: "test-knitId-a",
					},
				},
				Outputs: []runs.Assignment{
					{
						Mountpoint: plans.Mountpoint{
							Path: "/out/2",
							Tags: []tags.Tag{
								{Key: "type", Value: "training data"},
								{Key: "format", Value: "mask"},
							},
						},
						KnitId: "test-knitId-b",
					},
				},
				Log: &runs.LogSummary{
					LogPoint: plans.LogPoint{
						Tags: []tags.Tag{
							{Key: "type", Value: "log"},
							{Key: "format", Value: "jsonl"},
						},
					},
					KnitId: "test-knitId",
				},
			},
		},
		Then{wantError: false},
	))

	t.Run("when server response with 4xx, it returns error", theory(
		When{
			statusCode: http.StatusNotFound,
			responseError: apierr.ErrorMessage{
				Reason: "something wrong",
			},
		},
		Then{wantError: true},
	))

	t.Run("when server response with 5xx, it returns error", theory(
		When{
			statusCode: http.StatusInternalServerError,
			responseError: apierr.ErrorMessage{
				Reason: "something wrong",
			},
		},
		Then{wantError: true},
	))

}

func TestRunTearoff(t *testing.T) {
	type When struct {
		statusCode    int
		responseOk    runs.Detail
		responseError apierr.ErrorMessage
	}
	type Then struct {
		wantError bool
	}
	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			runId := "someRunId"

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut {
					t.Errorf("request is not PUT /api/runs/:runid/tearoff (actual method = %s)", r.Method)
				}
				if !strings.HasSuffix(r.URL.Path, fmt.Sprintf("/runs/%s/tearoff", runId)) {
					t.Errorf("request is not PUT /api/runs/:runid/tearoff (actual path = %s)", r.URL.Path)
				}

				w.Header().Add("Transfer-Encoding", "chunked")
				w.WriteHeader(when.statusCode)

				var buf []byte
				if when.statusCode == http.StatusOK {
					buf = try.To(json.Marshal(when.responseOk)).OrFatal(t)
				} else {
					buf = try.To(json.Marshal(when.responseError)).OrFatal(t)
				}
				w.Write(buf)
			}),
			)
			defer server.Close()

			prof := kprof.KnitProfile{ApiRoot: server.URL}

			testee := try.To(krst.NewClient(&prof)).OrFatal(t)

			ctx := context.Background()
			payload, err := testee.Tearoff(ctx, runId)

			if then.wantError {
				if err == nil {
					t.Error("Tearoff does not return error")
				}
				return
			}

			if err != nil {
				t.Fatalf("Abort returns error: %s", err)
			}

			if !payload.Equal(when.responseOk) {
				t.Errorf(
					"Tearoff returns wrong payload (actual, expected) = (%v, %v)",
					payload, when.responseOk,
				)
			}
		}
	}

	t.Run("when server response with 200, it returns the run detail", theory(
		When{
			statusCode: http.StatusOK,
			responseOk: runs.Detail{
				Summary: runs.Summary{
					RunId:  "test-runId",
					Status: "done",
					Plan: plans.Summary{
						PlanId: "test-Id",
						Image: &plans.Image{
							Repository: "test-image",
							Tag:        "test-version",
						},
						Name: "test-Name",
					},

					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-04-02T12:00:00+00:00",
					)).OrFatal(t),
				},
				Inputs: []runs.Assignment{
					{
						Mountpoint: plans.Mountpoint{
							Path: "/in/1",
							Tags: []tags.Tag{
								{Key: "type", Value: "raw data"},
								{Key: "format", Value: "rgb image"},
							},
						},
						KnitId: "test-knitId-a",
					},
				},
				Outputs: []runs.Assignment{
					{
						Mountpoint: plans.Mountpoint{
							Path: "/out/2",
							Tags: []tags.Tag{
								{Key: "type", Value: "training data"},
								{Key: "format", Value: "mask"},
							},
						},
						KnitId: "test-knitId-b",
					},
				},
				Log: &runs.LogSummary{
					LogPoint: plans.LogPoint{
						Tags: []tags.Tag{
							{Key: "type", Value: "log"},
							{Key: "format", Value: "jsonl"},
						},
					},
					KnitId: "test-knitId",
				},
			},
		},
		Then{wantError: false},
	))

	t.Run("when server response with 4xx, it returns error", theory(
		When{
			statusCode: http.StatusNotFound,
			responseError: apierr.ErrorMessage{
				Reason: "something wrong",
			},
		},
		Then{wantError: true},
	))

	t.Run("when server response with 5xx, it returns error", theory(
		When{
			statusCode: http.StatusInternalServerError,
			responseError: apierr.ErrorMessage{
				Reason: "something wrong",
			},
		},
		Then{wantError: true},
	))

}

func TestDeleteRun(t *testing.T) {
	t.Run("when server responses without err, it returns nil", func(t *testing.T) {
		runId := "someRunId"
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodDelete {
				t.Errorf("request is not DELETE /api/runs/:runid (actual method = %s)", r.Method)
			}
			if !strings.HasSuffix(r.URL.Path, "/runs/"+runId) {
				t.Errorf("request is not DELETE /api/runs/:runid (actual path = %s)", r.URL.Path)
			}
			w.WriteHeader(http.StatusOK)
		}),
		)
		defer server.Close()

		prof := kprof.KnitProfile{ApiRoot: server.URL}
		testee := try.To(krst.NewClient(&prof)).OrFatal(t)

		ctx := context.Background()
		if err := testee.DeleteRun(ctx, runId); err != nil {
			t.Fatalf("DeleteRun returns error: %s", err)
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

				testee := try.To(krst.NewClient(&profile)).OrFatal(t)
				runId := "test-Id"
				if err := testee.DeleteRun(ctx, runId); err == nil {
					t.Errorf("no error occured")
				}
			})
		}
	})

	t.Run("When send request to invalid host, it returns error", func(t *testing.T) {
		runId := "someRunId"
		ctx := context.Background()
		prof := kprof.KnitProfile{ApiRoot: "http://test.invalid"}

		testee := try.To(krst.NewClient(&prof)).OrFatal(t)
		err := testee.DeleteRun(ctx, runId)

		if err == nil {
			t.Fatalf("DeleteRun does not return error")
		} else if urlerr := new(url.Error); !errors.As(err, &urlerr) {
			t.Errorf(
				"DeleteRun does not return expected error (url.Error), but actual = %s (%#v)",
				err, err,
			)
		}
	})
}

func TestRetry(t *testing.T) {
	t.Run("when server responses without err, it returns nil", func(t *testing.T) {
		runId := "someRunId"
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut {
				t.Errorf("request is not PUT /api/runs/:runid/retry (actual method = %s)", r.Method)
			}
			if !strings.HasSuffix(r.URL.Path, "/runs/"+runId+"/retry") {
				t.Errorf("request is not PUT /api/runs/:runid/retry (actual path = %s)", r.URL.Path)
			}
			w.WriteHeader(http.StatusOK)
		}),
		)
		defer server.Close()

		prof := kprof.KnitProfile{ApiRoot: server.URL}
		testee := try.To(krst.NewClient(&prof)).OrFatal(t)

		ctx := context.Background()
		if err := testee.Retry(ctx, runId); err != nil {
			t.Fatalf("Retry returns error: %s", err)
		}
	})

	t.Run("a server responding with error is given", func(t *testing.T) {
		handlerFactory := func(t *testing.T, status int, message string) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Helper()
				w.WriteHeader(status)
				w.Header().Set("Content-Type", "application/json")

				buf := try.To(
					json.Marshal(apierr.ErrorMessage{Reason: message}),
				).OrFatal(t)
				w.Write(buf)
			})
		}
		for _, status := range []int{
			http.StatusNotFound,
			http.StatusConflict,
			http.StatusInternalServerError,
		} {
			t.Run(fmt.Sprintf("when server responding with %d, it returns error", status), func(t *testing.T) {
				ctx := context.Background()
				handler := handlerFactory(t, status, "something wrong")
				server := httptest.NewServer(handler)
				defer server.Close()
				profile := kprof.KnitProfile{ApiRoot: server.URL}

				testee := try.To(krst.NewClient(&profile)).OrFatal(t)
				runId := "test-Id"
				if err := testee.Retry(ctx, runId); err == nil {
					t.Errorf("no error occured")
				}
			})
		}

		t.Run("when server responding with 200, it returns nil", func(t *testing.T) {
			ctx := context.Background()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			profile := kprof.KnitProfile{ApiRoot: server.URL}

			testee := try.To(krst.NewClient(&profile)).OrFatal(t)
			runId := "test-Id"
			if err := testee.Retry(ctx, runId); err != nil {
				t.Errorf("no error occured")
			}
		})
	})
}
