package handlers_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab/cmd/knitd_backend/handlers"
	httptestutil "github.com/opst/knitfab/internal/testutils/http"
	kdb "github.com/opst/knitfab/pkg/db"
	dbmock "github.com/opst/knitfab/pkg/db/mocks"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/opst/knitfab/pkg/workloads"
	"github.com/opst/knitfab/pkg/workloads/dataagt"
	"github.com/opst/knitfab/pkg/workloads/worker"
)

func TestGetRunLogHandler(t *testing.T) {
	type when struct {
		respFromDataAgt responseDescriptor
		isZip           bool
	}

	type then struct {
		response responseDescriptor
	}

	type testcase struct {
		when when
		then then
	}

	for name, testcase := range map[string]testcase{
		"GetRunLog should proxy request & response: code 200, chunked and with trailer": {
			when: when{
				respFromDataAgt: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Content-Type":           {"application/tar+gzip"},
						"Transfer-Encoding":      {"chunked"},
						"Trailer":                {"Example-Trailer"},
						"X-Some-Header-For-Test": {"aaa", "bbb"},
					},
					body: []byte("quick brown fox jumps over a lazy dog"),
					trailer: &map[string][]string{
						"Example-Trailer": {"example trailer payload"},
					},
				},
				isZip: true,
			},
			then: then{
				response: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Content-Type":           {"plain/text"},
						"X-Some-Header-For-Test": {"aaa", "bbb"},
					},
					body: []byte("quick brown fox jumps over a lazy dog"),
				},
			},
		},
		"GetRunLog should proxy request & response: code 400, with trailer": {
			when: when{
				respFromDataAgt: responseDescriptor{
					code: 400,
					header: map[string][]string{
						"Content-Type":           {"application/json"},
						"Transfer-Encoding":      {"chunked"},
						"Trailer":                {"Example-Trailer"},
						"X-Some-Header-For-Test": {"aaa", "bbb"},
					},
					body: []byte(`{"message": "fake error."}`),
					trailer: &map[string][]string{
						"Example-Trailer": {"example trailer payload"},
					},
				},
				isZip: false,
			},
			then: then{
				response: responseDescriptor{
					code: 400,
					header: map[string][]string{
						"Content-Type":           {"application/json"},
						"X-Some-Header-For-Test": {"aaa", "bbb"},
					},
					body: []byte(`{"message": "fake error."}`),
					trailer: &map[string][]string{
						"Example-Trailer": {"example trailer payload"},
					},
				},
			},
		},
		"GetRunLog should proxy request & response: code 400, not chunked, no trailer": {
			when: when{
				respFromDataAgt: responseDescriptor{
					code: 400,
					header: map[string][]string{
						"Content-Type":           {"application/json"},
						"X-Some-Header-For-Test": {"aaa", "bbb"},
					},
					body: []byte(`{"message": "fake error."}`),
				},
				isZip: false,
			},
			then: then{
				response: responseDescriptor{
					code: 400,
					header: map[string][]string{
						"Content-Type":           {"application/json"},
						"X-Some-Header-For-Test": {"aaa", "bbb"},
					},
					body: []byte(`{"message": "fake error."}`),
				},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			var actualRequest *requestSnapshot
			hdr := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				actualRequest = try.To(Read(r)).OrFatal(t)
				if testcase.when.isZip {
					testcase.when.respFromDataAgt.WriteAsTarGzContainsSingleFile("/log/log", w)
				} else {
					testcase.when.respFromDataAgt.Write(w)
				}
			})

			svr := httptest.NewServer(hdr)
			defer svr.Close()

			targetData := kdb.KnitDataBody{
				KnitId:    "test-log-knit-id",
				VolumeRef: "pvc-test-log-knit-id",
			}
			targetRun := map[string]kdb.Run{
				"test-run-id": {
					RunBody: kdb.RunBody{
						Id: "test-run-id",
						PlanBody: kdb.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash",
						},
						Status: kdb.Done,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
					},
					Inputs: []kdb.Assignment{
						{
							KnitDataBody: kdb.KnitDataBody{
								KnitId: "test-in-knit-id", VolumeRef: "pvc-test-in-knit-id",
							},
							MountPoint: kdb.MountPoint{Id: 1, Path: "/testinpath/test"},
						},
					},
					Outputs: []kdb.Assignment{
						{
							KnitDataBody: kdb.KnitDataBody{
								KnitId: "test-out-knit-id", VolumeRef: "pvc-test-out-knit-id",
							},
							MountPoint: kdb.MountPoint{Id: 3, Path: "/testoutpath/test"},
						},
					},
					Log: &kdb.Log{Id: 2, KnitDataBody: targetData},
				},
			}

			mRunInterface := dbmock.NewRunInterface()
			mRunInterface.Impl.Get = func(ctx context.Context, runId []string) (map[string]kdb.Run, error) {
				expected := []string{"test-run-id"}
				if !cmp.SliceContentEq(runId, expected) {
					t.Errorf("unexpected query: runid: (actual, expected) = (%v, %v)", runId, expected)
				}

				return targetRun, nil
			}

			newDataAgent := kdb.DataAgent{
				Name:         "test-log-knit-id",
				Mode:         kdb.DataAgentRead,
				KnitDataBody: targetData,
			}

			mDataInterface := dbmock.NewDataInterface()
			mDataInterface.Impl.NewAgent = func(ctx context.Context, s string, dam kdb.DataAgentMode, d time.Duration) (kdb.DataAgent, error) {
				if s != targetData.KnitId {
					t.Errorf("unexpected query: knitId: (actual, expected) = (%v, %v)", s, targetData.KnitId)
				}
				if dam != kdb.DataAgentRead {
					t.Errorf("unexpected query: mode: (actual, expected) = (%v, %v)", dam, kdb.DataAgentRead)
				}

				return newDataAgent, nil
			}
			mDataInterface.Impl.RemoveAgent = func(ctx context.Context, s string) error {
				if s != newDataAgent.Name {
					t.Errorf("unexpected query: knitId: (actual, expected) = (%v, %v)", s, newDataAgent.Name)
				}
				return nil
			}

			dagt := NewMockedDataagt(svr)
			dagt.Impl.Close = func() error { return nil }

			e := echo.New()
			ectx, resprec := httptestutil.Get(
				e, path.Join("/api/backend/runs/test-run-id/log"),
				httptestutil.WithHeader("X-User-Custom-Header", "aaaa"),
			)

			ectx.SetPath("/api/backend/runs/:runid/log")
			ectx.SetParamNames("runid")
			ectx.SetParamValues("test-run-id")

			spawnDataagtCalled := 0
			testee := handlers.GetRunLogHandler(
				mRunInterface,
				mDataInterface,
				func(ctx context.Context, d kdb.DataAgent, deadline time.Time) (dataagt.Dataagt, error) {
					spawnDataagtCalled += 1

					if !newDataAgent.Equal(&d) {
						t.Errorf(
							"DataAgent\nactual:   %+v\nexpected: %+v",
							d, targetData,
						)
					}

					return dagt, nil
				},
				func(ctx context.Context, r kdb.Run) (worker.Worker, error) {
					t.Error("getWorker: should not be called")
					return nil, nil
				},
				"runid",
			)
			if err := testee(ectx); err != nil {
				t.Fatalf("testee returns error unexpectedly. %v", err)
			}

			// --- about request ---
			expectedReqHeader := map[string][]string{
				"x-user-custom-header": {"aaaa"},
			}
			if !cmp.MapGeqWith(
				actualRequest.header, expectedReqHeader, cmp.SliceContentEq[string],
			) {
				t.Errorf(
					"sent header is not proxied to dataagt. (actual, expected) = (%+v, %+v)",
					actualRequest.header, expectedReqHeader,
				)
			}
			if 0 < len(actualRequest.body) {
				t.Errorf("unexpected payload is sent to dataagt. payload = %s", actualRequest.body)
			}
			if 0 < len(actualRequest.trailer) {
				t.Errorf("unexpected trailer is sent to dataagt. trailers = %+v", actualRequest.trailer)
			}

			// --- about dataagt resource management ---
			if spawnDataagtCalled < 1 {
				t.Fatalf("spawnDataAgt called too many/less. actual = %d != 1", spawnDataagtCalled)
			}
			if dagt.Calls.Close.Times() < 1 {
				t.Errorf("dataagt.Close has not been called.")
			}

			if mRunInterface.Calls.Get.Times() < 1 {
				t.Errorf("RunInterface.Get has not been called.")
			}

			if mDataInterface.Calls.NewAgent.Times() < 1 {
				t.Errorf("DataInterface.NewAgent has not been called.")
			}

			if mDataInterface.Calls.RemoveAgent.Times() < 1 {
				t.Errorf("DataInterface.RemoveAgent has not been called.")
			}

			// --- about response ---
			if match, err := ResponseEq(*resprec, testcase.then.response); err != nil {
				t.Errorf("failed to load from actual response. %v", err)
			} else if !match.Match() {
				t.Errorf("response is wrong. %+v", match)
			}
		})
	}

	for name, testcase := range map[string]struct {
		// data               kdb.KnitDataBody
		run                     map[string]kdb.Run
		errorFromRunGet         error
		errorFromNewAgent       error
		errorFromSpawner        error
		errorFromDataAgentClose error
		expectedStatusCode      int
	}{
		"Run query error causes 500 error": {
			errorFromRunGet:    errors.New("fake error"),
			expectedStatusCode: 500,
		},
		"Run missing error causes 404 error": {
			run:                map[string]kdb.Run{},
			expectedStatusCode: 404,
		},
		"Invalidated Run causes 404 error": {
			run: map[string]kdb.Run{
				"test-run-id": {
					RunBody: kdb.RunBody{
						Id: "test-run-id", Status: kdb.Invalidated,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: kdb.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash",
							Image:  &kdb.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
							Pseudo: nil,
						},
					},
				},
			},
			expectedStatusCode: 404,
		},
		"Run with no LogMountPoint causes 404 error": {
			run: map[string]kdb.Run{
				"test-run-id": {
					RunBody: kdb.RunBody{
						Id: "test-run-id", Status: kdb.Waiting,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: kdb.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash",
							Image:  &kdb.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
							Pseudo: nil,
						},
					},
					Inputs: []kdb.Assignment{
						{
							KnitDataBody: kdb.KnitDataBody{
								KnitId:    "input-data",
								VolumeRef: "vpc-input-data",
							},
							MountPoint: kdb.MountPoint{Id: 1, Path: "mp1/path"},
						},
					},
					Outputs: []kdb.Assignment{
						{
							MountPoint: kdb.MountPoint{Id: 3, Path: "mp3/path"},
						},
					},
				},
			},
			expectedStatusCode: 404,
		},
		"not started Run causes 503 error(Waiting)": {
			run: map[string]kdb.Run{
				"test-run-id": {
					RunBody: kdb.RunBody{
						Id: "test-run-id", Status: kdb.Waiting,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: kdb.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image:  &kdb.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
							Pseudo: nil,
						},
					},
					Inputs: []kdb.Assignment{
						{
							KnitDataBody: kdb.KnitDataBody{
								KnitId:    "input-data",
								VolumeRef: "vpc-input-data",
							},
							MountPoint: kdb.MountPoint{Id: 1, Path: "mp1/path"},
						},
					},
					Outputs: []kdb.Assignment{
						{
							MountPoint: kdb.MountPoint{Id: 3, Path: "mp3/path"},
						},
					},
					Log: &kdb.Log{
						Id: 2,
						KnitDataBody: kdb.KnitDataBody{
							KnitId:    "log-output-data",
							VolumeRef: "vpc-log-output-data",
						},
					},
				},
			},
			expectedStatusCode: 503,
		},
		"not started Run causes 503 error(Starting)": {
			run: map[string]kdb.Run{
				"test-run-id": {
					RunBody: kdb.RunBody{
						Id: "test-run-id", Status: kdb.Waiting,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: kdb.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image:  &kdb.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
							Pseudo: nil,
						},
					},
					Inputs: []kdb.Assignment{
						{MountPoint: kdb.MountPoint{Id: 1, Path: "mp1/path"}},
					},
					Outputs: []kdb.Assignment{
						{MountPoint: kdb.MountPoint{Id: 3, Path: "mp3/path"}},
					},
					Log: &kdb.Log{
						Id: 2,
						KnitDataBody: kdb.KnitDataBody{
							KnitId:    "log-output-data",
							VolumeRef: "vpc-log-output-data",
						},
					},
				},
			},
			expectedStatusCode: 503,
		},
		"not started Run causes 503 error(Deactivated)": {
			run: map[string]kdb.Run{
				"test-run-id": {
					RunBody: kdb.RunBody{
						Id: "test-run-id", Status: kdb.Deactivated,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: kdb.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image:  &kdb.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
							Pseudo: nil,
						},
					},
					Inputs: []kdb.Assignment{
						{MountPoint: kdb.MountPoint{Id: 1, Path: "mp1/path"}},
					},
					Outputs: []kdb.Assignment{
						{MountPoint: kdb.MountPoint{Id: 2, Path: "mp2/path"}},
					},
					Log: &kdb.Log{Id: 3, Tags: nil},
				},
			},
			expectedStatusCode: 503,
		},
		"NewDataAgent failure causes 500 error": {
			run: map[string]kdb.Run{
				"test-run-id": {
					RunBody: kdb.RunBody{
						Id: "test-run-id", Status: kdb.Done,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: kdb.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image: &kdb.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
						},
					},
					Inputs: []kdb.Assignment{
						{MountPoint: kdb.MountPoint{Id: 1, Path: "mp1/path"}},
					},
					Outputs: []kdb.Assignment{
						{MountPoint: kdb.MountPoint{Id: 3, Path: "mp3/path"}},
					},
					Log: &kdb.Log{
						Id: 2,
						KnitDataBody: kdb.KnitDataBody{
							KnitId:    "test-log-knit-id",
							VolumeRef: "vpc-test-log-knit-id",
						},
					},
				},
			},
			errorFromNewAgent:  errors.New("fake error"),
			expectedStatusCode: 500,
		},
		"Data reader spawn failure by ErrDeadlineExceeded causes 503 error": {
			run: map[string]kdb.Run{
				"test-run-id": {
					RunBody: kdb.RunBody{
						Id: "test-run-id", Status: kdb.Done,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: kdb.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image: &kdb.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
						},
					},
					Inputs: []kdb.Assignment{
						{MountPoint: kdb.MountPoint{Id: 1, Path: "mp1/path"}},
					},
					Outputs: []kdb.Assignment{
						{MountPoint: kdb.MountPoint{Id: 3, Path: "mp3/path"}},
					},
					Log: &kdb.Log{
						Id: 2,
						KnitDataBody: kdb.KnitDataBody{
							KnitId:    "test-log-knit-id",
							VolumeRef: "vpc-test-log-knit-id",
						},
					},
				},
			},
			errorFromSpawner:   workloads.ErrDeadlineExceeded,
			expectedStatusCode: 503,
		},
		"Data reader spawn failure causes 500 error": {
			run: map[string]kdb.Run{
				"test-run-id": {
					RunBody: kdb.RunBody{
						Id: "test-run-id", Status: kdb.Done,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: kdb.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image: &kdb.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
						},
					},
					Inputs: []kdb.Assignment{
						{MountPoint: kdb.MountPoint{Id: 1, Path: "mp1/path"}},
					},
					Outputs: []kdb.Assignment{
						{MountPoint: kdb.MountPoint{Id: 3, Path: "mp3/path"}},
					},
					Log: &kdb.Log{
						Id: 2,
						KnitDataBody: kdb.KnitDataBody{
							KnitId:    "test-log-knit-id",
							VolumeRef: "vpc-test-log-knit-id",
						},
					},
				},
			},
			errorFromSpawner:   errors.New("fake error"),
			expectedStatusCode: 500,
		},
		"when closing Data reader is failed, data agent record should not be removed": {
			run: map[string]kdb.Run{
				"test-run-id": {
					RunBody: kdb.RunBody{
						Id: "test-run-id", Status: kdb.Done,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: kdb.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image: &kdb.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
						},
					},
					Inputs: []kdb.Assignment{
						{MountPoint: kdb.MountPoint{Id: 1, Path: "mp1/path"}},
					},
					Outputs: []kdb.Assignment{
						{MountPoint: kdb.MountPoint{Id: 3, Path: "mp3/path"}},
					},
					Log: &kdb.Log{
						Id: 2,
						KnitDataBody: kdb.KnitDataBody{
							KnitId:    "test-log-knit-id",
							VolumeRef: "vpc-test-log-knit-id",
						},
					},
				},
			},
			errorFromDataAgentClose: errors.New("fake error"),
			expectedStatusCode:      200,
		},
	} {
		t.Run("When tring to read run log "+name, func(t *testing.T) {
			mRunInterface := dbmock.NewRunInterface()
			mRunInterface.Impl.Get = func(ctx context.Context, runId []string) (map[string]kdb.Run, error) {
				if testcase.errorFromRunGet != nil {
					return nil, testcase.errorFromRunGet
				} else if testcase.run != nil {
					return testcase.run, nil
				}
				return map[string]kdb.Run{}, nil
			}

			mDataInterface := dbmock.NewDataInterface()
			mDataInterface.Impl.NewAgent = func(ctx context.Context, s string, dam kdb.DataAgentMode, d time.Duration) (kdb.DataAgent, error) {
				da := kdb.DataAgent{
					Name:         "test-log-knit-id",
					Mode:         kdb.DataAgentRead,
					KnitDataBody: kdb.KnitDataBody{KnitId: "test-log-knit-id", VolumeRef: "pvc-test-log-knit-id"},
				}

				if testcase.errorFromNewAgent != nil {
					return kdb.DataAgent{}, testcase.errorFromNewAgent
				}
				return da, nil
			}

			mDataInterface.Impl.RemoveAgent = func(ctx context.Context, s string) error {
				return nil
			}

			testee := handlers.GetRunLogHandler(
				mRunInterface,
				mDataInterface,
				func(context.Context, kdb.DataAgent, time.Time) (dataagt.Dataagt, error) {
					if testcase.errorFromSpawner != nil {
						return nil, testcase.errorFromSpawner
					}

					serv := httptest.NewServer(
						// No handlers are need for mock dataagt.
						// This API should not access DataAgent because of the caused error.
						nil,
					)
					t.Cleanup(serv.Close)
					dagt := NewMockedDataagt(serv)
					dagt.Impl.Close = func() error { return nil }
					t.Cleanup(func() { dagt.Close() })
					return dagt, nil
				},
				func(ctx context.Context, r kdb.Run) (worker.Worker, error) {
					t.Error("getWorker: should not be called")
					return nil, nil
				},
				"runid",
			)

			runId := "test-run-id"
			e := echo.New()
			ectx, resprec := httptestutil.Get(e, "/api/backend/runs/"+runId)
			ectx.SetPath("/api/backend/runs/:runid")
			ectx.SetParamNames("runid")
			ectx.SetParamValues("test-run-id")

			err := testee(ectx)
			if testcase.expectedStatusCode == 200 {
				if err != nil {
					t.Errorf("response is error, unexpectedly: %+v", err)
				}
				if mDataInterface.Calls.RemoveAgent.Times() < 1 {
					t.Errorf("DataInterface.RemoveAgent should be called.")
				}
				return
			}

			if err == nil {
				t.Errorf("response is success, unexpectedly: %+v", resprec)
			} else if httperr := new(echo.HTTPError); errors.As(err, &httperr) {
				if httperr.Code != testcase.expectedStatusCode {
					t.Errorf("error code is not %d. actual = %d", testcase.expectedStatusCode, httperr.Code)
				}
			}
			if 0 < mDataInterface.Calls.RemoveAgent.Times() {
				t.Errorf("DataInterface.RemoveAgent should not be called.")
			}
		})
	}

	t.Run("when dataagt is broken. response 500", func(t *testing.T) {

		databody := kdb.KnitDataBody{
			KnitId:    "test-log-knit-id",
			VolumeRef: "volume-ref",
		}
		run := map[string]kdb.Run{
			"test-run-id": {
				RunBody: kdb.RunBody{
					Id: "test-run-id",
					PlanBody: kdb.PlanBody{
						PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
						Image:  &kdb.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
						Pseudo: nil,
					},
					Status: kdb.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-10T01:00:01.000+09:00",
					)).OrFatal(t).Time(),
				},
				Inputs: []kdb.Assignment{
					{MountPoint: kdb.MountPoint{Id: 1, Path: "/mp1/path"}},
				},
				Outputs: []kdb.Assignment{
					{MountPoint: kdb.MountPoint{Id: 3, Path: "/mp3/path"}},
				},
				Log: &kdb.Log{Id: 2, KnitDataBody: databody},
			},
		}

		mRunInterface := dbmock.NewRunInterface()
		mRunInterface.Impl.Get = func(ctx context.Context, runId []string) (map[string]kdb.Run, error) {
			return run, nil
		}

		mDataInterface := dbmock.NewDataInterface()
		mDataInterface.Impl.NewAgent = func(ctx context.Context, s string, dam kdb.DataAgentMode, d time.Duration) (kdb.DataAgent, error) {
			return kdb.DataAgent{
				Name:         "test-log-knit-id",
				Mode:         kdb.DataAgentRead,
				KnitDataBody: databody,
			}, nil
		}
		mDataInterface.Impl.RemoveAgent = func(ctx context.Context, s string) error {
			if s != "test-log-knit-id" {
				t.Errorf("unexpected query: knitId: (actual, expected) = (%v, %v)", s, "test-log-knit-id")
			}
			return nil
		}

		dagt := NewBrokenDataagt()
		dagt.Impl.Close = func() error { return nil }

		testee := handlers.GetRunLogHandler(
			mRunInterface, mDataInterface,
			func(context.Context, kdb.DataAgent, time.Time) (dataagt.Dataagt, error) {
				return dagt, nil
			},
			func(ctx context.Context, r kdb.Run) (worker.Worker, error) {
				t.Error("getWorker: should not be called")
				return nil, nil
			},
			"runid",
		)

		runId := "test-run-id"
		e := echo.New()
		ectx, resprec := httptestutil.Get(e, "/api/backend/runs/"+runId)
		ectx.SetPath("/api/backend/run/:runid")
		ectx.SetParamNames("runid")
		ectx.SetParamValues("test-run-id")

		err := testee(ectx)
		if err == nil {
			t.Errorf("GetRunLogAgt handler does not error when dataagt is not up. resp = %+v", resprec)
		}

		if dagt.Calls.URL.Times() < 1 {
			t.Errorf("URI of dataagt is not queried.")
		}

		if dagt.Calls.Close.Times() < 1 {
			t.Errorf("dataagt.Close has not been called.")
		}

		if mDataInterface.Calls.RemoveAgent.Times() < 1 {
			t.Errorf("DataInterface.RemoveAgent has not been called.")
		}

		if herr := new(echo.HTTPError); !errors.As(err, &herr) {
			t.Errorf("error is not echo.HTTPError. actual = %+v", err)
		} else if herr.Code != 500 {
			t.Errorf("error code is not %d. actual = %d", 500, herr.Code)
		}
	})
}

func TestRunLogHandlerWithFollow(t *testing.T) {
	type When struct {
		respFromDataAgt responseDescriptor

		runStatus       kdb.KnitRunStatus
		errorFromRunGet error

		worker          worker.Worker
		errorFromWorker error
	}

	type Then struct {
		response responseDescriptor
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {

			targetData := kdb.KnitDataBody{
				KnitId:    "test-log-knit-id",
				VolumeRef: "pvc-test-log-knit-id",
			}
			targetRun := map[string]kdb.Run{
				"test-run-id": {
					RunBody: kdb.RunBody{
						Id: "test-run-id",
						PlanBody: kdb.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash",
						},
						Status: when.runStatus,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
					},
					Inputs: []kdb.Assignment{
						{
							KnitDataBody: kdb.KnitDataBody{
								KnitId: "test-in-knit-id", VolumeRef: "pvc-test-in-knit-id",
							},
							MountPoint: kdb.MountPoint{Id: 1, Path: "/testinpath/test"},
						},
					},
					Outputs: []kdb.Assignment{
						{
							KnitDataBody: kdb.KnitDataBody{
								KnitId: "test-out-knit-id", VolumeRef: "pvc-test-out-knit-id",
							},
							MountPoint: kdb.MountPoint{Id: 3, Path: "/testoutpath/test"},
						},
					},
					Log: &kdb.Log{Id: 2, KnitDataBody: targetData},
				},
			}

			mRunInterface := dbmock.NewRunInterface()
			mRunInterface.Impl.Get = func(ctx context.Context, runId []string) (map[string]kdb.Run, error) {
				wantRunId := []string{"test-run-id"}
				if !cmp.SliceContentEq(runId, wantRunId) {
					t.Errorf("unexpected query: runid: (actual, expected) = (%v, %v)", runId, wantRunId)
				}
				if when.errorFromRunGet != nil {
					return nil, when.errorFromRunGet
				}

				return targetRun, nil
			}

			mDataInterface := dbmock.NewDataInterface()
			spawnDataAgent := func(ctx context.Context, d kdb.DataAgent, deadline time.Time) (dataagt.Dataagt, error) {
				t.Errorf("spawnDataAgent: should not be called")
				return nil, nil
			}

			getWorker := func(ctx context.Context, r kdb.Run) (worker.Worker, error) {
				t.Error("getWorker: should not be called")
				return nil, when.errorFromWorker
			}

			switch when.runStatus {
			case kdb.Running, kdb.Starting:
				getWorker = func(ctx context.Context, r kdb.Run) (worker.Worker, error) {
					if when.worker != nil {
						return when.worker, nil
					}
					return nil, when.errorFromWorker
				}
			default:
				mDataInterface.Impl.NewAgent = func(ctx context.Context, s string, dam kdb.DataAgentMode, d time.Duration) (kdb.DataAgent, error) {
					if s != targetData.KnitId {
						t.Errorf("unexpected query: knitId: (actual, expected) = (%v, %v)", s, targetData.KnitId)
					}
					if dam != kdb.DataAgentRead {
						t.Errorf("unexpected query: mode: (actual, expected) = (%v, %v)", dam, kdb.DataAgentRead)
					}
					return kdb.DataAgent{
						Name:         targetData.KnitId,
						Mode:         kdb.DataAgentRead,
						KnitDataBody: targetData,
					}, nil
				}
				mDataInterface.Impl.RemoveAgent = func(ctx context.Context, s string) error {
					if s != targetData.KnitId {
						t.Errorf("unexpected query: knitId: (actual, expected) = (%v, %v)", s, targetData.KnitId)
					}
					return nil
				}
				spawnDataAgent = func(ctx context.Context, d kdb.DataAgent, deadline time.Time) (dataagt.Dataagt, error) {
					hdr := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						when.respFromDataAgt.WriteAsTarGzContainsSingleFile("/log/log", w)
					})
					svr := httptest.NewServer(hdr)
					t.Cleanup(svr.Close)

					dagt := NewMockedDataagt(svr)
					dagt.Impl.Close = func() error { return nil }
					t.Cleanup(func() { dagt.Close() })
					return dagt, nil
				}
			}

			testee := handlers.GetRunLogHandler(
				mRunInterface,
				mDataInterface,
				spawnDataAgent,
				getWorker,
				"runid",
			)

			runId := "test-run-id"
			e := echo.New()
			ectx, resprec := httptestutil.Get(e, "/api/backend/runs/"+runId+"/log?follow")
			ectx.SetPath("/api/backend/runs/:runid/log")
			ectx.SetParamNames("runid")
			ectx.SetParamValues("test-run-id")

			err := testee(ectx)
			if err != nil {
				if herr := new(echo.HTTPError); !errors.As(err, &herr) {
					t.Errorf("error is not echo.HTTPError. actual = %+v", err)
				} else if herr.Code != then.response.code {
					t.Errorf("error code is not %d. actual = %d", then.response.code, herr.Code)
				}
			}

			if mRunInterface.Calls.Get.Times() < 1 {
				t.Errorf("RunInterface.Get has not been called.")
			}

			if err == nil {
				actualContent := string(try.To(io.ReadAll(resprec.Result().Body)).OrFatal(t))
				expectedContent := string(then.response.body)
				if actualContent != expectedContent {
					t.Errorf(
						"response body is wrong.\n===actual===\n%s\n===expected===\n%s",
						actualContent, expectedContent,
					)
				}
			}
		}
	}

	{

		worker := NewMockWorker()
		worker.Impl.Log = func(ctx context.Context) (io.ReadCloser, error) {
			w := new(bytes.Buffer)
			io.WriteString(
				w,
				`line 1
line 2
lin3 3
`,
			)
			return io.NopCloser(w), nil
		}
		t.Run("running run should return log", theory(
			When{
				runStatus: kdb.Running,
				worker:    worker,
			},
			Then{
				response: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Transfer-Encoding": {"chunked"},
						"Content-Type":      {"text/plain"},
					},
					body: []byte(
						`line 1
line 2
lin3 3
`,
					),
				},
			},
		))
		t.Run("starting run should return log", theory(
			When{
				runStatus: kdb.Starting,
				worker:    worker,
			},
			Then{
				response: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Transfer-Encoding": {"chunked"},
						"Content-Type":      {"text/plain"},
					},
					body: []byte(
						`line 1
line 2
lin3 3
`,
					),
				},
			},
		))
	}

	{
		t.Run("waiting run should return 503", theory(
			When{
				runStatus: kdb.Waiting,
			},
			Then{
				response: responseDescriptor{
					code: 503,
					header: map[string][]string{
						"Content-Type": {"application/json"},
					},
				},
			},
		))
		t.Run("deactivated run should return 503", theory(
			When{
				runStatus: kdb.Deactivated,
			},
			Then{
				response: responseDescriptor{
					code: 503,
					header: map[string][]string{
						"Content-Type": {"application/json"},
					},
				},
			},
		))
		t.Run("ready run should return 503", theory(
			When{
				runStatus: kdb.Ready,
			},
			Then{
				response: responseDescriptor{
					code: 503,
					header: map[string][]string{
						"Content-Type": {"application/json"},
					},
				},
			},
		))
	}

	{
		t.Run("done run should return log", theory(
			When{
				runStatus: kdb.Done,
				respFromDataAgt: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Transfer-Encoding": {"chunked"},
						"Content-Type":      {"text/plain"},
					},
					body: []byte(
						`line 1
line 2
lin3 3
`,
					),
				},
			},
			Then{
				response: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Transfer-Encoding": {"chunked"},
						"Content-Type":      {"text/plain"},
					},
					body: []byte(
						`line 1
line 2
lin3 3
`,
					),
				},
			},
		))
		t.Run("completing run should return log", theory(
			When{
				runStatus: kdb.Completing,
				respFromDataAgt: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Transfer-Encoding": {"chunked"},
						"Content-Type":      {"text/plain"},
					},
					body: []byte(
						`line 1
line 2
lin3 3
`,
					),
				},
			},
			Then{
				response: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Transfer-Encoding": {"chunked"},
						"Content-Type":      {"text/plain"},
					},
					body: []byte(
						`line 1
line 2
lin3 3
`,
					),
				},
			},
		))
		t.Run("failed run should return log", theory(
			When{
				runStatus: kdb.Failed,
				respFromDataAgt: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Transfer-Encoding": {"chunked"},
						"Content-Type":      {"text/plain"},
					},
					body: []byte(
						`line 1
line 2
lin3 3
`,
					),
				},
			},
			Then{
				response: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Transfer-Encoding": {"chunked"},
						"Content-Type":      {"text/plain"},
					},
					body: []byte(
						`line 1
line 2
lin3 3
`,
					),
				},
			},
		))
		t.Run("aborting run should return log", theory(
			When{
				runStatus: kdb.Aborting,
				respFromDataAgt: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Transfer-Encoding": {"chunked"},
						"Content-Type":      {"text/plain"},
					},
					body: []byte(
						`line 1
line 2
lin3 3
`,
					),
				},
			},
			Then{
				response: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Transfer-Encoding": {"chunked"},
						"Content-Type":      {"text/plain"},
					},
					body: []byte(
						`line 1
line 2
lin3 3
`,
					),
				},
			},
		))
	}
}
