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
	"github.com/opst/knitfab/pkg/domain"
	dbdatamock "github.com/opst/knitfab/pkg/domain/data/db/mock"
	"github.com/opst/knitfab/pkg/domain/data/k8s/dataagt"
	dataK8sMock "github.com/opst/knitfab/pkg/domain/data/k8s/mock"
	k8serrors "github.com/opst/knitfab/pkg/domain/errors/k8serrors"
	dbrunmock "github.com/opst/knitfab/pkg/domain/run/db/mock"
	runK8sMock "github.com/opst/knitfab/pkg/domain/run/k8s/mock"
	"github.com/opst/knitfab/pkg/domain/run/k8s/worker"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
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
						"Content-Type":           {"application/octet-stream"},
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

			targetData := domain.KnitDataBody{
				KnitId:    "test-log-knit-id",
				VolumeRef: "pvc-test-log-knit-id",
			}
			targetRun := map[string]domain.Run{
				"test-run-id": {
					RunBody: domain.RunBody{
						Id: "test-run-id",
						PlanBody: domain.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash",
						},
						Status: domain.Done,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
					},
					Inputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "test-in-knit-id", VolumeRef: "pvc-test-in-knit-id",
							},
							MountPoint: domain.MountPoint{Id: 1, Path: "/testinpath/test"},
						},
					},
					Outputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "test-out-knit-id", VolumeRef: "pvc-test-out-knit-id",
							},
							MountPoint: domain.MountPoint{Id: 3, Path: "/testoutpath/test"},
						},
					},
					Log: &domain.Log{Id: 2, KnitDataBody: targetData},
				},
			}

			mRunDB := dbrunmock.NewRunInterface()
			mRunDB.Impl.Get = func(ctx context.Context, runId []string) (map[string]domain.Run, error) {
				expected := []string{"test-run-id"}
				if !cmp.SliceContentEq(runId, expected) {
					t.Errorf("unexpected query: runid: (actual, expected) = (%v, %v)", runId, expected)
				}

				return targetRun, nil
			}

			newDataAgent := domain.DataAgent{
				Name:         "test-log-knit-id",
				Mode:         domain.DataAgentRead,
				KnitDataBody: targetData,
			}

			mDataDB := dbdatamock.NewDataInterface()
			mDataDB.Impl.NewAgent = func(ctx context.Context, s string, dam domain.DataAgentMode, d time.Duration) (domain.DataAgent, error) {
				if s != targetData.KnitId {
					t.Errorf("unexpected query: knitId: (actual, expected) = (%v, %v)", s, targetData.KnitId)
				}
				if dam != domain.DataAgentRead {
					t.Errorf("unexpected query: mode: (actual, expected) = (%v, %v)", dam, domain.DataAgentRead)
				}

				return newDataAgent, nil
			}
			mDataDB.Impl.RemoveAgent = func(ctx context.Context, s string) error {
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

			mDataK8s := dataK8sMock.New(t)
			spawnDataagtCalled := 0
			mDataK8s.Impl.SpawnDataAgent = func(ctx context.Context, d domain.DataAgent, deadlint time.Time) (dataagt.DataAgent, error) {
				spawnDataagtCalled += 1

				if !newDataAgent.Equal(&d) {
					t.Errorf(
						"DataAgent\nactual:   %+v\nexpected: %+v",
						d, targetData,
					)
				}

				return dagt, nil
			}

			mRunK8s := runK8sMock.New(t)
			mRunK8s.Impl.FindWorker = func(ctx context.Context, r domain.RunBody) (worker.Worker, error) {
				t.Error("FindWorker: should not be called")
				return nil, nil
			}

			testee := handlers.GetRunLogHandler(
				mRunDB, mDataDB, mDataK8s, mRunK8s, "runid",
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

			if mRunDB.Calls.Get.Times() < 1 {
				t.Errorf("RunInterface.Get has not been called.")
			}

			if mDataDB.Calls.NewAgent.Times() < 1 {
				t.Errorf("DataInterface.NewAgent has not been called.")
			}

			if mDataDB.Calls.RemoveAgent.Times() < 1 {
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
		run                     map[string]domain.Run
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
			run:                map[string]domain.Run{},
			expectedStatusCode: 404,
		},
		"Invalidated Run causes 404 error": {
			run: map[string]domain.Run{
				"test-run-id": {
					RunBody: domain.RunBody{
						Id: "test-run-id", Status: domain.Invalidated,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash",
							Image:  &domain.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
							Pseudo: nil,
						},
					},
				},
			},
			expectedStatusCode: 404,
		},
		"Run with no LogMountPoint causes 404 error": {
			run: map[string]domain.Run{
				"test-run-id": {
					RunBody: domain.RunBody{
						Id: "test-run-id", Status: domain.Waiting,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash",
							Image:  &domain.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
							Pseudo: nil,
						},
					},
					Inputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId:    "input-data",
								VolumeRef: "vpc-input-data",
							},
							MountPoint: domain.MountPoint{Id: 1, Path: "mp1/path"},
						},
					},
					Outputs: []domain.Assignment{
						{
							MountPoint: domain.MountPoint{Id: 3, Path: "mp3/path"},
						},
					},
				},
			},
			expectedStatusCode: 404,
		},
		"not started Run causes 503 error(Waiting)": {
			run: map[string]domain.Run{
				"test-run-id": {
					RunBody: domain.RunBody{
						Id: "test-run-id", Status: domain.Waiting,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image:  &domain.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
							Pseudo: nil,
						},
					},
					Inputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId:    "input-data",
								VolumeRef: "vpc-input-data",
							},
							MountPoint: domain.MountPoint{Id: 1, Path: "mp1/path"},
						},
					},
					Outputs: []domain.Assignment{
						{
							MountPoint: domain.MountPoint{Id: 3, Path: "mp3/path"},
						},
					},
					Log: &domain.Log{
						Id: 2,
						KnitDataBody: domain.KnitDataBody{
							KnitId:    "log-output-data",
							VolumeRef: "vpc-log-output-data",
						},
					},
				},
			},
			expectedStatusCode: 503,
		},
		"not started Run causes 503 error(Starting)": {
			run: map[string]domain.Run{
				"test-run-id": {
					RunBody: domain.RunBody{
						Id: "test-run-id", Status: domain.Waiting,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image:  &domain.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
							Pseudo: nil,
						},
					},
					Inputs: []domain.Assignment{
						{MountPoint: domain.MountPoint{Id: 1, Path: "mp1/path"}},
					},
					Outputs: []domain.Assignment{
						{MountPoint: domain.MountPoint{Id: 3, Path: "mp3/path"}},
					},
					Log: &domain.Log{
						Id: 2,
						KnitDataBody: domain.KnitDataBody{
							KnitId:    "log-output-data",
							VolumeRef: "vpc-log-output-data",
						},
					},
				},
			},
			expectedStatusCode: 503,
		},
		"not started Run causes 503 error(Deactivated)": {
			run: map[string]domain.Run{
				"test-run-id": {
					RunBody: domain.RunBody{
						Id: "test-run-id", Status: domain.Deactivated,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image:  &domain.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
							Pseudo: nil,
						},
					},
					Inputs: []domain.Assignment{
						{MountPoint: domain.MountPoint{Id: 1, Path: "mp1/path"}},
					},
					Outputs: []domain.Assignment{
						{MountPoint: domain.MountPoint{Id: 2, Path: "mp2/path"}},
					},
					Log: &domain.Log{Id: 3, Tags: nil},
				},
			},
			expectedStatusCode: 503,
		},
		"NewDataAgent failure causes 500 error": {
			run: map[string]domain.Run{
				"test-run-id": {
					RunBody: domain.RunBody{
						Id: "test-run-id", Status: domain.Done,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image: &domain.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
						},
					},
					Inputs: []domain.Assignment{
						{MountPoint: domain.MountPoint{Id: 1, Path: "mp1/path"}},
					},
					Outputs: []domain.Assignment{
						{MountPoint: domain.MountPoint{Id: 3, Path: "mp3/path"}},
					},
					Log: &domain.Log{
						Id: 2,
						KnitDataBody: domain.KnitDataBody{
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
			run: map[string]domain.Run{
				"test-run-id": {
					RunBody: domain.RunBody{
						Id: "test-run-id", Status: domain.Done,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image: &domain.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
						},
					},
					Inputs: []domain.Assignment{
						{MountPoint: domain.MountPoint{Id: 1, Path: "mp1/path"}},
					},
					Outputs: []domain.Assignment{
						{MountPoint: domain.MountPoint{Id: 3, Path: "mp3/path"}},
					},
					Log: &domain.Log{
						Id: 2,
						KnitDataBody: domain.KnitDataBody{
							KnitId:    "test-log-knit-id",
							VolumeRef: "vpc-test-log-knit-id",
						},
					},
				},
			},
			errorFromSpawner:   k8serrors.ErrDeadlineExceeded,
			expectedStatusCode: 503,
		},
		"Data reader spawn failure causes 500 error": {
			run: map[string]domain.Run{
				"test-run-id": {
					RunBody: domain.RunBody{
						Id: "test-run-id", Status: domain.Done,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image: &domain.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
						},
					},
					Inputs: []domain.Assignment{
						{MountPoint: domain.MountPoint{Id: 1, Path: "mp1/path"}},
					},
					Outputs: []domain.Assignment{
						{MountPoint: domain.MountPoint{Id: 3, Path: "mp3/path"}},
					},
					Log: &domain.Log{
						Id: 2,
						KnitDataBody: domain.KnitDataBody{
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
			run: map[string]domain.Run{
				"test-run-id": {
					RunBody: domain.RunBody{
						Id: "test-run-id", Status: domain.Done,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
							Image: &domain.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
						},
					},
					Inputs: []domain.Assignment{
						{MountPoint: domain.MountPoint{Id: 1, Path: "mp1/path"}},
					},
					Outputs: []domain.Assignment{
						{MountPoint: domain.MountPoint{Id: 3, Path: "mp3/path"}},
					},
					Log: &domain.Log{
						Id: 2,
						KnitDataBody: domain.KnitDataBody{
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
			mRunDB := dbrunmock.NewRunInterface()
			mRunDB.Impl.Get = func(ctx context.Context, runId []string) (map[string]domain.Run, error) {
				if testcase.errorFromRunGet != nil {
					return nil, testcase.errorFromRunGet
				} else if testcase.run != nil {
					return testcase.run, nil
				}
				return map[string]domain.Run{}, nil
			}

			mDataDB := dbdatamock.NewDataInterface()
			mDataDB.Impl.NewAgent = func(ctx context.Context, s string, dam domain.DataAgentMode, d time.Duration) (domain.DataAgent, error) {
				da := domain.DataAgent{
					Name:         "test-log-knit-id",
					Mode:         domain.DataAgentRead,
					KnitDataBody: domain.KnitDataBody{KnitId: "test-log-knit-id", VolumeRef: "pvc-test-log-knit-id"},
				}

				if testcase.errorFromNewAgent != nil {
					return domain.DataAgent{}, testcase.errorFromNewAgent
				}
				return da, nil
			}

			mDataDB.Impl.RemoveAgent = func(ctx context.Context, s string) error {
				return nil
			}

			mDataK8s := dataK8sMock.New(t)
			mDataK8s.Impl.SpawnDataAgent = func(context.Context, domain.DataAgent, time.Time) (dataagt.DataAgent, error) {
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
			}

			mRunK8s := runK8sMock.New(t)
			mRunK8s.Impl.FindWorker = func(ctx context.Context, r domain.RunBody) (worker.Worker, error) {
				t.Error("FindWorker: should not be called")
				return nil, nil
			}

			testee := handlers.GetRunLogHandler(
				mRunDB, mDataDB, mDataK8s, mRunK8s, "runid",
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
				if mDataDB.Calls.RemoveAgent.Times() < 1 {
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
			if 0 < mDataDB.Calls.RemoveAgent.Times() {
				t.Errorf("DataInterface.RemoveAgent should not be called.")
			}
		})
	}

	t.Run("when dataagt is broken. response 500", func(t *testing.T) {

		databody := domain.KnitDataBody{
			KnitId:    "test-log-knit-id",
			VolumeRef: "volume-ref",
		}
		run := map[string]domain.Run{
			"test-run-id": {
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id", Active: true, Hash: "hash-plan",
						Image:  &domain.ImageIdentifier{Image: "plan-image", Version: "plan-version"},
						Pseudo: nil,
					},
					Status: domain.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-10T01:00:01.000+09:00",
					)).OrFatal(t).Time(),
				},
				Inputs: []domain.Assignment{
					{MountPoint: domain.MountPoint{Id: 1, Path: "/mp1/path"}},
				},
				Outputs: []domain.Assignment{
					{MountPoint: domain.MountPoint{Id: 3, Path: "/mp3/path"}},
				},
				Log: &domain.Log{Id: 2, KnitDataBody: databody},
			},
		}

		mRunDB := dbrunmock.NewRunInterface()
		mRunDB.Impl.Get = func(ctx context.Context, runId []string) (map[string]domain.Run, error) {
			return run, nil
		}

		mDataDB := dbdatamock.NewDataInterface()
		mDataDB.Impl.NewAgent = func(ctx context.Context, s string, dam domain.DataAgentMode, d time.Duration) (domain.DataAgent, error) {
			return domain.DataAgent{
				Name:         "test-log-knit-id",
				Mode:         domain.DataAgentRead,
				KnitDataBody: databody,
			}, nil
		}
		mDataDB.Impl.RemoveAgent = func(ctx context.Context, s string) error {
			if s != "test-log-knit-id" {
				t.Errorf("unexpected query: knitId: (actual, expected) = (%v, %v)", s, "test-log-knit-id")
			}
			return nil
		}

		dagt := NewBrokenDataagt()
		dagt.Impl.Close = func() error { return nil }

		mDataK8s := dataK8sMock.New(t)
		mDataK8s.Impl.SpawnDataAgent = func(ctx context.Context, d domain.DataAgent, deadline time.Time) (dataagt.DataAgent, error) {
			return dagt, nil
		}

		mRunK8s := runK8sMock.New(t)
		mRunK8s.Impl.FindWorker = func(ctx context.Context, r domain.RunBody) (worker.Worker, error) {
			t.Error("FindWorker: should not be called")
			return nil, nil
		}

		testee := handlers.GetRunLogHandler(
			mRunDB, mDataDB, mDataK8s, mRunK8s, "runid",
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

		if mDataDB.Calls.RemoveAgent.Times() < 1 {
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

		runStatus       domain.KnitRunStatus
		errorFromRunGet error

		worker          worker.Worker
		errorFromWorker error
	}

	type Then struct {
		response responseDescriptor
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {

			targetData := domain.KnitDataBody{
				KnitId:    "test-log-knit-id",
				VolumeRef: "pvc-test-log-knit-id",
			}
			targetRun := map[string]domain.Run{
				"test-run-id": {
					RunBody: domain.RunBody{
						Id: "test-run-id",
						PlanBody: domain.PlanBody{
							PlanId: "test-plan-id", Active: true, Hash: "hash",
						},
						Status: when.runStatus,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-10T01:00:01.000+09:00",
						)).OrFatal(t).Time(),
					},
					Inputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "test-in-knit-id", VolumeRef: "pvc-test-in-knit-id",
							},
							MountPoint: domain.MountPoint{Id: 1, Path: "/testinpath/test"},
						},
					},
					Outputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "test-out-knit-id", VolumeRef: "pvc-test-out-knit-id",
							},
							MountPoint: domain.MountPoint{Id: 3, Path: "/testoutpath/test"},
						},
					},
					Log: &domain.Log{Id: 2, KnitDataBody: targetData},
				},
			}

			mRunDB := dbrunmock.NewRunInterface()
			mRunDB.Impl.Get = func(ctx context.Context, runId []string) (map[string]domain.Run, error) {
				wantRunId := []string{"test-run-id"}
				if !cmp.SliceContentEq(runId, wantRunId) {
					t.Errorf("unexpected query: runid: (actual, expected) = (%v, %v)", runId, wantRunId)
				}
				if when.errorFromRunGet != nil {
					return nil, when.errorFromRunGet
				}

				return targetRun, nil
			}

			mDataDB := dbdatamock.NewDataInterface()
			mDataK8s := dataK8sMock.New(t)
			mDataK8s.Impl.SpawnDataAgent = func(ctx context.Context, d domain.DataAgent, deadline time.Time) (dataagt.DataAgent, error) {
				t.Errorf("spawnDataAgent: should not be called")
				return nil, nil
			}

			mRunK8s := runK8sMock.New(t)
			mRunK8s.Impl.FindWorker = func(ctx context.Context, r domain.RunBody) (worker.Worker, error) {
				t.Error("FindWorker: should not be called")
				return nil, when.errorFromWorker
			}

			switch when.runStatus {
			case domain.Running, domain.Starting:
				mRunK8s.Impl.FindWorker = func(ctx context.Context, r domain.RunBody) (worker.Worker, error) {
					if when.worker != nil {
						return when.worker, nil
					}
					return nil, when.errorFromWorker
				}
			default:
				mDataDB.Impl.NewAgent = func(ctx context.Context, s string, dam domain.DataAgentMode, d time.Duration) (domain.DataAgent, error) {
					if s != targetData.KnitId {
						t.Errorf("unexpected query: knitId: (actual, expected) = (%v, %v)", s, targetData.KnitId)
					}
					if dam != domain.DataAgentRead {
						t.Errorf("unexpected query: mode: (actual, expected) = (%v, %v)", dam, domain.DataAgentRead)
					}
					return domain.DataAgent{
						Name:         targetData.KnitId,
						Mode:         domain.DataAgentRead,
						KnitDataBody: targetData,
					}, nil
				}
				mDataDB.Impl.RemoveAgent = func(ctx context.Context, s string) error {
					if s != targetData.KnitId {
						t.Errorf("unexpected query: knitId: (actual, expected) = (%v, %v)", s, targetData.KnitId)
					}
					return nil
				}
				mDataK8s.Impl.SpawnDataAgent = func(ctx context.Context, d domain.DataAgent, deadline time.Time) (dataagt.DataAgent, error) {
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
				mRunDB, mDataDB, mDataK8s, mRunK8s, "runid",
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

			if mRunDB.Calls.Get.Times() < 1 {
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
				runStatus: domain.Running,
				worker:    worker,
			},
			Then{
				response: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Transfer-Encoding": {"chunked"},
						"Content-Type":      {"application/octet-stream"},
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
				runStatus: domain.Starting,
				worker:    worker,
			},
			Then{
				response: responseDescriptor{
					code: 200,
					header: map[string][]string{
						"Transfer-Encoding": {"chunked"},
						"Content-Type":      {"application/octet-stream"},
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
				runStatus: domain.Waiting,
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
				runStatus: domain.Deactivated,
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
				runStatus: domain.Ready,
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
				runStatus: domain.Done,
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
						"Content-Type":      {"application/octet-stream"},
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
				runStatus: domain.Completing,
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
						"Content-Type":      {"application/octet-stream"},
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
				runStatus: domain.Failed,
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
						"Content-Type":      {"application/octet-stream"},
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
				runStatus: domain.Aborting,
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
						"Content-Type":      {"application/octet-stream"},
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
