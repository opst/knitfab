package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	apiplans "github.com/opst/knitfab-api-types/plans"
	apiruns "github.com/opst/knitfab-api-types/runs"
	apitags "github.com/opst/knitfab-api-types/tags"
	handlers "github.com/opst/knitfab/cmd/knitd/handlers"
	httptestutil "github.com/opst/knitfab/internal/testutils/http"
	"github.com/opst/knitfab/pkg/domain"
	kerr "github.com/opst/knitfab/pkg/domain/errors"
	mockdb "github.com/opst/knitfab/pkg/domain/run/db/mock"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestRunFindHandler(t *testing.T) {

	t.Run("it returns OK with runs ", func(t *testing.T) {
		type when struct {
			request string
			Runs    []domain.Run
		}

		type then struct {
			query domain.RunFindQuery
			body  []apiruns.Detail
		}

		dummyUpdatedSince := try.To(rfctime.ParseRFC3339DateTime(
			"2024-04-01T12:00:00+00:00",
		)).OrFatal(t).Time()
		dummyUpdatedUntil := try.To(rfctime.ParseRFC3339DateTime(
			"2024-04-01T12:00:00+00:00",
		)).OrFatal(t).Time().Add(2*time.Hour + 30*time.Minute + 45*time.Second)

		for name, testcase := range map[string]struct {
			when
			then
		}{
			"as empty when no runs are found": {
				when{
					request: "/api/runs?plan=plan-x,plan-y&knitIdInput=in1,in2&knitIdOutput=out3,out4&status=waiting,running,done&since=2024-04-01T12%3A00%3A00%2B00%3A00&duration=2h30m45s",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{
						PlanId:       []string{"plan-x", "plan-y"},
						InputKnitId:  []string{"in1", "in2"},
						OutputKnitId: []string{"out3", "out4"},
						Status:       []domain.KnitRunStatus{domain.Waiting, domain.Running, domain.Done},
						UpdatedSince: &dummyUpdatedSince,
						UpdatedUntil: &dummyUpdatedUntil,
					},
					body: []apiruns.Detail{},
				},
			},
			"when it is queried all runs": {
				when{
					request: "/api/runs",
					Runs: []domain.Run{
						{
							RunBody: domain.RunBody{
								Id: "run-1", Status: domain.Done,
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
									"2022-11-15T01:00:00.123+09:00",
								)).OrFatal(t).Time(),
								PlanBody: domain.PlanBody{
									PlanId: "plan-1", Active: true,
									Image: &domain.ImageIdentifier{Image: "image-1", Version: "ver-1"},
								},
								Exit: &domain.RunExit{
									Code:    0,
									Message: "success",
								},
							},
							Inputs: []domain.Assignment{
								{
									KnitDataBody: domain.KnitDataBody{
										KnitId: "knitin1", VolumeRef: "pvc-knitin1",
									},
									MountPoint: domain.MountPoint{
										Id: 1, Path: "C:\\mp-1",
										Tags: domain.NewTagSet([]domain.Tag{
											{Key: "key1", Value: "tag-value"},
											{Key: "key2", Value: "value"},
										}),
									},
								},
								{
									KnitDataBody: domain.KnitDataBody{
										KnitId: "knitin2", VolumeRef: "pvc-knitin2",
									},
									MountPoint: domain.MountPoint{
										Id: 2, Path: "C:\\mp-2",
										Tags: domain.NewTagSet([]domain.Tag{
											{Key: "key1", Value: "tag-value"},
											{Key: "key2", Value: "value"},
										}),
									},
								},
								{
									KnitDataBody: domain.KnitDataBody{
										KnitId: "knitin3", VolumeRef: "pvc-knitin3",
									},
									MountPoint: domain.MountPoint{
										Id: 3, Path: "C:\\mp-3",
										Tags: domain.NewTagSet([]domain.Tag{
											{Key: "key1", Value: "tag-value"},
											{Key: "key2", Value: "value"},
										}),
									},
								},
							},
							Outputs: []domain.Assignment{
								{
									KnitDataBody: domain.KnitDataBody{
										KnitId: "knitout1", VolumeRef: "pvc-knitout1",
									},
									MountPoint: domain.MountPoint{
										Id: 4, Path: "C:\\mp-4",
										Tags: domain.NewTagSet([]domain.Tag{
											{Key: "key1", Value: "tag-value"},
											{Key: "key2", Value: "value"},
										}),
									},
								},
							},
							Log: &domain.Log{
								Id: 5,
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "key1", Value: "tag-value"},
									{Key: "key2", Value: "value"},
								}),
								KnitDataBody: domain.KnitDataBody{
									KnitId: "knitlog1", VolumeRef: "pvc-knitlog1",
								},
							},
						},
						{
							RunBody: domain.RunBody{

								Id: "run-2", Status: domain.Running,
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
									"2022-11-15T02:00:00.123+09:00",
								)).OrFatal(t).Time(),
								PlanBody: domain.PlanBody{
									PlanId: "plan-1", Active: true,
									Image: &domain.ImageIdentifier{Image: "image-1", Version: "ver-1"},
								},
							},
							Inputs: []domain.Assignment{
								{
									KnitDataBody: domain.KnitDataBody{KnitId: "knitin1", VolumeRef: "ref-knitin1"},
									MountPoint:   domain.MountPoint{Id: 1, Path: "C:\\mp-1"},
								},
								{
									KnitDataBody: domain.KnitDataBody{KnitId: "knitin2", VolumeRef: "ref-knitin2"},
									MountPoint:   domain.MountPoint{Id: 2, Path: "C:\\mp-2"},
								},
								{
									KnitDataBody: domain.KnitDataBody{KnitId: "knitin4", VolumeRef: "ref-knitin4"},
									MountPoint:   domain.MountPoint{Id: 3, Path: "C:\\mp-3"},
								},
							},
							Outputs: []domain.Assignment{
								{
									KnitDataBody: domain.KnitDataBody{KnitId: "knitout2", VolumeRef: "ref-knitout2"},
									MountPoint:   domain.MountPoint{Id: 4, Path: "C:\\mp-4"},
								},
							},
							Log: &domain.Log{
								Id:           5,
								KnitDataBody: domain.KnitDataBody{KnitId: "knitlog2", VolumeRef: "ref-knitlog2"},
							},
						},
						{
							RunBody: domain.RunBody{
								Id: "run-3", Status: domain.Waiting,
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
									"2022-11-15T03:00:00.123+09:00",
								)).OrFatal(t).Time(),
								PlanBody: domain.PlanBody{
									PlanId: "plan-2", Active: true,
									Pseudo: &domain.PseudoPlanDetail{Name: "name-2"},
								},
							},
							Inputs: []domain.Assignment{
								{
									KnitDataBody: domain.KnitDataBody{KnitId: "knitin5", VolumeRef: "ref-knitin5"},
									MountPoint:   domain.MountPoint{Id: 6, Path: "C:\\mp-6"},
								},
								{
									KnitDataBody: domain.KnitDataBody{KnitId: "knitin6", VolumeRef: "ref-knitin6"},
									MountPoint:   domain.MountPoint{Id: 7, Path: "C:\\mp-7"},
								},
							},
							Outputs: []domain.Assignment{
								{
									KnitDataBody: domain.KnitDataBody{KnitId: "knitout3", VolumeRef: "ref-knitout3"},
									MountPoint:   domain.MountPoint{Id: 8, Path: "C:\\mp-8"},
								},
							},
						},
					},
				},
				then{
					query: domain.RunFindQuery{}, // empty, means "match everything".
					body: []apiruns.Detail{
						{
							Summary: apiruns.Summary{
								RunId:     "run-1",
								Status:    string(domain.Done),
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime("2022-11-15T01:00:00.123+09:00")).OrFatal(t),
								Plan: apiplans.Summary{
									PlanId: "plan-1",
									Image:  &apiplans.Image{Repository: "image-1", Tag: "ver-1"},
									Name:   "",
								},
								Exit: &apiruns.Exit{
									Code:    0,
									Message: "success",
								},
							},
							Inputs: []apiruns.Assignment{
								{
									KnitId: "knitin1",
									Mountpoint: apiplans.Mountpoint{
										Path: "C:\\mp-1",
										Tags: []apitags.Tag{
											{Key: "key1", Value: "tag-value"},
											{Key: "key2", Value: "value"},
										},
									},
								},
								{
									KnitId: "knitin2",
									Mountpoint: apiplans.Mountpoint{
										Path: "C:\\mp-2",
										Tags: []apitags.Tag{
											{Key: "key1", Value: "tag-value"},
											{Key: "key2", Value: "value"},
										},
									},
								},
								{
									KnitId: "knitin3",
									Mountpoint: apiplans.Mountpoint{
										Path: "C:\\mp-3",
										Tags: []apitags.Tag{
											{Key: "key1", Value: "tag-value"},
											{Key: "key2", Value: "value"},
										},
									},
								},
							},
							Outputs: []apiruns.Assignment{
								{
									KnitId: "knitout1",
									Mountpoint: apiplans.Mountpoint{
										Path: "C:\\mp-4",
										Tags: []apitags.Tag{
											{Key: "key1", Value: "tag-value"},
											{Key: "key2", Value: "value"},
										},
									},
								},
							},
							Log: &apiruns.LogSummary{
								KnitId: "knitlog1",
								LogPoint: apiplans.LogPoint{
									Tags: []apitags.Tag{
										{Key: "key1", Value: "tag-value"},
										{Key: "key2", Value: "value"},
									},
								},
							},
						},
						{
							Summary: apiruns.Summary{
								RunId:     "run-2",
								Status:    string(domain.Running),
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime("2022-11-15T02:00:00.123+09:00")).OrFatal(t),
								Plan: apiplans.Summary{
									PlanId: "plan-1",
									Image:  &apiplans.Image{Repository: "image-1", Tag: "ver-1"},
									Name:   "",
								},
							},
							Inputs: []apiruns.Assignment{
								{
									KnitId:     "knitin1",
									Mountpoint: apiplans.Mountpoint{Path: "C:\\mp-1"},
								},
								{
									KnitId:     "knitin2",
									Mountpoint: apiplans.Mountpoint{Path: "C:\\mp-2"},
								},
								{
									KnitId:     "knitin4",
									Mountpoint: apiplans.Mountpoint{Path: "C:\\mp-3"},
								},
							},
							Outputs: []apiruns.Assignment{
								{
									KnitId:     "knitout2",
									Mountpoint: apiplans.Mountpoint{Path: "C:\\mp-4"},
								},
							},
							Log: &apiruns.LogSummary{KnitId: "knitlog2"},
						},
						{
							Summary: apiruns.Summary{
								RunId:     "run-3",
								Status:    string(domain.Waiting),
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime("2022-11-15T03:00:00.123+09:00")).OrFatal(t),
								Plan: apiplans.Summary{
									PlanId: "plan-2",
									Image:  nil,
									Name:   "name-2",
								},
							},
							Inputs: []apiruns.Assignment{
								{
									KnitId:     "knitin5",
									Mountpoint: apiplans.Mountpoint{Path: "C:\\mp-6"},
								},
								{
									KnitId:     "knitin6",
									Mountpoint: apiplans.Mountpoint{Path: "C:\\mp-7"},
								},
							},
							Outputs: []apiruns.Assignment{
								{
									KnitId:     "knitout3",
									Mountpoint: apiplans.Mountpoint{Path: "C:\\mp-8"},
								},
							},
							Log: nil,
						},
					},
				},
			},
			"when it is queried all dimensions with empty value": {
				when{
					request: "/api/runs?plan=&knitIdInput=&knitIdOutput=&status=&since=&duration=",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{},
					body:  []apiruns.Detail{},
				},
			},
			"when it is queried about planIds": {
				when{
					request: "/api/runs?plan=plan-1,plan-2",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{
						PlanId: []string{"plan-1", "plan-2"},
					},
					body: []apiruns.Detail{},
				},
			},
			"when it is queried about input data": {
				when{
					request: "/api/runs?knitIdInput=knitin1,knitin2",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{
						InputKnitId: []string{"knitin1", "knitin2"},
					},
					body: []apiruns.Detail{},
				},
			},
			"when it is queried about output data": {
				when{
					request: "/api/runs?knitIdOutput=knitout3,knitlog1",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{
						OutputKnitId: []string{"knitout3", "knitlog1"},
					},
					body: []apiruns.Detail{},
				},
			},
			"when it is queried about status": {
				when{
					request: "/api/runs?status=done,running",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{
						Status: []domain.KnitRunStatus{domain.Running, domain.Done},
					},
					body: []apiruns.Detail{},
				},
			},
			"when it is queried about since": {
				when{
					request: "/api/runs?since=2024-04-01T12%3A00%3A00%2B00%3A00",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{
						Status:       []domain.KnitRunStatus{},
						UpdatedSince: &dummyUpdatedSince,
					},
					body: []apiruns.Detail{},
				},
			},
			// duration is assumed to be used in conjunction with since.
			"when it is queried about since and duration": {
				when{
					request: "/api/runs?since=2024-04-01T12%3A00%3A00%2B00%3A00&duration=2h30m45s",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{
						Status:       []domain.KnitRunStatus{},
						UpdatedSince: &dummyUpdatedSince,
						UpdatedUntil: &dummyUpdatedUntil,
					},
					body: []apiruns.Detail{},
				},
			},
			"when it is queried about all dimensions except planId": {
				when{
					request: "/api/runs?knitIdInput=in1,in2&knitIdOutput=out3,out4&status=waiting,running,done&since=2024-04-01T12%3A00%3A00%2B00%3A00&duration=2h30m45s",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{
						InputKnitId:  []string{"in1", "in2"},
						OutputKnitId: []string{"out3", "out4"},
						Status:       []domain.KnitRunStatus{domain.Waiting, domain.Running, domain.Done},
						UpdatedSince: &dummyUpdatedSince,
						UpdatedUntil: &dummyUpdatedUntil,
					},
					body: []apiruns.Detail{},
				},
			},
			"when it is queried about all dimensions except input knit id": {
				when{
					request: "/api/runs?plan=plan-x,plan-y&knitIdOutput=out3,out4&status=waiting,running,done&since=2024-04-01T12%3A00%3A00%2B00%3A00&duration=2h30m45s",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{
						PlanId:       []string{"plan-x", "plan-y"},
						OutputKnitId: []string{"out3", "out4"},
						Status:       []domain.KnitRunStatus{domain.Waiting, domain.Running, domain.Done},
						UpdatedSince: &dummyUpdatedSince,
						UpdatedUntil: &dummyUpdatedUntil,
					},
					body: []apiruns.Detail{},
				},
			},
			"when it is queried about all dimensions except output knit id": {
				when{
					request: "/api/runs?plan=plan-x,plan-y&knitIdInput=in1,in2&status=waiting,running,done&since=2024-04-01T12%3A00%3A00%2B00%3A00&duration=2h30m45s",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{
						PlanId:       []string{"plan-x", "plan-y"},
						InputKnitId:  []string{"in1", "in2"},
						Status:       []domain.KnitRunStatus{domain.Waiting, domain.Running, domain.Done},
						UpdatedSince: &dummyUpdatedSince,
						UpdatedUntil: &dummyUpdatedUntil,
					},
					body: []apiruns.Detail{},
				},
			},
			"when it is queried about all dimensions except status": {
				when{
					request: "/api/runs?plan=plan-x,plan-y&knitIdInput=in1,in2&knitIdOutput=out3,out4&since=2024-04-01T12%3A00%3A00%2B00%3A00&duration=2h30m45s",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{
						PlanId:       []string{"plan-x", "plan-y"},
						InputKnitId:  []string{"in1", "in2"},
						OutputKnitId: []string{"out3", "out4"},
						UpdatedSince: &dummyUpdatedSince,
						UpdatedUntil: &dummyUpdatedUntil,
					},
					body: []apiruns.Detail{},
				},
			},
			"when it is queried about all dimensions except since": {
				when{
					request: "/api/runs?plan=plan-x,plan-y&knitIdInput=in1,in2&knitIdOutput=out3,out4&status=waiting,running,done",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{
						PlanId:       []string{"plan-x", "plan-y"},
						InputKnitId:  []string{"in1", "in2"},
						OutputKnitId: []string{"out3", "out4"},
						Status:       []domain.KnitRunStatus{domain.Waiting, domain.Running, domain.Done},
					},
					body: []apiruns.Detail{},
				},
			},
			"when it is queried about all dimensions except duration": {
				when{
					request: "/api/runs?plan=plan-x,plan-y&knitIdInput=in1,in2&knitIdOutput=out3,out4&status=waiting,running,done&since=2024-04-01T12%3A00%3A00%2B00%3A00",
					Runs:    []domain.Run{},
				},
				then{
					query: domain.RunFindQuery{
						PlanId:       []string{"plan-x", "plan-y"},
						InputKnitId:  []string{"in1", "in2"},
						OutputKnitId: []string{"out3", "out4"},
						Status:       []domain.KnitRunStatus{domain.Waiting, domain.Running, domain.Done},
						UpdatedSince: &dummyUpdatedSince,
					},
					body: []apiruns.Detail{},
				},
			},
		} {
			t.Run(name, func(t *testing.T) {

				mockRun := mockdb.NewRunInterface()

				mockRun.Impl.Find = func(ctx context.Context, q domain.RunFindQuery) ([]string, error) {
					runIds := utils.Map(testcase.when.Runs, func(r domain.Run) string { return r.Id })
					return runIds, nil
				}
				mockRun.Impl.Get = func(ctx context.Context, runId []string) (map[string]domain.Run, error) {
					runs := utils.ToMap(testcase.when.Runs, func(r domain.Run) string { return r.Id })
					return runs, nil
				}

				e := echo.New()
				c, respRec := httptestutil.Get(e, testcase.when.request)

				testee := handlers.FindRunHandler(mockRun)

				if err := testee(c); err != nil {
					t.Fatalf("response is not illegal. error = %v", err)
				}

				if !cmp.SliceEqWith(
					mockRun.Calls.Find, []domain.RunFindQuery{testcase.query},
					domain.RunFindQuery.Equal,
				) {
					t.Errorf(
						"unmatch: params for RunInterface.Find:\n- actual:\n%+v\n- expected:\n%+v",
						mockRun.Calls.Find, []domain.RunFindQuery{testcase.query},
					)
				}

				if !cmp.SliceEqWith(
					mockRun.Calls.Get,
					[][]string{utils.Map(testcase.when.Runs, func(r domain.Run) string { return r.Id })},
					cmp.SliceContentEq[string],
				) {
					t.Errorf(
						"unmatch: params for RunInterface.Get\n- actual:\n%+v\n\n- expected:%+v",
						mockRun.Calls.Get,
						[][]string{utils.Map(testcase.when.Runs, func(r domain.Run) string { return r.Id })},
					)
				}

				{
					expected := 200
					actual := respRec.Result().StatusCode
					if actual != expected {
						t.Errorf("status code %d != %d", actual, expected)
					}
				}

				{
					expected := "application/json"
					actual := strings.Split(respRec.Result().Header.Get("Content-Type"), ";")[0]
					if actual != expected {
						t.Errorf("Content-Type: %s != %s", actual, expected)
					}
				}

				{
					actual := []apiruns.Detail{}
					body := respRec.Body.String()
					if err := json.Unmarshal([]byte(body), &actual); err != nil {
						t.Fatalf("response is not json: error = %v:\n===body===\n%s", err, body)
					}
					if !cmp.SliceEqWith(actual, testcase.then.body, apiruns.Detail.Equal) {
						t.Errorf(
							"data does not match. (actual, expected) = \n(%+v, \n%+v)",
							actual, testcase.then.body,
						)
					}
				}
			})
		}
	})

	t.Run("it returns error response", func(t *testing.T) {

		type when struct {
			request     string
			errorOnFind error
			errorOnGet  error
		}

		type then struct {
			statusCode int
		}

		for name, testcase := range map[string]struct {
			when
			then
		}{
			"(Internal Server Error) when RunInterface.Find cause error": {
				when{
					request:     "/api/runs?",
					errorOnFind: errors.New("dummy error"),
				},
				then{
					statusCode: http.StatusInternalServerError,
				},
			},
			"(Internal Server Error) when RunInterface.Get cause error": {
				when{
					request:    "/api/runs?",
					errorOnGet: errors.New("dummy error"),
				},
				then{
					statusCode: http.StatusInternalServerError,
				},
			},
			"(Bad Request) when statuses in query is unknwon value": {
				when{
					request: "/api/runs?status=unknown,running",
				},
				then{
					statusCode: http.StatusBadRequest,
				},
			},
			"(Bad Request) when statuses in query is invalidated": {
				when{
					request: "/api/runs?status=" + strings.ToLower(string(domain.Invalidated)), // this is known value, but...
				},
				then{
					statusCode: http.StatusBadRequest,
				},
			},
		} {
			t.Run(name, func(t *testing.T) {

				mockRun := mockdb.NewRunInterface()

				mockRun.Impl.Find = func(ctx context.Context, q domain.RunFindQuery) ([]string, error) {
					return nil, testcase.when.errorOnFind
				}
				mockRun.Impl.Get = func(ctx context.Context, runId []string) (map[string]domain.Run, error) {
					return nil, testcase.when.errorOnGet
				}

				e := echo.New()
				c, respRec := httptestutil.Get(e, testcase.when.request)

				testee := handlers.FindRunHandler(mockRun)

				err := testee(c)
				if err == nil {
					t.Fatalf("no error but it is not expected result")
				}

				var echoErr *echo.HTTPError
				if !errors.As(err, &echoErr) {
					t.Fatalf("error is not echo.HTTPError. acutal = %#v", err)
				}
				if echoErr.Code != testcase.then.statusCode {
					t.Fatalf("unmatch error code:%d, expeced:%d", echoErr.Code, testcase.then.statusCode)
				}

				{
					expected := "application/json"
					actual := strings.Split(respRec.Result().Header.Get("Content-Type"), ";")[0]
					if actual != expected {
						t.Errorf("Content-Type: %s != %s", actual, expected)
					}
				}
			})
		}
	})
}

func TestGetRunHandler(t *testing.T) {

	t.Run("it responses OK with runs in json, when no errors have caused: ", func(t *testing.T) {
		for runId, testcase := range map[string]struct {
			when domain.Run
			then apiruns.Detail
		}{
			"run-1/input-only": {
				when: domain.Run{
					RunBody: domain.RunBody{
						Id:     "run-1/input-only",
						Status: domain.Done,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-08T01:10:25.111+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "plan-1", Active: true,
							Image: &domain.ImageIdentifier{Image: "repo.invalid/image", Version: "v1.1"},
						},
					},
					Inputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "run-1@/in/1", VolumeRef: "pvc-run-1@/in/1",
							},
							MountPoint: domain.MountPoint{
								Id: 1100, Path: "/in/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-1#in/1"},
								}),
							},
						},
					},
				},
				then: apiruns.Detail{
					Summary: apiruns.Summary{
						RunId:  "run-1/input-only",
						Status: string(domain.Done),
						Plan: apiplans.Summary{
							PlanId: "plan-1",
							Image:  &apiplans.Image{Repository: "repo.invalid/image", Tag: "v1.1"},
						},
						UpdatedAt: try.To(
							rfctime.ParseRFC3339DateTime("2022-11-08T01:10:25.111+09:00"),
						).OrFatal(t),
					},
					Inputs: []apiruns.Assignment{
						{
							KnitId: "run-1@/in/1",
							Mountpoint: apiplans.Mountpoint{
								Path: "/in/1",
								Tags: []apitags.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-1#in/1"},
								},
							},
						},
					},
				},
			},
			"run-2/output-only": {
				when: domain.Run{
					RunBody: domain.RunBody{
						Id: "run-2/output-only", Status: domain.Running,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-08T01:10:26.111+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "plan-2", Active: true,
							Pseudo: &domain.PseudoPlanDetail{Name: "pseudo-2"},
						},
					},
					Outputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "run-2@/out/1", VolumeRef: "pvc-run-2@/out/1",
							},
							MountPoint: domain.MountPoint{
								Id: 2010, Path: "/out/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for pseudo-2#/out/1"},
								}),
							},
						},
					},
				},
				then: apiruns.Detail{
					Summary: apiruns.Summary{
						RunId: "run-2/output-only", Status: string(domain.Running),
						Plan: apiplans.Summary{PlanId: "plan-2", Name: "pseudo-2"},
						UpdatedAt: try.To(
							rfctime.ParseRFC3339DateTime("2022-11-08T01:10:26.111+09:00"),
						).OrFatal(t),
					},
					Outputs: []apiruns.Assignment{
						{
							KnitId: "run-2@/out/1",
							Mountpoint: apiplans.Mountpoint{
								Path: "/out/1",
								Tags: []apitags.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for pseudo-2#/out/1"},
								},
							},
						},
					},
				},
			},
			"run-3/in+out": {
				when: domain.Run{
					RunBody: domain.RunBody{
						Id: "run-3/in+out", Status: domain.Failed,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-08T01:10:27.111+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "plan-3", Active: true,
							Image: &domain.ImageIdentifier{Image: "repo.invalid/image-x", Version: "v0.0"},
						},
					},
					Inputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "run-3@/in/1", VolumeRef: "pvc-run-3@/in/1",
							},
							MountPoint: domain.MountPoint{
								Id: 3100, Path: "/in/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-3#/in/1"},
								}),
							},
						},
					},
					Outputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "run-3@/out/1", VolumeRef: "pvc-run-3@/out/1",
							},
							MountPoint: domain.MountPoint{
								Id: 3010, Path: "/out/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-3#/out/1"},
								}),
							},
						},
					},
				},
				then: apiruns.Detail{
					Summary: apiruns.Summary{
						RunId:  "run-3/in+out",
						Status: string(domain.Failed),
						UpdatedAt: try.To(
							rfctime.ParseRFC3339DateTime("2022-11-08T01:10:27.111+09:00"),
						).OrFatal(t),
						Plan: apiplans.Summary{
							PlanId: "plan-3",
							Image:  &apiplans.Image{Repository: "repo.invalid/image-x", Tag: "v0.0"},
						},
					},
					Inputs: []apiruns.Assignment{
						{
							KnitId: "run-3@/in/1",
							Mountpoint: apiplans.Mountpoint{
								Path: "/in/1",
								Tags: []apitags.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-3#/in/1"},
								},
							},
						},
					},
					Outputs: []apiruns.Assignment{
						{
							KnitId: "run-3@/out/1",
							Mountpoint: apiplans.Mountpoint{
								Path: "/out/1",
								Tags: []apitags.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-3#/out/1"},
								},
							},
						},
					},
				},
			},
			"run-4/in+out+log": {
				when: domain.Run{
					RunBody: domain.RunBody{
						Id: "run-4/in+out+log", Status: domain.Failed,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-08T01:10:28.111+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "plan-4", Active: true,
							Image: &domain.ImageIdentifier{Image: "repo.invalid/image-x", Version: "v4.0"},
						},
					},
					Inputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "run-4@/in/1", VolumeRef: "ref-run-4@/in/1",
							},
							MountPoint: domain.MountPoint{
								Id: 4100, Path: "/in/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-4#/in/1"},
								}),
							},
						},
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "run-4@/in/2", VolumeRef: "ref-run-4@/in/2",
							},
							MountPoint: domain.MountPoint{
								Id: 4200, Path: "/in/2",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-4#/in/2"},
								}),
							},
						},
					},
					Outputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "run-3@/out/1", VolumeRef: "ref-run-3@/out/1",
							},
							MountPoint: domain.MountPoint{
								Id: 4010, Path: "/out/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-4#/out/1"},
								}),
							},
						},
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "run-3@/out/2", VolumeRef: "ref-run-3@/out/2",
							},
							MountPoint: domain.MountPoint{
								Id: 4020, Path: "/out/2",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-4#/out/2"},
								}),
							},
						},
					},
					Log: &domain.Log{
						Id: 4001,
						KnitDataBody: domain.KnitDataBody{
							KnitId: "run-3@/log", VolumeRef: "ref-run-3@/log",
						},
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "shared", Value: "val1"},
							{Key: "special", Value: "for plan-4#/log"},
						}),
					},
				},
				then: apiruns.Detail{
					Summary: apiruns.Summary{
						RunId:  "run-4/in+out+log",
						Status: string(domain.Failed),
						UpdatedAt: try.To(
							rfctime.ParseRFC3339DateTime("2022-11-08T01:10:28.111+09:00"),
						).OrFatal(t),
						Plan: apiplans.Summary{
							PlanId: "plan-4",
							Image:  &apiplans.Image{Repository: "repo.invalid/image-x", Tag: "v4.0"},
						},
					},
					Inputs: []apiruns.Assignment{
						{
							KnitId: "run-4@/in/1",
							Mountpoint: apiplans.Mountpoint{
								Path: "/in/1",
								Tags: []apitags.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-4#/in/1"},
								},
							},
						},
						{
							KnitId: "run-4@/in/2",
							Mountpoint: apiplans.Mountpoint{
								Path: "/in/2",
								Tags: []apitags.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-4#/in/2"},
								},
							},
						},
					},
					Outputs: []apiruns.Assignment{
						{
							KnitId: "run-3@/out/1",
							Mountpoint: apiplans.Mountpoint{
								Path: "/out/1",
								Tags: []apitags.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-4#/out/1"},
								},
							},
						},
						{
							KnitId: "run-3@/out/2",
							Mountpoint: apiplans.Mountpoint{
								Path: "/out/2",
								Tags: []apitags.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-4#/out/2"},
								},
							},
						},
					},
					Log: &apiruns.LogSummary{
						KnitId: "run-3@/log",
						LogPoint: apiplans.LogPoint{
							Tags: []apitags.Tag{
								{Key: "shared", Value: "val1"},
								{Key: "special", Value: "for plan-4#/log"},
							},
						},
					},
				},
			},
			"run-5/waiting": {
				when: domain.Run{
					RunBody: domain.RunBody{
						Id: "run-5/waiting", Status: domain.Waiting,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-08T01:10:28.111+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "plan-5", Active: true,
							Image: &domain.ImageIdentifier{Image: "repo.invalid/image-x", Version: "v5.0"},
						},
					},
					Inputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "run-4@/in/1", VolumeRef: "ref-run-4@/in/1",
							},
							MountPoint: domain.MountPoint{
								Id: 4100, Path: "/in/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-4#/in/1"},
								}),
							},
						},
					},
					Outputs: []domain.Assignment{
						{
							// KnitId: not set = will be assigned. but not now
							MountPoint: domain.MountPoint{
								Id: 4020, Path: "/out/2",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-4#/out/2"},
								}),
							},
						},
					},
					Log: &domain.Log{
						// KnitId: not set = will be assigned. but not now
						Id: 4001,
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "shared", Value: "val1"},
							{Key: "special", Value: "for plan-4#/log"},
						}),
					},
				},
				then: apiruns.Detail{
					Summary: apiruns.Summary{
						RunId:  "run-5/waiting",
						Status: string(domain.Waiting),
						UpdatedAt: try.To(
							rfctime.ParseRFC3339DateTime("2022-11-08T01:10:28.111+09:00"),
						).OrFatal(t),
						Plan: apiplans.Summary{
							PlanId: "plan-5",
							Image:  &apiplans.Image{Repository: "repo.invalid/image-x", Tag: "v5.0"},
						},
					},
					Inputs: []apiruns.Assignment{
						{
							KnitId: "run-4@/in/1",
							Mountpoint: apiplans.Mountpoint{
								Path: "/in/1",
								Tags: []apitags.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-4#/in/1"},
								},
							},
						},
					},
					Outputs: []apiruns.Assignment{
						{
							// no data assigned.
							Mountpoint: apiplans.Mountpoint{
								Path: "/out/2",
								Tags: []apitags.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-4#/out/2"},
								},
							},
						},
					},
					Log: &apiruns.LogSummary{
						LogPoint: apiplans.LogPoint{
							Tags: []apitags.Tag{
								{Key: "shared", Value: "val1"},
								{Key: "special", Value: "for plan-4#/log"},
							},
						},
					},
				},
			},
		} {
			t.Run(runId, func(t *testing.T) {
				mockRun := mockdb.NewRunInterface()
				mockRun.Impl.Get = func(context.Context, []string) (map[string]domain.Run, error) {
					return map[string]domain.Run{runId: testcase.when}, nil
				}

				e := echo.New()
				c, respRec := httptestutil.Get(e, "/api/runs/"+runId)
				c.SetPath("/runs/:runId")
				c.SetParamNames("runId")
				c.SetParamValues(runId)

				testee := handlers.GetRunHandler(mockRun)

				if err := testee(c); err != nil {
					t.Fatal(err)
				}

				{
					actual := mockRun.Calls.Get
					expected := [][]string{{runId}}
					if !cmp.SliceEqWith(actual, expected, cmp.SliceContentEq[string]) {
						t.Errorf(
							"unmatch: query for RunInterface.Get: (actual, expected) = (%+v, %+v)",
							actual, expected,
						)
					}
				}

				{
					actual := respRec.Result().StatusCode
					expected := 200
					if actual != expected {
						t.Fatalf("unmatch: status code: %d != %d", actual, expected)
					}
				}

				{
					actual := strings.Split(respRec.Result().Header.Get("Content-Type"), ";")[0]
					expected := "application/json"
					if actual != expected {
						t.Fatalf("unmatch: Content-Type header: %s != %s", actual, expected)
					}
				}

				{
					actual := apiruns.Detail{}
					if err := json.Unmarshal(respRec.Body.Bytes(), &actual); err != nil {
						t.Fatalf("response is not illegal. error = %v", err)
					}

					if !actual.Equal(testcase.then) {
						t.Fatalf(
							"unmatch: payload: (actual, expected) = \n(%+v, \n%+v)",
							actual, testcase.then,
						)
					}
				}
			})
		}
	})

	t.Run("it responses error ", func(t *testing.T) {
		type when struct {
			runId     string
			returnGet map[string]domain.Run
			errorGet  error
		}

		type then struct {
			statusCode int
		}

		for name, testcase := range map[string]struct {
			when
			then
		}{
			"Not Found: when found runs do not have specified run id": {
				when{
					runId:     "run-1",
					returnGet: map[string]domain.Run{}, // empty does not have any runId.
				},
				then{
					statusCode: http.StatusNotFound,
				},
			},
			"Internal Server Error: when RunInterface.Get causes error": {
				when{
					runId:    "run-1",
					errorGet: errors.New("fake error"),
				},
				then{
					statusCode: http.StatusInternalServerError,
				},
			},
		} {
			t.Run(name, func(t *testing.T) {
				mockRun := mockdb.NewRunInterface()
				mockRun.Impl.Get = func(context.Context, []string) (map[string]domain.Run, error) {
					return testcase.when.returnGet, testcase.when.errorGet
				}

				e := echo.New()
				c, _ := httptestutil.Get(e, "/api/runs/"+testcase.when.runId)
				c.SetPath("/runs/:runId")
				c.SetParamNames("runId")
				c.SetParamValues(testcase.when.runId)

				testee := handlers.GetRunHandler(mockRun)

				err := testee(c)
				if httperr := new(echo.HTTPError); !errors.As(err, &httperr) {
					t.Fatalf("unmatch: error type: %+v is not echo.HTTPError", err)
				} else {
					actual := httperr.Code
					expected := testcase.then.statusCode
					if actual != expected {
						t.Fatalf("unmatch: status code: %d != %d", actual, expected)
					}
				}

				{
					actual := mockRun.Calls.Get
					expected := [][]string{{testcase.when.runId}}
					if !cmp.SliceEqWith(actual, expected, cmp.SliceContentEq[string]) {
						t.Errorf(
							"unmatch: query for RunInterface.Get: (actual, expected) = (%+v, %+v)",
							actual, expected,
						)
					}
				}
			})
		}
	})
}

func TestAbortRun(t *testing.T) {
	type When struct {
		RunId           string
		SetStatusResult error
		SetExitResult   error
		GetResult       map[string]domain.Run
		GetError        error
	}

	type Then struct {
		StatusCode int
		Response   apiruns.Detail
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			mockRun := mockdb.NewRunInterface()

			mockRun.Impl.SetStatus = func(ctx context.Context, runId string, status domain.KnitRunStatus) error {
				if when.RunId != runId {
					t.Errorf(
						"Get called with unexpected runId: %s (want: %s)",
						runId, when.RunId,
					)
				}
				if status != domain.Aborting {
					t.Errorf(
						"SetStatus called with unexpected status: %s (want: %s)",
						status, domain.Aborting,
					)
				}
				return when.SetStatusResult
			}

			mockRun.Impl.SetExit = func(ctx context.Context, runId string, exit domain.RunExit) error {
				if when.RunId != runId {
					t.Errorf(
						"Get called with unexpected runId: %s (want: %s)",
						runId, when.RunId,
					)
				}
				want := domain.RunExit{Code: 253, Message: "aborted by user"}
				if exit != want {
					t.Errorf(
						"SetExit called with unexpected exit: %+v (want: %+v)",
						exit, want,
					)
				}
				return when.SetExitResult
			}

			mockRun.Impl.Get = func(ctx context.Context, runId []string) (map[string]domain.Run, error) {
				if !cmp.SliceContentEq([]string{when.RunId}, runId) {
					t.Errorf(
						"Get called with unexpected runId: %s (want: %s)",
						runId, when.RunId,
					)
				}
				return when.GetResult, when.GetError
			}

			e := echo.New()
			c, respRec := httptestutil.Put(
				e, fmt.Sprintf("/api/runs/%s/abort", when.RunId), nil,
			)
			c.SetParamNames("runId")
			c.SetParamValues(when.RunId)

			testee := handlers.AbortRunHandler(mockRun, "runId")

			err := testee(c)

			if err != nil {
				if 200 <= then.StatusCode && then.StatusCode < 300 {
					t.Fatalf("error is not expected. error = %v", err)
				}

				if herr := new(echo.HTTPError); !errors.As(err, &herr) {
					t.Fatalf("unmatch: error type: %+v is not echo.HTTPError", err)
				} else {
					actual := herr.Code
					expected := then.StatusCode
					if actual != expected {
						t.Fatalf("unmatch: status code: %d != %d", actual, expected)
					}
				}

				return
			}

			if respRec.Code != http.StatusOK {
				t.Errorf("unmatch: status code: %d (want: %d)", respRec.Code, http.StatusOK)
			}

			if respRec.Result().Header.Get("Content-Type") != "application/json" {
				t.Errorf(
					"unmatch: Content-Type: %s (want: %s)",
					respRec.Result().Header.Get("Content-Type"), "application/json",
				)
			}

			actual := new(apiruns.Detail)
			if err := json.Unmarshal(respRec.Body.Bytes(), actual); err != nil {
				t.Fatalf(
					"response is not json: error = %v:\n===body===\n%s",
					err, respRec.Body.String(),
				)
			}

			if !then.Response.Equal(*actual) {
				t.Errorf(
					"unmatch: response:\n===actual===\n%+v\n===expected===\n%+v",
					actual, then.Response,
				)
			}
		}
	}

	t.Run("it responses OK with runs in json, when no errors have caused", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: nil,
			GetResult: map[string]domain.Run{
				"run-1": {
					RunBody: domain.RunBody{
						Id: "run-1", Status: domain.Aborting,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-08T01:10:25.111+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "plan-1", Active: true,
							Image: &domain.ImageIdentifier{Image: "repo.invalid/image", Version: "v1.1"},
						},
					},
					Inputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "run-1@/in/1", VolumeRef: "pvc-run-1@/in/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: domain.KeyKnitId, Value: "run-1@/in/1"},
									{Key: "shared", Value: "val1"},
								}),
							},
							MountPoint: domain.MountPoint{
								Id: 1100, Path: "/in/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-1#in/1"},
								}),
							},
						},
					},
					Outputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "run-1@/out/1", VolumeRef: "pvc-run-1@/out/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: domain.KeyKnitId, Value: "run-1@/out/1"},
									{Key: "shared", Value: "val1"},
								}),
							},
							MountPoint: domain.MountPoint{
								Id: 1010, Path: "/out/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-1#out/1"},
								}),
							},
						},
					},
					Log: &domain.Log{
						Id: 1001,
						KnitDataBody: domain.KnitDataBody{
							KnitId: "run-1@/log", VolumeRef: "pvc-run-1@/log",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: domain.KeyKnitId, Value: "run-1@/log"},
								{Key: "shared", Value: "val1"},
							}),
						},
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "shared", Value: "val1"},
							{Key: "special", Value: "for plan-1#log"},
						}),
					},
				},
			},
		},
		Then{
			StatusCode: http.StatusOK,
			Response: apiruns.Detail{
				Summary: apiruns.Summary{
					RunId:  "run-1",
					Status: string(domain.Aborting),
					Plan: apiplans.Summary{
						PlanId: "plan-1",
						Image:  &apiplans.Image{Repository: "repo.invalid/image", Tag: "v1.1"},
					},
					UpdatedAt: try.To(
						rfctime.ParseRFC3339DateTime("2022-11-08T01:10:25.111+09:00"),
					).OrFatal(t),
				},
				Inputs: []apiruns.Assignment{
					{
						KnitId: "run-1@/in/1",
						Mountpoint: apiplans.Mountpoint{
							Path: "/in/1",
							Tags: []apitags.Tag{
								{Key: "shared", Value: "val1"},
								{Key: "special", Value: "for plan-1#in/1"},
							},
						},
					},
				},
				Outputs: []apiruns.Assignment{
					{
						KnitId: "run-1@/out/1",
						Mountpoint: apiplans.Mountpoint{
							Path: "/out/1",
							Tags: []apitags.Tag{
								{Key: "shared", Value: "val1"},
								{Key: "special", Value: "for plan-1#out/1"},
							},
						},
					},
				},
				Log: &apiruns.LogSummary{
					KnitId: "run-1@/log",
					LogPoint: apiplans.LogPoint{
						Tags: []apitags.Tag{
							{Key: "shared", Value: "val1"},
							{Key: "special", Value: "for plan-1#log"},
						},
					},
				},
			},
		},
	))

	t.Run("it responses error (InternalServerError), when RunInterface.SetStatus causes error", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: errors.New("fake error"),
			GetResult:       nil,
			GetError:        nil,
		},
		Then{
			StatusCode: http.StatusInternalServerError,
		},
	))

	t.Run("it responses error (InternalServerError), when RunInterface.SetExit causes error", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: nil,
			SetExitResult:   errors.New("fake error"),
			GetResult:       nil,
			GetError:        nil,
		},
		Then{
			StatusCode: http.StatusInternalServerError,
		},
	))

	t.Run("it responses error (InternalServerError), when RunInterface.Get causes error", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: nil,
			GetResult:       nil,
			GetError:        errors.New("fake error"),
		},
		Then{
			StatusCode: http.StatusInternalServerError,
		},
	))

	t.Run("it responses error (NotFound), when RunInterface.SetStatus returns ErrMissing", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: kerr.ErrMissing,
			GetResult:       nil,
			GetError:        nil,
		},
		Then{
			StatusCode: http.StatusNotFound,
		},
	))

	t.Run("it responses error (NotFound), when RunInterface.Get returns empty", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: nil,
			GetResult:       map[string]domain.Run{},
			GetError:        nil,
		},
		Then{
			StatusCode: http.StatusNotFound,
		},
	))

	t.Run("it responses error (Conflict), when RunInterface.SetStatus returns ErrInvalidRunStateChanging", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: domain.ErrInvalidRunStateChanging,
			GetResult:       map[string]domain.Run{},
			GetError:        nil,
		},
		Then{
			StatusCode: http.StatusConflict,
		},
	))

}

func TestTearoffRun(t *testing.T) {
	type When struct {
		RunId           string
		SetStatusResult error
		SetExitResult   error
		GetResult       map[string]domain.Run
		GetError        error
	}

	type Then struct {
		StatusCode int
		Response   apiruns.Detail
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			mockRun := mockdb.NewRunInterface()

			mockRun.Impl.SetStatus = func(ctx context.Context, runId string, status domain.KnitRunStatus) error {
				if when.RunId != runId {
					t.Errorf(
						"Get called with unexpected runId: %s (want: %s)",
						runId, when.RunId,
					)
				}
				if status != domain.Completing {
					t.Errorf(
						"SetStatus called with unexpected status: %s (want: %s)",
						status, domain.Completing,
					)
				}
				return when.SetStatusResult
			}
			mockRun.Impl.SetExit = func(ctx context.Context, runId string, exit domain.RunExit) error {
				if when.RunId != runId {
					t.Errorf(
						"Get called with unexpected runId: %s (want: %s)",
						runId, when.RunId,
					)
				}
				want := domain.RunExit{Code: 0, Message: "stopped by user"}
				if exit != want {
					t.Errorf(
						"SetExit called with unexpected exit: %+v (want: %+v)",
						exit, want,
					)
				}
				return when.SetExitResult
			}

			mockRun.Impl.Get = func(ctx context.Context, runId []string) (map[string]domain.Run, error) {
				if !cmp.SliceContentEq([]string{when.RunId}, runId) {
					t.Errorf(
						"Get called with unexpected runId: %s (want: %s)",
						runId, when.RunId,
					)
				}
				return when.GetResult, when.GetError
			}

			e := echo.New()
			c, respRec := httptestutil.Put(
				e, fmt.Sprintf("/api/runs/%s/tearoff", when.RunId), nil,
			)
			c.SetParamNames("runId")
			c.SetParamValues(when.RunId)

			testee := handlers.TearoffRunHandler(mockRun, "runId")

			err := testee(c)

			if err != nil {
				if 200 <= then.StatusCode && then.StatusCode < 300 {
					t.Fatalf("error is not expected. error = %v", err)
				}

				if herr := new(echo.HTTPError); !errors.As(err, &herr) {
					t.Fatalf("unmatch: error type: %+v is not echo.HTTPError", err)
				} else {
					actual := herr.Code
					expected := then.StatusCode
					if actual != expected {
						t.Fatalf("unmatch: status code: %d != %d", actual, expected)
					}
				}

				return
			}

			if respRec.Code != http.StatusOK {
				t.Errorf("unmatch: status code: %d (want: %d)", respRec.Code, http.StatusOK)
			}

			if respRec.Result().Header.Get("Content-Type") != "application/json" {
				t.Errorf(
					"unmatch: Content-Type: %s (want: %s)",
					respRec.Result().Header.Get("Content-Type"), "application/json",
				)
			}

			actual := new(apiruns.Detail)
			if err := json.Unmarshal(respRec.Body.Bytes(), actual); err != nil {
				t.Fatalf(
					"response is not json: error = %v:\n===body===\n%s",
					err, respRec.Body.String(),
				)
			}

			if !then.Response.Equal(*actual) {
				t.Errorf(
					"unmatch: response:\n===actual===\n%+v\n===expected===\n%+v",
					actual, then.Response,
				)
			}
		}
	}

	t.Run("it responses OK with runs in json, when no errors have caused", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: nil,
			GetResult: map[string]domain.Run{
				"run-1": {
					RunBody: domain.RunBody{
						Id: "run-1", Status: domain.Aborting,
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-08T01:10:25.111+09:00",
						)).OrFatal(t).Time(),
						PlanBody: domain.PlanBody{
							PlanId: "plan-1", Active: true,
							Image: &domain.ImageIdentifier{Image: "repo.invalid/image", Version: "v1.1"},
						},
					},
					Inputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "run-1@/in/1", VolumeRef: "pvc-run-1@/in/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: domain.KeyKnitId, Value: "run-1@/in/1"},
									{Key: "shared", Value: "val1"},
								}),
							},
							MountPoint: domain.MountPoint{
								Id: 1100, Path: "/in/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-1#in/1"},
								}),
							},
						},
					},
					Outputs: []domain.Assignment{
						{
							KnitDataBody: domain.KnitDataBody{
								KnitId: "run-1@/out/1", VolumeRef: "pvc-run-1@/out/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: domain.KeyKnitId, Value: "run-1@/out/1"},
									{Key: "shared", Value: "val1"},
								}),
							},
							MountPoint: domain.MountPoint{
								Id: 1010, Path: "/out/1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "shared", Value: "val1"},
									{Key: "special", Value: "for plan-1#out/1"},
								}),
							},
						},
					},
					Log: &domain.Log{
						Id: 1001,
						KnitDataBody: domain.KnitDataBody{
							KnitId: "run-1@/log", VolumeRef: "pvc-run-1@/log",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: domain.KeyKnitId, Value: "run-1@/log"},
								{Key: "shared", Value: "val1"},
							}),
						},
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "shared", Value: "val1"},
							{Key: "special", Value: "for plan-1#log"},
						}),
					},
				},
			},
		},
		Then{
			StatusCode: http.StatusOK,
			Response: apiruns.Detail{
				Summary: apiruns.Summary{
					RunId:  "run-1",
					Status: string(domain.Aborting),
					Plan: apiplans.Summary{
						PlanId: "plan-1",
						Image:  &apiplans.Image{Repository: "repo.invalid/image", Tag: "v1.1"},
					},
					UpdatedAt: try.To(
						rfctime.ParseRFC3339DateTime("2022-11-08T01:10:25.111+09:00"),
					).OrFatal(t),
				},
				Inputs: []apiruns.Assignment{
					{
						KnitId: "run-1@/in/1",
						Mountpoint: apiplans.Mountpoint{
							Path: "/in/1",
							Tags: []apitags.Tag{
								{Key: "shared", Value: "val1"},
								{Key: "special", Value: "for plan-1#in/1"},
							},
						},
					},
				},
				Outputs: []apiruns.Assignment{
					{
						KnitId: "run-1@/out/1",
						Mountpoint: apiplans.Mountpoint{
							Path: "/out/1",
							Tags: []apitags.Tag{
								{Key: "shared", Value: "val1"},
								{Key: "special", Value: "for plan-1#out/1"},
							},
						},
					},
				},
				Log: &apiruns.LogSummary{
					KnitId: "run-1@/log",
					LogPoint: apiplans.LogPoint{
						Tags: []apitags.Tag{
							{Key: "shared", Value: "val1"},
							{Key: "special", Value: "for plan-1#log"},
						},
					},
				},
			},
		},
	))

	t.Run("it responses error (InternalServerError), when RunInterface.SetStatus causes error", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: errors.New("fake error"),
			GetResult:       nil,
			GetError:        nil,
		},
		Then{
			StatusCode: http.StatusInternalServerError,
		},
	))

	t.Run("it responses error (InternalServerError), when RunInterface.SetExit causes error", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: nil,
			SetExitResult:   errors.New("fake error"),
			GetResult:       nil,
			GetError:        nil,
		},
		Then{
			StatusCode: http.StatusInternalServerError,
		},
	))

	t.Run("it responses error (InternalServerError), when RunInterface.Get causes error", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: nil,
			GetResult:       nil,
			GetError:        errors.New("fake error"),
		},
		Then{
			StatusCode: http.StatusInternalServerError,
		},
	))

	t.Run("it responses error (NotFound), when RunInterface.SetStatus returns ErrMissing", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: kerr.ErrMissing,
			GetResult:       nil,
			GetError:        nil,
		},
		Then{
			StatusCode: http.StatusNotFound,
		},
	))

	t.Run("it responses error (NotFound), when RunInterface.Get returns empty", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: nil,
			GetResult:       map[string]domain.Run{},
			GetError:        nil,
		},
		Then{
			StatusCode: http.StatusNotFound,
		},
	))

	t.Run("it responses error (Conflict), when RunInterface.SetStatus returns ErrInvalidRunStateChanging", theory(
		When{
			RunId:           "run-1",
			SetStatusResult: domain.ErrInvalidRunStateChanging,
			GetResult:       map[string]domain.Run{},
			GetError:        nil,
		},
		Then{
			StatusCode: http.StatusConflict,
		},
	))

}

func TestDeleteRun(t *testing.T) {

	type when struct {
		QueryResult error
	}

	type then struct {
		isErr       bool
		contentType string
		statusCode  int
	}

	for name, testcase := range map[string]struct {
		when
		then
	}{
		"When RunInterface.Delete returns nil(success delete), it should return http.StatusNoContent": {
			when: when{
				QueryResult: nil,
			},
			then: then{
				isErr:       false,
				contentType: "application/json",
				statusCode:  http.StatusNoContent,
			},
		},
		"When RunInterface.Delete returns RunIdNotFound Err, it should return http.StatusNotFound": {
			when: when{
				QueryResult: kerr.ErrMissing,
			},
			then: then{
				isErr:       true,
				contentType: "application/json",
				statusCode:  http.StatusNotFound,
			},
		},
		"When RunInterface.Delete returns ErrRunIdProtected Err, it should return http.StatusConflict": {
			when: when{
				QueryResult: domain.ErrRunIsProtected,
			},
			then: then{
				isErr:       true,
				contentType: "application/json",
				statusCode:  http.StatusConflict,
			},
		},
		"When RunInterface.Delete returns unexpected error, it should return http.StatusInternalServerError": {
			when: when{
				QueryResult: errors.New("dummy SQL Err"),
			},
			then: then{
				isErr:       true,
				contentType: "application/json",
				statusCode:  http.StatusInternalServerError,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {

			mockRun := mockdb.NewRunInterface()

			mockRun.Impl.Delete = func(ctx context.Context, runId string) error {
				return testcase.when.QueryResult
			}

			e := echo.New()
			c, respRec := httptestutil.Delete(e, "/api/runs/run-dummy")

			testee := handlers.DeleteRunHandler(mockRun)

			err := testee(c)

			actualContentType := strings.Split(respRec.Result().Header.Get("Content-Type"), ";")[0]
			if actualContentType != testcase.then.contentType {
				t.Errorf("Content-Type: %s != %s", actualContentType, testcase.then.contentType)
			}

			if testcase.then.isErr {
				if err == nil {
					t.Fatalf("error is nothing")
				}
				var echoErr *echo.HTTPError
				if !errors.As(err, &echoErr) {
					t.Fatalf("error is not echo.HTTPError. acutal = %#v", err)
				}
				if echoErr.Code != testcase.then.statusCode {
					t.Fatalf("unmatch error code:%d, expeced:%d", echoErr.Code, testcase.then.statusCode)
				}
			} else {
				if err != nil {
					t.Fatalf("response is not illegal. error = %v", err)
				}

				actualStatusCode := respRec.Result().StatusCode
				if actualStatusCode != testcase.then.statusCode {
					t.Errorf("status code %d != %d", actualStatusCode, testcase.then.statusCode)
				}
			}
		})
	}

	t.Run("When handler receives query parameter, it passed it to RunInterface.Delete.", func(t *testing.T) {

		mockRun := mockdb.NewRunInterface()
		mockRun.Impl.Delete = func(ctx context.Context, runId string) error {
			return nil
		}

		e := echo.New()
		c, _ := httptestutil.Delete(
			e, "/api/runs/run-dummy",
		)

		c.SetParamNames("runId")
		runId := "run-1"
		c.SetParamValues(runId)

		testee := handlers.DeleteRunHandler(mockRun)

		testee(c)

		if mockRun.Calls.Delete.Times() != 1 {
			t.Fatalf("Delete did not call correctly")
		}
		if !(mockRun.Calls.Delete[0] == runId) {
			t.Fatalf("Delete did not call with correct args. (actual, expected) = \n(%#v, \n%#v)",
				mockRun.Calls.Delete[0], runId)
		}
	})
}

func TestRetryRun(t *testing.T) {
	type When struct {
		RunId    string
		ErrRetry error
	}
	type Then struct {
		StatusCode int
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			mockRun := mockdb.NewRunInterface()

			mockRun.Impl.Retry = func(ctx context.Context, runId string) error {
				if when.RunId != runId {
					t.Errorf(
						"Retry called with unexpected runId: %s (want: %s)",
						runId, when.RunId,
					)
				}
				return when.ErrRetry
			}

			e := echo.New()
			c, respRec := httptestutil.Put(
				e, fmt.Sprintf("/api/runs/%s/retry", when.RunId), nil,
			)
			c.SetParamNames("runId")
			c.SetParamValues(when.RunId)

			testee := handlers.RetryRunHandler(mockRun, "runId")

			err := testee(c)

			if err != nil {
				if 200 <= then.StatusCode && then.StatusCode < 300 {
					t.Fatalf("error is not expected. error = %v", err)
				}

				if herr := new(echo.HTTPError); !errors.As(err, &herr) {
					t.Fatalf("unmatch: error type: %+v is not echo.HTTPError", err)
				} else {
					actual := herr.Code
					expected := then.StatusCode
					if actual != expected {
						t.Fatalf("unmatch: status code: %d != %d", actual, expected)
					}
				}

				return
			}

			if respRec.Code != http.StatusOK {
				t.Errorf("unmatch: status code: %d (want: %d)", respRec.Code, http.StatusOK)
			}
		}
	}

	t.Run("it responses OK, when no errors have caused", theory(
		When{
			RunId:    "run-1",
			ErrRetry: nil,
		},
		Then{
			StatusCode: http.StatusOK,
		},
	))

	t.Run("it responses error (InternalServerError), when RunInterface.Retry causes error", theory(
		When{
			RunId:    "run-1",
			ErrRetry: errors.New("fake error"),
		},
		Then{
			StatusCode: http.StatusInternalServerError,
		},
	))

	t.Run("it responses error (NotFound), when RunInterface.Retry returns ErrMissing", theory(
		When{
			RunId:    "run-1",
			ErrRetry: kerr.ErrMissing,
		},
		Then{
			StatusCode: http.StatusNotFound,
		},
	))

	t.Run("it responses error (Conflict), when RunInterface.Retry returns ErrRunIsProtected", theory(
		When{
			RunId:    "run-1",
			ErrRetry: domain.ErrRunIsProtected,
		},
		Then{
			StatusCode: http.StatusConflict,
		},
	))
	t.Run("it responses error (Conflict), when RunInterface.Retry returns ErrInvalidRunStateChanging", theory(
		When{
			RunId:    "run-1",
			ErrRetry: domain.ErrInvalidRunStateChanging,
		},
		Then{
			StatusCode: http.StatusConflict,
		},
	))

}
