package find_test

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	run_find "github.com/opst/knitfab/cmd/knit/subcommands/run/find"
	apiplan "github.com/opst/knitfab/pkg/api/types/plans"
	apirun "github.com/opst/knitfab/pkg/api/types/runs"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/cmp"
	kflag "github.com/opst/knitfab/pkg/commandline/flag"
	"github.com/opst/knitfab/pkg/commandline/usage"
	ptr "github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestFindCommand(t *testing.T) {

	type When struct {
		flag         run_find.Flag
		presentation []apirun.Detail
		err          error
	}

	type Then struct {
		err error
	}

	presentationItems := []apirun.Detail{
		{
			Summary: apirun.Summary{
				RunId:  "test-runId",
				Status: "done",
				Plan: apiplan.Summary{
					PlanId: "test-Id",
					Image: &apiplan.Image{
						Repository: "test-image", Tag: "test-version",
					},
					Name: "test-Name",
				},
				UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
					"2022-04-02T12:00:00+00:00",
				)).OrFatal(t),
			},
			Inputs: []apirun.Assignment{
				{
					Mountpoint: apiplan.Mountpoint{
						Path: "/in/1",
						Tags: []apitag.Tag{
							{Key: "type", Value: "raw data"},
							{Key: "format", Value: "rgb image"},
						},
					},
					KnitId: "test-knitId-a",
				},
			},
			Outputs: []apirun.Assignment{
				{
					Mountpoint: apiplan.Mountpoint{
						Path: "/out/2",
						Tags: []apitag.Tag{
							{Key: "type", Value: "training data"},
							{Key: "format", Value: "mask"},
						},
					},
					KnitId: "test-knitId-b",
				}},
			Log: &apirun.LogSummary{
				LogPoint: apiplan.LogPoint{
					Tags: []apitag.Tag{
						{Key: "type", Value: "log"},
						{Key: "format", Value: "jsonl"},
					},
				},
				KnitId: "test-knitId",
			},
		},
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			client := try.To(krst.NewClient(
				&kprof.KnitProfile{ApiRoot: "http://api.knit.invalid"},
			)).OrFatal(t)

			task := func(
				_ context.Context,
				_ *log.Logger,
				_ krst.KnitClient,
				parameter krst.FindRunParameter,
			) ([]apirun.Detail, error) {

				checkSliceEq(t, "planId", parameter.PlanId, ptr.SafeDeref(when.flag.PlanId))
				checkSliceEq(t, "knitIdIn", parameter.KnitIdIn, ptr.SafeDeref(when.flag.KnitIdIn))
				checkSliceEq(t, "knitIdOut", parameter.KnitIdOut, ptr.SafeDeref(when.flag.KnitIdOut))
				checkSliceEq(t, "status", parameter.Status, ptr.SafeDeref(when.flag.Status))
				if *parameter.Since != time.Time(ptr.SafeDeref(when.flag.Since)) {
					t.Errorf("wrong since: (actual, expected) != (%s, %s)", *parameter.Since, when.flag.Since)
				}

				if *parameter.Duration != when.flag.Duration {
					t.Errorf("wrong duration: (actual, expected) != (%s, %s)", *parameter.Duration, when.flag.Duration)
				}

				return when.presentation, when.err
			}

			testee := run_find.New(run_find.WithTask(task))
			ctx := context.Background()

			pr, pw, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer pw.Close()
			defer pr.Close()

			{
				orig := os.Stdout
				defer func() { os.Stdout = orig }()
				os.Stdout = pw
			}

			//test start
			actual := testee.Execute(
				ctx, logger.Null(), *kenv.New(), client,
				usage.FlagSet[run_find.Flag]{
					Flags: when.flag,
				},
			)
			pw.Close() // to tearoff writer.

			if !errors.Is(actual, then.err) {
				t.Errorf(
					"wrong status: (actual, expected) != (%d, %d)",
					actual, then.err,
				)
			}

			if then.err == nil {
				var actualValue []apirun.Detail
				if err := json.NewDecoder(pr).Decode(&actualValue); err != nil {
					t.Fatal(err)
				}
				if !cmp.SliceContentEqWith(
					actualValue, when.presentation,
					func(a, b apirun.Detail) bool { return a.Equal(&b) },
				) {
					t.Errorf(
						"stdout:\n===actual===\n%+v\n===expected===\n%+v",
						actualValue, when.presentation,
					)
				}
			}
		}
	}

	t.Run("when value for flag is not passed, it should call task without flags value", theory(
		When{
			flag:         run_find.Flag{},
			presentation: presentationItems,
			err:          nil,
		},
		Then{
			err: nil,
		},
	))

	{
		timestamp := try.To(rfctime.ParseRFC3339DateTime("2024-04-22T00:00:00.000+09:00")).OrFatal(t).Time()
		since := kflag.LooseRFC3339(timestamp)
		t.Run("when values for each flag are passed, it should call task with these values", theory(
			When{
				flag: run_find.Flag{
					PlanId: &kflag.Argslice{
						"plan1", "plan2",
					},
					KnitIdIn: &kflag.Argslice{
						"knit1", "knit2",
					},
					KnitIdOut: &kflag.Argslice{
						"knit3", "knit4",
					},
					Status: &kflag.Argslice{
						"waiting", "running",
					},
					Since:    &since,
					Duration: time.Duration(2 * time.Hour),
				},
				presentation: presentationItems,
			},
			Then{
				err: nil,
			},
		))
	}

	t.Run("when since is not specified and duration is specified, it should return ErrUage", theory(
		When{
			flag: run_find.Flag{
				Duration: time.Duration(2 * time.Hour),
			},
			presentation: presentationItems,
		},
		Then{
			err: kcmd.ErrUsage,
		},
	))

	err := errors.New("fake error")
	t.Run("when task returns error, it should return with error", theory(
		When{
			flag:         run_find.Flag{},
			presentation: presentationItems,
			err:          err,
		},
		Then{
			err: err,
		},
	))
}

func TestRunFindRun(t *testing.T) {
	t.Run("When client does not cause any error, it should return plan returned by client as is", func(t *testing.T) {
		expectedValue := []apirun.Detail{
			{
				Summary: apirun.Summary{
					RunId:  "test-runId",
					Status: "done",
					Plan: apiplan.Summary{
						PlanId: "test-Id",
						Image: &apiplan.Image{
							Repository: "test-image", Tag: "test-version",
						},
						Name: "test-Name",
					},
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-04-02T12:00:00+00:00",
					)).OrFatal(t),
				},
				Inputs: []apirun.Assignment{
					{
						Mountpoint: apiplan.Mountpoint{
							Path: "/in/1",
							Tags: []apitag.Tag{
								{Key: "type", Value: "raw data"},
								{Key: "format", Value: "rgb image"},
							},
						},
						KnitId: "test-knitId-a",
					},
				},
				Outputs: []apirun.Assignment{
					{
						Mountpoint: apiplan.Mountpoint{
							Path: "/out/2",
							Tags: []apitag.Tag{
								{Key: "type", Value: "training data"},
								{Key: "format", Value: "mask"},
							},
						},
						KnitId: "test-knitId-b",
					}},
				Log: &apirun.LogSummary{
					LogPoint: apiplan.LogPoint{
						Tags: []apitag.Tag{
							{Key: "type", Value: "log"},
							{Key: "format", Value: "jsonl"},
						},
					},
					KnitId: "test-knitId",
				},
			},
		}
		ctx := context.Background()
		log := logger.Null()
		mock := mock.New(t)
		mock.Impl.FindRun = func(
			ctx context.Context, query krst.FindRunParameter,
		) ([]apirun.Detail, error) {
			return expectedValue, nil
		}

		// arguments set up
		since := try.To(rfctime.ParseRFC3339DateTime("2024-04-22T00:00:00.000+09:00")).OrFatal(t).Time()
		duration := time.Duration(2 * time.Hour)

		parameter := krst.FindRunParameter{
			PlanId:    []string{"test-planId"},
			KnitIdIn:  []string{"test-inputKnitId"},
			KnitIdOut: []string{"test-outputKnitId"},
			Status:    []string{"test-status"},
			Since:     &since,
			Duration:  &duration,
		}

		// test start
		actual := try.To(run_find.RunFindRun(
			ctx, log, mock, parameter)).OrFatal(t)

		//check actual
		if !cmp.SliceContentEqWith(
			actual, expectedValue,
			func(a, b apirun.Detail) bool { return a.Equal(&b) },
		) {
			t.Errorf(
				"response is in unexpected form:\n===actual===\n%+v\n===expected===\n%+v",
				actual, expectedValue,
			)
		}
	})

	t.Run("when client returns error, it should return the error as is", func(t *testing.T) {
		ctx := context.Background()
		log := logger.Null()
		var expectedValue []apirun.Detail = nil
		expectedError := errors.New("fake error")

		mock := mock.New(t)
		mock.Impl.FindRun = func(
			ctx context.Context, query krst.FindRunParameter,
		) ([]apirun.Detail, error) {
			return expectedValue, expectedError
		}

		// argements set up
		since := try.To(rfctime.ParseRFC3339DateTime("2024-04-22T00:00:00.000+09:00")).OrFatal(t).Time()
		duration := time.Duration(2 * time.Hour)

		parameter := krst.FindRunParameter{
			PlanId:    []string{"test-planId"},
			KnitIdIn:  []string{"test-inputKnitId"},
			KnitIdOut: []string{"test-outputKnitId"},
			Status:    []string{"test-status"},
			Since:     &since,
			Duration:  &duration,
		}

		// test start
		actual, err := run_find.RunFindRun(
			ctx, log, mock, parameter)

		//check actual and err
		if actual != nil {
			t.Errorf("unexpected value is returned: %+v", actual)
		}
		if !errors.Is(err, expectedError) {
			t.Errorf("returned error is not expected one: %+v", err)
		}
	})
}

func checkSliceEq(t *testing.T, name string, actual []string, expected []string) {
	if !cmp.SliceEq(actual, expected) {
		t.Errorf(
			"wrong %s: (actual, expected) != (%+v, %+v)",
			name, actual, expected,
		)
	}
}
