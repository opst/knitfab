package find_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	run_find "github.com/opst/knitfab/cmd/knit/subcommands/run/find"
	kargs "github.com/opst/knitfab/pkg/utils/args"
	"github.com/opst/knitfab/pkg/utils/cmp"
	ptr "github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/youta-t/flarc"
)

func TestFindCommand(t *testing.T) {

	type When struct {
		flag         run_find.Flag
		presentation []runs.Detail
		err          error
	}

	type Then struct {
		err error
	}

	presentationItems := []runs.Detail{
		{
			Summary: runs.Summary{
				RunId:  "test-runId",
				Status: "done",
				Plan: plans.Summary{
					PlanId: "test-Id",
					Image: &plans.Image{
						Repository: "test-image", Tag: "test-version",
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
			) ([]runs.Detail, error) {

				checkSliceEq(t, "planId", parameter.PlanId, ptr.SafeDeref(when.flag.PlanId))
				checkSliceEq(t, "knitIdIn", parameter.KnitIdIn, ptr.SafeDeref(when.flag.KnitIdIn))
				checkSliceEq(t, "knitIdOut", parameter.KnitIdOut, ptr.SafeDeref(when.flag.KnitIdOut))
				checkSliceEq(t, "status", parameter.Status, ptr.SafeDeref(when.flag.Status))
				if want := when.flag.Since.Time(); want == nil {
					if parameter.Since != nil {
						t.Errorf("wrong since: (actual, expected) != (%s, %s)", parameter.Since, when.flag.Since)
					}
				} else if parameter.Since == nil || !want.Equal(*parameter.Since) {
					t.Errorf("wrong since: (actual, expected) != (%s, %s)", *parameter.Since, when.flag.Since)
				}

				if want := when.flag.Duration.Duration(); want == nil {
					if parameter.Duration != nil {
						t.Errorf("wrong duration: (actual, expected) != (%s, %s)", parameter.Duration, want)
					}
				} else if parameter.Duration == nil || *want != *parameter.Duration {
					t.Errorf("wrong duration: (actual, expected) != (%s, %s)", *parameter.Duration, want)
				}

				return when.presentation, when.err
			}

			testee := run_find.Task(task)
			ctx := context.Background()

			stdout := new(strings.Builder)

			//test start
			actual := testee(
				ctx, logger.Null(), *kenv.New(), client,
				commandline.MockCommandline[run_find.Flag]{
					Fullname_: "knit run find",
					Stdout_:   stdout,
					Stderr_:   io.Discard,
					Flags_:    when.flag,
					Args_:     nil,
				},
				[]any{},
			)

			if !errors.Is(actual, then.err) {
				t.Errorf(
					"wrong status: (actual, expected) != (%d, %d)",
					actual, then.err,
				)
			}

			if then.err == nil {
				var actualValue []runs.Detail
				if err := json.Unmarshal([]byte(stdout.String()), &actualValue); err != nil {
					t.Fatal(err)
				}
				if !cmp.SliceContentEqWith(actualValue, when.presentation, runs.Detail.Equal) {
					t.Errorf(
						"stdout:\n===actual===\n%+v\n===expected===\n%+v",
						actualValue, when.presentation,
					)
				}
			}
		}
	}

	t.Run("when value for flags is not passed, it should call task without flags value", theory(
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
		since := kargs.LooseRFC3339(timestamp)

		d := 2 * time.Hour
		duration := new(kargs.OptionalDuration)
		if err := duration.Set(d.String()); err != nil {
			t.Fatal(err)
		}

		t.Run("when values for each flag are passed, it should call task with these values", theory(
			When{
				flag: run_find.Flag{
					PlanId: &kargs.Argslice{
						"plan1", "plan2",
					},
					KnitIdIn: &kargs.Argslice{
						"knit1", "knit2",
					},
					KnitIdOut: &kargs.Argslice{
						"knit3", "knit4",
					},
					Status: &kargs.Argslice{
						"waiting", "running",
					},
					Since:    &since,
					Duration: duration,
				},
				presentation: presentationItems,
			},
			Then{
				err: nil,
			},
		))
	}

	{

		d := 2 * time.Hour
		duration := new(kargs.OptionalDuration)
		if err := duration.Set(d.String()); err != nil {
			t.Fatal(err)
		}

		t.Run("when since is not specified and duration is specified, it should return ErrUage", theory(
			When{
				flag: run_find.Flag{
					Duration: duration,
				},
				presentation: presentationItems,
			},
			Then{
				err: flarc.ErrUsage,
			},
		))
	}

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
		expectedValue := []runs.Detail{
			{
				Summary: runs.Summary{
					RunId:  "test-runId",
					Status: "done",
					Plan: plans.Summary{
						PlanId: "test-Id",
						Image: &plans.Image{
							Repository: "test-image", Tag: "test-version",
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
			},
		}
		ctx := context.Background()
		log := logger.Null()
		mock := mock.New(t)
		mock.Impl.FindRun = func(
			ctx context.Context, query krst.FindRunParameter,
		) ([]runs.Detail, error) {
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
		if !cmp.SliceContentEqWith(actual, expectedValue, runs.Detail.Equal) {
			t.Errorf(
				"response is in unexpected form:\n===actual===\n%+v\n===expected===\n%+v",
				actual, expectedValue,
			)
		}
	})

	t.Run("when client returns error, it should return the error as is", func(t *testing.T) {
		ctx := context.Background()
		log := logger.Null()
		var expectedValue []runs.Detail = nil
		expectedError := errors.New("fake error")

		mock := mock.New(t)
		mock.Impl.FindRun = func(
			ctx context.Context, query krst.FindRunParameter,
		) ([]runs.Detail, error) {
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
