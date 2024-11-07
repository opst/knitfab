package find_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/tags"
	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	plan_find "github.com/opst/knitfab/cmd/knit/subcommands/plan/find"
	"github.com/opst/knitfab/pkg/domain"
	kargs "github.com/opst/knitfab/pkg/utils/args"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/logic"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/youta-t/flarc"
)

func TestFindCommand(t *testing.T) {

	type When struct {
		flag         plan_find.Flag
		presentation []plans.Detail
		err          error
	}

	type Then struct {
		err      error
		active   logic.Ternary
		imageVer domain.ImageIdentifier
	}

	presentationItems := []plans.Detail{
		{
			Summary: plans.Summary{
				PlanId: "test-Id",
				Image: &plans.Image{
					Repository: "test-image", Tag: "test-version",
				},
				Name: "test-Name",
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{
						Path: "/in/1",
						Tags: []tags.Tag{
							{Key: "type", Value: "raw data"},
							{Key: "format", Value: "rgb image"},
						},
					},
					Upstreams: []plans.Upstream{
						{
							Summary: plans.Summary{
								PlanId: "upstream-plan-id",
								Image: &plans.Image{
									Repository: "upstream-image", Tag: "upstream-version",
								},
								Entrypoint: []string{"/upstream/entrypoint"},
								Args:       []string{"upstream-arg"},
								Annotations: []plans.Annotation{
									{Key: "upstream-annotation-key", Value: "upstream-annotation-value"},
								},
							},
						},
					},
				},
			},
			Outputs: []plans.Output{
				{
					Mountpoint: plans.Mountpoint{
						Path: "/out/2",
						Tags: []tags.Tag{
							{Key: "type", Value: "training data"},
							{Key: "format", Value: "mask"},
						},
					},
					Downstreams: []plans.Downstream{
						{
							Summary: plans.Summary{
								PlanId: "downstream-plan-id",
								Image: &plans.Image{
									Repository: "downstream-image", Tag: "downstream-version",
								},
								Entrypoint: []string{"/downstream/entrypoint"},
								Args:       []string{"downstream-arg"},
								Annotations: []plans.Annotation{
									{Key: "downstream-annotation-key", Value: "downstream-annotation-value"},
								},
							},
						},
					},
				},
			},
			Log: &plans.Log{
				LogPoint: plans.LogPoint{
					Tags: []tags.Tag{
						{Key: "type", Value: "log"},
						{Key: "format", Value: "jsonl"},
					},
				},
			},
			Active: true,
		},
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			profile := &kprof.KnitProfile{ApiRoot: "http://api.knit.invalid"}
			client := try.To(krst.NewClient(profile)).OrFatal(t)

			task := func(
				_ context.Context, _ *log.Logger, _ krst.KnitClient,
				active logic.Ternary,
				image domain.ImageIdentifier,
				inTags []tags.Tag,
				outTags []tags.Tag,
			) ([]plans.Detail, error) {
				if active != then.active {
					t.Errorf(
						"wrong active: (actual, expected) != (%d, %d)",
						active, then.active,
					)
				}

				if image != then.imageVer {
					t.Errorf(
						"wrong image: (actual, expected) != (%+v, %+v)",
						image, then.imageVer,
					)
				}

				expectedInTags := []tags.Tag{}
				if when.flag.InTags != nil {
					expectedInTags = *when.flag.InTags
				}
				if !cmp.SliceEq(inTags, expectedInTags) {
					t.Errorf(
						"wrong inTags: (actual, expected) != (%+v, %+v)",
						inTags, expectedInTags,
					)
				}

				expectedOutTags := []tags.Tag{}
				if when.flag.OutTags != nil {
					expectedOutTags = *when.flag.OutTags
				}
				if !cmp.SliceEq(outTags, expectedOutTags) {
					t.Errorf(
						"wrong outTags: (actual, expected) != (%+v, %+v)",
						outTags, expectedOutTags,
					)
				}

				return when.presentation, when.err
			}

			testee := plan_find.Task(task)

			ctx := context.Background()

			stdout := new(strings.Builder)

			//test start
			actual := testee(
				ctx, logger.Null(), *kenv.New(), client,
				commandline.MockCommandline[plan_find.Flag]{
					Stdout_: stdout,
					Stderr_: io.Discard,
					Flags_:  when.flag,
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
				var actualValue []plans.Detail
				if err := json.Unmarshal([]byte(stdout.String()), &actualValue); err != nil {
					t.Fatal(err)
				}

				if !cmp.SliceContentEqWith(actualValue, when.presentation, plans.Detail.Equal) {
					t.Errorf(
						"stdout:\n===actual===\n%+v\n===expected===\n%+v",
						actualValue, when.presentation,
					)
				}
			}
		}
	}

	t.Run("when no flag is specified, it should call task with active:Indeterminate and return exitsuccess", theory(
		When{
			flag: plan_find.Flag{
				Active: "both",
			},
			presentation: presentationItems,
			err:          nil,
		},
		Then{
			active: logic.Indeterminate,
			err:    nil,
		},
	))
	t.Run("when '--active true' is specified, it should call task with active:true and return exitsuccess", theory(
		When{
			flag: plan_find.Flag{
				Active: "true",
			},
			presentation: presentationItems,
			err:          nil,
		},
		Then{
			active: logic.True,
			err:    nil,
		},
	))
	t.Run("when '--active yes' is specified, it should call task with active:true and return exitsuccess", theory(
		When{
			flag: plan_find.Flag{
				Active: "yes",
			},
			presentation: presentationItems,
			err:          nil,
		},
		Then{
			active: logic.True,
			err:    nil,
		},
	))
	t.Run("when '--active false' is specified, it should call task with active:false", theory(
		When{
			flag: plan_find.Flag{
				Active: "false",
			},
			presentation: presentationItems,
			err:          nil,
		},
		Then{
			active: logic.False,
			err:    nil,
		},
	))
	t.Run("when '--active no' is specified, it should call task with active:false", theory(
		When{
			flag: plan_find.Flag{
				Active: "no",
			},
			presentation: presentationItems,
			err:          nil,
		},
		Then{
			active: logic.False,
			err:    nil,
		},
	))
	t.Run("when '--image image:version' is specified, it should call task with image:version", theory(
		When{
			flag: plan_find.Flag{
				Active: "both",
				Image:  "image-test:version-test",
			},
			presentation: presentationItems,
			err:          nil,
		},
		Then{
			active: logic.Indeterminate,
			imageVer: domain.ImageIdentifier{
				Image: "image-test", Version: "version-test",
			},
			err: nil,
		},
	))
	t.Run("when '--image image' is specified, it should call task with image", theory(
		When{
			flag: plan_find.Flag{
				Active: "both",
				Image:  "image-test",
			},
			presentation: presentationItems,
			err:          nil,
		},
		Then{
			active: logic.Indeterminate,
			imageVer: domain.ImageIdentifier{
				Image: "image-test", Version: "",
			},
		},
	))
	t.Run("when '--image :version' is specified, it should return ErrUsage", theory(
		When{
			flag: plan_find.Flag{
				Active: "both",
				Image:  ":test-version",
			},
			presentation: presentationItems,
			err:          nil,
		},
		Then{
			err: flarc.ErrUsage,
		},
	))
	t.Run("when input tags and output tags are passed it should call task with all tags", theory(
		When{
			flag: plan_find.Flag{
				Active: "both",
				InTags: &kargs.Tags{
					{Key: "foo", Value: "bar"},
					{Key: "example", Value: "tag"},
				},
				OutTags: &kargs.Tags{
					{Key: "knit#id", Value: "some-knit-id"},
					{Key: "baz", Value: "quux"},
				},
			},
			presentation: presentationItems,
			err:          nil,
		},
		Then{
			active:   logic.Indeterminate,
			imageVer: domain.ImageIdentifier{},
		},
	))

	{
		err := errors.New("fake error")
		t.Run("when task returns error, it should return with ExitFailure", theory(
			When{
				flag: plan_find.Flag{
					Active: "both",
					InTags: &kargs.Tags{
						{Key: "foo", Value: "bar"},
					},
					OutTags: &kargs.Tags{
						{Key: "knit#id", Value: "some-knit-id"},
					},
				},
				presentation: presentationItems,
				err:          err,
			},
			Then{
				err:      err,
				active:   logic.Indeterminate,
				imageVer: domain.ImageIdentifier{},
			},
		))
	}
}

func TestRunFindPlan(t *testing.T) {
	expectedValue := []plans.Detail{
		{
			Summary: plans.Summary{
				PlanId: "test-Id",
				Image: &plans.Image{
					Repository: "test-image", Tag: "test-version",
				},
				Name: "test-Name",
			},
			Inputs: []plans.Input{
				{
					Mountpoint: plans.Mountpoint{
						Path: "/in/1",
						Tags: []tags.Tag{
							{Key: "type", Value: "raw data"},
							{Key: "format", Value: "rgb image"},
						},
					},
					Upstreams: []plans.Upstream{
						{
							Summary: plans.Summary{
								PlanId: "upstream-plan-id",
								Image: &plans.Image{
									Repository: "upstream-image", Tag: "upstream-version",
								},
								Entrypoint: []string{"/upstream/entrypoint"},
								Args:       []string{"upstream-arg"},
								Annotations: []plans.Annotation{
									{Key: "upstream-annotation-key", Value: "upstream-annotation-value"},
								},
							},
						},
					},
				},
			},
			Outputs: []plans.Output{
				{
					Mountpoint: plans.Mountpoint{
						Path: "/out/2",
						Tags: []tags.Tag{
							{Key: "type", Value: "training data"},
							{Key: "format", Value: "mask"},
						},
					},
					Downstreams: []plans.Downstream{
						{
							Summary: plans.Summary{
								PlanId: "downstream-plan-id",
								Image: &plans.Image{
									Repository: "downstream-image", Tag: "downstream-version",
								},
							},
						},
					},
				},
			},
			Log: &plans.Log{
				LogPoint: plans.LogPoint{
					Tags: []tags.Tag{
						{Key: "type", Value: "log"},
						{Key: "format", Value: "jsonl"},
					},
				},
				Downstreams: []plans.Downstream{
					{
						Summary: plans.Summary{
							PlanId: "downstream-plan-id",
							Image: &plans.Image{
								Repository: "downstream-image", Tag: "downstream-version",
							},
						},
					},
				},
			},
			Active: true,
		},
	}
	t.Run("When client does not cause any error, it should return plan returned by client as is", func(t *testing.T) {
		ctx := context.Background()
		log := logger.Null()
		mock := mock.New(t)
		mock.Impl.FindPlan = func(
			ctx context.Context, active logic.Ternary, imageVer domain.ImageIdentifier,
			inTags []tags.Tag, outTags []tags.Tag,
		) ([]plans.Detail, error) {
			return expectedValue, nil
		}

		// arguments set up
		imageVer := domain.ImageIdentifier{
			Image: "test-image", Version: "test-version",
		}
		input := []tags.Tag{{Key: "foo", Value: "bar"}}
		output := []tags.Tag{{Key: "foo", Value: "bar"}}

		// test start
		actual := try.To(plan_find.RunFindPlan(
			ctx, log, mock, logic.Indeterminate, imageVer, input, output)).OrFatal(t)

		if !cmp.SliceContentEqWith(actual, expectedValue, plans.Detail.Equal) {
			t.Errorf(
				"response is in unexpected form:\n===actual===\n%+v\n===expected===\n%+v",
				actual, expectedValue,
			)
		}

	})

	t.Run("when client returns error, it should return the error as is", func(t *testing.T) {
		ctx := context.Background()
		log := logger.Null()
		expectedError := errors.New("fake error")

		mock := mock.New(t)
		mock.Impl.FindPlan = func(
			ctx context.Context, active logic.Ternary, imageVer domain.ImageIdentifier,
			inTags []tags.Tag, outTags []tags.Tag,
		) ([]plans.Detail, error) {
			return nil, expectedError
		}

		// argements set up
		imageVer := domain.ImageIdentifier{
			Image: "test-image", Version: "test-version",
		}
		input := []tags.Tag{{Key: "foo", Value: "bar"}}
		output := []tags.Tag{{Key: "foo", Value: "bar"}}

		// test start
		actual, err := plan_find.RunFindPlan(
			ctx, log, mock, logic.Indeterminate, imageVer, input, output)

		if actual != nil {
			t.Errorf("unexpected value is returned: %+v", actual)
		}

		if !errors.Is(err, expectedError) {
			t.Errorf("returned error is not expected one: %+v", err)
		}

	})
}
