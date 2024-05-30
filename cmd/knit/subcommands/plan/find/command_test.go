package find_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"strings"
	"testing"

	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	plan_find "github.com/opst/knitfab/cmd/knit/subcommands/plan/find"
	apiplan "github.com/opst/knitfab/pkg/api/types/plans"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/commandline/flag"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils/logic"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/youta-t/flarc"
)

func TestFindCommand(t *testing.T) {

	type When struct {
		flag         plan_find.Flag
		presentation []apiplan.Detail
		err          error
	}

	type Then struct {
		err      error
		active   logic.Ternary
		imageVer kdb.ImageIdentifier
	}

	presentationItems := []apiplan.Detail{
		{
			Summary: apiplan.Summary{
				PlanId: "test-Id",
				Image: &apiplan.Image{
					Repository: "test-image", Tag: "test-version",
				},
				Name: "test-Name",
			},
			Inputs: []apiplan.Mountpoint{
				{
					Path: "/in/1",
					Tags: []apitag.Tag{
						{Key: "type", Value: "raw data"},
						{Key: "format", Value: "rgb image"},
					},
				},
			},
			Outputs: []apiplan.Mountpoint{
				{
					Path: "/out/2",
					Tags: []apitag.Tag{
						{Key: "type", Value: "training data"},
						{Key: "format", Value: "mask"},
					},
				},
			},
			Log: &apiplan.LogPoint{
				Tags: []apitag.Tag{
					{Key: "type", Value: "log"},
					{Key: "format", Value: "jsonl"},
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
				image kdb.ImageIdentifier,
				inTags []apitag.Tag,
				outTags []apitag.Tag,
			) ([]apiplan.Detail, error) {
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

				expectedInTags := []apitag.Tag{}
				if when.flag.InTags != nil {
					expectedInTags = *when.flag.InTags
				}
				if !cmp.SliceEq(inTags, expectedInTags) {
					t.Errorf(
						"wrong inTags: (actual, expected) != (%+v, %+v)",
						inTags, expectedInTags,
					)
				}

				expectedOutTags := []apitag.Tag{}
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
				var actualValue []apiplan.Detail
				if err := json.Unmarshal([]byte(stdout.String()), &actualValue); err != nil {
					t.Fatal(err)
				}

				if !cmp.SliceContentEqWith(
					actualValue, when.presentation,
					func(a, b apiplan.Detail) bool { return a.Equal(&b) },
				) {
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
			imageVer: kdb.ImageIdentifier{
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
			imageVer: kdb.ImageIdentifier{
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
				InTags: &flag.Tags{
					{Key: "foo", Value: "bar"},
					{Key: "example", Value: "tag"},
				},
				OutTags: &flag.Tags{
					{Key: "knit#id", Value: "some-knit-id"},
					{Key: "baz", Value: "quux"},
				},
			},
			presentation: presentationItems,
			err:          nil,
		},
		Then{
			active:   logic.Indeterminate,
			imageVer: kdb.ImageIdentifier{},
		},
	))

	{
		err := errors.New("fake error")
		t.Run("when task returns error, it should return with ExitFailure", theory(
			When{
				flag: plan_find.Flag{
					Active: "both",
					InTags: &flag.Tags{
						{Key: "foo", Value: "bar"},
					},
					OutTags: &flag.Tags{
						{Key: "knit#id", Value: "some-knit-id"},
					},
				},
				presentation: presentationItems,
				err:          err,
			},
			Then{
				err:      err,
				active:   logic.Indeterminate,
				imageVer: kdb.ImageIdentifier{},
			},
		))
	}
}

func TestRunFindPlan(t *testing.T) {
	expectedValue := []apiplan.Detail{
		{
			Summary: apiplan.Summary{
				PlanId: "test-Id",
				Image: &apiplan.Image{
					Repository: "test-image", Tag: "test-version",
				},
				Name: "test-Name",
			},
			Inputs: []apiplan.Mountpoint{
				{
					Path: "/in/1",
					Tags: []apitag.Tag{
						{Key: "type", Value: "raw data"},
						{Key: "format", Value: "rgb image"},
					},
				},
			},
			Outputs: []apiplan.Mountpoint{
				{
					Path: "/out/2",
					Tags: []apitag.Tag{
						{Key: "type", Value: "training data"},
						{Key: "format", Value: "mask"},
					},
				},
			},
			Log: &apiplan.LogPoint{
				Tags: []apitag.Tag{
					{Key: "type", Value: "log"},
					{Key: "format", Value: "jsonl"},
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
			ctx context.Context, active logic.Ternary, imageVer kdb.ImageIdentifier,
			inTags []apitag.Tag, outTags []apitag.Tag,
		) ([]apiplan.Detail, error) {
			return expectedValue, nil
		}

		// arguments set up
		imageVer := kdb.ImageIdentifier{
			Image: "test-image", Version: "test-version",
		}
		input := []apitag.Tag{{Key: "foo", Value: "bar"}}
		output := []apitag.Tag{{Key: "foo", Value: "bar"}}

		// test start
		actual := try.To(plan_find.RunFindPlan(
			ctx, log, mock, logic.Indeterminate, imageVer, input, output)).OrFatal(t)

		if !cmp.SliceContentEqWith(
			actual, expectedValue,
			func(a, b apiplan.Detail) bool { return a.Equal(&b) },
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
		expectedError := errors.New("fake error")

		mock := mock.New(t)
		mock.Impl.FindPlan = func(
			ctx context.Context, active logic.Ternary, imageVer kdb.ImageIdentifier,
			inTags []apitag.Tag, outTags []apitag.Tag,
		) ([]apiplan.Detail, error) {
			return nil, expectedError
		}

		// argements set up
		imageVer := kdb.ImageIdentifier{
			Image: "test-image", Version: "test-version",
		}
		input := []apitag.Tag{{Key: "foo", Value: "bar"}}
		output := []apitag.Tag{{Key: "foo", Value: "bar"}}

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
