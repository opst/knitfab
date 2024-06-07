package show_test

import (
	"context"
	"errors"
	"io"
	"testing"

	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	plan_show "github.com/opst/knitfab/cmd/knit/subcommands/plan/show"
	apiplans "github.com/opst/knitfab/pkg/api/types/plans"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestShowCommand(t *testing.T) {
	plandata := apiplans.Detail{
		Summary: apiplans.Summary{
			PlanId: "test-Id",
			Image: &apiplans.Image{
				Repository: "test-Image", Tag: "test-Version",
			},
			Name: "test-Name",
		},
		Inputs: []apiplans.Mountpoint{
			{
				Path: "/in/1",
				Tags: []apitag.Tag{
					{Key: "type", Value: "raw data"},
					{Key: "format", Value: "rgb image"},
				},
			},
		},
		Outputs: []apiplans.Mountpoint{
			{
				Path: "/out/2",
				Tags: []apitag.Tag{
					{Key: "type", Value: "training data"},
					{Key: "format", Value: "mask"},
				},
			},
		},
		Log: &apiplans.LogPoint{
			Tags: []apitag.Tag{
				{Key: "type", Value: "log"},
				{Key: "format", Value: "jsonl"},
			},
		},
		Active: true,
	}

	type when struct {
		planId []string
		plan   apiplans.Detail
		err    error
	}

	type then struct {
		err    error
		planId string
	}

	theory := func(when when, then then) func(*testing.T) {
		return func(t *testing.T) {
			profile := &kprof.KnitProfile{ApiRoot: "http://api.knit.invalid"}
			client := try.To(krst.NewClient(profile)).OrFatal(t)

			show := func(
				ctx context.Context,
				client krst.KnitClient,
				planId string,
			) (apiplans.Detail, error) {
				if planId != then.planId {
					t.Errorf("wrong planId: %s", planId)
				}
				return when.plan, when.err
			}

			testee := plan_show.Task(show)

			ctx := context.Background()
			actual := testee(
				ctx, logger.Null(), *env.New(), client,
				commandline.MockCommandline[struct{}]{
					Fullname_: "knit plan show",
					Stdout_:   io.Discard,
					Stderr_:   io.Discard,
					Flags_:    struct{}{},
					Args_: map[string][]string{
						plan_show.ARG_PLAN_ID: when.planId,
					},
				},
				[]any{},
			)

			if !errors.Is(actual, then.err) {
				t.Errorf(
					"wrong status: (actual, expected) != (%d, %d)",
					actual, then.err,
				)
			}
		}
	}

	t.Run("when it is passed existed planId, it should return exitsuccess", theory(
		when{
			planId: []string{"test-Id"},
			plan:   plandata,
			err:    nil,
		},
		then{
			err:    nil,
			planId: "test-Id",
		},
	))

	{
		expectedError := errors.New("fake error")
		t.Run("when, error is caused in client, it returns the error", theory(
			when{
				planId: []string{"test-Id"},
				plan:   apiplans.Detail{},
				err:    expectedError,
			},
			then{
				err:    expectedError,
				planId: "test-Id",
			},
		))
	}

}

func TestRunShowplan(t *testing.T) {
	t.Run("When client does not cause any error, it should return the planId returned by client as is", func(t *testing.T) {
		ctx := context.Background()
		mock := mock.New(t)
		expectedValue := apiplans.Detail{
			Summary: apiplans.Summary{
				PlanId: "test-Id",
				Image: &apiplans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
				Name: "test-Name",
			},
			Inputs: []apiplans.Mountpoint{
				{
					Path: "/in/1",
					Tags: []apitag.Tag{
						{Key: "type", Value: "raw data"},
						{Key: "format", Value: "rgb image"},
					},
				},
			},
			Outputs: []apiplans.Mountpoint{
				{
					Path: "/out/2",
					Tags: []apitag.Tag{
						{Key: "type", Value: "training data"},
						{Key: "format", Value: "mask"},
					},
				},
			},
			Log: &apiplans.LogPoint{
				Tags: []apitag.Tag{
					{Key: "type", Value: "log"},
					{Key: "format", Value: "jsonl"},
				},
			},
			Active: true,
		}
		mock.Impl.GetPlans = func(ctx context.Context, planId string) (apiplans.Detail, error) {
			return expectedValue, nil
		}

		actual := try.To(plan_show.RunShowPlan(ctx, mock, "test-Id")).OrFatal(t)

		if !actual.Equal(&expectedValue) {
			t.Errorf("response is not equal (actual,expected): %v,%v", actual, expectedValue)
		}

	})

	t.Run("when client returns error, it should return the error as is", func(t *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("fake error")

		mock := mock.New(t)
		mock.Impl.GetPlans = func(ctx context.Context, planId string) (apiplans.Detail, error) {
			return apiplans.Detail{}, expectedError
		}

		actual, err := plan_show.RunShowPlan(ctx, mock, "test-Id")

		expectedValue := apiplans.Detail{}
		if !actual.Equal(&expectedValue) {
			t.Errorf("response is not equal (actual,expected): %v,%v", actual, expectedValue)
		}

		if !errors.Is(err, expectedError) {
			t.Errorf("returned error is not expected one: %+v", err)
		}
	})
}
