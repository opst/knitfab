package plan_test

import (
	"context"
	"errors"
	"testing"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	plan_active "github.com/opst/knitfab/cmd/knit/subcommands/plan/active"
	apiplan "github.com/opst/knitfab/pkg/api/types/plans"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestActiveCommand(t *testing.T) {

	type task struct {
		plan apiplan.Detail
		err  error
	}

	type When struct {
		args map[string][]string
		task task
	}

	type Then struct {
		err error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			profile := &kprof.KnitProfile{ApiRoot: "http://api.knit.invalid"}
			client := try.To(krst.NewClient(profile)).OrFatal(t)

			mocktask := func(
				ctx context.Context,
				client krst.KnitClient,
				planId string,
				isActive bool,
			) (apiplan.Detail, error) {
				return when.task.plan, when.task.err
			}

			testee := plan_active.New(
				plan_active.WithTask(mocktask),
			)

			ctx := context.Background()

			// test start
			actual := testee.Execute(
				ctx, logger.Null(), *env.New(), client,
				usage.FlagSet[struct{}]{
					Flags: struct{}{}, Args: when.args,
				},
			)

			if !errors.Is(actual, then.err) {
				t.Errorf(
					"wrong status: (actual, expected) != (%d, %d)",
					actual, then.err,
				)
			}
		}
	}

	t.Run("when <mode> is 'yes' and task dose not cause any error, it should return exitsuccess", theory(
		When{
			args: map[string][]string{
				plan_active.ARG_MODE:    {"yes"},
				plan_active.ARG_PLAN_ID: {"test-Id"},
			},
			task: task{
				plan: dummyplan(true),
				err:  nil,
			},
		},
		Then{err: nil},
	))

	t.Run("when <mode> is 'no' and task dose not cause any error, it should return exitsuccess", theory(
		When{
			args: map[string][]string{
				plan_active.ARG_MODE:    {"no"},
				plan_active.ARG_PLAN_ID: {"test-Id"},
			},
			task: task{
				plan: dummyplan(false),
				err:  nil,
			},
		},
		Then{err: nil},
	))

	{
		expectedError := errors.New("fake error")
		t.Run("when <mode> is 'yes' and task causes error, it should return the error", theory(
			When{
				args: map[string][]string{
					plan_active.ARG_MODE:    {"yes"},
					plan_active.ARG_PLAN_ID: {"test-Id"},
				},
				task: task{
					plan: apiplan.Detail{},
					err:  expectedError,
				},
			},
			Then{err: expectedError},
		))
	}

	{
		expectedError := errors.New("fake error")
		t.Run("when <mode> is 'no' and task causes error, it should return the error", theory(
			When{
				args: map[string][]string{
					plan_active.ARG_MODE:    {"no"},
					plan_active.ARG_PLAN_ID: {"test-Id"},
				},
				task: task{
					plan: apiplan.Detail{},
					err:  expectedError,
				},
			},
			Then{err: expectedError},
		))
	}

	t.Run("when <mode> is neither 'yes' nor 'no', it should return ErrUsage", theory(
		When{
			args: map[string][]string{
				plan_active.ARG_MODE:    {"default"},
				plan_active.ARG_PLAN_ID: {"test-Id"},
			},

			task: task{
				plan: apiplan.Detail{},
				err:  nil,
			},
		},
		Then{err: kcmd.ErrUsage},
	))

}

func TestRunActivatePlan(t *testing.T) {
	t.Run("When client does not cause any error, it should return the planId returned by client as is", func(t *testing.T) {
		ctx := context.Background()
		mock := mock.New(t)
		expectedValue := dummyplan(true)
		mock.Impl.PutPlanForActivate = func(
			ctx context.Context,
			planId string,
			isActive bool,
		) (apiplan.Detail, error) {
			return expectedValue, nil
		}

		// test start
		actual := try.To(plan_active.RunActivatePlan(ctx, mock, "test-Id", true)).OrFatal(t)
		if !actual.Equal(&expectedValue) {
			t.Errorf("response is not equal (actual,expected): %v,%v", actual, expectedValue)
		}

	})

	t.Run("when client returns error, it should return the error as is", func(t *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("fake error")

		mock := mock.New(t)
		mock.Impl.PutPlanForActivate = func(
			ctx context.Context,
			planId string,
			isActive bool,
		) (apiplan.Detail, error) {
			return apiplan.Detail{}, expectedError
		}

		// test start
		actual, err := plan_active.RunActivatePlan(ctx, mock, "test-Id", true)
		expectedValue := apiplan.Detail{}
		if !actual.Equal(&expectedValue) {
			t.Errorf("response is not equal (actual,expected): %v,%v", actual, expectedValue)
		}

		if !errors.Is(err, expectedError) {
			t.Errorf("returned error is not expected one: %+v", err)
		}
	})

}

// setting data for test
func dummyplan(isActivate bool) apiplan.Detail {
	return apiplan.Detail{
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
		Active: isActivate,
	}
}
