package plan_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/tags"
	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	plan_active "github.com/opst/knitfab/cmd/knit/subcommands/plan/active"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/youta-t/flarc"
)

func TestActiveCommand(t *testing.T) {

	type task struct {
		plan plans.Detail
		err  error
	}

	type When struct {
		args map[string][]string
		task task
	}

	type Then struct {
		planId                          string
		updateActivenessShouldBeInvoked bool
		toBeActive                      bool
		err                             error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			profile := &kprof.KnitProfile{ApiRoot: "http://api.knit.invalid"}
			client := try.To(krst.NewClient(profile)).OrFatal(t)

			updateActivenessHasBeenInvoked := false
			updateActiveness := func(
				ctx context.Context,
				client krst.KnitClient,
				planId string,
				isActive bool,
			) (plans.Detail, error) {
				updateActivenessHasBeenInvoked = true
				if planId != then.planId {
					t.Errorf("planId is not expected one: %s", planId)
				}

				if isActive != then.toBeActive {
					t.Errorf("isActive is not expected one: %t", isActive)
				}

				return when.task.plan, when.task.err
			}

			testee := plan_active.Task(updateActiveness)

			ctx := context.Background()

			// test start
			actual := testee(
				ctx, logger.Null(), *env.New(), client,
				commandline.MockCommandline[struct{}]{
					Fullname_: "knit plan active",
					Stdout_:   io.Discard,
					Stderr_:   io.Discard,
					Flags_:    struct{}{},
					Args_:     when.args,
				},
				[]any{},
			)

			if !errors.Is(actual, then.err) {
				t.Errorf(
					"wrong status: (actual, expected) != (%d, %d)",
					actual, then.err,
				)
			}

			if updateActivenessHasBeenInvoked != then.updateActivenessShouldBeInvoked {
				t.Errorf(
					"updateActivenessHasBeenInvoked: want %v, but got %v",
					then.updateActivenessShouldBeInvoked, updateActivenessHasBeenInvoked,
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
		Then{
			err:                             nil,
			toBeActive:                      true,
			planId:                          "test-Id",
			updateActivenessShouldBeInvoked: true,
		},
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
		Then{
			err:                             nil,
			toBeActive:                      false,
			planId:                          "test-Id",
			updateActivenessShouldBeInvoked: true,
		},
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
					plan: plans.Detail{},
					err:  expectedError,
				},
			},
			Then{
				err:                             expectedError,
				toBeActive:                      true,
				planId:                          "test-Id",
				updateActivenessShouldBeInvoked: true,
			},
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
					plan: plans.Detail{},
					err:  expectedError,
				},
			},
			Then{
				err:                             expectedError,
				toBeActive:                      false,
				planId:                          "test-Id",
				updateActivenessShouldBeInvoked: true,
			},
		))
	}

	t.Run("when <mode> is neither 'yes' nor 'no', it should return ErrUsage", theory(
		When{
			args: map[string][]string{
				plan_active.ARG_MODE:    {"default"},
				plan_active.ARG_PLAN_ID: {"test-Id"},
			},

			task: task{
				plan: plans.Detail{},
				err:  nil,
			},
		},
		Then{
			err:                             flarc.ErrUsage,
			updateActivenessShouldBeInvoked: false,
		},
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
		) (plans.Detail, error) {
			return expectedValue, nil
		}

		// test start
		actual := try.To(plan_active.UpdateActivatePlan(ctx, mock, "test-Id", true)).OrFatal(t)
		if !actual.Equal(expectedValue) {
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
		) (plans.Detail, error) {
			return plans.Detail{}, expectedError
		}

		// test start
		actual, err := plan_active.UpdateActivatePlan(ctx, mock, "test-Id", true)
		expectedValue := plans.Detail{}
		if !actual.Equal(expectedValue) {
			t.Errorf("response is not equal (actual,expected): %v,%v", actual, expectedValue)
		}

		if !errors.Is(err, expectedError) {
			t.Errorf("returned error is not expected one: %+v", err)
		}
	})

}

// setting data for test
func dummyplan(isActivate bool) plans.Detail {
	return plans.Detail{
		Summary: plans.Summary{
			PlanId: "test-Id",
			Image: &plans.Image{
				Repository: "test-image", Tag: "test-version",
			},
			Name: "test-Name",
		},
		Inputs: []plans.Mountpoint{
			{
				Path: "/in/1",
				Tags: []tags.Tag{
					{Key: "type", Value: "raw data"},
					{Key: "format", Value: "rgb image"},
				},
			},
		},
		Outputs: []plans.Mountpoint{
			{
				Path: "/out/2",
				Tags: []tags.Tag{
					{Key: "type", Value: "training data"},
					{Key: "format", Value: "mask"},
				},
			},
		},
		Log: &plans.LogPoint{
			Tags: []tags.Tag{
				{Key: "type", Value: "log"},
				{Key: "format", Value: "jsonl"},
			},
		},
		Active: isActivate,
	}
}
