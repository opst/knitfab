package apply_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/tags"
	restmock "github.com/opst/knitfab/cmd/knit/rest/mock"
	plan_apply "github.com/opst/knitfab/cmd/knit/subcommands/plan/apply"
)

func TestApplyPlan(t *testing.T) {
	theory := func(spec plans.PlanSpec, detail plans.Detail, expectedErr error) func(*testing.T) {
		return func(t *testing.T) {

			client := restmock.New(t)
			client.Impl.RegisterPlan = func(
				ctx context.Context,
				actualSpec plans.PlanSpec,
			) (plans.Detail, error) {
				if !spec.Equal(actualSpec) {
					t.Errorf(
						"spec in request:\n===actual===\n%v\n===expected===\n%v",
						actualSpec, spec,
					)
				}
				return detail, expectedErr
			}
			ctx := context.Background()
			actual, err := plan_apply.ApplyPlan(ctx, client, spec)
			if !errors.Is(err, expectedErr) {
				t.Errorf("returned error is not expected one: %+v", err)
			}
			if err != nil {
				return
			}
			if !actual.Equal(detail) {
				t.Errorf(
					"response\n===actual===\n%+v\n===expected===\n%+v",
					actual, detail,
				)
			}
		}

	}

	t.Run("when client return plan detail, it return that detail", theory(
		plans.PlanSpec{
			Image: plans.Image{
				Repository: "test-image", Tag: "test-version",
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
			Active: ref(true),
		},
		plans.Detail{
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
		nil,
	))

	expectedError := errors.New("test-error")
	t.Run("when client return error, it return that error", theory(
		plans.PlanSpec{
			Image: plans.Image{
				Repository: "test-image", Tag: "test-version",
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
			Active: ref(true),
		},
		plans.Detail{},
		expectedError,
	))
}

func ref[T any](v T) *T {
	return &v
}
