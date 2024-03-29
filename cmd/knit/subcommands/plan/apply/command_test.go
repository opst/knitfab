package apply_test

import (
	"context"
	"errors"
	"testing"

	restmock "github.com/opst/knitfab/cmd/knit/rest/mock"
	plan_apply "github.com/opst/knitfab/cmd/knit/subcommands/plan/apply"
	apiplan "github.com/opst/knitfab/pkg/api/types/plans"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
)

func TestApplyPlan(t *testing.T) {
	theory := func(spec apiplan.PlanSpec, detail apiplan.Detail, expectedErr error) func(*testing.T) {
		return func(t *testing.T) {

			client := restmock.New(t)
			client.Impl.RegisterPlan = func(
				ctx context.Context,
				actualSpec apiplan.PlanSpec,
			) (apiplan.Detail, error) {
				if !spec.Equal(&actualSpec) {
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
			if !actual.Equal(&detail) {
				t.Errorf(
					"response\n===actual===\n%+v\n===expected===\n%+v",
					actual, detail,
				)
			}
		}

	}

	t.Run("when client return plan detail, it return that detail", theory(
		apiplan.PlanSpec{
			Image: apiplan.Image{
				Repository: "test-image", Tag: "test-version",
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
			Active: ref(true),
		},
		apiplan.Detail{
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
		nil,
	))

	expectedError := errors.New("test-error")
	t.Run("when client return error, it return that error", theory(
		apiplan.PlanSpec{
			Image: apiplan.Image{
				Repository: "test-image", Tag: "test-version",
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
			Active: ref(true),
		},
		apiplan.Detail{},
		expectedError,
	))
}

func ref[T any](v T) *T {
	return &v
}
