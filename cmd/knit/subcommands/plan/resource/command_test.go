package resource_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	plan_resource "github.com/opst/knitfab/cmd/knit/subcommands/plan/resource"
	apiplans "github.com/opst/knitfab/pkg/api/types/plans"
	apitags "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestResourceQuantityList_Set(t *testing.T) {
	type fields struct {
		Type     string
		Quantity resource.Quantity
	}
	type args struct {
		value string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "valid",
			fields: fields{
				Type:     "test",
				Quantity: resource.MustParse("1"),
			},
			args: args{
				value: "test=2",
			},
			wantErr: false,
		},
		{
			name: "valid with suffix",
			fields: fields{
				Type:     "test",
				Quantity: resource.MustParse("2Gi"),
			},
			args: args{
				value: "test=2Gi",
			},
			wantErr: false,
		},
		{
			name: "invalid",
			fields: fields{
				Type:     "test",
				Quantity: resource.MustParse("1"),
			},
			args: args{
				value: "test=invalid",
			},
			wantErr: true,
		},
		{
			name: "invalid",
			fields: fields{
				Type:     "test",
				Quantity: resource.MustParse("1"),
			},
			args: args{
				value: "test",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		rqn := plan_resource.ResourceQuantityList{}
		t.Run(tt.name, func(t *testing.T) {
			if err := rqn.Set(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("ResourceQuantityList.Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCommand(t *testing.T) {
	type When struct {
		Args usage.FlagSet[plan_resource.Flag]

		PlanReturned apiplans.Detail
		Error        error
	}

	type Then struct {
		WantApiCalled bool
		PlanId        string
		Change        apiplans.ResourceLimitChange
		Err           error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			client := mock.New(t)

			client.Impl.UpdateResources = func(ctx context.Context, planId string, res apiplans.ResourceLimitChange) (apiplans.Detail, error) {
				if planId != then.PlanId {
					t.Errorf("UpdateResources() planId = %v, want %v", planId, then.PlanId)
				}

				if !cmp.MapEqWith(then.Change.Set, res.Set, resource.Quantity.Equal) {
					t.Errorf("UpdateResources() res.Set = %v, want %v", res.Set, then.Change.Set)
				}

				if !cmp.SliceContentEq(then.Change.Unset, res.Unset) {
					t.Errorf("UpdateResources() res.Unset = %v, want %v", res.Unset, then.Change.Unset)
				}

				return when.PlanReturned, then.Err
			}

			logger := logger.Null()

			buf := new(bytes.Buffer)
			cmd := plan_resource.New(
				plan_resource.WithOutput(buf),
			)

			err := cmd.Execute(
				context.Background(), logger, env.KnitEnv{}, client, when.Args,
			)

			if then.WantApiCalled != (0 < len(client.Calls.UpdateResources)) {
				t.Errorf("Execute() api called = %v, want %v", 0 < len(client.Calls.UpdateResources), then.WantApiCalled)
			}

			if !errors.Is(err, then.Err) {
				t.Errorf("Execute() error = %v, wantErr %v", err, then.Err)
			}

			if err != nil {
				return
			}

			if then.WantApiCalled {
				actual := new(apiplans.Detail)
				if err := json.Unmarshal(buf.Bytes(), actual); err != nil {
					t.Fatal(err)
				}
				if !when.PlanReturned.Equal(actual) {
					t.Errorf("Execute() = %v, want %v", actual, when.PlanReturned)
				}
			}
		}
	}

	planRetuened := apiplans.Detail{
		Summary: apiplans.Summary{
			PlanId: "test",
			Image: &apiplans.Image{
				Repository: "image.invalid/test",
				Tag:        "v1.0",
			},
		},
		Resources: apiplans.Resources{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
		},
		Inputs: []apiplans.Mountpoint{
			{
				Path: "/in/1",
				Tags: []apitags.Tag{
					{Key: "tag", Value: "value"},
				},
			},
		},
		Outputs: []apiplans.Mountpoint{
			{
				Path: "/out/1",
				Tags: []apitags.Tag{
					{Key: "tag", Value: "value"},
				},
			},
		},
		Log: &apiplans.LogPoint{
			Tags: []apitags.Tag{
				{Key: "tag", Value: "value"},
			},
		},
	}

	t.Run("no --set nor --unset", theory(
		When{
			Args: usage.FlagSet[plan_resource.Flag]{
				Flags: plan_resource.Flag{},
				Args: map[string][]string{
					plan_resource.ARGS_PLAN_ID: {"test"},
				},
			},
			PlanReturned: planRetuened,
		},
		Then{
			WantApiCalled: false,
		},
	))

	t.Run("with --set", theory(
		When{
			Args: usage.FlagSet[plan_resource.Flag]{
				Flags: plan_resource.Flag{
					Set: plan_resource.ResourceQuantityList{
						"cpu": resource.MustParse("2"),
					},
				},
				Args: map[string][]string{
					plan_resource.ARGS_PLAN_ID: {"test"},
				},
			},
			PlanReturned: planRetuened,
			Error:        errors.New("test"),
		},
		Then{
			WantApiCalled: true,
			PlanId:        "test",
			Change: apiplans.ResourceLimitChange{
				Set: map[string]resource.Quantity{
					"cpu": resource.MustParse("2"),
				},
			},
		},
	))

	t.Run("with --unset", theory(
		When{
			Args: usage.FlagSet[plan_resource.Flag]{
				Flags: plan_resource.Flag{
					Unset: plan_resource.Types{"cpu"},
				},
				Args: map[string][]string{
					plan_resource.ARGS_PLAN_ID: {"test"},
				},
			},
			PlanReturned: planRetuened,
		},
		Then{
			WantApiCalled: true,
			PlanId:        "test",
			Change: apiplans.ResourceLimitChange{
				Unset: []string{"cpu"},
			},
		},
	))

	t.Run("with --set and --unset", theory(
		When{
			Args: usage.FlagSet[plan_resource.Flag]{
				Flags: plan_resource.Flag{
					Set:   plan_resource.ResourceQuantityList{"cpu": resource.MustParse("2")},
					Unset: plan_resource.Types{"memory"},
				},
				Args: map[string][]string{
					plan_resource.ARGS_PLAN_ID: {"test"},
				},
			},
			PlanReturned: planRetuened,
		},
		Then{
			WantApiCalled: true,
			PlanId:        "test",
			Change: apiplans.ResourceLimitChange{
				Set:   map[string]resource.Quantity{"cpu": resource.MustParse("2")},
				Unset: []string{"memory"},
			},
		},
	))

	fakeError := errors.New("fake error")
	t.Run("client returns error", theory(
		When{
			Args: usage.FlagSet[plan_resource.Flag]{
				Flags: plan_resource.Flag{
					Set:   plan_resource.ResourceQuantityList{"cpu": resource.MustParse("2")},
					Unset: plan_resource.Types{"memory"},
				},
				Args: map[string][]string{
					plan_resource.ARGS_PLAN_ID: {"test"},
				},
			},
			PlanReturned: planRetuened,
			Error:        fakeError,
		},
		Then{
			WantApiCalled: true,
			PlanId:        "test",
			Change: apiplans.ResourceLimitChange{
				Set:   map[string]resource.Quantity{"cpu": resource.MustParse("2")},
				Unset: []string{"memory"},
			},
			Err: fakeError,
		},
	))

}
