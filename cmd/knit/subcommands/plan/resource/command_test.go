package resource_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	plan_resource "github.com/opst/knitfab/cmd/knit/subcommands/plan/resource"
	"github.com/opst/knitfab/pkg/utils/cmp"
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
		Flags plan_resource.Flag
		Args  map[string][]string

		PlanReturned plans.Detail
		Error        error
	}

	type Then struct {
		WantApiCalled bool
		PlanId        string
		Change        plans.ResourceLimitChange
		Err           error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			client := mock.New(t)

			client.Impl.UpdateResources = func(ctx context.Context, planId string, res plans.ResourceLimitChange) (plans.Detail, error) {
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
			cmd := plan_resource.Task()

			err := cmd(
				context.Background(), logger, env.KnitEnv{}, client,
				commandline.MockCommandline[plan_resource.Flag]{
					Fullname_: "knit plan resource",
					Stdout_:   buf,
					Stderr_:   io.Discard,
					Flags_:    when.Flags,
					Args_:     when.Args,
				},
				[]any{},
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
				actual := new(plans.Detail)
				if err := json.Unmarshal(buf.Bytes(), actual); err != nil {
					t.Fatal(err)
				}
				if !when.PlanReturned.Equal(*actual) {
					t.Errorf("Execute() = %v, want %v", actual, when.PlanReturned)
				}
			}
		}
	}

	planRetuened := plans.Detail{
		Summary: plans.Summary{
			PlanId: "test",
			Image: &plans.Image{
				Repository: "image.invalid/test",
				Tag:        "v1.0",
			},
		},
		Resources: plans.Resources{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
		},
		Inputs: []plans.Mountpoint{
			{
				Path: "/in/1",
				Tags: []tags.Tag{
					{Key: "tag", Value: "value"},
				},
			},
		},
		Outputs: []plans.Mountpoint{
			{
				Path: "/out/1",
				Tags: []tags.Tag{
					{Key: "tag", Value: "value"},
				},
			},
		},
		Log: &plans.LogPoint{
			Tags: []tags.Tag{
				{Key: "tag", Value: "value"},
			},
		},
	}

	t.Run("no --set nor --unset", theory(
		When{
			Flags: plan_resource.Flag{},
			Args: map[string][]string{
				plan_resource.ARGS_PLAN_ID: {"test"},
			},
			PlanReturned: planRetuened,
		},
		Then{
			WantApiCalled: false,
		},
	))

	t.Run("with --set", theory(
		When{
			Flags: plan_resource.Flag{
				Set: &plan_resource.ResourceQuantityList{
					"cpu": resource.MustParse("2"),
				},
			},
			Args: map[string][]string{
				plan_resource.ARGS_PLAN_ID: {"test"},
			},
			PlanReturned: planRetuened,
			Error:        errors.New("test"),
		},
		Then{
			WantApiCalled: true,
			PlanId:        "test",
			Change: plans.ResourceLimitChange{
				Set: map[string]resource.Quantity{
					"cpu": resource.MustParse("2"),
				},
			},
		},
	))

	t.Run("with --unset", theory(
		When{
			Flags: plan_resource.Flag{
				Unset: &plan_resource.Types{"cpu"},
			},
			Args: map[string][]string{
				plan_resource.ARGS_PLAN_ID: {"test"},
			},
			PlanReturned: planRetuened,
		},
		Then{
			WantApiCalled: true,
			PlanId:        "test",
			Change: plans.ResourceLimitChange{
				Unset: []string{"cpu"},
			},
		},
	))

	t.Run("with --set and --unset", theory(
		When{
			Flags: plan_resource.Flag{
				Set:   &plan_resource.ResourceQuantityList{"cpu": resource.MustParse("2")},
				Unset: &plan_resource.Types{"memory"},
			},
			Args: map[string][]string{
				plan_resource.ARGS_PLAN_ID: {"test"},
			},
			PlanReturned: planRetuened,
		},
		Then{
			WantApiCalled: true,
			PlanId:        "test",
			Change: plans.ResourceLimitChange{
				Set:   map[string]resource.Quantity{"cpu": resource.MustParse("2")},
				Unset: []string{"memory"},
			},
		},
	))

	fakeError := errors.New("fake error")
	t.Run("client returns error", theory(
		When{
			Flags: plan_resource.Flag{
				Set:   &plan_resource.ResourceQuantityList{"cpu": resource.MustParse("2")},
				Unset: &plan_resource.Types{"memory"},
			},
			Args: map[string][]string{
				plan_resource.ARGS_PLAN_ID: {"test"},
			},
			PlanReturned: planRetuened,
			Error:        fakeError,
		},
		Then{
			WantApiCalled: true,
			PlanId:        "test",
			Change: plans.ResourceLimitChange{
				Set:   map[string]resource.Quantity{"cpu": resource.MustParse("2")},
				Unset: []string{"memory"},
			},
			Err: fakeError,
		},
	))

}
