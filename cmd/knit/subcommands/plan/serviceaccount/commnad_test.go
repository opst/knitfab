package serviceaccount_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	"github.com/opst/knitfab/cmd/knit/subcommands/plan/serviceaccount"
	"github.com/youta-t/flarc"
)

func TestTask(t *testing.T) {
	type When struct {
		Args  map[string][]string
		Flags serviceaccount.Flag

		Response plans.Detail
		RespErr  error
	}

	type Then struct {
		SetIsCalled bool
		PlanId      string

		UnsetIsCalled bool

		Err error
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			logger := logger.Null()
			e := env.New()

			stdout := new(strings.Builder)
			stderr := new(strings.Builder)

			cl := commandline.MockCommandline[serviceaccount.Flag]{
				Fullname_: "annotate",
				Flags_:    when.Flags,
				Args_:     when.Args,
				Stdin_:    nil, // not used
				Stdout_:   stdout,
				Stderr_:   stderr,
			}

			client := mock.New(t)
			client.Impl.SetServiceAccount = func(ctx context.Context, planId string, setServiceAccount plans.SetServiceAccount) (plans.Detail, error) {
				then.SetIsCalled = true

				if planId != then.PlanId {
					t.Errorf("planId: got %v, want %v", planId, then.PlanId)
				}

				if setServiceAccount.ServiceAccount != when.Flags.Set {
					t.Errorf("setServiceAccount.Name: got %v, want %v", setServiceAccount.ServiceAccount, when.Flags.Set)
				}

				return when.Response, when.RespErr
			}

			client.Impl.UnsetServiceAccount = func(ctx context.Context, planId string) (plans.Detail, error) {
				then.UnsetIsCalled = true

				if planId != then.PlanId {
					t.Errorf("planId: got %v, want %v", planId, then.PlanId)
				}

				return when.Response, when.RespErr
			}

			err := serviceaccount.Task()(
				ctx, logger, *e, client, cl, []interface{}{},
			)
			if err != nil {
				if then.Err == nil {
					t.Errorf("unexpected error: %v", err)
				} else if !errors.Is(err, then.Err) {
					t.Errorf("expected error: %v, got: %v", then.Err, err)
				}
				return
			}

			if then.Err != nil {
				t.Errorf("error is expected, but not got: %v", then.Err)
			}

			got := new(plans.Detail)
			if err := json.Unmarshal([]byte(stdout.String()), got); err != nil {
				t.Errorf("failed to unmarshal stdout: %v", err)
			}

			if !got.Equal(when.Response) {
				t.Errorf("response: got %v, want %v", got, when.Response)
			}
		}
	}

	t.Run("set", theory(
		When{
			Args:  map[string][]string{"PLAN_ID": {"plan-1"}},
			Flags: serviceaccount.Flag{Set: "service-account-1"},
			Response: plans.Detail{
				Summary:        plans.Summary{PlanId: "plan-1"},
				ServiceAccount: "service-account-1",
			},
			RespErr: nil,
		},
		Then{
			SetIsCalled: true,
			PlanId:      "plan-1",
			Err:         nil,
		},
	))

	t.Run("unset", theory(
		When{
			Args:  map[string][]string{"PLAN_ID": {"plan-1"}},
			Flags: serviceaccount.Flag{Unset: true},
			Response: plans.Detail{
				Summary: plans.Summary{PlanId: "plan-1"},
			},
			RespErr: nil,
		},
		Then{
			UnsetIsCalled: true,
			PlanId:        "plan-1",
			Err:           nil,
		},
	))

	t.Run("set and unset (should be error)", theory(
		When{
			Args:  map[string][]string{"PLAN_ID": {"plan-1"}},
			Flags: serviceaccount.Flag{Set: "service-account-1", Unset: true},
			Response: plans.Detail{
				Summary: plans.Summary{PlanId: "plan-1"},
			},
			RespErr: nil,
		},
		Then{
			SetIsCalled:   false,
			UnsetIsCalled: false,
			Err:           flarc.ErrUsage,
		},
	))

	t.Run("neither set not unset (should be error)", theory(
		When{
			Args:  map[string][]string{"PLAN_ID": {"plan-1"}},
			Flags: serviceaccount.Flag{},
			Response: plans.Detail{
				Summary: plans.Summary{PlanId: "plan-1"},
			},
			RespErr: nil,
		},
		Then{
			SetIsCalled:   false,
			UnsetIsCalled: false,
			Err:           flarc.ErrUsage,
		},
	))

	{
		wantErr := errors.New("test-error")
		t.Run("client error on set", theory(
			When{
				Args:  map[string][]string{"PLAN_ID": {"plan-1"}},
				Flags: serviceaccount.Flag{Set: "service-account-1"},
				Response: plans.Detail{
					Summary:        plans.Summary{PlanId: "plan-1"},
					ServiceAccount: "service-account-1",
				},
				RespErr: wantErr,
			},
			Then{
				SetIsCalled: true,
				PlanId:      "plan-1",
				Err:         wantErr,
			},
		))
	}

	{
		wantErr := errors.New("test-error")
		t.Run("client error on unset", theory(
			When{
				Args:  map[string][]string{"PLAN_ID": {"plan-1"}},
				Flags: serviceaccount.Flag{Unset: true},
				Response: plans.Detail{
					Summary: plans.Summary{PlanId: "plan-1"},
				},
				RespErr: wantErr,
			},
			Then{
				UnsetIsCalled: true,
				PlanId:        "plan-1",
				Err:           wantErr,
			},
		))
	}
}
