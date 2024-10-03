package annotate_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	"github.com/opst/knitfab/cmd/knit/subcommands/plan/annotate"
	"github.com/opst/knitfab/pkg/cmp"
)

func TestCommand(t *testing.T) {
	type When struct {
		Flag annotate.Flag
		Args map[string][]string

		UpdateAnnotationsReturn plans.Detail
		UpdateAnnotationsError  error
	}

	type Then struct {
		UpdateAnnotationsArgsPlanId string
		UpdateAnnotationsArgsChange plans.AnnotationChange

		Err error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			logger := logger.Null()
			e := env.New()

			stdout := new(strings.Builder)
			stderr := new(strings.Builder)

			cl := commandline.MockCommandline[annotate.Flag]{
				Fullname_: "annotate",
				Flags_:    when.Flag,
				Args_:     when.Args,
				Stdin_:    nil, // not used
				Stdout_:   stdout,
				Stderr_:   stderr,
			}

			client := mock.New(t)
			client.Impl.UpdateAnnotations = func(
				ctx context.Context,
				planId string,
				change plans.AnnotationChange,
			) (plans.Detail, error) {
				if planId != then.UpdateAnnotationsArgsPlanId {
					t.Errorf(
						"planId in request:\n===actual===\n%v\n===expected===\n%v",
						planId, then.UpdateAnnotationsArgsPlanId,
					)
				}

				if !cmp.SliceContentEq(change.Add, then.UpdateAnnotationsArgsChange.Add) {
					t.Errorf(
						"change.Add in request:\n===actual===\n%v\n===expected===\n%v",
						change.Add, then.UpdateAnnotationsArgsChange.Add,
					)
				}

				if !cmp.SliceContentEq(change.Remove, then.UpdateAnnotationsArgsChange.Remove) {
					t.Errorf(
						"change.Remove in request:\n===actual===\n%v\n===expected===\n%v",
						change.Remove, then.UpdateAnnotationsArgsChange.Remove,
					)
				}

				return when.UpdateAnnotationsReturn, when.UpdateAnnotationsError
			}

			err := annotate.Task()(ctx, logger, *e, client, cl, []any{})
			if err != nil {
				if then.Err == nil {
					t.Fatalf("unexpected error: %+v", err)
				} else if !errors.Is(err, then.Err) {
					t.Errorf("returned error is not expected one: %+v", err)
				}
				return
			} else if then.Err != nil {
				t.Fatalf("expected error but got nil")
			}

			var actual plans.Detail
			if err := json.Unmarshal([]byte(stdout.String()), &actual); err != nil {
				t.Fatalf("failed to decode stdout: %v", err)
			}
			if !actual.Equal(when.UpdateAnnotationsReturn) {
				t.Errorf(
					"response\n===actual===\n%+v\n===expected===\n%+v",
					actual, when.UpdateAnnotationsReturn,
				)
			}
		}
	}

	t.Run("when client return plan detail, it return that detail", theory(
		When{
			Flag: annotate.Flag{
				Add:    []string{"key1=value1", "key2=value2"},
				Remove: []string{"key3=value3", "key4=value4"},
			},
			Args: map[string][]string{annotate.ARGS_PLAN_ID: {"test-plan-id"}},
			UpdateAnnotationsReturn: plans.Detail{
				Summary: plans.Summary{
					PlanId: "test-plan-id",
					Image:  &plans.Image{Repository: "repo.invalid/test-image", Tag: "test-version"},
					Annotations: plans.Annotations{
						{Key: "key1", Value: "value1"},
						{Key: "key2", Value: "value2"},
					},
				},
				Active: true,
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
			},
			UpdateAnnotationsError: nil,
		},
		Then{
			UpdateAnnotationsArgsPlanId: "test-plan-id",

			UpdateAnnotationsArgsChange: plans.AnnotationChange{
				Add:    plans.Annotations{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}},
				Remove: plans.Annotations{{Key: "key3", Value: "value3"}, {Key: "key4", Value: "value4"}},
			},
		},
	))

	{
		wantErr := errors.New("test-error")
		t.Run("when client return error, it return that error", theory(
			When{
				Flag: annotate.Flag{
					Add:    []string{"key1=value1", "key2=value2"},
					Remove: []string{"key3=value3", "key4=value4"},
				},
				Args:                    map[string][]string{annotate.ARGS_PLAN_ID: {"test-plan-id"}},
				UpdateAnnotationsReturn: plans.Detail{},
				UpdateAnnotationsError:  wantErr,
			},
			Then{
				UpdateAnnotationsArgsPlanId: "test-plan-id",

				UpdateAnnotationsArgsChange: plans.AnnotationChange{
					Add:    plans.Annotations{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}},
					Remove: plans.Annotations{{Key: "key3", Value: "value3"}, {Key: "key4", Value: "value4"}},
				},
				Err: wantErr,
			},
		))
	}
}
