package rm_test

import (
	"context"
	"errors"
	"testing"

	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	run_rm "github.com/opst/knitfab/cmd/knit/subcommands/run/rm"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestDeleteCommand(t *testing.T) {
	type when struct {
		runId []string
		err   error
	}

	type then struct {
		err error
	}

	theory := func(when when, then then) func(*testing.T) {
		return func(t *testing.T) {
			profile := &kprof.KnitProfile{ApiRoot: "http://api.knit.invalid"}
			client := try.To(krst.NewClient(profile)).OrFatal(t)

			task := func(
				ctx context.Context,
				client krst.KnitClient,
				runId string,
			) error {
				return when.err
			}

			testee := run_rm.New(run_rm.WithTask(task))

			ctx := context.Background()
			err := testee.Execute(
				ctx,
				logger.Null(),
				*kenv.New(),
				client,
				usage.FlagSet[struct{}]{
					Flags: struct{}{},
					Args: map[string][]string{
						run_rm.ARG_RUNID: when.runId,
					},
				},
			)

			if !errors.Is(err, then.err) {
				t.Errorf(
					"wrong status: (actual, expected) != (%d, %d)",
					err, then.err,
				)
			}
		}
	}
	t.Run("when it is passed existed runId, it should return exitsuccess", theory(
		when{
			runId: []string{"test-Id"},
			err:   nil,
		},
		then{
			err: nil,
		},
	))
	{
		expectedError := errors.New("fake error")
		t.Run("when error is caused in client, it returns the error", theory(
			when{
				runId: []string{"test-Id"},
				err:   expectedError,
			},
			then{
				err: expectedError,
			},
		))
	}
}

func TestRunDeleteRun(t *testing.T) {
	t.Run("When client does not cause any error, it should return the content returned by client as is", func(t *testing.T) {
		ctx := context.Background()
		mock := mock.New(t)
		mock.Impl.DeleteRun = func(ctx context.Context, runId string) error {
			return nil
		}

		err := run_rm.RunDeleteRun(ctx, mock, "test-runId")
		if err != nil {
			t.Fatalf("RunShowRunforLog returns error unexpectedly: %s (%+v)", err.Error(), err)
		}
	})

	t.Run("when client returns error, it should return the error as is", func(t *testing.T) {
		ctx := context.Background()
		mock := mock.New(t)
		expectedError := errors.New("fake error")
		mock.Impl.DeleteRun = func(ctx context.Context, runId string) error {
			return expectedError
		}

		err := run_rm.RunDeleteRun(ctx, mock, "test-runId")
		if !errors.Is(err, expectedError) {
			t.Errorf("returned error is not expected one: %+v", err)
		}
	})
}
