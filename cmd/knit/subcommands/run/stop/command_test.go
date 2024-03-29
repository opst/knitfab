package stop_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab/cmd/knit/env"
	krst_mock "github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	run_stop "github.com/opst/knitfab/cmd/knit/subcommands/run/stop"
	apirun "github.com/opst/knitfab/pkg/api/types/runs"
	"github.com/opst/knitfab/pkg/commandline/usage"
)

func TestCommand_WithFail(t *testing.T) {

	type When struct {
		runId string
		err   error
	}

	theory := func(when When) func(*testing.T) {
		return func(t *testing.T) {
			client := krst_mock.New(t)
			client.Impl.Abort = func(
				ctx context.Context,
				runId string,
			) (apirun.Detail, error) {
				if runId != when.runId {
					t.Errorf("expected %+v, got %+v", when.runId, runId)
				}
				return apirun.Detail{}, when.err
			}

			l := logger.Null()

			testee := run_stop.New()
			actual := testee.Execute(
				context.Background(),
				l,
				*env.New(),
				client,
				usage.FlagSet[run_stop.Flag]{
					Flags: run_stop.Flag{
						Fail: true,
					},
					Args: map[string][]string{
						run_stop.ARG_RUNID: {when.runId},
					},
				},
			)

			if !errors.Is(actual, when.err) {
				t.Errorf("expected %+v, got %+v", when.err, actual)
			}
		}
	}

	t.Run("when client returns error, it should returns the error", theory(
		When{
			runId: "runId",
			err:   errors.New("task error"),
		},
	))

	t.Run("when client returns no error, it should returns nil", theory(
		When{
			runId: "runId",
			err:   nil,
		},
	))
}

func TestCommand_WithoutFail(t *testing.T) {

	type When struct {
		runId string
		err   error
	}

	theory := func(when When) func(*testing.T) {
		return func(t *testing.T) {
			client := krst_mock.New(t)
			client.Impl.Tearoff = func(
				ctx context.Context,
				runId string,
			) (apirun.Detail, error) {
				if runId != when.runId {
					t.Errorf("expected %+v, got %+v", when.runId, runId)
				}
				return apirun.Detail{}, when.err
			}

			l := logger.Null()

			testee := run_stop.New()
			actual := testee.Execute(
				context.Background(),
				l,
				*env.New(),
				client,
				usage.FlagSet[run_stop.Flag]{
					Flags: run_stop.Flag{
						Fail: false,
					},
					Args: map[string][]string{
						run_stop.ARG_RUNID: {when.runId},
					},
				},
			)

			if !errors.Is(actual, when.err) {
				t.Errorf("expected %+v, got %+v", when.err, actual)
			}
		}
	}

	t.Run("when client returns error, it should returns the error", theory(
		When{
			runId: "runId",
			err:   errors.New("task error"),
		},
	))

	t.Run("when client returns no error, it should returns nil", theory(
		When{
			runId: "runId",
			err:   nil,
		},
	))
}
