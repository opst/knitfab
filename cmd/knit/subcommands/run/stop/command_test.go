package stop_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/knit/env"
	krst_mock "github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	run_stop "github.com/opst/knitfab/cmd/knit/subcommands/run/stop"
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
			) (runs.Detail, error) {
				if runId != when.runId {
					t.Errorf("expected %+v, got %+v", when.runId, runId)
				}
				return runs.Detail{}, when.err
			}

			l := logger.Null()

			testee := run_stop.Task()

			stdout := new(strings.Builder)
			stderr := new(strings.Builder)
			actual := testee(
				context.Background(),
				l,
				*env.New(),
				client,
				commandline.MockCommandline[run_stop.Flag]{
					Fullname_: "knit run stop",
					Stdout_:   stdout,
					Stderr_:   stderr,
					Flags_:    run_stop.Flag{Fail: true},
					Args_: map[string][]string{
						run_stop.ARG_RUNID: {when.runId},
					},
				},
				[]any{},
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
			) (runs.Detail, error) {
				if runId != when.runId {
					t.Errorf("expected %+v, got %+v", when.runId, runId)
				}
				return runs.Detail{}, when.err
			}

			l := logger.Null()

			testee := run_stop.Task()

			stdout := new(strings.Builder)
			stderr := new(strings.Builder)

			actual := testee(
				context.Background(),
				l,
				*env.New(),
				client,
				commandline.MockCommandline[run_stop.Flag]{
					Fullname_: "knit run stop",
					Stdout_:   stdout,
					Stderr_:   stderr,
					Flags_:    run_stop.Flag{Fail: false},
					Args_: map[string][]string{
						run_stop.ARG_RUNID: {when.runId},
					},
				},
				[]any{},
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
