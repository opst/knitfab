package rm_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	run_rm "github.com/opst/knitfab/cmd/knit/subcommands/run/rm"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestDeleteCommand(t *testing.T) {
	type when struct {
		runId []string
		err   error
	}

	type then struct {
		runId string
		err   error
	}

	theory := func(when when, then then) func(*testing.T) {
		return func(t *testing.T) {
			profile := &kprof.KnitProfile{ApiRoot: "http://api.knit.invalid"}
			client := try.To(krst.NewClient(profile)).OrFatal(t)

			removeMock := func(
				ctx context.Context,
				client krst.KnitClient,
				runId string,
			) error {
				if runId != then.runId {
					t.Errorf("runId: got %s, but want %s", runId, then.runId)
				}
				return when.err
			}

			testee := run_rm.Task(removeMock)

			stdout := new(strings.Builder)
			stderr := new(strings.Builder)

			ctx := context.Background()
			err := testee(
				ctx,
				logger.Null(),
				*kenv.New(),
				client,
				commandline.MockCommandline[struct{}]{
					Fullname_: "knit run rm",
					Stdout_:   stdout,
					Stderr_:   stderr,
					Flags_:    struct{}{},
					Args_: map[string][]string{
						run_rm.ARG_RUNID: when.runId,
					},
				},
				[]any{},
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
			err:   nil,
			runId: "test-Id",
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
				err:   expectedError,
				runId: "test-Id",
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
