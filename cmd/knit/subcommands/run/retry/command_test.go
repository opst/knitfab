package retry_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	"github.com/opst/knitfab/cmd/knit/subcommands/run/retry"
)

func TestRetry(t *testing.T) {

	theory := func(clientError error) func(*testing.T) {
		return func(t *testing.T) {
			// Given
			ctx := context.Background()
			kc := mock.New(t)
			kc.Impl.Retry = func(ctx context.Context, runId string) error {
				return clientError
			}

			testee := retry.Task
			logger := logger.Null()

			stdout := new(strings.Builder)
			stderr := new(strings.Builder)

			// When
			err := testee(
				ctx, logger, env.KnitEnv{}, kc,
				commandline.MockCommandline[struct{}]{
					Stdout_: stdout,
					Stderr_: stderr,
					Args_: map[string][]string{
						retry.ARG_RUNID: {"given-run-id"},
					},
				},
				[]any{},
			)

			// Then
			if !errors.Is(err, clientError) {
				t.Errorf("unexpected error: got %+v, want %+v", err, clientError)
			}
		}
	}

	t.Run("on client returns error, command also return it", theory(
		errors.New("client error"),
	))

	t.Run("on client returns nil, command also return nil", theory(nil))

}
