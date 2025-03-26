package purge_test

import (
	"context"
	"errors"
	"io"
	"testing"

	kenv "github.com/opst/knitfab/v2/cmd/knit/env"
	"github.com/opst/knitfab/v2/cmd/knit/rest/mock"
	"github.com/opst/knitfab/v2/cmd/knit/subcommands/data/purge"
	"github.com/opst/knitfab/v2/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/v2/cmd/knit/subcommands/logger"
)

func TestTask(t *testing.T) {

	type When struct {
		knitId string
		err    error
	}

	type Then struct {
		wantError error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			mockClient := mock.New(t)
			mockClient.Impl.PurgeData = func(ctx context.Context, knitId string) error {
				if knitId != when.knitId {
					t.Errorf("unexpected knitId: want %s, got %s", when.knitId, knitId)
				}
				return when.err
			}

			testee := purge.Task()
			err := testee(
				context.Background(),
				logger.Null(),
				*kenv.New(),
				mockClient,
				commandline.MockCommandline[struct{}]{
					Fullname_: "knit data purge",
					Stdout_:   io.Discard,
					Stderr_:   io.Discard,
					Args_: map[string][]string{
						purge.KNIT_ID: {when.knitId},
					},
				},
				[]any{},
			)

			if !errors.Is(err, then.wantError) {
				t.Errorf("unexpected error: want %v, got %v", then.wantError, err)
			}
		}
	}

	t.Run("When success, Then no error", theory(
		When{
			knitId: "knit-id",
			err:    nil,
		},
		Then{
			wantError: nil,
		},
	))

	wantErr := errors.New("purge error")
	t.Run("When purge error, Then error", theory(
		When{
			knitId: "knit-id",
			err:    wantErr,
		},
		Then{
			wantError: wantErr,
		},
	))
}
