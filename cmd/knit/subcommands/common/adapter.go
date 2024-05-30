package common

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/opst/knitfab/cmd/knit/config/profiles"
	"github.com/opst/knitfab/cmd/knit/env"
	krest "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/youta-t/flarc"
)

type KnitTaskWithCommonFlag[T any] func(
	ctx context.Context,
	logger *log.Logger,
	commonFlag CommonFlags,
	cl flarc.Commandline[T],
	params []any,
) error

func NewTaskWithCommonFlag[T any](task KnitTaskWithCommonFlag[T]) flarc.Task[T] {
	return func(ctx context.Context, cl flarc.Commandline[T], pos []any) error {
		var commonFlag CommonFlags
		found := false
		newpos := make([]any, 0, len(pos))
		for _, p := range pos {
			switch v := p.(type) {
			case CommonFlags:
				found = true
				commonFlag = v
			default:
				newpos = append(newpos, p)
			}
		}
		if !found {
			return errors.New("programming error: common flags not found")
		}

		logger := log.New(cl.Stderr(), "", log.LstdFlags)
		logger.SetPrefix(fmt.Sprintf("[%s] ", cl.Fullname()))

		return task(
			ctx,
			logger,
			commonFlag,
			cl,
			newpos,
		)
	}
}

type Task[T any] func(
	ctx context.Context,
	logger *log.Logger,
	knitEnv env.KnitEnv,
	client krest.KnitClient,
	cl flarc.Commandline[T],
	params []any,
) error

func NewTask[T any](task Task[T]) flarc.Task[T] {

	return NewTaskWithCommonFlag(func(
		ctx context.Context,
		logger *log.Logger,
		commonFlag CommonFlags,
		cl flarc.Commandline[T],
		params []any,
	) error {
		profile, err := profiles.LoadProfileStore(commonFlag.ProfileStore)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf(
					"%w: knitprofile store (%s) is not found. Please try `knit init` first. Ask your admin to get knitprofile",
					err, commonFlag.ProfileStore,
				)
			}
			return fmt.Errorf(
				"%w: failed to load knitprofile store (%s)",
				err, commonFlag.ProfileStore,
			)
		}
		prof, ok := profile[commonFlag.Profile]
		if !ok {
			return fmt.Errorf(
				"profile '%s' not found in the profile store (%s)",
				commonFlag.Profile, commonFlag.ProfileStore,
			)
		}

		e, err := env.LoadKnitEnv(commonFlag.Env)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%w: failed to load knitenv", err)
			}
		}

		client, err := krest.NewClient(prof)
		if err != nil {
			return fmt.Errorf(
				"%w: failed to create knit client. Your knitprofile (%s in %s) can be broken.\n\nRemove it and try `knit init` again. Ask your admin to get knitprofile",
				err, commonFlag.Profile, commonFlag.ProfileStore,
			)
		}
		return task(ctx, logger, *e, client, cl, params)
	})
}
