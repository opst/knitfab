package command_test

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"log"

	"github.com/google/subcommands"
	knit "github.com/opst/knitfab/cmd/knit/commandline/command"
	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	apitags "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"github.com/opst/knitfab/pkg/utils/try"
)

type MockFlags struct {
	StrFlag string `flag:"str"`
	IntFlag int    `flag:"int"`
}

type MockCommand struct {
	task func(
		context.Context, *log.Logger, env.KnitEnv, krst.KnitClient, usage.FlagSet[MockFlags],
	) error
}

func (MockCommand) Name() string {
	return "mock"
}

func (MockCommand) Help() knit.Help {
	return knit.Help{
		Synopsis: "mock",
	}
}

func (MockCommand) Usage() usage.Usage[MockFlags] {
	return usage.New(
		MockFlags{
			StrFlag: "default",
			IntFlag: 42,
		},
		usage.Args{
			{Name: "arg1", Required: true},
			{Name: "arg2", Repeatable: true},
			{Name: "arg3", Required: true},
		},
	)
}

func (mc MockCommand) Execute(
	ctx context.Context,
	l *log.Logger,
	e env.KnitEnv,
	c krst.KnitClient,
	flags usage.FlagSet[MockFlags],
) error {
	return mc.task(ctx, l, e, c, flags)
}

func TestKnitCommand(t *testing.T) {
	t.Run("it loads env (by default), and parse flags/argv", func(t *testing.T) {
		ctx := context.Background()

		logdest := new(strings.Builder)
		l := log.New(logdest, "", 0)

		invoked := false
		testee := MockCommand{
			task: func(
				ctx context.Context, l *log.Logger, e env.KnitEnv, c krst.KnitClient, flags usage.FlagSet[MockFlags],
			) error {
				invoked = true
				expectedTags := []apitags.Tag{
					{Key: "project", Value: "test"},
					{Key: "example", Value: "tag"},
				}
				if !cmp.SliceContentEq(e.Tags(), expectedTags) {
					t.Errorf("env.tags\nwant: %+v\n got: %v+", expectedTags, e.Tags())
				}

				expectedFlags := MockFlags{
					StrFlag: "string value",
					IntFlag: 200,
				}
				if flags.Flags != expectedFlags {
					t.Errorf("flags\nwant: %+v\n got: %+v", expectedFlags, flags.Flags)
				}

				expectedArgv := map[string][]string{
					"arg1": {"a"},
					"arg2": {"b", "c", "d"},
					"arg3": {"e"},
				}

				if !cmp.MapEqWith(flags.Args, expectedArgv, cmp.SliceEq[string]) {
					t.Errorf("argv\nwant: %+v\n got: %+v", expectedArgv, flags.Args)
				}

				return nil
			},
		}

		cmd := knit.Build[MockFlags](
			testee,
			knit.CommonFlags{
				Profile:      "test",
				ProfileStore: "./testdata/home/.knit/profile",
				Env:          "./testdata/current/knitenv",
			},
		)
		f := flag.NewFlagSet("test", flag.ContinueOnError)

		cmd.SetFlags(f)
		f.Parse([]string{
			"--str", "string value",
			"--int", "200",
			"a", "b", "c", "d", "e",
		})
		ret := cmd.Execute(ctx, f, l)

		if !invoked {
			t.Errorf("task is not invoked")
		}

		if ret != subcommands.ExitSuccess {
			t.Errorf("wrong status: %d, want: %d", ret, subcommands.ExitSuccess)
		}
	})

	t.Run("it loads env (via flag), and parse flags/argv", func(t *testing.T) {
		ctx := context.Background()

		logdest := new(strings.Builder)
		l := log.New(logdest, "", 0)

		invoked := false
		testee := MockCommand{
			task: func(
				ctx context.Context, l *log.Logger, e env.KnitEnv, c krst.KnitClient, flags usage.FlagSet[MockFlags],
			) error {
				invoked = true
				expectedTags := []apitags.Tag{
					{Key: "project", Value: "test"},
					{Key: "example", Value: "tag"},
				}
				if !cmp.SliceContentEq(e.Tags(), expectedTags) {
					t.Errorf("env.tags\nwant: %+v\n got: %+v", expectedTags, e.Tags())
				}

				expectedFlags := MockFlags{
					StrFlag: "string value",
					IntFlag: 200,
				}
				if flags.Flags != expectedFlags {
					t.Errorf("flags\nwant: %+v\n got: %+v", expectedFlags, flags.Flags)
				}

				expectedArgv := map[string][]string{
					"arg1": {"a"},
					"arg2": {"b", "c", "d"},
					"arg3": {"e"},
				}

				if !cmp.MapEqWith(flags.Args, expectedArgv, cmp.SliceEq[string]) {
					t.Errorf("argv\nwant: %+v\n got: %+v", expectedArgv, flags.Args)
				}

				return nil
			},
		}

		cmd := knit.Build[MockFlags](testee, knit.CommonFlags{})
		f := flag.NewFlagSet("test", flag.ContinueOnError)

		cmd.SetFlags(f)
		f.Parse([]string{
			"--env", "./testdata/current/knitenv",
			"--profile-store", "./testdata/home/.knit/profile",
			"--profile", "test",
			"--str", "string value",
			"--int", "200",
			"a", "b", "c", "d", "e",
		})
		ret := cmd.Execute(ctx, f, l)

		if !invoked {
			t.Errorf("task is not invoked")
		}

		if ret != subcommands.ExitSuccess {
			t.Errorf("wrong status: %d, want: %d", ret, subcommands.ExitSuccess)
		}
	})

	t.Run("logger passed when Execute is called is used", func(t *testing.T) {
		ctx := context.Background()

		logdest := new(strings.Builder)
		l := log.New(logdest, "super", 0)

		testee := MockCommand{
			task: func(
				ctx context.Context, l *log.Logger, e env.KnitEnv, c krst.KnitClient, flags usage.FlagSet[MockFlags],
			) error {
				l.Println("hello")
				return nil
			},
		}

		cmd := knit.Build[MockFlags](
			testee,
			knit.CommonFlags{
				Profile:      "test",
				ProfileStore: "./testdata/home/.knit/profile",
				Env:          "./testdata/current/knitenv",
			},
		)
		f := flag.NewFlagSet("test", flag.ContinueOnError)

		cmd.SetFlags(f)
		f.Parse([]string{
			"--str", "string value",
			"--int", "200",
			"a", "b", "c", "d", "e",
		})

		cmd.Execute(ctx, f, l)

		expectedLog := "[super mock] hello\n"
		if logdest.String() != expectedLog {
			t.Errorf("wrong log:\n%s\n want: %s", logdest.String(), expectedLog)
		}
	})

	t.Run("it returns error if profile is not found", func(t *testing.T) {
		ctx := context.Background()

		logdest := new(strings.Builder)
		l := log.New(logdest, "", 0)
		invoked := false
		testee := MockCommand{
			task: func(
				ctx context.Context, l *log.Logger, e env.KnitEnv, c krst.KnitClient, flags usage.FlagSet[MockFlags],
			) error {
				invoked = true
				return nil
			},
		}

		cmd := knit.Build[MockFlags](
			testee,
			knit.CommonFlags{
				Profile:      "no-such-profile",
				ProfileStore: "./testdata/home/.knit/profile",
				Env:          "./testdata/current/knitenv",
			},
		)
		f := flag.NewFlagSet("test", flag.ContinueOnError)

		cmd.SetFlags(f)
		f.Parse([]string{
			"--str", "string value",
			"--int", "200",
			"a", "b", "c", "d", "e",
		})

		ret := cmd.Execute(ctx, f, l)

		if invoked {
			t.Errorf("task is invoked")
		}
		if ret != subcommands.ExitFailure {
			t.Errorf("wrong status: %d, want: %d", ret, subcommands.ExitFailure)
		}
	})

	t.Run("it returns error if profile is not found (via flag)", func(t *testing.T) {
		ctx := context.Background()

		logdest := new(strings.Builder)
		l := log.New(logdest, "", 0)
		invoked := false
		testee := MockCommand{
			task: func(
				ctx context.Context, l *log.Logger, e env.KnitEnv, c krst.KnitClient, flags usage.FlagSet[MockFlags],
			) error {
				invoked = true
				return nil
			},
		}

		cmd := knit.Build[MockFlags](
			testee,
			knit.CommonFlags{
				Env: "./testdata/current/knitenv",
			},
		)
		f := flag.NewFlagSet("test", flag.ContinueOnError)

		cmd.SetFlags(f)
		f.Parse([]string{
			"--profile-store", "./testdata/home/.knit/profile",
			"--profile", "no-such-profile",
			"--str", "string value",
			"--int", "200",
			"a", "b", "c", "d", "e",
		})

		ret := cmd.Execute(ctx, f, l)

		if invoked {
			t.Errorf("task is invoked")
		}
		if ret != subcommands.ExitFailure {
			t.Errorf("wrong status: %d, want: %d", ret, subcommands.ExitFailure)
		}
	})

	t.Run("it returns ExitFailure if task returns error", func(t *testing.T) {
		ctx := context.Background()

		logdest := new(strings.Builder)
		l := log.New(logdest, "", 0)
		invoked := false
		testee := MockCommand{
			task: func(
				ctx context.Context, l *log.Logger, e env.KnitEnv, c krst.KnitClient, flags usage.FlagSet[MockFlags],
			) error {
				invoked = true
				return errors.New("fake error")
			},
		}

		cmd := knit.Build[MockFlags](
			testee,
			knit.CommonFlags{
				Profile:      "test",
				ProfileStore: "./testdata/home/.knit/profile",
				Env:          "./testdata/current/knitenv",
			},
		)
		f := flag.NewFlagSet("test", flag.ContinueOnError)

		cmd.SetFlags(f)
		f.Parse([]string{
			"--str", "string value",
			"--int", "200",
			"a", "b", "c", "d", "e",
		})

		ret := cmd.Execute(ctx, f, l)

		if !invoked {
			t.Errorf("task is not invoked")
		}
		if ret != subcommands.ExitFailure {
			t.Errorf("wrong status: %d, want: %d", ret, subcommands.ExitFailure)
		}
	})

	t.Run("it returns ExitUsageError if invoked with wrong positional arguments", func(t *testing.T) {
		ctx := context.Background()

		logdest := new(strings.Builder)
		l := log.New(logdest, "", 0)
		invoked := false
		testee := MockCommand{
			task: func(
				ctx context.Context, l *log.Logger, e env.KnitEnv, c krst.KnitClient, flags usage.FlagSet[MockFlags],
			) error {
				invoked = true
				return nil
			},
		}

		cmd := knit.Build[MockFlags](
			testee,
			knit.CommonFlags{
				Profile:      "test",
				ProfileStore: "./testdata/home/.knit/profile",
				Env:          "./testdata/current/knitenv",
			},
		)
		f := flag.NewFlagSet("test", flag.ContinueOnError)

		cmd.SetFlags(f)
		f.Parse([]string{
			"--str", "string value",
			"--int", "200",
			"a",
		})

		{
			// discard STDERR
			pw, pr, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer pr.Close()
			defer pw.Close()

			origErr := os.Stderr
			defer func() { os.Stderr = origErr }()
			os.Stderr = pw
		}
		ret := cmd.Execute(ctx, f, l)

		if invoked {
			t.Errorf("task is invoked")
		}
		if ret != subcommands.ExitUsageError {
			t.Errorf("wrong status: %d, want: %d", ret, subcommands.ExitUsageError)
		}
	})

	t.Run("it returns ExitUsageError if task returns ErrUsage", func(t *testing.T) {
		ctx := context.Background()

		logdest := new(strings.Builder)
		l := log.New(logdest, "", 0)
		invoked := false
		testee := MockCommand{
			task: func(
				ctx context.Context, l *log.Logger, e env.KnitEnv, c krst.KnitClient, flags usage.FlagSet[MockFlags],
			) error {
				invoked = true
				return knit.ErrUsage
			},
		}

		cmd := knit.Build[MockFlags](
			testee,
			knit.CommonFlags{
				Profile:      "test",
				ProfileStore: "./testdata/home/.knit/profile",
				Env:          "./testdata/current/knitenv",
			},
		)
		f := flag.NewFlagSet("test", flag.ContinueOnError)

		cmd.SetFlags(f)
		f.Parse([]string{
			"--str", "string value",
			"--int", "200",
			"a", "b", "c", "d", "e",
		})

		{
			// discard STDERR
			pw, pr, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer pr.Close()
			defer pw.Close()

			origErr := os.Stderr
			defer func() { os.Stderr = origErr }()
			os.Stderr = pw
		}

		ret := cmd.Execute(ctx, f, l)

		if !invoked {
			t.Errorf("task is not invoked")
		}
		if ret != subcommands.ExitUsageError {
			t.Errorf("wrong status: %d, want: %d", ret, subcommands.ExitUsageError)
		}
	})

}

func TestDefaultCommonFlags(t *testing.T) {
	t.Run("it returns default value from given directory", func(t *testing.T) {
		cf := try.To(knit.DefaultCommonFlags(
			"./testdata/current",
			knit.WithHome("./testdata/home"),
		)).OrFatal(t)

		if try.To(filepath.Abs(cf.ProfileStore)).OrFatal(t) != try.To(filepath.Abs("./testdata/home/.knit/profile")).OrFatal(t) {
			t.Errorf("wrong profile store: %s", cf.ProfileStore)
		}

		if cf.Profile != "test" {
			t.Errorf("wrong profile: %s", cf.Profile)
		}

		if cf.Env != try.To(filepath.Abs("./testdata/current/knitenv")).OrFatal(t) {
			t.Errorf("wrong env: %s", cf.Env)
		}
	})

	t.Run("it returns default value from ancestors of given directory", func(t *testing.T) {
		cf := try.To(knit.DefaultCommonFlags(
			"./testdata/current/children/folder",
			knit.WithHome("./testdata/home"),
		)).OrFatal(t)

		if try.To(filepath.Abs(cf.ProfileStore)).OrFatal(t) != try.To(filepath.Abs("./testdata/home/.knit/profile")).OrFatal(t) {
			t.Errorf("wrong profile store: %s", cf.ProfileStore)
		}

		if cf.Profile != "test" {
			t.Errorf("wrong profile: %s", cf.Profile)
		}

		if cf.Env != try.To(filepath.Abs("./testdata/current/knitenv")).OrFatal(t) {
			t.Errorf("wrong env: %s", cf.Env)
		}
	})
}
