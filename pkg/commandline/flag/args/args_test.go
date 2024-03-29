package args_test

import (
	"errors"
	"testing"

	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/commandline/flag/args"
)

func TestArgs(t *testing.T) {

	type When struct {
		testee args.Args
		argv   []string
	}

	type Then struct {
		argvMap map[string][]string
		err     error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			argvMap, err := when.testee.Parse(when.argv)
			if !errors.Is(err, then.err) {
				t.Errorf("expected: %v, got: %v", then.err, err)
			}
			if !cmp.MapEqWith(argvMap, then.argvMap, cmp.SliceEq[string]) {
				t.Errorf("expected: %v, got: %v", then.argvMap, argvMap)
			}
		}
	}

	t.Run("parses args", theory(
		When{
			testee: args.New(
				args.Arg{Name: "arg1", Required: true},
				args.Arg{Name: "arg2"},
				args.Arg{Name: "arg3", Repeatable: true},
				args.Arg{Name: "arg4"},
				args.Arg{Name: "arg5", Required: true},
			),
			argv: []string{
				"a", "b", "c", "d", "e", "f", "g",
			},
		},
		Then{
			argvMap: map[string][]string{
				"arg1": {"a"},
				"arg2": {"b"},
				"arg3": {"c", "d", "e", "f"},
				"arg4": {},
				"arg5": {"g"},
			},
			err: nil,
		},
	))

	t.Run("parses args for single eager argument (required)", theory(
		When{
			testee: args.New(
				args.Arg{Name: "arg1", Repeatable: true, Required: true},
			),
			argv: []string{
				"a", "b", "c",
			},
		},
		Then{
			argvMap: map[string][]string{
				"arg1": {"a", "b", "c"},
			},
			err: nil,
		},
	))

	t.Run("parses args for single eager argument (optional)", theory(
		When{
			testee: args.New(
				args.Arg{Name: "arg1", Repeatable: true},
			),
			argv: []string{
				"a", "b", "c",
			},
		},
		Then{
			argvMap: map[string][]string{
				"arg1": {"a", "b", "c"},
			},
			err: nil,
		},
	))

	t.Run("skips optional repeated arg", theory(
		When{
			testee: args.New(
				args.Arg{Name: "arg1", Repeatable: true, Required: true},
				args.Arg{Name: "arg2", Repeatable: true},
				args.Arg{Name: "arg3", Repeatable: true, Required: true},
			),
			argv: []string{
				"a", "b",
			},
		},
		Then{
			argvMap: map[string][]string{
				"arg1": {"a"},
				"arg2": {},
				"arg3": {"b"},
			},
			err: nil,
		},
	))

	t.Run("`repeatable required` before `optional required` takes all arguments", theory(
		When{
			testee: args.New(
				args.Arg{Name: "arg1", Repeatable: true, Required: true},
				args.Arg{Name: "arg2", Repeatable: true},
			),
			argv: []string{
				"a", "b", "c",
			},
		},
		Then{
			argvMap: map[string][]string{
				"arg1": {"a", "b", "c"},
				"arg2": {},
			},
			err: nil,
		},
	))

	t.Run("`repeatable required` after `optional required` takes the last one arguments", theory(
		When{
			testee: args.New(
				args.Arg{Name: "arg1", Repeatable: true},
				args.Arg{Name: "arg2", Repeatable: true, Required: true},
			),
			argv: []string{
				"a", "b", "c",
			},
		},
		Then{
			argvMap: map[string][]string{
				"arg1": {"a", "b"},
				"arg2": {"c"},
			},
			err: nil,
		},
	))

	t.Run("too many args, it causes error", theory(
		When{
			testee: args.New(
				args.Arg{Name: "arg1", Required: true},
				args.Arg{Name: "arg2", Required: true},
			),
			argv: []string{
				"a", "b", "c",
			},
		},
		Then{
			argvMap: nil,
			err:     args.ErrTooMany,
		},
	))

	t.Run("not enough args, it causes error", theory(
		When{
			testee: args.New(
				args.Arg{Name: "arg1", Required: true},
				args.Arg{Name: "arg2", Required: true},
				args.Arg{Name: "arg3", Required: true},
			),
			argv: []string{
				"a", "b",
			},
		},
		Then{
			argvMap: nil,
			err:     args.ErrNotEnough,
		},
	))
}
