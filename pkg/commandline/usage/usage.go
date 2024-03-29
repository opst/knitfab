package usage

import (
	"flag"
	"strings"

	"github.com/opst/knitfab/pkg/commandline/flag/args"
	"github.com/opst/knitfab/pkg/commandline/flag/flagger"
)

// Flags and arguments definition
type Usage[T any] struct {
	// Flag specification.
	//
	// Struct T's field with tag "flag" is used as flag name.
	//
	// Returned value is passed to flagger.New .
	// For more details, see flagger.New .
	//
	// # Example
	//
	// Let T be:
	//
	//	type MyFlags struct {
	//	    Flag1 string `flag:"flag1,help=message for flag1"`
	//	    Flag2 int    `flag:"custom-flag-name"`
	//	    Flag3 string `flag:""` // flag name is "flag3" after the field name,
	//	}
	//
	// As you show, you can specify flag name and help by tag.
	// When name is omitted, flag name is field name in lower-kebab-case.
	//
	// And, let someKnitCommand be:
	//
	//	func (someKnitCommand) Flags() MyFlags {
	//		return MyFlags{
	//			Flag1: "default value",
	//			Flag2: 100,
	// 			Falg3: "more default value",
	//		}
	//	}
	//
	// Then, someKnitCommand.Flags() accepts flag like
	//
	//	COMMAND --flag1=VALUE --custom-flag-name=VALUE --flag3=VALUE
	//
	// Given flags are passed to Execute as T.
	//
	f *flagger.Flagger[T]

	// positional arguments specification.
	args args.Args
}

func (u Usage[T]) Args() args.Args {
	return u.args
}

func (u Usage[T]) Flags() []flagger.Flag {
	return u.f.Flags
}

func (u Usage[T]) SetFlags(fls *flag.FlagSet) {
	u.f.SetFlags(fls)
}

func (u Usage[T]) String() string {
	return strings.TrimSpace(u.f.String() + " " + u.args.String())
}

// Parse argv and return parsed flags and positional arguments.
//
// If argv is too many/too less, return error.
//
// Before calling this method,
// you should call `Parse()` of FlagSet passed to `SetFlag`.
//
// # Example
//
//	flags := flag.NewFlagSet("command", flag.ExitOnError)
//	usage := New(..., ArgSpec{...})
//	usage.SetFlags(flags)
//	flags.Parse(os.Args[1:])
//	parsed, err := usage.Parse(os.Args[1:])
func (u Usage[T]) Parse(argv []string) (FlagSet[T], error) {
	flags, err := u.args.Parse(argv)
	if err != nil {
		return FlagSet[T]{Flags: *u.f.Values, Args: nil}, err
	}

	return FlagSet[T]{Flags: *u.f.Values, Args: flags}, nil
}

// Args is positional arguments specification.
type Args []args.Arg

// Build new Usage.
func New[T any](flag T, a Args) Usage[T] {
	return Usage[T]{
		f:    flagger.New(flag),
		args: args.New(a...),
	}
}

// Parsed flags and positional arguments.
type FlagSet[T any] struct {
	// Parsed flags.
	Flags T

	// Parsed positional arguments.
	Args map[string][]string
}
