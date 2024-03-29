package args

import (
	"errors"
	"fmt"
)

type Arg struct {
	// name of the argument
	Name string

	// help message
	Help string

	// set true if the argument is mandatory
	Required bool

	// set true if the argument is repeatable
	Repeatable bool
}

func (a Arg) String() string {
	str := a.Name
	if a.Repeatable {
		str += "..."
	}
	if a.Required {
		str = "<" + str + ">"
	} else {
		str = "[" + str + "]"
	}
	return str
}

type Args []Arg

func (a Args) String() string {
	str := ""
	for _, arg := range a {
		str += " " + arg.String()
	}
	return str
}

func New(args ...Arg) Args {
	return args
}

var ErrArgs = errors.New("arguments error")
var ErrNotEnough = fmt.Errorf("%w: not enough", ErrArgs)
var ErrTooMany = fmt.Errorf("%w: too many", ErrArgs)

func (args Args) Parse(argv []string) (map[string][]string, error) {
	argvMap := map[string][]string{}
	mandatories := 0
	for _, a := range args {
		if a.Required {
			mandatories++
		}
		argvMap[a.Name] = []string{}
	}

	rest := argv[:]
	dest := args[:]
	for mandatories < len(rest) {
		val := rest[0]
		if len(dest) == 0 {
			return nil, ErrTooMany
		}
		d := dest[0]
		argvMap[d.Name] = append(argvMap[d.Name], val)
		if d.Required && len(argvMap[d.Name]) == 1 {
			mandatories--
		}

		rest = rest[1:]
		if !d.Repeatable {
			dest = dest[1:]
		}
	}
	for _, d := range dest {
		if !d.Required {
			continue
		}
		if d.Repeatable && 0 < len(argvMap[d.Name]) {
			continue
		}
		if len(rest) == 0 {
			return nil, ErrNotEnough
		}
		argvMap[d.Name] = append(argvMap[d.Name], rest[0])
		rest = rest[1:]
	}

	if 0 < len(rest) {
		return nil, ErrTooMany
	}

	return argvMap, nil
}
