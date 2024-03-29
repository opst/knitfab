package args_test

import (
	"errors"
	"flag"
	"strconv"
	"testing"

	"github.com/opst/knitfab/pkg/utils/args"
)

type Even int

func AsEven(s string) (Even, error) {
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	if v%2 != 0 {
		return 0, errors.New("odd number!")
	}

	return Even(v), nil
}

func (e Even) String() string {
	return strconv.Itoa(int(e))
}

func TestArgs(t *testing.T) {
	t.Run("when it parses an acceptable value, parsing success", func(t *testing.T) {
		testee := args.Parser(AsEven)
		if testee.IsSet() {
			t.Error("it is set, unexpectedly")
		}
		if zero := new(Even); testee.Value() != *zero {
			t.Error("it is not initialized with zero value: ", testee.Value())
		}

		f := flag.NewFlagSet("test", flag.ContinueOnError)
		f.Var(testee, "arg", "")

		if err := f.Parse([]string{"-arg", "12"}); err != nil {
			t.Fatal(err)
		}

		expected := 12
		if testee.Value() != Even(expected) {
			t.Errorf("unmatch: Value(): (actual, expected) = (%d, %d)", testee.Value(), expected)
		}

		if !testee.IsSet() {
			t.Error("it is not set")
		}
	})

	t.Run("when it parses an unacceptable value, parsing errors", func(t *testing.T) {
		testee := args.Parser(AsEven)

		f := flag.NewFlagSet("test", flag.ContinueOnError)
		f.Var(testee, "arg", "")

		if err := f.Parse([]string{"-arg", "1"}); err == nil {
			t.Error("expected error does not happen")
		}

		if testee.IsSet() {
			t.Error("it is set, unexpectedly")
		}
	})
}
