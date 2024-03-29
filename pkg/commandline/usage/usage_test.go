package usage_test

import (
	"flag"
	"testing"

	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"github.com/opst/knitfab/pkg/utils/try"
)

type MockFlags struct {
	StrFlag string `flag:"str"`
	IntFlag int    `flag:"int"`
}

func TestUsage(t *testing.T) {
	testee := usage.New(
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

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	testee.SetFlags(fs)
	if err := fs.Parse([]string{
		"--str", "string value",
		"--int", "123",
		"a", "b", "c", "d", "e",
	}); err != nil {
		t.Fatal(err)
	}

	actual := try.To(testee.Parse(fs.Args())).OrFatal(t)

	{
		expected := MockFlags{
			StrFlag: "string value",
			IntFlag: 123,
		}

		if actual.Flags != expected {
			t.Errorf("flags:\n  expected: %+v, actual: %+v", expected, actual.Flags)
		}
	}

	{
		expected := map[string][]string{
			"arg1": {"a"},
			"arg2": {"b", "c", "d"},
			"arg3": {"e"},
		}

		if !cmp.MapEqWith(actual.Args, expected, cmp.SliceEq[string]) {
			t.Errorf("args:\n  expected: %+v, actual: %+v", expected, actual.Args)
		}
	}
}
