package strings_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/opst/knitfab/pkg/utils/cmp"
	kstr "github.com/opst/knitfab/pkg/utils/strings"
)

func TestTrimPefixAll(t *testing.T) {
	type when struct {
		s      string
		prefix string
	}

	for name, testcase := range map[string]struct {
		when when
		then string
	}{
		"when string has one prefix, it returns s without prefix": {
			when: when{
				s: "aaabbbccc", prefix: "aaab",
			},
			then: "bbccc",
		},
		"when string has repeated prefixes, it returns s without all prefix": {
			when: when{
				s: "aaabbbccc", prefix: "a",
			},
			then: "bbbccc",
		},
		"when string has same pattern with prefix in mid, it returns s without prefixes only": {
			when: when{
				s: "aaabbbaaacccaaa", prefix: "a",
			},
			then: "bbbaaacccaaa",
		},
		"when string has no prefix, it returns s without prefixes only": {
			when: when{
				s: "aaabbbaaacccaaa", prefix: "b",
			},
			then: "aaabbbaaacccaaa",
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual := kstr.TrimPrefixAll(testcase.when.s, testcase.when.prefix)
			if actual != testcase.then {
				t.Errorf("wrong result: (actual, expected) = (%s, %s)", actual, testcase.then)
			}
		})
	}
}

func TestSupplySuffix(t *testing.T) {
	type when struct {
		text   string
		suffix string
	}
	type testcase struct {
		when when
		then string
	}
	for name, testcase := range map[string]testcase{
		"when text does not have suffix, it returns text + suffix": {
			when: when{
				text:   "foobar",
				suffix: "baz",
			},
			then: "foobarbaz",
		},
		"when text has suffix, it returns as input": {
			when: when{
				text:   "foobar",
				suffix: "ar",
			},
			then: "foobar",
		},
		"when text is empty, it returns suffix": {
			when: when{
				text:   "",
				suffix: "foo",
			},
			then: "foo",
		},
		"when suffix is empty, it retuns input text": {
			when: when{
				text:   "bar",
				suffix: "",
			},
			then: "bar",
		},
		"when text and suffix are empty, it returns empty": {
			when: when{
				text:   "",
				suffix: "",
			},
			then: "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual := kstr.SuppySuffix(testcase.when.text, testcase.when.suffix)
			if actual != testcase.then {
				t.Errorf(
					`unexpected result: SupplySuffix("%s", "%s") --> %v`,
					testcase.when.text, testcase.when.suffix, actual,
				)
			}
		})
	}
}

func TestRandomHex(t *testing.T) {
	notLowerHex := regexp.MustCompile(`[^0-9a-f]`)

	for name, expectedLen := range map[string]uint{
		"zero": 0,
		"one":  1,
		"even": 8,
		"odd":  9,
	} {
		t.Run("it generates random hex string with"+name+"length", func(t *testing.T) {
			s, err := kstr.RandomHex(expectedLen)
			if err != nil {
				t.Fatal("unexpected error:", err)
			}
			if len(s) != int(expectedLen) {
				t.Error("wrong length:", s)
			}
			if notLowerHex.MatchString(s) {
				t.Error("non-hex char found:", s)
			}
		})
	}

	t.Run("it generates", func(t *testing.T) {
		tries := 1024
		length := uint(32) // 32-chars hex string = 16 bytes = 128 bits (same length as UUID)

		// We test here that return values of RandomHex are "unique" and "unordered"
		// as surrogates of randomness.
		//
		// # uniqueness (or no collisions)
		//
		// If RandomHex was NOT random enough,
		// this may return same strings twice or more during repeated calls,
		// in short, may make collisions.
		//
		// > In theory, MD5 hashes or UUIDs, being roughly 128 bits,
		// > should stay within that range until about 820 billion documents,
		// > even if its possible outputs are many more.
		//
		// (https://en.wikipedia.org/wiki/Birthday_attack#Mathematics)
		//
		// So, believing that, drawing 1k times or so is kind of "nothing".
		// It should not make collisions for (almost all) samples of 128 bit x 1k.
		//
		// We use the inverse of that as a null hypothesis of this test
		// (if it collides, it is not random enough, maybe).
		//
		// # unordered
		//
		// Generating collision-free strings can be done by returning count up sequense,
		// but it is predictable and confound the expectations of "randomness".
		//
		// Equip the safety harness for trivial implimentations.
		//

		collision := map[string]struct{}{}
		ordered := make([]string, 0, tries)
		for i := 0; i < tries; i++ {

			s, err := kstr.RandomHex(length)
			if err != nil {
				t.Fatal("unexpected error:", err)
			}
			collision[s] = struct{}{}
			ordered = append(ordered, s)
		}

		t.Run("varying strings for each calls", func(t *testing.T) {
			if len(collision) != tries {
				t.Error("it generates collisions")
			}
		})

		t.Run("unordered strings", func(t *testing.T) {
			for i := 1; i < (tries - 1); i++ {
				prev := ordered[i-1]
				item := ordered[i]
				next := ordered[i+1]

				if !(prev < item && item < next) && // not monotonic increase, and also
					!(prev > item && item > next) { // not monotonic decrease

					// once is enough. this is counterexample of ordering.
					return
				}
			}

			t.Error("it is ordered")
		})
	})

}

func TestSplitIfNotEmpty(t *testing.T) {
	t.Run("it does not split empty string", func(t *testing.T) {
		actual := kstr.SplitIfNotEmpty("", ",")
		if len(actual) != 0 {
			t.Errorf(`"%s" -> %+v`, "", actual)
		}
	})

	for _, pattern := range []string{
		"aa,bbb,ccc",
		",aaa,bb", // leading separator
		"aa,bb,",  // trailing separator
		",,,",     // separator only sequence
		",",       // single separator
	} {
		t.Run("it does split non-empty string like strings.Split", func(t *testing.T) {
			actual := kstr.SplitIfNotEmpty(pattern, ",")
			expected := strings.Split(pattern, ",")
			if !cmp.SliceEq(actual, expected) {
				t.Errorf(`"%s" -> (actual, expected) = (%+v, %+v)`, pattern, actual, expected)
			}
		})
	}
}

func TestSprintMany(t *testing.T) {
	t.Run("it genrates strings with given choices", func(t *testing.T) {

		actual := kstr.SprintMany(
			"%s-%d-%s",
			[]any{"a", "b"},
			[]any{1, 2, 3},
			[]any{"f", "g"},
		)

		expected := []string{
			"a-1-f", "b-1-f",
			"a-2-f", "b-2-f",
			"a-3-f", "b-3-f",

			"a-1-g", "b-1-g",
			"a-2-g", "b-2-g",
			"a-3-g", "b-3-g",
		}

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch: actual = %v, expected = %v", actual, expected)
		}
	})

	t.Run("it genrates empty if there are empty choice", func(t *testing.T) {

		actual := kstr.SprintMany(
			"%s-%s-%s",
			[]any{"a", "b"},
			[]any{},
			[]any{"f", "g"},
		)

		expected := []string{}

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch: actual = %v, expected = %v", actual, expected)
		}
	})
}
