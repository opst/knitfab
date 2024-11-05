package cmp_test

import (
	"testing"

	"github.com/opst/knitfab/pkg/utils/cmp"
)

func TestMapOp(t *testing.T) {
	t.Run("mapeq detect two maps are equal", func(t *testing.T) {
		a := map[string]string{
			"key1": "foo",
			"key2": "bar",
		}
		b := map[string]string{
			"key1": "foo",
			"key2": "bar",
		}
		if !cmp.MapEq(a, b) {
			t.Error("a != b, unexpectedly.")
		}
		if !cmp.MapEq(b, a) {
			t.Error("b != a, unexpectedly.")
		}
	})
	t.Run("mapeqwith detect two maps are equal", func(t *testing.T) {
		a := map[string]string{
			"key1": "foo...",
			"key2": "bar@@@",
		}
		b := map[string]string{
			"key1": "foo!!!",
			"key2": "bar???",
		}
		if !cmp.MapEqWith(a, b, func(a string, b string) bool { return a[:3] == b[:3] }) {
			t.Error("a != b, unexpectedly.")
		}
		if !cmp.MapEqWith(b, a, func(b string, a string) bool { return b[:3] == a[:3] }) {
			t.Error("b != a, unexpectedly.")
		}
	})
	t.Run("mapeq detect two maps have same keys are different", func(t *testing.T) {
		a := map[string]string{
			"key1": "foo",
			"key2": "bar",
			"key3": "baz",
		}
		b := map[string]string{
			"key1": "foo",
			"key2": "bar",
			"key3": "quux",
		}
		if cmp.MapEq(a, b) {
			t.Error("a == b, unexpectedly.")
		}
		if cmp.MapEq(b, a) {
			t.Error("b == a, unexpectedly.")
		}
	})
	t.Run("mapeqwith detect two maps have same keys are different", func(t *testing.T) {
		a := map[string]string{
			"key1": "foo",
			"key2": "bar",
			"key3": "baz",
		}
		b := map[string]string{
			"key1": "foo",
			"key2": "bar",
			"key3": "bam",
		}
		if cmp.MapEqWith(a, b, func(a, b string) bool { return a[:3] == b[:3] }) {
			t.Error("a == b, unexpectedly.")
		}
		if cmp.MapEqWith(b, a, func(b, a string) bool { return b[:3] == a[:3] }) {
			t.Error("b == a, unexpectedly.")
		}
	})
	t.Run("mapeq detect two maps have different keys are different", func(t *testing.T) {
		a := map[string]string{
			"key1": "foo",
			"key2": "bar",
			"key3": "baz",
		}
		b := map[string]string{
			"key1": "foo",
			"key2": "bar",
			"key4": "baz",
		}
		if cmp.MapEq(a, b) {
			t.Error("a == b, unexpectedly.")
		}
		if cmp.MapEq(b, a) {
			t.Error("b == a, unexpectedly.")
		}
	})
	t.Run("mapeqwith detect two maps have different keys are different", func(t *testing.T) {
		a := map[string]string{
			"key1": "foo",
			"key2": "bar",
			"key3": "baz",
		}
		b := map[string]string{
			"key1": "foo",
			"key2": "bar",
			"key4": "baz",
		}
		if cmp.MapEqWith(a, b, func(a, b string) bool { return a[:3] == b[:3] }) {
			t.Error("a == b, unexpectedly.")
		}
		if cmp.MapEqWith(b, a, func(b, a string) bool { return b[:3] == a[:3] }) {
			t.Error("b == a, unexpectedly.")
		}
	})
	t.Run("mapeq detect two maps have different key size are different", func(t *testing.T) {
		a := map[string]string{
			"key1": "foo",
			"key2": "bar",
			"key3": "baz",
		}
		b := map[string]string{
			"key1": "foo",
			"key2": "bar",
		}
		if cmp.MapEq(a, b) {
			t.Error("a == b, unexpectedly.")
		}
		if cmp.MapEq(b, a) {
			t.Error("b == a, unexpectedly.")
		}
	})
	t.Run("mapeqwith detect two maps have different key size are different", func(t *testing.T) {
		a := map[string]string{
			"key1": "foo",
			"key2": "bar",
			"key3": "baz",
		}
		b := map[string]string{
			"key1": "foo",
			"key2": "bar",
		}
		if cmp.MapEqWith(a, b, func(a, b string) bool { return a[:3] == b[:3] }) {
			t.Error("a == b, unexpectedly.")
		}
		if cmp.MapEqWith(b, a, func(b, a string) bool { return b[:3] == a[:3] }) {
			t.Error("b == a, unexpectedly.")
		}
	})
}

func TestMapXeq(t *testing.T) {
	t.Run("[MapLeq/MapGeq] subset contains superset", func(t *testing.T) {
		haystack := map[string]string{
			"a": "apple",
			"b": "balloon",
			"c": "cherry",
			"d": "dream",
			"e": "evergreen",
		}
		for _, keys := range [][]string{
			// power set of key of haystack.
			{}, {"a", "b", "c", "d", "e"},
			{"a"}, {"b", "c", "d", "e"},
			{"b"}, {"a", "c", "d", "e"},
			{"c"}, {"a", "b", "d", "e"},
			{"d"}, {"a", "b", "c", "e"},
			{"e"}, {"a", "b", "c", "d"},
			{"a", "b"}, {"c", "d", "e"},
			{"a", "c"}, {"b", "d", "e"},
			{"a", "d"}, {"b", "c", "e"},
			{"a", "e"}, {"b", "c", "d"},
			{"b", "c"}, {"a", "d", "e"},
			{"b", "d"}, {"a", "c", "e"},
			{"b", "e"}, {"a", "c", "d"},
			{"c", "d"}, {"a", "b", "e"},
			{"c", "e"}, {"a", "b", "d"},
			{"d", "e"}, {"a", "b", "c"},
		} {
			needle := map[string]string{}
			for _, k := range keys {
				needle[k] = haystack[k]
			}

			if !cmp.MapGeq(haystack, needle) {
				t.Errorf("unexpectedly, %v >= %v.", haystack, needle)
			}

			if !cmp.MapLeq(needle, haystack) {
				t.Errorf("unexpectedly, %v <= %v.", needle, haystack)
			}
		}
	})

	t.Run("[MapLeqWith/mapGeqWith] subset contains superset", func(t *testing.T) {
		haystack := map[string]string{
			"a": "all",
			"b": "balloon",
			"c": "cherry",
			"d": "dream",
			"e": "evergreen",
		}
		needles := map[string]int{
			"a": 3,
			"b": 7,
			"c": 6,
			"d": 5,
			"e": 9,
		}
		for _, keys := range [][]string{
			// power set of key of haystack.
			{}, {"a", "b", "c", "d", "e"},
			{"a"}, {"b", "c", "d", "e"},
			{"b"}, {"a", "c", "d", "e"},
			{"c"}, {"a", "b", "d", "e"},
			{"d"}, {"a", "b", "c", "e"},
			{"e"}, {"a", "b", "c", "d"},
			{"a", "b"}, {"c", "d", "e"},
			{"a", "c"}, {"b", "d", "e"},
			{"a", "d"}, {"b", "c", "e"},
			{"a", "e"}, {"b", "c", "d"},
			{"b", "c"}, {"a", "d", "e"},
			{"b", "d"}, {"a", "c", "e"},
			{"b", "e"}, {"a", "c", "d"},
			{"c", "d"}, {"a", "b", "e"},
			{"c", "e"}, {"a", "b", "d"},
			{"d", "e"}, {"a", "b", "c"},
		} {
			needle := map[string]int{}
			for _, k := range keys {
				needle[k] = needles[k]
			}

			if !cmp.MapGeqWith(haystack, needle, func(a string, b int) bool { return len(a) == b }) {
				t.Errorf("unexpectedly, %v >= %v.", haystack, needle)
			}

			if !cmp.MapLeqWith(needle, haystack, func(a int, b string) bool { return len(b) == a }) {
				t.Errorf("unexpectedly, %v <= %v.", needle, haystack)
			}
		}
	})

	t.Run("If one has uncommon element, these are not superset or subset.", func(t *testing.T) {
		alpha := map[string]string{
			"a": "apple",
			"b": "balloon",
			"c": "cherry",
			"d": "dream",
			"e": "evergreen",
		}
		beta := map[string]string{
			"a": "apple",
			"b": "balloon",
			"c": "cherry",
			"d": "dream",
			"f": "flower", // diff!
		}

		if cmp.MapGeq(alpha, beta) {
			t.Errorf("unexpectedly, %v >= %v", alpha, beta)
		}

		if cmp.MapGeq(beta, alpha) {
			t.Errorf("unexpectedly, %v >= %v", beta, alpha)
		}

		if cmp.MapLeq(alpha, beta) {
			t.Errorf("unexpectedly, %v <= %v", alpha, beta)
		}

		if cmp.MapLeq(beta, alpha) {
			t.Errorf("unexpectedly, %v <= %v", beta, alpha)
		}

	})
}

func TestMapMatch(t *testing.T) {
	truther := func(string) bool { return true }
	falser := func(string) bool { return false }

	t.Run("MapMatch return true for all entry returns true.", func(t *testing.T) {
		a := map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		}
		predicators := map[string]func(string) bool{
			"a": truther,
			"b": truther,
			"c": truther,
		}

		if !cmp.MapMatch(a, predicators) {
			t.Error("MapMatch return false if all entry return true.")
		}
	})

	t.Run("MapMatch return false for any entry returns false.", func(t *testing.T) {
		a := map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		}

		for toBeFalse := range a {
			predicators := map[string]func(string) bool{
				"a": truther,
				"b": truther,
				"c": truther,
			}
			predicators[toBeFalse] = falser

			if cmp.MapMatch(a, predicators) {
				t.Error("MapMatch return true if some entry return false.")
			}
		}
	})

	t.Run("MapMatch return false if predicators has extra keys rather map has", func(t *testing.T) {
		a := map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		}
		predicators := map[string]func(string) bool{
			"a": truther,
			"b": truther,
			"c": truther,
			"d": truther,
		}

		if cmp.MapMatch(a, predicators) {
			t.Error("MapMatch return true if predicators has extra keys.")
		}
	})

	t.Run("MapMatch return false if map has extra keys rather predicators has", func(t *testing.T) {
		a := map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		}
		predicators := map[string]func(string) bool{
			"a": truther,
			"b": truther,
		}

		if cmp.MapMatch(a, predicators) {
			t.Error("MapMatch return true if map has extra keys.")
		}
	})

	t.Run("MapMatch tests all entries", func(t *testing.T) {
		a := map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		}

		actual := map[string]string{}

		predicators := map[string]func(string) bool{
			"a": func(v string) bool {
				actual["a"] = v
				return true
			},
			"b": func(v string) bool { actual["b"] = v; return true },
			"c": func(v string) bool { actual["c"] = v; return true },
		}

		cmp.MapMatch(a, predicators)

		if a["a"] != actual["a"] || a["b"] != actual["b"] || a["c"] != actual["c"] {
			t.Errorf(
				"MapMatch is not receive all of map entries. (actual, expeced) = (%v, %v)",
				a, actual,
			)
		}
	})
}
