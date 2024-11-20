package cmp_test

import (
	"fmt"
	"testing"

	"github.com/opst/knitfab/pkg/utils/cmp"
)

func TestSliceOp(t *testing.T) {
	t.Run("sliceeq detect two slices are equal", func(t *testing.T) {
		a := []string{"a", "b", "c"}
		b := []string{"a", "b", "c"}
		if !cmp.SliceEq(a, b) {
			t.Error("two slices are not equal, unexpectedly.")
		}
		if !cmp.SliceEq(b, a) {
			t.Error("two slices are not equal, unexpectedly.")
		}
	})
	t.Run("sliceeq detect two slices with different content are not equal", func(t *testing.T) {
		a := []string{"a", "b", "c"}
		b := []string{"a", "b", "d"}
		if cmp.SliceEq(a, b) {
			t.Error("two slices are equal, unexpectedly.")
		}
		if cmp.SliceEq(b, a) {
			t.Error("two slices are equal, unexpectedly.")
		}
	})
	t.Run("sliceeq detect two slices with different length are not equal", func(t *testing.T) {
		a := []string{"a", "b", "c"}
		b := []string{"a", "b"}
		if cmp.SliceEq(a, b) {
			t.Error("two slices are equal, unexpectedly.")
		}
		if cmp.SliceEq(b, a) {
			t.Error("two slices are equal, unexpectedly.")
		}
	})

	t.Run("SliceEqWith detect two slices in some comparing rule", func(t *testing.T) {
		a := []string{"foobar", "", "baz"}
		b := []int{6, 0, 3}
		equalInLen := func(a string, b int) bool { return len(a) == b }

		if !cmp.SliceEqWith(a, b, equalInLen) {
			t.Error("two slices are not equal, unexpectedly.")
		}
	})

	t.Run("SliceEqWith detect two slices with different content (after mapped) are not equal", func(t *testing.T) {
		a := []string{"foobar", "", "baz"}
		b := []int{6, 1, 3}
		equalInLen := func(a string, b int) bool { return len(a) == b }

		if cmp.SliceEqWith(a, b, equalInLen) {
			t.Error("two slices are equal, unexpectedly.")
		}
	})
	t.Run("SliceEqWith detect two slices with different length are not equal", func(t *testing.T) {
		a := []string{"foobar", "", "baz"}
		b := []int{6, 1}
		equalInLen := func(a string, b int) bool { return len(a) == b }

		if cmp.SliceEqWith(a, b, equalInLen) {
			t.Error("two slices are equal, unexpectedly.")
		}
	})
}

func TestSliceContains(t *testing.T) {
	t.Run("SliceContains detect a pattern in haystack", func(t *testing.T) {
		haystack := []string{"foo", "bar", "baz", "quux", "whoop"}

		// check for all partial sequence.
		for l := range haystack {
			length := l + 1
			for nth := range haystack[:len(haystack)-l] {
				needle := haystack[nth : nth+length]
				if len(needle) < 1 {
					break
				}
				if !cmp.SliceContains(haystack, needle) {
					t.Errorf("SliceContains do not found %v from %v", needle, haystack)
				}
			}
		}
	})

	t.Run("SliceContains does not detect a pattern not in haystack", func(t *testing.T) {
		haystack := []string{"foo", "bar", "baz", "quux", "whoop"}

		for _, needle := range [][]string{
			{"there", "are", "not"},
			{"foo", "bar", "baz", "quux", "whoop", "!"},
			{"^", "foo", "bar", "baz", "quux", "whoop"},
		} {
			if cmp.SliceContains(haystack, needle) {
				t.Errorf("SliceContains unexpectedly finds %v in %v", needle, haystack)
			}
		}
	})

	t.Run("SliceContains find empty pattern", func(t *testing.T) {
		for _, haystack := range [][]string{
			{},
			{"a", "b", "c"},
		} {
			if !cmp.SliceContains(haystack, []string{}) {
				t.Errorf("SliceContains cannot find empty pattern from %v", haystack)
			}
		}
	})
}

func TestSliceContentEq(t *testing.T) {
	type when struct {
		a []string
		b []string
	}
	type testcase struct {
		when     when
		expected bool
	}
	for _, testcase := range []testcase{
		{
			when: when{
				a: []string{"a", "b", "c"},
				b: []string{"a", "b", "c"},
			},
			expected: true,
		},
		{
			when: when{
				a: []string{"a", "b", "c"},
				b: []string{"a", "b", "d"},
			},
			expected: false,
		},
		{
			when: when{
				a: []string{"a", "b", "c"},
				b: []string{"c", "a", "b"},
			},
			expected: true,
		},
		{
			when: when{
				a: []string{"a", "b", "c"},
				b: []string{"a", "b", "c", "c"},
			},
			expected: false,
		},
		{
			when: when{
				a: []string{"c", "a", "b", "c"},
				b: []string{"a", "b", "c", "c"},
			},
			expected: true,
		},
	} {
		a := testcase.when.a
		b := testcase.when.b
		expected := testcase.expected
		t.Run(
			fmt.Sprintf(
				"SliceContentEq(%#v, %#v) should be %v, commutative",
				a, b, expected,
			),
			func(t *testing.T) {
				if cmp.SliceContentEq(a, b) != expected {
					t.Errorf("SliceContentEq(%#v, %#v) != %v", a, b, expected)
				}
				if cmp.SliceContentEq(b, a) != expected {
					t.Errorf("SliceContentEq(%#v, %#v) != %v", b, a, expected)
				}
			},
		)
	}
}

func TestSliceContentEqWith(t *testing.T) {
	type T struct {
		header  string
		trailer string
	}
	equiv := func(a, b T) bool {
		return a.header+a.trailer == b.header+b.trailer
	}

	type when struct{ a, b []T }

	for name, testcase := range map[string]struct {
		when when
		then bool
	}{
		"when two slices are equal, it returns true": {
			when: when{
				a: []T{{"ab", "cd"}, {"ef", "gh"}, {"ij", "kl"}},
				b: []T{{"ab", "cd"}, {"ef", "gh"}, {"ij", "kl"}},
			},
			then: true,
		},
		"when two slices are equal except ordering, it returns true": {
			when: when{
				a: []T{{"ab", "cd"}, {"ef", "gh"}, {"ij", "kl"}},
				b: []T{{"ij", "kl"}, {"ab", "cd"}, {"ef", "gh"}},
			},
			then: true,
		},
		"when two slices are equivarent, it returns true": {
			when: when{
				a: []T{{"ab", "cd"}, {"ef", "gh"}, {"ij", "kl"}},
				b: []T{{"i", "jkl"}, {"abc", "d"}, {"", "efgh"}},
			},
			then: true,
		},
		"when two slices are different in length, it returns false": {
			when: when{
				a: []T{{"ab", "cd"}, {"ef", "gh"}, {"ij", "kl"}},
				b: []T{{"ab", "cd"}, {"ef", "gh"}},
			},
			then: false,
		},
		"when two slices are different in element, it returns false": {
			when: when{
				a: []T{{"ab", "cd"}, {"ef", "gh"}, {"ij", "kl"}},
				b: []T{{"ab", "cd"}, {"ef", "gh"}, {"mn", "op"}},
			},
			then: false,
		},
		"when two slices are equivarent and have duplicated value, it returns true": {
			when: when{
				a: []T{{"ab", "cd"}, {"ef", "gh"}, {"ef", "gh"}, {"ij", "kl"}},
				b: []T{{"ab", "cd"}, {"e", "fgh"}, {"ef", "gh"}, {"ij", "kl"}},
			},
			then: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if actual := cmp.SliceContentEqWith(
				testcase.when.a, testcase.when.b, equiv,
			); actual != testcase.then {
				t.Errorf(
					"wrong result: SliceContentEqWith(a = %#v, b = %#v, equiv) -> %v",
					testcase.when.a, testcase.when.b, actual,
				)
			}
			if actual := cmp.SliceContentEqWith(
				testcase.when.b, testcase.when.a, equiv,
			); actual != testcase.then {
				t.Errorf(
					"wrong result: SliceContentEqWith(b = %#v, a = %#v, equiv) -> %v",
					testcase.when.b, testcase.when.a, actual,
				)
			}
		})
	}
}

func TestSliceSubsetWith(t *testing.T) {

	if shouldTrue := cmp.SliceSubsetWith(
		[]int{1, 2, 3, 4, 5},
		[]int{3, 4},
		func(ae, be int) bool { return ae == be },
	); !shouldTrue {
		t.Error("it should {1, 2, 3, 4, 5} ⊇ {3, 4}")
	}

	if shouldTrue := cmp.SliceSubsetWith(
		[]int{1, 2, 3, 4, 5},
		[]int{1, 2, 3, 4, 5},
		func(ae, be int) bool { return ae == be },
	); !shouldTrue {
		t.Error("it should {1, 2, 3, 4, 5} ⊇ {1, 2, 3, 4, 5}")
	}

	if shouldTrue := cmp.SliceSubsetWith(
		[]int{1},
		[]int{1},
		func(ae, be int) bool { return ae == be },
	); !shouldTrue {
		t.Error("it should {1} ⊇ {1}")
	}

	if shouldTrue := cmp.SliceSubsetWith(
		[]int{},
		[]int{},
		func(ae, be int) bool { return ae == be },
	); !shouldTrue {
		t.Error("it should {} ⊇ {}")
	}

	if shouldTrue := cmp.SliceSubsetWith(
		[]int{1, 2, 3, 4, 5},
		[]int{4, 3},
		func(ae, be int) bool { return ae == be },
	); !shouldTrue {
		t.Error("ordering should not matter")
	}

	if shouldTrue := cmp.SliceSubsetWith(
		[]int{1, 2, 3, 4, 5},
		[]int{2, 5},
		func(ae, be int) bool { return ae == be },
	); !shouldTrue {
		t.Error("sub-sequenceness should not matter")
	}

	if shouldFalse := cmp.SliceSubsetWith(
		[]int{1, 2, 3, 4, 5},
		[]int{3, 4, 5, 6},
		func(ae, be int) bool { return ae == be },
	); shouldFalse {
		t.Error("6 is not in {1, 2, 3, 4, 5}")
	}

	if shouldFalse := cmp.SliceSubsetWith(
		[]int{1, 2, 3, 4, 5},
		[]int{3, 3, 4},
		func(ae, be int) bool { return ae == be },
	); shouldFalse {
		t.Error("3s cannot be found two times or more in A")
	}

	if shouldTrue := cmp.SliceSubsetWith(
		[]int{1, 2, 3, 4, 5},
		[]int{3, 3, 4},
		func(ae, be int) bool { return ae%2 == be%2 },
	); !shouldTrue {
		t.Error("it should find two odds and one even in {1, 2, 3, 4, 5}")
	}
}
