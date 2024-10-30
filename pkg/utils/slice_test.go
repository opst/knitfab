package utils_test

import (
	"fmt"
	"testing"

	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/cmp"
)

func TestSliceUtils(t *testing.T) {
	t.Run("Map maps slice to another", func(t *testing.T) {
		input := []int{3, 5, 7, 11}
		called := 0
		mapper := func(v int) int {
			called += 1
			return v * 2
		}
		output := utils.Map(input, mapper)

		if called != len(input) {
			t.Errorf("mapper has not been called enough. (actual, expected) = (%d, %d)", called, len(input))
		}

		expected := []int{6, 10, 14, 22}
		if !cmp.SliceEq(output, expected) {
			t.Errorf("mapped result is wrong. (actual, expected) = (%v, %v)", output, expected)
		}
	})

	t.Run("ToMap converts slice to map", func(t *testing.T) {
		type T struct {
			key   string
			value int
		}
		values := []T{
			{key: "a", value: 3},
			{key: "b", value: 99},
			{key: "c", value: 100},
			{key: "d", value: 2},
		}

		result := utils.ToMap(values, func(v T) string { return v.key })

		expected := map[string]T{
			"a": {key: "a", value: 3},
			"b": {key: "b", value: 99},
			"c": {key: "c", value: 100},
			"d": {key: "d", value: 2},
		}

		if !cmp.MapEq(result, expected) {
			t.Errorf(
				"ToMap generates wrong map. (actual, expected) = (%v, %v)",
				result, expected,
			)
		}
	})

	t.Run("KeysOf and ValuesOf makes slice from values of map", func(t *testing.T) {
		input := map[int]string{
			1: "foo",
			2: "bar",
			3: "baz",
		}
		{
			actual := utils.ValuesOf(input)
			expected := []string{"foo", "bar", "baz"}

			if !cmp.SliceContentEq(actual, expected) {
				t.Errorf(
					"slice elements are wrong:\nactual   = %+v\nexpected = %+v",
					actual, expected,
				)
			}
		}
		{
			actual := utils.KeysOf(input)
			expected := []int{1, 2, 3}
			if !cmp.SliceContentEq(actual, expected) {
				t.Errorf(
					"slice elements are wrong:\nactual   = %+v\nexpected = %+v",
					actual, expected,
				)
			}
		}
	})

	t.Run("First finds the first element which predicator matches", func(t *testing.T) {
		haystack := []string{"our", "needle", "is", "nice"}
		ret, ok := utils.First(haystack, func(s string) bool { return s[0] == 'n' })
		if !ok {
			t.Error("First could not find target.")
		}
		if ret != "needle" {
			t.Errorf("First finds wrong word. (actual, expected) = (%s, %s)", ret, "needle")
		}
	})

	t.Run("First returns (zerovalue, false) if predicator does never match.", func(t *testing.T) {
		haystack := []string{"this", "haystack", "is", "pure", "and", "dust-free!"}
		ret, ok := utils.First(haystack, func(s string) bool { return s[0] == 'n' })
		if ok {
			t.Errorf("First finds wrong target. %v", ret)
		}
		if ret != "" {
			t.Errorf("First returns non-zero value.: %s", ret)
		}
	})

	t.Run("ApplyAll applies all modifiers to target", func(t *testing.T) {
		type container struct{ value string }
		input := &container{value: "ab"}
		actual := utils.ApplyAll(
			input,
			func(c *container) *container {
				c.value += "cd"
				return c
			},
			func(c *container) *container {
				c.value += "efg"
				return c
			},
		)

		if actual.value != "abcdefg" {
			t.Errorf("not all morifier are applied: actual = %s, expected = abcdefg", actual.value)
		}
	})
}

func TestSorted(t *testing.T) {
	type Elem struct {
		foo int
		bar int
	}

	sortByFoo := func(a, b Elem) bool {
		return a.foo < b.foo
	}

	sortByBar := func(a, b Elem) bool {
		return a.bar < b.bar
	}

	t.Run("when empty slice is given, it returns empty", func(t *testing.T) {
		input := []Elem{}
		result := utils.Sorted(input, sortByFoo)
		if len(result) != 0 {
			t.Errorf("result has length %d != 0", len(result))
		}

		if &input == &result {
			t.Error("it works destructive")
		}
	})

	t.Run("when non-empty slice with non-unique elements is given, it return new sorted slice", func(t *testing.T) {
		input := []Elem{
			{foo: 5, bar: 1},
			{foo: 3, bar: 2},
			{foo: 5, bar: 3},
			{foo: 3, bar: 4},
			{foo: 2, bar: 5},
			{foo: 6, bar: 6},
		}

		result := utils.Sorted(input, sortByFoo)

		expectedFoos := []int{2, 3, 3, 5, 5, 6}
		actualFoos := utils.Map(result, func(el Elem) int { return el.foo })

		if !cmp.SliceEq(actualFoos, expectedFoos) {
			t.Errorf("it is not sorted by foo: %#v", result)
		}

		if &input == &result {
			t.Error("it works destructive")
		}
	})

	t.Run("when non-empty slice with unique elements is given, it return new sorted slice", func(t *testing.T) {
		input := []Elem{
			{foo: 2, bar: 5},
			{foo: 3, bar: 4},
			{foo: 3, bar: 2},
			{foo: 5, bar: 3},
			{foo: 5, bar: 1},
			{foo: 6, bar: 6},
		}

		result := utils.Sorted(input, sortByBar)

		expectedBars := []int{1, 2, 3, 4, 5, 6}
		actualBars := utils.Map(result, func(el Elem) int { return el.bar })

		if !cmp.SliceEq(actualBars, expectedBars) {
			t.Errorf("it is not sorted by bar: %#v", result)
		}

		if &input == &result {
			t.Error("it works destructive")
		}
	})
}

func TestBinarySearch(t *testing.T) {
	type Elem struct {
		foo int
		bar int
	}

	fooOrdering := func(a, b Elem) bool {
		return a.foo < b.foo
	}

	barOrdering := func(a, b Elem) bool {
		return a.bar < b.bar
	}

	t.Run("when empty slice is given, it returns 0", func(t *testing.T) {
		sli := []Elem{}
		index := utils.BinarySearch(sli, Elem{foo: 100, bar: 10}, fooOrdering)
		if index != 0 {
			t.Errorf("returned index is %d != 0", index)
		}

		if len(sli) != 0 {
			t.Errorf("given slice is updated: %#v", sli)
		}
	})

	t.Run("For given slice which has unique elements and ordered by property 'bar'", func(t *testing.T) {
		sli := func() []Elem {
			// declare test input & assure not to be changed between tests
			return []Elem{
				// 0 (index to be inserted here)
				{foo: 5, bar: 1},
				// 1
				{foo: 3, bar: 2},
				// 2
				{foo: 5, bar: 3},
				// 3
				{foo: 3, bar: 4},
				// 4
				{foo: 2, bar: 5},
				// 5
				{foo: 6, bar: 6},
				// 6
			}
		}

		for _, testcase := range []struct {
			item          Elem
			expectedIndex int
		}{
			{item: Elem{foo: 42, bar: 0}, expectedIndex: 0},
			{item: Elem{foo: 42, bar: 1}, expectedIndex: 0},
			{item: Elem{foo: 42, bar: 2}, expectedIndex: 1},
			{item: Elem{foo: 42, bar: 3}, expectedIndex: 2},
			{item: Elem{foo: 42, bar: 4}, expectedIndex: 3},
			{item: Elem{foo: 42, bar: 5}, expectedIndex: 4},
			{item: Elem{foo: 42, bar: 6}, expectedIndex: 5},
			{item: Elem{foo: 42, bar: 7}, expectedIndex: 6},
		} {
			t.Run(
				fmt.Sprintf("when it searches index for bar=%d, it should return %d", testcase.item.bar, testcase.expectedIndex),
				func(t *testing.T) {
					s := sli()
					actual := utils.BinarySearch(s, testcase.item, barOrdering)
					if actual != testcase.expectedIndex {
						t.Errorf("returned index is wrong: (actual)%d != (expected)%d", actual, testcase.expectedIndex)
					}

					if !cmp.SliceEq(s, sli()) {
						t.Errorf("input slice is changed: %#v", s)
					}
				},
			)
		}
	})

	t.Run("For given slice which has non-unique elements and ordered by property 'bar'", func(t *testing.T) {
		sli := func() []Elem {
			// declare test input & assure not to be changed between tests
			return []Elem{
				// 0 (index to be inserted here)
				{foo: 2, bar: 5},
				// 1
				{foo: 3, bar: 2},
				{foo: 3, bar: 4},
				// 3
				{foo: 5, bar: 3},
				{foo: 5, bar: 1},
				// 5
				{foo: 6, bar: 6},
				// 6
			}
		}

		for _, testcase := range []struct {
			item          Elem
			expectedIndex int
		}{
			{item: Elem{foo: 0, bar: 42}, expectedIndex: 0},
			{item: Elem{foo: 1, bar: 42}, expectedIndex: 0},
			{item: Elem{foo: 2, bar: 42}, expectedIndex: 0},
			{item: Elem{foo: 3, bar: 42}, expectedIndex: 1},
			{item: Elem{foo: 4, bar: 42}, expectedIndex: 3},
			{item: Elem{foo: 5, bar: 42}, expectedIndex: 3},
			{item: Elem{foo: 6, bar: 42}, expectedIndex: 5},
			{item: Elem{foo: 7, bar: 42}, expectedIndex: 6},
		} {
			t.Run(
				fmt.Sprintf("when it searches index for bar=%d, it should return %d", testcase.item.bar, testcase.expectedIndex),
				func(t *testing.T) {
					s := sli()
					actual := utils.BinarySearch(s, testcase.item, fooOrdering)
					if actual != testcase.expectedIndex {
						t.Errorf("returned index is wrong: (actual)%d != (expected)%d", actual, testcase.expectedIndex)
					}

					if !cmp.SliceEq(s, sli()) {
						t.Errorf("input slice is changed: %#v", s)
					}
				},
			)
		}
	})

}

func TestConcat(t *testing.T) {
	t.Run("it concatenates slices which have items", func(t *testing.T) {
		original := []int{1, 2, 3, 4, 5, 6, 7}
		actual := utils.Concat(original[:2], original[2:5], original[5:])

		if !cmp.SliceEq(original, actual) {
			t.Errorf("unexpected result: (actual, expected) = (%+v, %+v)", actual, original)
		}
	})

	t.Run("it concatenates slices ignoreing empty slices", func(t *testing.T) {
		original := []int{1, 2, 3, 4, 5, 6, 7}
		actual := utils.Concat(
			[]int{}, original[:2],
			[]int{}, original[2:5], []int{},
			[]int{}, original[5:],
			[]int{},
		)

		if !cmp.SliceEq(original, actual) {
			t.Errorf("unexpected result: (actual, expected) = (%+v, %+v)", actual, original)
		}
	})

	t.Run("it does not change passed slices", func(t *testing.T) {
		a := []int{1, 2, 3}
		b := []int{4, 5, 6}
		utils.Concat(a, b)

		if !cmp.SliceEq(a, []int{1, 2, 3}) {
			t.Errorf("unexpected result: (actual, expected) = (%+v, %+v)", a, []int{1, 2, 3})
		}

		if !cmp.SliceEq(b, []int{4, 5, 6}) {
			t.Errorf("unexpected result: (actual, expected) = (%+v, %+v)", b, []int{4, 5, 6})
		}
	})
}

func TestFilter(t *testing.T) {
	for name, testcase := range map[string]struct {
		values   []int
		pred     func(int) bool
		expected []int
	}{
		"it filters values with predicator": {
			values:   []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
			pred:     func(i int) bool { return i%2 != 0 },
			expected: []int{1, 3, 5, 7, 9},
		},
		"it returns empty for empty slice": {
			values:   []int{},
			pred:     func(int) bool { return true },
			expected: []int{},
		}, "it returns empty for nil": {
			values:   nil,
			pred:     func(int) bool { return true },
			expected: []int{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual := utils.Filter(testcase.values, testcase.pred)

			if !cmp.SliceContentEq(actual, testcase.expected) {
				t.Errorf(
					"unmatch result:\n===actual===\n%v\n===expected===\n%v",
					actual, testcase.expected,
				)
			}

			if &testcase.values == &actual {
				t.Errorf("slice is reused, but should not")
			}
		})
	}
}

func TestFlatten(t *testing.T) {

	for name, testcase := range map[string]struct {
		when [][]int
		then []int
	}{
		"flatten slices": {
			when: [][]int{{1, 2, 3}, {4, 5, 6}},
			then: []int{1, 2, 3, 4, 5, 6},
		},
		"flatten single slice": {
			when: [][]int{{1, 2, 3}},
			then: []int{1, 2, 3},
		},
		"flatten empty slice": {
			when: [][]int{},
			then: []int{},
		},
		"it skips empty slice": {
			when: [][]int{{1, 2, 3}, {}, {4, 5, 6}},
			then: []int{1, 2, 3, 4, 5, 6},
		},
	} {
		t.Run(name, func(t *testing.T) {
			when, then := testcase.when, testcase.then
			actual := utils.Flatten(when)

			if !cmp.SliceEq(actual, then) {
				t.Errorf("unmatch: (actual, expected) = (%v, %v)", actual, then)
			}
		})
	}
}

func TestGroup(t *testing.T) {
	t.Run("it splits slice into groups along predicator", func(t *testing.T) {
		values := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

		even, odd := utils.Group(values, func(v int) bool { return v%2 == 0 })

		{
			expected := []int{2, 4, 6, 8, 10}
			if !cmp.SliceContentEq(even, expected) {
				t.Errorf("unmatch: true group: (actual, expected) = (%v, %v)", even, expected)
			}
		}
		{
			expected := []int{1, 3, 5, 7, 9}
			if !cmp.SliceContentEq(odd, expected) {
				t.Errorf("unmatch: false group: (actual, expected) = (%v, %v)", odd, expected)
			}
		}
	})

	t.Run("when all items are match, notmatch group is empty", func(t *testing.T) {
		values := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

		small, large := utils.Group(values, func(v int) bool { return v < 100 })

		{
			if !cmp.SliceContentEq(small, values) {
				t.Errorf("unmatch: true group: (actual, expected) = (%v, %v)", small, values)
			}
		}
		{
			if len(large) != 0 {
				t.Errorf("unmatch: false group: (actual, expected) = (%v, %v)", large, []int{})
			}
		}
	})

	t.Run("when no items are match, match group is empty", func(t *testing.T) {
		values := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

		small, large := utils.Group(values, func(v int) bool { return v < -1 })

		{
			if len(small) != 0 {
				t.Errorf("unmatch: true group: (actual, expected) = (%v, %v)", small, []int{})
			}
		}
		{
			if !cmp.SliceContentEq(large, values) {
				t.Errorf("unmatch: false group: (actual, expected) = (%v, %v)", large, values)
			}
		}
	})
}
