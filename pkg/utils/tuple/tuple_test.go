package tuple_test

import (
	"testing"

	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/utils/tuple"
)

func TestUnzipPair(t *testing.T) {
	input := []tuple.Pair[string, int]{
		tuple.PairOf("one", 1),
		tuple.PairOf("two", 2),
		tuple.PairOf("three", 3),
		tuple.PairOf("four", 4),
	}

	actualStr, actualInt := tuple.UnzipPair(input)

	expectedStr := []string{"one", "two", "three", "four"}
	expectedInt := []int{1, 2, 3, 4}

	if !cmp.SliceEq(actualStr, expectedStr) {
		t.Errorf("unmatch: first: (actual, expected) != (%+v, %+v)", actualStr, expectedStr)
	}

	if !cmp.SliceEq(actualInt, expectedInt) {
		t.Errorf("unmatch: second: (actual, expected) != (%+v, %+v)", actualInt, expectedInt)
	}
}
