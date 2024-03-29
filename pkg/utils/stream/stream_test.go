package stream_test

import (
	"testing"

	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/utils/stream"
)

func TestSliceStream(t *testing.T) {
	t.Run("it contains all items of the source slice", func(t *testing.T) {
		source := []int{1, 1, 2, 3, 5, 8, 13}
		testee := stream.FromSlice(source)
		output := testee.Slice()

		if !cmp.SliceEq(source, output) {
			t.Errorf("FromSlice(...).Slice() changes content: (input, output) = (%+v, %+v)", source, output)
		}
	})

	t.Run("Filter drops all of unmatched items", func(t *testing.T) {
		source := []int{1, 1, 2, 3, 5, 8, 13}
		testee := stream.FromSlice(source).Filter(func(i int) bool { return i%2 == 0 })
		output := testee.Slice()
		expected := []int{2, 8}
		if !cmp.SliceEq(output, expected) {
			t.Errorf("Filter() does not drop expected items: (output, expected) = (%+v, %+v)", output, expected)
		}
	})

	t.Run("DropWhile discards elements until a non-match element is found", func(t *testing.T) {
		source := []int{3, 1, 4, 1, 5, 9}
		testee := stream.FromSlice(source).DropWhile(func(i int) bool { return i%2 != 0 })
		output := testee.Slice()
		expected := []int{4, 1, 5, 9}
		if !cmp.SliceEq(output, expected) {
			t.Errorf("DropWhile() does not drop expected items: (output, expected) = (%+v, %+v)", output, expected)
		}
	})

	t.Run("Concat Streams", func(t *testing.T) {
		sourceA := []int{1, 1, 2, 3}
		a := stream.FromSlice(sourceA)
		sourceB := []int{5, 8, 13}
		b := stream.FromSlice(sourceB)

		output := a.Concat(b).Slice()
		expected := append(sourceA, sourceB...)
		if !cmp.SliceEq(output, expected) {
			t.Errorf("Concat() does not concat items: (output, expected) = (%+v, %+v)", output, expected)
		}
	})
}
