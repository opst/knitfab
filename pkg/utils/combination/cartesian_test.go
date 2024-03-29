package combination_test

import (
	"testing"

	"github.com/opst/knitfab/pkg/cmp"
	combo "github.com/opst/knitfab/pkg/utils/combination"
)

func TestMapCartesian(t *testing.T) {
	t.Run("generates Cartesian product along keys", func(t *testing.T) {
		when := map[string][]string{
			"head":   {"baseball cap", "straw hat"},
			"top":    {"t-shirt", "blouse", "pullover"},
			"bottom": {"jeans", "skirt"},
		}

		expected := []map[string]string{
			// 2 x 3 x 2 = 12 patterns
			{
				"head":   "baseball cap",
				"top":    "t-shirt",
				"bottom": "jeans",
			},
			{
				"head":   "baseball cap",
				"top":    "t-shirt",
				"bottom": "skirt",
			},
			{
				"head":   "baseball cap",
				"top":    "blouse",
				"bottom": "jeans",
			},
			{
				"head":   "baseball cap",
				"top":    "blouse",
				"bottom": "skirt",
			},
			{
				"head":   "baseball cap",
				"top":    "pullover",
				"bottom": "jeans",
			},
			{
				"head":   "baseball cap",
				"top":    "pullover",
				"bottom": "skirt",
			},
			{
				"head":   "straw hat",
				"top":    "t-shirt",
				"bottom": "jeans",
			},
			{
				"head":   "straw hat",
				"top":    "t-shirt",
				"bottom": "skirt",
			},
			{
				"head":   "straw hat",
				"top":    "blouse",
				"bottom": "jeans",
			},
			{
				"head":   "straw hat",
				"top":    "blouse",
				"bottom": "skirt",
			},
			{
				"head":   "straw hat",
				"top":    "pullover",
				"bottom": "jeans",
			},
			{
				"head":   "straw hat",
				"top":    "pullover",
				"bottom": "skirt",
			},
		}

		actual := combo.MapCartesian(when)

		if !cmp.SliceContentEqWith(actual, expected, cmp.MapEq[string, string]) {
			t.Errorf(
				"unmatch:\n- actual (len=%d)   : %+v\n- expected (len=%d) : %+v",
				len(actual), actual,
				len(expected), expected,
			)
		}
	})

	t.Run("empty map generates empty", func(t *testing.T) {
		actual := combo.MapCartesian(map[int][]string{})
		if len(actual) != 0 {
			t.Error("unexpected items found: ", actual)
		}
	})

	t.Run("when there is an empty dimension, it generates empty", func(t *testing.T) {
		when := map[string][]string{
			"head":   {}, // empty!
			"top":    {"t-shirt", "blouse", "pullover"},
			"bottom": {"jeans", "skirt"},
		}

		actual := combo.MapCartesian(when)
		if len(actual) != 0 {
			t.Error("unexpected items found: ", actual)
		}
	})

	t.Run("when the basis is 1-dimensional, it works as just flattening", func(t *testing.T) {
		when := map[int][]string{
			1: {"tic", "tac", "toe"},
		}

		actual := combo.MapCartesian(when)

		expected := []map[int]string{
			{1: "tic"},
			{1: "tac"},
			{1: "toe"},
		}

		if !cmp.SliceContentEqWith(actual, expected, cmp.MapEq[int, string]) {
			t.Errorf(
				"unmatch:\n- actual (len=%d)   : %+v\n- expected (len=%d) : %+v",
				len(actual), actual,
				len(expected), expected,
			)
		}
	})
}
