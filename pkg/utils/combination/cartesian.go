package combination

import "github.com/opst/knitfab/pkg/utils/slices"

// from map[K][]T, choices one item for each keys and generate cartesian product.
//
// # Example:
//
// [example 1.] generating recipe...
//
//	MapCartesian(map[string][]string{
//		"make": {"jerry", "compote", "cake"},
//		"using": {"apple", "peach", "grape"},
//	})
//
// generates cartecian product ("make" × "using") as below.
//
//	[]map[string]string{
//		{"make": "jerry", "using": "apple"},
//		{"make": "jerry", "using": "peach"},
//		{"make": "jerry", "using": "grape"},
//
//		{"make": "compote", "using": "apple"},
//		{"make": "compote", "using": "peach"},
//		{"make": "compote", "using": "grape"},
//
//		{"make": "cake", "using": "apple"},
//		{"make": "cake", "using": "peach"},
//		{"make": "cake", "using": "grape"},
//	}
//
// [example 2.] generating 1 digit x 1 digit product table
//
//	MapCartesian(map[rune][]int{
//		'l': {1,2,3,4,5,6,7,8,9},
//		'r': {1,2,3,4,5,6,7,8,9},
//	})
//
// generates
//
//	[]map[rune]int{
//		{'l': 1, 'r': 1},  // 1 × 1
//		{'l': 1, 'r': 2},  // 1 × 2
//		{'l': 1, 'r': 3},  // 1 × 3
//		        ... snip ...
//		{'l': 9, 'r': 7},  // 9 × 7
//		{'l': 9, 'r': 8},  // 9 × 8
//		{'l': 9, 'r': 9},  // 9 × 9
//	}
//
// # args:
//
// - basis : basis of cartesian product.
//
// # returning:
//
// - []map[K]V : Each item has same keys in basis.
// For each key for each item, the value is one of basis[key].
func MapCartesian[K comparable, V any](basis map[K][]V) []map[K]V {
	dims := len(basis)
	if dims == 0 {
		return []map[K]V{}
	}

	keys := make([]K, 0, dims)
	for k, p := range basis {
		size := len(p)
		if size == 0 {
			// if any dimensions are zero-width, given space is empty.
			return []map[K]V{}
		}
		keys = append(keys, k)
	}

	var cartesian func(known []map[K]V, rem []K) []map[K]V // prepare for recursion.
	cartesian = func(known []map[K]V, rem []K) []map[K]V {
		if len(rem) <= 0 {
			return known
		}

		topic := rem[0]
		newKnwon := []map[K]V{}

		for _, item := range basis[topic] {
			clone := slices.Map(known, mapCopy[K, V])

			for i := range clone {
				clone[i][topic] = item
			}

			newKnwon = append(newKnwon, clone...)
		}

		return cartesian(newKnwon, rem[1:])
	}

	seed := keys[0]
	rem := keys[1:]

	known := slices.Map(basis[seed], func(item V) map[K]V {
		return map[K]V{seed: item}
	})

	return cartesian(known, rem)
}

func mapCopy[K comparable, V any](base map[K]V) map[K]V {
	new := make(map[K]V, len(base))
	for k := range base {
		new[k] = base[k]
	}
	return new
}
