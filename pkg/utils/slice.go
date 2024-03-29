package utils

import (
	"sort"
)

// map each element in sli.
//
// args:
//     - sli : slice of `T`s
//     - mapper : mapping function from T to R
// return:
//     slice of `R`s.
//     each element indexed `N` is given with `mapper(sli[N])` .
func Map[T any, R any](sli []T, mapper func(v T) R) []R {
	ret := make([]R, len(sli))
	for nth, v := range sli {
		ret[nth] = mapper(v)
	}
	return ret
}

// convert slice-of-values to slice-of-pointers
func RefOf[T any](sli []T) []*T {
	return Map(sli, func(v T) *T { return &v })
}

// convert slice-of-pointers to slice-of-values
func DerefOf[T any](sli []*T) []T {
	return Map(sli, func(v *T) T { return *v })
}

// Map over sli with mapper.
//
// If mapper causes error, return (nil, error).
//
// Otherwise, return (mapping result, nil).
func MapUntilError[T any, R any](sli []T, mapper func(v T) (R, error)) ([]R, error) {
	ret := make([]R, len(sli))
	for nth, v := range sli {
		r, err := mapper(v)
		if err != nil {
			return nil, err
		}
		ret[nth] = r
	}
	return ret, nil
}

// convert slice to map.
//
// If keys given with getkey collides, a value coming latter takes over previous.
//
// args:
//     - sli: source slice
//     - getkey: get key from an element of sli
// returns:
//     map{
//         genkey(sli[0]): sli[0], ...
//         genkey(sli[n]): sli[n], ...
//         genkey(sli[len(sli-1)]): sli[len(sli)-1],
//     }
//
//
func ToMap[T any, K comparable](sli []T, getkey func(v T) K) map[K]T {
	m := map[K]T{}

	for _, v := range sli {
		m[getkey(v)] = v
	}

	return m
}

func ToMultiMap[T any, K comparable, R any](sli []T, pair func(v T) (K, R)) map[K][]R {
	m := map[K][]R{}
	for _, i := range sli {
		k, v := pair(i)
		m[k] = append(m[k], v)
	}
	return m
}

// flatten map to slice
//
// args:
//   - m: map to be flatten
// returns:
//   slice which contains keys of `m`
func KeysOf[T any, K comparable](m map[K]T) []K {
	sli := make([]K, 0, len(m))
	for k := range m {
		sli = append(sli, k)
	}
	return sli
}

// flatten map to slice
//
// args:
//   - m: map to be flatten
// returns:
//   slice which contains values of `m`
func ValuesOf[T any, K comparable](m map[K]T) []T {
	sli := make([]T, 0, len(m))
	for _, value := range m {
		sli = append(sli, value)
	}
	return sli
}

// filter elements match with predicator
//
// args:
//
// - vs: slice
//
// - predicator: function returns true for each element to be remain in result
//
// returns:
//
// - []T: elements in vs which predicator evaluates as true.
func Filter[T any](vs []T, predicator func(T) bool) []T {
	ret := []T{}
	if len(vs) == 0 {
		return ret
	}

	for _, v := range vs {
		if predicator(v) {
			ret = append(ret, v)
		}
	}
	return ret
}

// find first element match with predicator.
//
// args:
//     - sli: slice to be scannd
//     - predicator: function return true iff given value is your searching one.
// retruns:
//     (T, true) if found. otherwise, (zero value of T, false)
func First[T any](sli []T, predicator func(T) bool) (T, bool) {
	for _, v := range sli {
		if predicator(v) {
			return v, true
		}
	}

	var zero T
	return zero, false
}

// apply all modifier operator
//
// args:
//     - value : modification subject
//     - modifier : modifier operator, which takes `*T` value and update it.
// returns:
//     value after modifier applied
func ApplyAll[T any](value *T, modifier ...func(*T) *T) *T {
	for _, mod := range modifier {
		value = mod(value)
	}
	return value
}

// sort slice. this does non-stable sort.
//
// args:
//     - []T : slice to be sorted
//     - less :  ordering function. see: `sort.Interface.Less`
func Sorted[T any](sli []T, less func(a, b T) bool) []T {
	sorted := make([]T, len(sli))
	copy(sorted, sli)

	sort.Slice(sorted, func(i, j int) bool {
		return less(sorted[i], sorted[j])
	})
	return sorted
}

// search index for `item` to be inserted keeping `sli` is sorted.
//
// Note: this function ASSUMES AND RELYS ON `sli` IS SORTED.
//       If you pass not-sorted `sli`, you may get unexpected result.
//
// args:
//     - sli : sorted slice. containing duplicated value is ok.
//     - item : new item to be inserted
//     - less : ordering function. see: `sort.Interface.Less`
// return:
//     index before the equal or next grater value of `item`.
func BinarySearch[T any](sli []T, item T, less func(a, b T) bool) int {
	length := len(sli)
	if length == 0 {
		return 0
	}

	pos := int(len(sli) / 2)
	pivot := sli[pos]

	if !less(pivot, item) { // item <= pivot
		return BinarySearch(sli[:pos], item, less)
	} else {
		return pos + 1 + BinarySearch(sli[pos+1:], item, less)
	}
}

// concatenate slices
func Concat[T any](sli ...[]T) []T {
	l := 0
	for _, s := range sli {
		l += len(s)
	}

	dest := make([]T, 0, l)
	for _, s := range sli {
		dest = append(dest, s...)
	}
	return dest
}

// flatten slice of slice to simple slice.
//
// Example
//
// 	Flatten([][]int{{1, 2, 3}, {4, 5, 6}}) // -> []int{1, 2, 3, 4, 5, 6}
//
func Flatten[T any](complex [][]T) []T {
	if l := len(complex); l == 0 {
		return []T{}
	} else if l == 1 {
		new := make([]T, len(complex[0]))
		copy(new, complex[0])
		return new
	}

	total := 0
	for _, i := range complex {
		total += len(i)
	}

	dest := make([]T, 0, total)
	for _, i := range complex {
		dest = append(dest, i...)
	}

	return dest
}

// Grouping slices into 2 part, match and notmatch in predicator p .
func Group[T any](s []T, p func(T) bool) (match []T, notmatch []T) {
	for i := range s {
		v := s[i]
		if p(v) {
			match = append(match, v)
		} else {
			notmatch = append(notmatch, v)
		}
	}

	return
}
