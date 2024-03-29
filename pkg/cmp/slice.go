package cmp

import "github.com/opst/knitfab/pkg/utils"

func SliceEq[T comparable](a []T, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for nth, va := range a {
		if va != b[nth] {
			return false
		}
	}
	return true
}

func SliceEqWith[T any, U any](a []T, b []U, pred func(a T, b U) bool) bool {
	if len(a) != len(b) {
		return false
	}

	for nth := range a {
		if !pred(a[nth], b[nth]) {
			return false
		}
	}

	return true
}

// Check a has b as its sub-sequence.
//
// Example
//
//	SliceContains(
//		[]int{1, 2, 3, 4, 5},
//		[]int{3, 4},
//	)  // => true
//
//	SliceContains(
//		[]int{1, 2, 3, 4, 5},
//		[]int{3, 5},
//	) // => false. should be sub-sequence.
//
//	SliceContains(
//		[]int{1, 2, 3, 4, 5},
//		[]int{4, 3},
//	) // => false. ordering matters
//
//	SliceContains(
//		[]int{1, 2, 3, 4, 5},
//		[]int{},
//	) // => true. empty is everywhere, of cource.
//
//	SliceContains(
//		[]int{1, 2, 3, 4, 5},
//		[]int{1, 2, 3, 4, 5},
//	) // => true.
func SliceContains[T comparable](a []T, b []T) bool {
	// This is O(n^2) order function.
	//
	// There are space to prune calcurations,
	// but not have done yet for simplicity.

	if len(a) < len(b) {
		return false
	}

	head := a[:len(b)]
	if SliceEq(head, b) {
		return true
	}

	// this can be more smart. find a[i] = b[0] and do SliceContains(a[n:], b)
	return SliceContains(a[1:], b)
}

// Check A ⊇ B in some equivarency.
//
// In other words, when we can select an equivarent element in A for each elements in B,
// it returns true.
//
// In contrast of SliceContains, this function does not matter ordering.
//
// # Args
//
// - a, b []T: compared slice A and B.
//
// If and only if a equals b or a is a superset of b, it returns true.
//
// Otherwise, it returns false,
//
// - pred: function returning true if elements comming from A and B are equivarent.
//
// # Return
//
// B is subset A (true) or not (false).
//
// # Example
//
// It behaves as below:
//
//	SliceSubsetWith(
//		[]int{1, 2, 3, 4, 5},
//		[]int{3, 4},
//		func(ae, be int) bool { return ae == be },
//	) // => true. {1, 2, 3, 4, 5} ⊇ {3, 4} .
//
//	SliceSubsetWith(
//		[]int{1, 2, 3, 4, 5},
//		[]int{1, 2, 3, 4, 5},
//		func(ae, be int) bool { return ae == be },
//	) // => true. {1, 2, 3, 4, 5} ⊇ {1, 2, 3, 4, 5}, of cource.
//
//	SliceSubsetWith(
//		[]int{1, 2, 3, 4, 5},
//		[]int{4, 3},
//		func(ae, be int) bool { return ae == be },
//	) // => true. Ordering does not matter.
//
//	SliceSubsetWith(
//		[]int{1, 2, 3, 4, 5},
//		[]int{2, 5},
//		func(ae, be int) bool { return ae == be },
//	) // => true. Sub-sequenceness does not matter.
//
//	SliceSubsetWith(
//		[]int{1, 2, 3, 4, 5},
//		[]int{3, 4, 5, 6},
//		func(ae, be int) bool { return ae == be },
//	) // => false. 6 is not in {1, 2, 3, 4, 5} .
//
//	SliceSubsetWith(
//		[]int{1, 2, 3, 4, 5},
//		[]int{3, 3, 4},
//		func(ae, be int) bool { return ae == be },
//	) // => false. Two 3s are not found in {1, 2, 3, 4, 5} .
//
//	SliceSubsetWith(
//		[]int{1, 2, 3, 4, 5},
//		[]int{3, 3, 4},
//		func(ae, be int) bool { return ae%2 == be%2 },
//	) // => true. We can find two odds and one even in {1, 2, 3, 4, 5} .
func SliceSubsetWith[A, B any](a []A, b []B, pred func(A, B) bool) bool {
	if len(b) == 0 {
		return true
	}

	if len(a) < len(b) {
		return false
	}

	rest := utils.RefOf(a)

NEXT_B:
	for _, be := range b {
		for i, ae := range rest {
			if !pred(*ae, be) {
				continue
			}
			// drop i-th element, since it is used.
			rest = append(rest[:i], rest[i+1:]...)
			continue NEXT_B
		}
		return false
	}

	return true
}

// check 2 slices has same content but its ordering.
//
// In other words, this function answers equality of two bags (or multi-sets).
//
// example:
//
//	SliceContentEq([]string{"a", "b", "c"}, []string{"c", "b", "a"})            // ==> true
//	SliceContentEq([]string{"a", "b", "c"}, []string{"c", "b", "a", "z"})       // ==> false
//	SliceContentEq([]string{"a", "b", "c"}, []string{"c", "b", "z"})            // ==> false
//	SliceContentEq([]string{"a", "b", "c", "c"}, []string{"a", "b", "c"})       // ==> false. left has 2 "c"s but right has only 1.
//	SliceContentEq([]string{"a", "b", "c", "c"}, []string{"a", "b", "c", "c"})  // ==> true
func SliceContentEq[T comparable](a, b []T) bool {
	return SliceContentEqWith(a, b, EqEq[T])
}

// check 2 slice has equivarent content but its ordering.
//
// In other words, this function answers equivalence of two bags (or multi-sets).
//
// args:
//   - a []S, b []T: slices to be compaired
//   - equiv: predicator says that two instance of T are equiverent or not.
//
// return:
//
//	true when slices `a` and `b` are equiverent (as bag).
//	otherwise, false.
func SliceContentEqWith[S, T any](a []S, b []T, equiv BiPredicator[S, T]) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}

	bm := make(map[int]*T, len(b))
	for i := range b {
		bm[i] = &b[i]
	}

NEXT_A:
	for _, va := range a {
		for k, vb := range bm {
			if equiv(va, *vb) {
				delete(bm, k)
				continue NEXT_A
			}
		}
		return false
	}

	return len(bm) == 0
}
