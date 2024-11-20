package cmp

type BiPredicator[V any, U any] func(a V, b U) bool

// check a == b
func MapEq[K comparable, V comparable](a map[K]V, b map[K]V) bool {
	if len(a) != len(b) {
		return false
	}
	return MapGeq(a, b) && MapLeq(a, b)
}

// check a == b, in context of comparator
func MapEqWith[K comparable, V any, U any](a map[K]V, b map[K]U, comparator BiPredicator[V, U]) bool {
	if len(a) != len(b) {
		return false
	}
	return MapGeqWith(a, b, comparator) && MapLeqWith(a, b, comparator)
}

// check a ⊆ b
func MapLeq[K comparable, V comparable](a map[K]V, b map[K]V) bool {
	// a >= b ?
	for ka, va := range a {
		vb, ok := b[ka]
		if !ok || vb != va {
			return false
		}
	}

	return true
}

// check a ⊆ b, in context of comparator
func MapLeqWith[K comparable, V any, U any](a map[K]V, b map[K]U, comparator BiPredicator[V, U]) bool {
	// a >= b ?
	for ka, va := range a {
		vb, ok := b[ka]
		if !ok || !comparator(va, vb) {
			return false
		}
	}

	return true
}

// check b ⊆ a
func MapGeq[K comparable, V comparable](a map[K]V, b map[K]V) bool {
	for kb, vb := range b {
		va, ok := a[kb]
		if !ok || va != vb {
			return false
		}
	}
	return true
}

// check b ⊆ a, in context of comparator
func MapGeqWith[K comparable, V any, U any](a map[K]V, b map[K]U, comparator BiPredicator[V, U]) bool {
	for kb, vb := range b {
		va, ok := a[kb]
		if !ok || !comparator(va, vb) {
			return false
		}
	}
	return true
}

// Compare map with predicators
//
// args:
//     - a: map to be tested
//     - predicators: map from key to predicator for key.
//
// returns:
//     `true` when key set of `a` and `predicators` are equal,
//     and all of `predicators[k](a[k])` are true for each key of `a`.
//     otherwise `false`.
//
func MapMatch[K comparable, V any](a map[K]V, predicators map[K]func(v V) bool) bool {
	for k, v := range a {
		p, ok := predicators[k]
		if !ok {
			return false
		}
		if !p(v) {
			return false
		}
	}
	for k := range predicators {
		if _, ok := a[k]; !ok {
			return false
		}
	}
	return true
}
