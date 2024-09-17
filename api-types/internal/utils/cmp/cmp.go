package cmp

import "maps"

func SliceEqualUnordered[T interface{ Equal(T) bool }](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}

	// make a copy of b
	b = append([]T(nil), b...)

A:
	for _, x := range a {
		for i, y := range b {
			if x.Equal(y) {
				// remove y from b
				b = append(b[:i], b[i+1:]...)
				continue A
			}
		}
		return false
	}

	return len(b) == 0
}

func MapEqual[K comparable, V interface{ Equal(V) bool }](a, b map[K]V) bool {
	if len(a) != len(b) {
		return false
	}

	// copy b
	b = maps.Clone(b)

	for k, va := range a {
		vb, ok := b[k]
		if !ok || !va.Equal(vb) {
			return false
		}
		delete(b, k)
	}

	return len(b) == 0
}
