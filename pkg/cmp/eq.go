package cmp

type Eq interface {
	Equal(b Eq) bool
}

// Check two values a, b are equal.
//
// args:
//   - a, b: values under checking
//
// return: true if two values are equal. Otherwise false.
func Equal[T Eq](a T, b T) bool {
	return a.Equal(b)
}

// a == b as BiPredicator function
func EqEq[T comparable](a, b T) bool {
	return a == b
}

// *a == *b as BiPredicator function
func PEqEq[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func PEqualWith[T any](a, b *T, pred func(T, T) bool) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return pred(*a, *b)
}
