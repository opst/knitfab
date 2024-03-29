package function

// convert func(T) to func(T)R returning zerovalue of R.
func Void[R any, T any](f func(t T)) func(t T) R {
	return func(t T) R {
		f(t)

		var r R
		return r
	}
}

// return true for all T
func Every[T any](T) bool {
	return true
}

// return false for all T
func Never[T any](T) bool {
	return false
}
