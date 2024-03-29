package pointer

func Ref[T any](t T) *T {
	return &t
}

func Deref[T any](ptr *T) T {
	return *ptr
}

func SafeDeref[T any](val *T) T {
	if val == nil {
		return *new(T)
	}
	return *val
}
