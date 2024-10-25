package hook

// None is a hook that does nothing.
type None[T any] struct{}

func (None[T]) Before(value T) (struct{}, error) {
	return struct{}{}, nil
}

func (None[T]) After(value T) error {
	return nil
}
