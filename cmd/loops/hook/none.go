package hook

// None is a hook that does nothing.
type None[T any] struct{}

func (None[T]) Before(value T) error {
	return nil
}

func (None[T]) After(value T) error {
	return nil
}
