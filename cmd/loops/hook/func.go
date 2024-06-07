package hook

import "errors"

// Func is a hook that calls functions before and after processing the value T.
type Func[T any] struct {
	// BeforeFn is a function to call before processing the value T.
	//
	// If BeforeFn is nil, it is not called.
	BeforeFn func(T) error

	// AfterFn is a function to call after processing the value T.
	//
	// If AfterFn is nil, it is not called.
	AfterFn func(T) error
}

func (f Func[T]) Before(value T) error {
	if f.BeforeFn == nil {
		return nil
	}
	err := f.BeforeFn(value)
	if err != nil {
		return errors.Join(err, ErrHookFailed)
	}
	return nil
}

func (f Func[T]) After(value T) error {
	if f.AfterFn == nil {
		return nil
	}
	err := f.AfterFn(value)
	if err != nil {
		return errors.Join(err, ErrHookFailed)
	}
	return nil
}
