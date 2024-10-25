package hook

import "errors"

// Func is a hook that calls functions before and after processing the value T.
type Func[T any, R any] struct {
	// BeforeFn is a function to call before processing the value T.
	//
	// If BeforeFn is nil, it is not called.
	BeforeFn func(T) (R, error)

	// AfterFn is a function to call after processing the value T.
	//
	// If AfterFn is nil, it is not called.
	AfterFn func(T) error
}

func (f Func[T, R]) Before(value T) (R, error) {
	if f.BeforeFn == nil {
		return *new(R), nil
	}
	ret, err := f.BeforeFn(value)
	if err != nil {
		return ret, errors.Join(err, ErrHookFailed)
	}
	return ret, nil
}

func (f Func[T, R]) After(value T) error {
	if f.AfterFn == nil {
		return nil
	}
	err := f.AfterFn(value)
	if err != nil {
		return errors.Join(err, ErrHookFailed)
	}
	return nil
}
