package hook

import (
	"errors"
)

// Hook is an interface for before/after hooks.
type Hook[T any, R any] interface {
	// Before is called before the value T is processed.
	Before(T) (R, error)

	// After is called after the value T is processed.
	After(T) error
}

var ErrHookFailed = errors.New("hook failed")
