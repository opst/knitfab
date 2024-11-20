package k8s

import (
	"errors"
	"fmt"

	xe "github.com/opst/knitfab/pkg/errors"
)

type wrappingError struct {
	message  string
	causedBy error
}

func as[E error](err error) bool {
	if err == nil {
		return false
	}
	p := new(E)
	return errors.As(err, p)
}

func format(e struct {
	message  string
	causedBy error
}) string {
	if e.causedBy == nil {
		return e.message
	}
	if e.message == "" {
		return fmt.Sprintf("caused by: %+v", e.causedBy)
	}

	return fmt.Sprintf("%s / caused by: %+v", e.message, e.causedBy)
}

// Requested resource does not exists.
type ErrMissing wrappingError

var AsMissingError = as[*ErrMissing]

// returns ErrMissing with message.
func NewMissing(message string) error {
	return xe.WrapAsOuter(&ErrMissing{message: message}, 1)
}

func NewMissingCausedBy(message string, err error) error {
	return xe.WrapAsOuter(&ErrMissing{message: message, causedBy: err}, 1)
}

func (e *ErrMissing) Error() string {
	return format(*e)
}

func (e *ErrMissing) Unwrap() error {
	return e.causedBy
}

// Failed to provisioning k8s resource (e.g., pod, service, pvc, ...) since it is already exists.
type ErrConflict wrappingError

var AsConflict = as[*ErrConflict]

func NewConflict(message string) error {
	return xe.WrapAsOuter(&ErrConflict{message: message}, 1)
}

func NewConflictCausedBy(message string, err error) error {
	return xe.WrapAsOuter(&ErrConflict{message: message, causedBy: err}, 1)
}

func (e *ErrConflict) Error() string {
	return format(*e)
}

func (e *ErrConflict) Unwrap() error {
	return e.causedBy
}

// provisioning takes too long time.
var ErrDeadlineExceeded = errors.New("deadline exceeded")
