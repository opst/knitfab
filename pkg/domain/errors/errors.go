package types

import (
	"errors"
)

var ErrMissing = errors.New("item missing")
var ErrTooMuch = errors.New("found more than expected")
