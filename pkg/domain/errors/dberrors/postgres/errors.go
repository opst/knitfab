package postgres

import (
	"fmt"

	domerr "github.com/opst/knitfab/pkg/domain/errors"
)

// requested data is missing.
type Missing struct {
	Table    string
	Identity string
}

var _ error = Missing{}

func (m Missing) Error() string {
	return fmt.Sprintf("%s is not found in %s ", m.Identity, m.Table)
}
func (m Missing) Unwrap() error {
	return domerr.ErrMissing
}

// requested data is found too much.
type TooMuch struct {
	Table    string
	Identity string
	Expected int
}

var _ error = TooMuch{}

func (t TooMuch) Error() string {
	return fmt.Sprintf(
		"%s is found in %s more than %d times",
		t.Identity, t.Table, t.Expected,
	)
}

func (t TooMuch) Unwrap() error {
	return domerr.ErrTooMuch
}
