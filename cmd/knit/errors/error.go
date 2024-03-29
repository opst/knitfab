package errors

import (
	"fmt"
	"strings"
)

type Verbose interface {
	Verbose() string
}

type CUIError interface {
	error
	Verbose
}

type cuierror struct {
	summary     string
	verbose     string
	printDetail func(summary string) (string, error)
	base        error
}

func (ce *cuierror) Unwrap() error {
	return ce.base
}

func (ce *cuierror) Error() string {
	if ce.printDetail == nil {
		return ce.summary
	}
	message, err := ce.printDetail(ce.summary)
	if err != nil {
		message = fmt.Sprintf(
			"%s\n(building detailed message causes error: %s)",
			ce.summary, err.Error(),
		)
	}
	return message
}

func (ce *cuierror) Verbose() string {
	message := []string{ce.Error()}
	if ce.verbose != "" {
		message = append(message, " ("+ce.verbose+") ")
	}

	switch base := ce.base.(type) {
	case nil:
		// no-op
	case Verbose:
		message = append(message, "caused by: ", base.Verbose())
	default:
		message = append(message, "caused by: ", base.Error())
	}
	return strings.Join(message, "\n")
}

type CuiErrorOption func(cerr *cuierror) *cuierror

func NewCuiError(
	summary string,
	options ...CuiErrorOption,
) CUIError {
	err := &cuierror{summary: summary}
	for _, o := range options {
		err = o(err)
	}
	return err
}

func WithVerbose(verbose string) CuiErrorOption {
	return func(cerr *cuierror) *cuierror {
		cerr.verbose = verbose
		return cerr
	}
}

func WithDetail(printer func(summary string) (string, error)) CuiErrorOption {
	return func(cerr *cuierror) *cuierror {
		cerr.printDetail = printer
		return cerr
	}
}

func WithCause(err error) CuiErrorOption {
	return func(cerr *cuierror) *cuierror {
		cerr.base = err
		return cerr
	}
}
