package errors_test

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"

	xe "github.com/opst/knitfab/pkg/errors"
)

type MyErr struct{}

func (MyErr) Error() string {
	return "error type for test"
}

func createError(message string) error {
	return xe.New(message)
}

func TestNewError(t *testing.T) {
	t.Run("it knows location where it is created.", func(t *testing.T) {
		testee := createError("test error")
		errMessage := testee.Error()

		_, thisFile, _, _ := runtime.Caller(0)

		if !strings.Contains(errMessage, "createError") {
			t.Errorf("it does not know function name: %s", errMessage)
		}

		if !strings.Contains(errMessage, thisFile) {
			t.Errorf("it does not know file (%s): %s", thisFile, errMessage)
		}
	})

	t.Run("it supports errors protocol", func(t *testing.T) {
		rootError := MyErr{}

		err := xe.Wrap(
			fmt.Errorf(
				"%w",
				fmt.Errorf("%w", rootError),
			),
		)

		if !errors.Is(err, rootError) {
			t.Error("it does not support unwrapping.")
		}
	})

}
