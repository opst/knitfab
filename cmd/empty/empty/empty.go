package empty

import (
	"errors"
	"io"
	"os"
)

// # returns
//
// nil if path is pointing to an empty directory.
//
// non-nil error value if...
//
// - it is a file
//
// - it is a directory contains something
//
// - it is not existing or not accessible
func Assert(p string) error {
	fi, err := os.Open(p)
	if err != nil {
		return err
	}
	defer fi.Close()

	entry, err := fi.ReadDir(1)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	if len(entry) != 0 {
		return errors.New("it has some contents")
	}

	return nil
}
