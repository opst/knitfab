package io

import (
	"os"
	"path/filepath"
)

// create file with its parent direcrtory, if missing.
//
// args:
//   - name: filepath to be created.
//   - fmod: os.FileMode for file.
//   - dmod: os.FileMode for directory.
//
// Note that `dmod` effects to only newly-created direcotries.
// So, directoreis which have existed are not effected with `dmod`.
//
// return (*os.File, err):
//   When a file is created successfully, `(file, nil)` pair will be returned.
//   Or, if it failed creating one of file or direcories, `(nil, err)` pair will be returned.
//
func CreateAll(name string, fmod os.FileMode, dmod os.FileMode) (*os.File, error) {

	dirname := filepath.Dir(name)
	if err := os.MkdirAll(dirname, dmod); err != nil {
		return nil, err
	}

	return os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, fmod)
}
