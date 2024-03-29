package utils

import (
	"errors"
	"os"
	"path/filepath"
)

var ErrSearchFile = errors.New("could not search file")

func SearchFilePathtoUpward(root string, fileName string) (*string, error) {

	path := filepath.Join(root, fileName)
	if _, err := os.Stat(path); err == nil {
		return &path, nil
	}

	parent := filepath.Dir(root)
	if parent != root {
		return SearchFilePathtoUpward(parent, fileName)
	} else {
		return nil, ErrSearchFile
	}
}
