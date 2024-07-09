package io

import (
	"io"
	"os"
	"path/filepath"
)

// CreateAll creates file with its parent direcrtory, if missing.
//
// # Args
//
// - name: filepath to be created.
//
// - fmod: os.FileMode for file.
//
// - dmod: os.FileMode for directory.
//
// Note that `dmod` effects to only newly-created direcotries.
// So, directoreis which have existed are not effected with `dmod`.
//
// # Return
//
// - *os.File: created file, if successful.
//
// - err: error, if failed.
//
// When a file is created successfully, `(file, nil)` pair will be returned.
// Or, if it failed creating one of file or direcories, `(nil, err)` pair will be returned.
func CreateAll(name string, fmod os.FileMode, dmod os.FileMode) (*os.File, error) {

	dirname := filepath.Dir(name)
	if err := os.MkdirAll(dirname, dmod); err != nil {
		return nil, err
	}

	return os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, fmod)
}

// DirCopy copies all files in the directory `src` to the directory `dest`.
//
// # Args
//
// - src: source directory.
//
// - dest: destination directory.
//
// # Return
//
// - err: error, if failed.
func DirCopy(src string, dest string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		if info.IsDir() {
			if err := os.Mkdir(filepath.Join(dest, rel), info.Mode()); err != nil {
				if !os.IsExist(err) {
					return err
				}
			}
			return nil
		}

		destPath := filepath.Join(dest, rel)
		dest, err := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, info.Mode())
		if err != nil {
			return err
		}
		defer dest.Close()

		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()

		if _, err := io.Copy(dest, src); err != nil {
			return err
		}

		return nil
	})
}
