//go:build !windows

package open

import "os"

// NewSafeFile creates a new empty file which is accessible only by the current user.
//
// If the file already exists, it will be truncated.
func NewSafeFile(filepath string) (*os.File, error) {
	f, err := os.OpenFile(filepath, os.O_TRUNC|os.O_CREATE|os.O_RDWR, os.FileMode(0600))
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(0); err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}
	return f, nil
}
