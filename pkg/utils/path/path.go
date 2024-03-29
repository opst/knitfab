package path

import (
	"os"
	"path/filepath"
	"strings"
)

const tilde = "~" + string(filepath.Separator)

// return absolute representation of path, with expanding "~" to user's home directory.
//
// args:
//     - pathstring: path to be resolved
// return:
//     - string: resolved filepath
//     - error
func Resolve(pathstring string) (string, error) {
	if strings.HasPrefix(pathstring, tilde) {
		homedir, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		pathstring = filepath.Join(homedir, pathstring[2:])
	}
	return filepath.Abs(pathstring)
}
