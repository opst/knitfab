package testutils

import (
	"os"
	"testing"

	prof "github.com/opst/knitfab/cmd/knit/config/profiles"
	"gopkg.in/yaml.v3"
)

// create profile file for test.
//
// the created file is removed after testcase automaticaly.
//
// args:
//   - *testing.T
//   - name: name
//   - profile: profile to be created for test
//
// returns:
//   - string: filepath to profile file. if creating is failed, it will be `""`
//   - error: error caused during creating profile file. if creating is not failed, it will be `nil`.
func TempProfile(t *testing.T, name string, profile *prof.KnitProfile) (string, error) {
	t.Helper()

	tmprof, err := os.CreateTemp("", "")
	if err != nil {
		return "", err
	}
	defer tmprof.Close()
	filepath := tmprof.Name()

	t.Cleanup(func() { os.Remove(filepath) })

	if err := yaml.NewEncoder(tmprof).Encode(prof.ProfileStore{name: profile}); err != nil {
		return "", err
	}

	return filepath, nil
}
