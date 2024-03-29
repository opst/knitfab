package path_test

import (
	"os"
	"path"
	"path/filepath"
	"testing"

	kpath "github.com/opst/knitfab/pkg/utils/path"
)

func TestResolve(t *testing.T) {
	t.Run("it does nothing for absolute path", func(t *testing.T) {
		input := "/a/b/c"
		result, err := kpath.Resolve(input)

		if err != nil {
			t.Fatalf("unexpected error: %s (%+v)", err.Error(), err)
		}
		if result != input {
			t.Errorf(
				"it changes absolute path: (actual, expected) != (%s, %s)", result, input,
			)
		}
	})

	t.Run("it expands tilde(~) into user home", func(t *testing.T) {
		userhome, err := os.UserHomeDir()
		if err != nil {
			t.Fatalf("can not get home dir: %s (%+v)", err.Error(), err)
		}
		input := "~/a/b/c"
		result, err := kpath.Resolve(input)
		expected := path.Join(userhome, "a/b/c")

		if err != nil {
			t.Fatalf("unexpected error: %s (%+v)", err.Error(), err)
		}
		if result != expected {
			t.Errorf(
				"~ is not resolved into home dir: (actual, expected) != (%s, %s)", result, expected,
			)
		}
	})

	t.Run("it resolves relative path into absolute path", func(t *testing.T) {
		pwd, err := os.Getwd()
		if err != nil {
			t.Fatalf("can not get workdir: %s (%+v)", err.Error(), err)
		}
		input := "./a/b/c"
		result, err := kpath.Resolve(input)
		expected := path.Join(pwd, "a/b/c")

		if err != nil {
			t.Fatalf("unexpected error: %s (%v)", err.Error(), err)
		}
		if result != expected {
			t.Errorf(
				"relative path is not resolved: (actual, expected) != (%s, %s)", result, expected,
			)
		}
	})

	t.Run("it cleanes path", func(t *testing.T) {
		input := "/a/x/../y/z/../../b/c/d/.."
		result, err := kpath.Resolve(input)
		expected := "/a/b/c"

		if err != nil {
			t.Fatalf("unexpected error: %s (%v)", err.Error(), err)
		}
		if result != expected {
			t.Errorf(
				"path is not cleaned: (actual, expected) != (%s, %s)", result, expected,
			)
		}
	})

	t.Run("it cleanes relative path", func(t *testing.T) {
		pwd, err := os.Getwd()
		if err != nil {
			t.Fatalf("can not get workdir: %s (%+v)", err.Error(), err)
		}
		parent, _ := filepath.Split(pwd)
		input := "../a/x/../y/z/../../b/c/d/.."
		result, err := kpath.Resolve(input)
		expected := path.Join(parent, "a/b/c")

		if err != nil {
			t.Fatalf("unexpected error: %s (%v)", err.Error(), err)
		}
		if result != expected {
			t.Errorf(
				"path is not cleaned: (actual, expected) != (%s, %s)", result, expected,
			)
		}
	})

	t.Run("when path contains too many ../, it returns root", func(t *testing.T) {
		input := "/a/b/c/../../../../"
		result, err := kpath.Resolve(input)
		expected := "/"

		if err != nil {
			t.Fatalf("unexpected error: %s (%v)", err.Error(), err)
		}
		if result != expected {
			t.Errorf(
				"path is not cleaned: (actual, expected) != (%s, %s)", result, expected,
			)
		}
	})
}
