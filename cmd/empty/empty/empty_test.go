package empty_test

import (
	"os"
	"path"
	"testing"

	"github.com/opst/knitfab/cmd/empty/empty"
)

func TestAssertEmpty(t *testing.T) {

	t.Run("it returns nil for an empty directory", func(t *testing.T) {
		td := t.TempDir()

		err := empty.Assert(td)
		if err != nil {
			t.Error("unexpected err: ", err)
		}
	})

	t.Run("it returns error for an empty directory containing a file", func(t *testing.T) {
		td := t.TempDir()
		{
			f, err := os.OpenFile(
				path.Join(td, "file"), os.O_CREATE|os.O_RDWR, os.FileMode(0x777),
			)
			if err != nil {
				t.Fatal("can not add a file: ", err)
			}
			defer f.Close()
		}

		err := empty.Assert(td)
		if err == nil {
			t.Error("err is not caused")
		}
	})

	t.Run("it returns error for an empty directory containing a directory", func(t *testing.T) {
		td := t.TempDir()
		if err := os.Mkdir(path.Join(td, "directory"), os.FileMode(0x777)); err != nil {
			t.Fatal("can not add a file: ", err)
		}

		err := empty.Assert(td)
		if err == nil {
			t.Error("err is not caused")
		}
	})

	t.Run("it returns error for an empty directory containing a file", func(t *testing.T) {
		td := t.TempDir()
		f := path.Join(td, "file")
		{
			f, err := os.OpenFile(
				f, os.O_CREATE|os.O_RDWR, os.FileMode(0x777),
			)
			if err != nil {
				t.Fatal("can not add a file: ", err)
			}
			defer f.Close()
		}

		err := empty.Assert(td)
		if err == nil {
			t.Error("err is not caused")
		}
	})

	t.Run("it returns error for a filepath pointing nothing", func(t *testing.T) {
		td := t.TempDir()
		f := path.Join(td, "file")

		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Fatal("file exists unexpectedly")
		}

		err := empty.Assert(f)
		if err == nil {
			t.Error("err is not caused")
		}
	})

}
