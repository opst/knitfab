package io_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	kio "github.com/opst/knitfab/pkg/io"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestCreateAll(t *testing.T) {

	t.Run("it creates a file in directory", func(t *testing.T) {
		defaultUmask := syscall.Umask(0)
		defer syscall.Umask(defaultUmask)

		root, err := os.MkdirTemp("", "")
		if err != nil {
			t.Fatal("fail to create workdir.", err)
		}
		defer os.RemoveAll(root)

		kio.CreateAll(filepath.Join(root, "foo", "bar", "targetFile"), 0700, 0707)

		fooStat, err := os.Stat(filepath.Join(root, "foo"))
		if err != nil || !fooStat.IsDir() {
			t.Fatal("cannot create directory (stat, err):", fooStat, err)
		}
		if fooStat.Mode().Perm() != 0707 {
			t.Fatal("directory mod is wrong. (actual, expected): ", fooStat.Mode(), fs.FileMode(0707))
		}

		barStat, err := os.Stat(filepath.Join(root, "foo", "bar"))
		if err != nil || !barStat.IsDir() {
			t.Fatal("cannot create directory (stat, err):", barStat, err)
		}
		if barStat.Mode().Perm() != 0707 {
			t.Fatal("directory mod is wrong. (actual, expected): ", barStat.Mode(), fs.FileMode(0707))
		}

		fStat, err := os.Stat(filepath.Join(root, "foo", "bar", "targetFile"))
		if err != nil || fStat.IsDir() {
			t.Fatal("cannot create targetFile (stat, err):", fStat, err)
		}
		if fStat.Mode().Perm() != 0700 {
			t.Fatal("target file mod is wrong. (actual, expected): ", fStat.Mode(), fs.FileMode(0700))
		}
	})

	t.Run("it creates a file directly", func(t *testing.T) {
		defaultUmask := syscall.Umask(0)
		defer syscall.Umask(defaultUmask)

		root, err := os.MkdirTemp("", "")
		if err != nil {
			t.Fatal("fail to create workdir.", err)
		}
		defer os.RemoveAll(root)

		kio.CreateAll(filepath.Join(root, "targetFile"), 0777, 0700)

		fStat, err := os.Stat(filepath.Join(root, "targetFile"))
		if err != nil || fStat.IsDir() || !fStat.Mode().IsRegular() {
			t.Fatal("cannot create targetFile (stat, err):", fStat, err)
		}
		if fStat.Mode().Perm() != 0777 {
			t.Fatal("target file mod is wrong. (actual, expected): ", fStat.Mode(), fs.FileMode(0777))
		}
	})
}

func TestDirCopy(t *testing.T) {
	// prepare source directory
	//
	// <ROOT>/
	//   |── d1/
	//   |     |── f1
	//   |     |── f2
	//   |     |── d2/
	//   |           |── f3
	//   |── d3/
	//         |── f4
	//         |── d4/  // empty dir!

	src := t.TempDir()
	if err := func() error {
		d1 := filepath.Join(src, "d1")
		if err := os.Mkdir(d1, 0755); err != nil {
			return err
		}
		f1, err := os.Create(filepath.Join(d1, "f1"))
		if err != nil {
			return err
		}
		if _, err := f1.Write([]byte("f1: hello")); err != nil {
			return err
		}
		f2, err := os.Create(filepath.Join(d1, "f2"))
		if err != nil {
			return err
		}
		if _, err := f2.Write([]byte("f2: hello")); err != nil {
			return err
		}

		d2 := filepath.Join(d1, "d2")
		if err := os.Mkdir(d2, 0755); err != nil {
			return err
		}
		f3, err := os.Create(filepath.Join(d2, "f3"))
		if err != nil {
			return err
		}
		if _, err := f3.Write([]byte("f3: hello")); err != nil {
			return err
		}

		d3 := filepath.Join(src, "d3")
		if err := os.Mkdir(d3, 0755); err != nil {
			return err
		}
		f4, err := os.Create(filepath.Join(d3, "f4"))
		if err != nil {
			return err
		}
		if _, err := f4.Write([]byte("f4: hello")); err != nil {
			return err
		}
		d4 := filepath.Join(d3, "d4")
		if err := os.Mkdir(d4, 0755); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		t.Fatal("fail to create test directory.", err)
	}

	dest := t.TempDir()
	if err := func() error {
		d3 := filepath.Join(dest, "d3")
		if err := os.Mkdir(d3, 0755); err != nil {
			return err
		}

		f4, err := os.Create(filepath.Join(d3, "f4"))
		if err != nil {
			return err
		}
		if _, err := f4.Write([]byte("f4: to be overwritten")); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		t.Fatal("fail to create test directory.", err)
	}

	if err := kio.DirCopy(src, dest); err != nil {
		t.Fatal("fail to copy directory.", err)
	}

	// check the copied files
	f1 := try.To(os.ReadFile(filepath.Join(dest, "d1", "f1"))).OrFatal(t)
	if want := "f1: hello"; string(f1) != want {
		t.Fatalf("f1 content is wrong. (actual, expected): %s, %s", f1, want)
	}
	f2 := try.To(os.ReadFile(filepath.Join(dest, "d1", "f2"))).OrFatal(t)
	if want := "f2: hello"; string(f2) != want {
		t.Fatalf("f2 content is wrong. (actual, expected): %s, %s", f2, want)
	}
	f3 := try.To(os.ReadFile(filepath.Join(dest, "d1", "d2", "f3"))).OrFatal(t)
	if want := "f3: hello"; string(f3) != want {
		t.Fatalf("f3 content is wrong. (actual, expected): %s, %s", f3, want)
	}
	f4 := try.To(os.ReadFile(filepath.Join(dest, "d3", "f4"))).OrFatal(t)
	if want := "f4: hello"; string(f4) != want {
		t.Fatalf("f4 content is wrong. (actual, expected): %s, %s", f4, want)
	}

	// check the empty directory
	d4stat := try.To(os.Stat(filepath.Join(dest, "d3", "d4"))).OrFatal(t)
	if !d4stat.IsDir() {
		t.Fatal("d4 is not a directory.")
	}
}
