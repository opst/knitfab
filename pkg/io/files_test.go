package io

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
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

		CreateAll(filepath.Join(root, "foo", "bar", "targetFile"), 0700, 0707)

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

		CreateAll(filepath.Join(root, "targetFile"), 0777, 0700)

		fStat, err := os.Stat(filepath.Join(root, "targetFile"))
		if err != nil || fStat.IsDir() || !fStat.Mode().IsRegular() {
			t.Fatal("cannot create targetFile (stat, err):", fStat, err)
		}
		if fStat.Mode().Perm() != 0777 {
			t.Fatal("target file mod is wrong. (actual, expected): ", fStat.Mode(), fs.FileMode(0777))
		}
	})
}
