package internal_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/opst/knitfab/pkg/archive/internal"
	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
)

type FileEntry struct {
	Path string

	IsDir bool

	// for regular file.
	Content []byte

	// for symlink.
	Linkname string
}

func TestLWalk(t *testing.T) {
	type when struct {
		fs        []FileEntry
		skipDirOn string
		errOn     string
		err       error
	}
	type Then struct {
		visitOrder []string
		leaveOrder []string
		wantErr    error
	}

	theory := func(when when, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			t.Helper()
			from := t.TempDir()
			wantFiles := map[string]os.FileInfo{}
			{
				relfrom := try.To(filepath.Rel(from, from)).OrFatal(t)
				rootInfo := try.To(os.Lstat(from)).OrFatal(t)
				wantFiles[relfrom] = rootInfo
			}
			for _, v := range when.fs {
				path := filepath.Join(from, v.Path)
				if v.Linkname != "" {
					if err := os.Symlink(v.Linkname, path); err != nil {
						t.Fatal(err)
					}
				} else if v.IsDir {
					if err := os.MkdirAll(path, 0755); err != nil {
						t.Fatal(err)
					}

				} else {
					if err := os.WriteFile(path, v.Content, 0644); err != nil {
						t.Fatal(err)
					}
				}
				info := try.To(os.Lstat(path)).OrFatal(t)
				wantFiles[v.Path] = info
			}

			visitOrder := []string{}
			leaveOrder := []string{}
			gotFiles := map[string]os.FileInfo{}

			err := internal.LWalk(
				from,
				func(path string, info os.FileInfo, err error) error {
					relpath := try.To(filepath.Rel(from, path)).OrFatal(t)
					gotFiles[relpath] = info
					visitOrder = append(visitOrder, relpath)
					if err != nil {
						return err
					}
					if relpath == when.skipDirOn {
						return filepath.SkipDir
					}
					if relpath == when.errOn {
						return when.err
					}
					return nil
				},
				func(path string, info os.FileInfo, err error) error {
					relpath := try.To(filepath.Rel(from, path)).OrFatal(t)
					leaveOrder = append(leaveOrder, relpath)
					return err
				},
			)
			if err != then.wantErr {
				t.Errorf("wantErr: %v, got: %v", then.wantErr, err)
			}

			if !cmp.SliceEq(visitOrder, then.visitOrder) {
				t.Fatalf(
					"visit order:\n ===want===\n%v\n ===got===\n%v",
					then.visitOrder, visitOrder,
				)
			}

			if !cmp.SliceEq(leaveOrder, then.leaveOrder) {
				t.Errorf(
					"leave order:\n ===want===\n%v\n ===got===\n%v",
					then.leaveOrder, leaveOrder,
				)
			}

			for _, file := range then.visitOrder {
				got, ok := gotFiles[file]
				if !ok {
					t.Errorf("file info: got is not found %v", file)
					continue
				}
				want, ok := wantFiles[file]
				if !ok {
					t.Fatalf("file info: want is not found: %s", file)
				}

				if want.Mode()&os.ModeSymlink != 0 {
					if got.Mode()&os.ModeSymlink == 0 {
						t.Errorf(
							"symlinkness @ %s: want: %v, got: %v",
							file,
							want.Mode()&os.ModeSymlink, got.Mode()&os.ModeSymlink,
						)
					}
					wantLinkname := try.To(os.Readlink(filepath.Join(from, file))).OrFatal(t)
					gotLinkname := try.To(os.Readlink(filepath.Join(from, file))).OrFatal(t)
					if wantLinkname != gotLinkname {
						t.Errorf(
							"linkname @ %s: want: %v, got: %v",
							file, wantLinkname, gotLinkname,
						)
					}
					continue
				}

				if got.Name() != want.Name() {
					t.Errorf("name: want: %v, got: %v", want.Name(), got.Name())
				}
				if got.Mode() != want.Mode() {
					t.Errorf("mode: want: %v, got: %v", want.Mode(), got.Mode())
				}
				if want.IsDir() != got.IsDir() {
					t.Errorf("isDir: want: %v, got: %v", want.IsDir(), got.IsDir())
				}
				if !want.IsDir() {
					wantContent := try.To(os.ReadFile(filepath.Join(from, file))).OrFatal(t)
					gotContent := try.To(os.ReadFile(filepath.Join(from, file))).OrFatal(t)
					if !bytes.Equal(wantContent, gotContent) {
						t.Errorf(
							"content @ %s: want: %v, got: %v",
							file, wantContent, gotContent,
						)
					}
				}
			}
		}
	}

	t.Run("empty", theory(
		when{},
		Then{
			visitOrder: []string{"."},
			leaveOrder: []string{"."},
			wantErr:    nil,
		},
	))

	t.Run("single file", theory(
		when{
			fs: []FileEntry{
				{Path: "a.txt", Content: []byte("hello")},
			},
		},
		Then{
			visitOrder: []string{".", "a.txt"},
			leaveOrder: []string{"a.txt", "."},
			wantErr:    nil,
		},
	))

	t.Run("single dir", theory(
		when{
			fs: []FileEntry{
				{Path: "a", IsDir: true},
			},
		},
		Then{
			visitOrder: []string{".", "a"},
			leaveOrder: []string{"a", "."},
			wantErr:    nil,
		},
	))

	t.Run("nested", theory(
		when{
			fs: []FileEntry{
				{Path: "a", IsDir: true},
				{Path: "a/b", IsDir: true},
				{Path: "a/b/b.txt", Content: []byte("hey")},
				{Path: "a/b/c.txt", Content: []byte("hello")},
				{Path: "a/d", IsDir: true},
				{Path: "a/d/e.txt", Content: []byte("world")},
			},
		},
		Then{
			visitOrder: []string{".", "a", "a/b", "a/b/b.txt", "a/b/c.txt", "a/d", "a/d/e.txt"},
			leaveOrder: []string{"a/b/b.txt", "a/b/c.txt", "a/b", "a/d/e.txt", "a/d", "a", "."},
			wantErr:    nil,
		},
	))

	t.Run("skip dir", theory(
		when{
			fs: []FileEntry{
				{Path: "a", IsDir: true},
				{Path: "a/b", IsDir: true},
				{Path: "a/b/c.txt", Content: []byte("hello")},
				{Path: "a/d", IsDir: true},
				{Path: "a/d/e.txt", Content: []byte("world")},
			},
			skipDirOn: "a/b",
		},
		Then{
			visitOrder: []string{".", "a", "a/b", "a/d", "a/d/e.txt"},
			leaveOrder: []string{"a/d/e.txt", "a/d", "a", "."},
			wantErr:    nil,
		},
	))

	wantErr := errors.New("error")
	t.Run("error", theory(
		when{
			fs: []FileEntry{
				{Path: "a", IsDir: true},
				{Path: "a/b", IsDir: true},
				{Path: "a/b/c.txt", Content: []byte("hello")},
				{Path: "a/d", IsDir: true},
				{Path: "a/d/e.txt", Content: []byte("world")},
			},
			errOn: "a/d",
			err:   wantErr,
		},
		Then{
			visitOrder: []string{".", "a", "a/b", "a/b/c.txt", "a/d"},
			leaveOrder: []string{"a/b/c.txt", "a/b", "a", "."},
			wantErr:    wantErr,
		},
	))

	t.Run("symlink to file", theory(
		when{
			fs: []FileEntry{
				{Path: "a", IsDir: true},
				{Path: "a/b", IsDir: true},
				{Path: "a/b/c.txt", Content: []byte("hello")},
				{Path: "a/d", IsDir: true},
				{Path: "a/d/e.txt", Content: []byte("world")},
				{Path: "a/f.txt", Linkname: "b/c.txt"},
			},
		},
		Then{
			visitOrder: []string{".", "a", "a/b", "a/b/c.txt", "a/d", "a/d/e.txt", "a/f.txt"},
			leaveOrder: []string{"a/b/c.txt", "a/b", "a/d/e.txt", "a/d", "a/f.txt", "a", "."},
			wantErr:    nil,
		},
	))

	t.Run("symlink to file", theory(
		when{
			fs: []FileEntry{
				{Path: "a", IsDir: true},
				{Path: "a/b", IsDir: true},
				{Path: "a/b/c.txt", Content: []byte("hello")},
				{Path: "a/d", IsDir: true},
				{Path: "a/d/e.txt", Content: []byte("world")},
				{Path: "a/f.txt", Linkname: "b/c.txt"},
			},
		},
		Then{
			visitOrder: []string{".", "a", "a/b", "a/b/c.txt", "a/d", "a/d/e.txt", "a/f.txt"},
			leaveOrder: []string{"a/b/c.txt", "a/b", "a/d/e.txt", "a/d", "a/f.txt", "a", "."},
			wantErr:    nil,
		},
	))

	t.Run("symlink to dir", theory(
		when{
			fs: []FileEntry{
				{Path: "a", IsDir: true},
				{Path: "a/b", IsDir: true},
				{Path: "a/b/c.txt", Content: []byte("hello")},
				{Path: "a/d", IsDir: true},
				{Path: "a/d/e.txt", Content: []byte("world")},
				{Path: "a/f", Linkname: "b"},
			},
		},
		Then{
			visitOrder: []string{".", "a", "a/b", "a/b/c.txt", "a/d", "a/d/e.txt", "a/f"},
			leaveOrder: []string{"a/b/c.txt", "a/b", "a/d/e.txt", "a/d", "a/f", "a", "."},
			wantErr:    nil,
		},
	))

	t.Run("deadlink", theory(
		when{
			fs: []FileEntry{
				{Path: "f", Linkname: "b/c.txt"},
			},
		},
		Then{
			visitOrder: []string{".", "f"},
			leaveOrder: []string{"f", "."},
			wantErr:    nil,
		},
	))

	t.Run("circular symlink", theory(
		when{
			fs: []FileEntry{
				{Path: "a", Linkname: "b"},
				{Path: "b", Linkname: "a"},
			},
		},
		Then{
			visitOrder: []string{".", "a", "b"},
			leaveOrder: []string{"a", "b", "."},
			wantErr:    nil,
		},
	))
}
