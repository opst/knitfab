package archive_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opst/knitfab/pkg/utils/archive"
	"github.com/opst/knitfab/pkg/utils/archive/internal"
	"github.com/opst/knitfab/pkg/utils/try"
)

func aspath(path string) string {
	if os.IsPathSeparator('/') {
		return path
	}
	return strings.ReplaceAll(path, "/", string(os.PathSeparator))
}

type FileWithMode struct {
	Mode    int64
	Content []byte
}

func TestArchive(t *testing.T) {
	t.Run("archive non-existing-path", func(t *testing.T) {
		ctx := context.Background()
		dest := new(bytes.Buffer)
		progress := archive.GoTar(
			ctx,
			filepath.Join(t.TempDir(), "non-existing-path"),
			dest,
		)

		if err := progress.Error(); err == nil {
			t.Fatal("Archive did not cause error:", err)
		}

		<-progress.Done()
	})

	type When struct {
		root           string
		followSymlinks bool
	}
	type Then struct {
		root        string
		wantErr     error
		wantSomeErr bool
	}
	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			thenroot := try.To(filepath.Abs(then.root)).OrFatal(t)

			dest := new(bytes.Buffer)
			options := []archive.TarOption{}
			if when.followSymlinks {
				options = append(options, archive.FollowSymlinks())
			}
			progTar := archive.GoTar(ctx, when.root, dest, options...)
			<-progTar.Done()
			if err := progTar.Error(); err != nil {
				if then.wantSomeErr {
					return
				}
				if !errors.Is(err, then.wantErr) {
					t.Errorf("caused unexpected error: %v", err)
				}
				return
			} else if then.wantSomeErr {
				t.Fatal("did not cause error")
			}

			tempdir := t.TempDir()
			progUntar := archive.GoUntar(ctx, dest, tempdir)
			<-progUntar.Done()
			if err := progUntar.Error(); err != nil {
				t.Fatal("failed to untar", err)
			}

			err := internal.LWalk(
				thenroot,
				func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}

					relpath := try.To(filepath.Rel(thenroot, path)).OrFatal(t)

					untaredFile := filepath.Join(tempdir, relpath)
					gotStat := try.To(os.Lstat(untaredFile)).OrFatal(t)

					if info.Mode()&os.ModeSymlink != 0 {
						if gotStat.Mode()&os.ModeSymlink == 0 {
							t.Errorf("entry %s is not a symlink", path)
						} else {

							wantLinkname := try.To(os.Readlink(path)).OrFatal(t)
							gotLinkname := try.To(os.Readlink(untaredFile)).OrFatal(t)

							if wantLinkname != gotLinkname {
								t.Errorf("symlink unmatch: @%s (expected, actual) = (%s, %s)", path, wantLinkname, gotLinkname)
							}
						}

						return nil
					}
					if info.IsDir() {
						return nil
					}

					if info.Mode() != gotStat.Mode() {
						t.Errorf(
							"mode unmatch: @%s (expected, actual) = (%v, %v)",
							path, info.Mode(), gotStat.Mode(),
						)
					}

					wantContent := try.To(os.ReadFile(path)).OrFatal(t)
					gotContent := try.To(os.ReadFile(untaredFile)).OrFatal(t)
					if !bytes.Equal(wantContent, gotContent) {
						t.Errorf(
							"file unmatch: @%s\n=== expected ===\n%s\n=== actual ===\n%s",
							relpath, wantContent, gotContent,
						)
					}
					return nil
				},
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("testcase-1: having symlink to file (no follow)", theory(
		When{root: "./testdata/testcase-1/linked", followSymlinks: false},
		Then{root: "./testdata/testcase-1/linked", wantErr: nil},
	))

	t.Run("testcase-1: having symlink to file (follow)", theory(
		When{root: "./testdata/testcase-1/linked", followSymlinks: true},
		Then{root: "./testdata/testcase-1/resolved", wantErr: nil},
	))

	t.Run("testcase-2: having symlink to directory (no follow)", theory(
		When{root: "./testdata/testcase-2/linked", followSymlinks: false},
		Then{root: "./testdata/testcase-2/linked", wantErr: nil},
	))

	t.Run("testcase-2: having symlink to directory (follow)", theory(
		When{root: "./testdata/testcase-2/linked", followSymlinks: true},
		Then{root: "./testdata/testcase-2/resolved", wantErr: nil},
	))

	t.Run("testcase-3: having symlink flip-flop (no follow)", theory(
		When{root: "./testdata/testcase-3/linked", followSymlinks: false},
		Then{root: "./testdata/testcase-3/linked", wantErr: nil},
	))

	t.Run("testcase-3: having symlink flip-flop (follow)", theory(
		When{root: "./testdata/testcase-3/linked", followSymlinks: true},
		Then{wantErr: archive.ErrLoopSymlink},
	))

	t.Run("testcase-4: having circuler symlink (no follow)", theory(
		When{root: "./testdata/testcase-4/linked", followSymlinks: false},
		Then{root: "./testdata/testcase-4/linked", wantErr: nil},
	))

	t.Run("testcase-4: having circuler symlink (follow)", theory(
		When{root: "./testdata/testcase-4/linked", followSymlinks: true},
		Then{wantSomeErr: true},
	))
}

func TestTarGzWalk(t *testing.T) {
	var f bytes.Buffer

	gout := gzip.NewWriter(&f)
	defer gout.Close()

	tarout := tar.NewWriter(gout)
	defer tarout.Close()

	index := map[string]FileWithMode{}
	index[aspath("foo")] = FileWithMode{Mode: 0777, Content: []byte("file1")}
	index[aspath("bar/baz")] = FileWithMode{Mode: 0700, Content: []byte("file2\n\ncontent")}
	index[aspath("bar/quux")] = FileWithMode{Mode: 0765, Content: []byte("ファイル3: multibyte chars support")}
	index[aspath("hoge/fuga")] = FileWithMode{Mode: 0707, Content: []byte("")}

	for k, v := range index {
		if err := tarout.WriteHeader(&tar.Header{
			Name: k,
			Size: int64(len(v.Content)),
			Mode: v.Mode,
		}); err != nil {
			t.Fatalf("fail to write header: %v", err)
		}

		if _, err := tarout.Write(v.Content); err != nil {
			t.Fatalf("fail to write content: %v", err)
		}
	}
	if err := tarout.Close(); err != nil {
		t.Fatalf("tarfile is not be created.: %v", err)
	}
	if err := gout.Close(); err != nil {
		t.Fatalf("gz is not be created.: %v", err)
	}

	actual := map[string]FileWithMode{}
	reader := bytes.NewReader(f.Bytes())
	reader.Seek(0, 0)
	if err := archive.TarGzWalk(reader, func(header *tar.Header, payload io.Reader, err error) error {
		if err != nil {
			t.Fatal("traverse tar.gz caused unexpected error:", err)
		}
		content, err := io.ReadAll(payload)
		if err != nil {
			t.Fatal("traverse tar.gz caused unexpected error:", err)
		}
		actual[header.Name] = FileWithMode{
			Mode:    header.Mode,
			Content: content,
		}
		return nil
	}); err != nil {
		t.Fatal("traverse tar.gz caused unexpected error:", err)
	}

	// ASSERTS!

	for k, v := range index {
		file, ok := actual[k]
		if !ok {
			t.Fatalf("entry %s is missing.", k)
		}
		if v.Mode != file.Mode {
			t.Fatalf(
				"entry %s has wrong mode (expected, actual) = (%d, %d)",
				k, v.Mode, file.Mode,
			)
		}
		if !bytes.Equal(v.Content, file.Content) {
			t.Fatalf(
				"entry %s has different content (expected, actual) = (%s, %s)",
				k, string(v.Content), string(file.Content),
			)
		}
	}

	for k, v := range actual {
		_, ok := index[k]
		if !ok {
			t.Fatalf(
				"actual has an extra entry %s (content: %s)",
				k, string(v.Content),
			)
		}
	}

}
