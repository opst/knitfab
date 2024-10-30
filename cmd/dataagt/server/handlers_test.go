package server_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/opst/knitfab/cmd/dataagt/server"
	httptestutil "github.com/opst/knitfab/internal/testutils/http"
	"github.com/opst/knitfab/pkg/archive"
	"github.com/opst/knitfab/pkg/utils/cmp"
	kio "github.com/opst/knitfab/pkg/utils/io"
	"github.com/opst/knitfab/pkg/utils/try"

	"github.com/labstack/echo/v4"
)

func b(str string) []byte {
	return []byte(str)
}

func TestReader(t *testing.T) {
	t.Run("it responses empty tar.gz when attached to empty data", func(t *testing.T) {
		root := t.TempDir()
		defer os.RemoveAll(root)

		testee := server.Reader(root)
		e := echo.New()
		ctx, resprec := httptestutil.Get(e, "/")
		if err := testee(ctx); err != nil {
			t.Fatal("unexpected error", err)
		}
		resp := resprec.Result()
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Error("status code 200 !=", resp.StatusCode)
		}

		expectedContentType := "application/tar+gzip"
		if resp.Header.Get("Content-Type") != expectedContentType {
			t.Error("Content-Type:", expectedContentType, "!=", resp.Header.Get("Content-Type"))
		}

		filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if p == root {
				return nil
			}
			t.Errorf("unexpected file: %s", p)
			return nil
		})
		if _, ok := resp.Trailer[http.CanonicalHeaderKey("x-checksum-md5")]; !ok {
			t.Error("response has no checksum")
		}
	})

	t.Run("it responses directory content packed in tar.gz", func(t *testing.T) {
		testee := server.Reader("./testdata/root")
		e := echo.New()
		ctx, resprec := httptestutil.Get(e, "/")
		if err := testee(ctx); err != nil {
			t.Fatal("unexpected error", err)
		}
		resp := resprec.Result()
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Error("status code 200 !=", resp.StatusCode)
		}

		expectedContentType := "application/tar+gzip"
		if resp.Header.Get("Content-Type") != expectedContentType {
			t.Error("Content-Type:", expectedContentType, "!=", resp.Header.Get("Content-Type"))
		}

		got := t.TempDir()
		gzr := try.To(gzip.NewReader(resp.Body)).OrFatal(t)
		prog := archive.GoUntar(context.Background(), gzr, got)
		<-prog.Done()
		if err := prog.Error(); err != nil {
			t.Fatal("fail to untar", err)
		}

		filepath.Walk("./testdata/root", func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			wantStat := try.To(os.Lstat(p)).OrFatal(t)
			if wantStat.IsDir() {
				return nil
			}
			relpath := try.To(filepath.Rel("./testdata/root", p)).OrFatal(t)

			gotPath := filepath.Join(got, relpath)
			gotStat := try.To(os.Lstat(gotPath)).OrFatal(t)
			if wantStat.Mode()&os.ModeSymlink != 0 {
				if gotStat.Mode()&os.ModeSymlink == 0 {
					t.Errorf("entry %s is not a symlink", p)
					return nil
				}

				wantLinkname := try.To(os.Readlink(p)).OrFatal(t)
				gotLinkname := try.To(os.Readlink(gotPath)).OrFatal(t)
				if wantLinkname != gotLinkname {
					t.Errorf("symlink unmatch: @%s (expected, actual) = (%s, %s)", p, wantLinkname, gotLinkname)
				}
				return nil
			}

			if wantStat.Mode() != gotStat.Mode() {
				t.Errorf(
					"mode unmatch: @%s (expected, actual) = (%v, %v)",
					p, wantStat.Mode(), gotStat.Mode(),
				)
			}

			wantContent := try.To(os.ReadFile(p)).OrFatal(t)
			gotContent := try.To(os.ReadFile(gotPath)).OrFatal(t)
			if !bytes.Equal(wantContent, gotContent) {
				t.Errorf(
					"file unmatch: @%s\n=== expected ===\n%s\n=== actual ===\n%s",
					relpath, wantContent, gotContent,
				)
			}
			return nil
		})

		if _, ok := resp.Trailer[http.CanonicalHeaderKey("x-checksum-md5")]; !ok {
			t.Error("response has no checksum")
		}
	})

	t.Run("if content root is missing, it should response 404", func(t *testing.T) {
		root, err := os.MkdirTemp("", "")
		if err != nil {
			t.Error("fail: MkdirTemp", err)
		}
		defer os.RemoveAll(root)
		nowhere := filepath.Join(root, "nowhere")
		testee := server.Reader(nowhere)
		e := echo.New()
		ctx, resprec := httptestutil.Get(e, "/")
		err = testee(ctx)
		resp := resprec.Result()
		defer resp.Body.Close()

		errResp, ok := err.(*echo.HTTPError)
		if !ok {
			t.Fatalf("error is not echo.HTTPError. but %+v", err)
		}

		expectedStatusCode := 404
		if errResp.Code != expectedStatusCode {
			t.Error("status code", expectedStatusCode, "!=", resp.StatusCode)
		}
	})

	t.Run("if content root is not directory, it should response 404", func(t *testing.T) {
		root, err := os.MkdirTemp("", "")
		if err != nil {
			t.Error("fail: MkdirTemp", err)
		}
		defer os.RemoveAll(root)
		nowhere := filepath.Join(root, "nowhere")
		os.Create(nowhere)
		testee := server.Reader(nowhere)
		e := echo.New()
		ctx, resprec := httptestutil.Get(e, "/")
		err = testee(ctx)
		resp := resprec.Result()
		defer resp.Body.Close()

		errResp, ok := err.(*echo.HTTPError)
		if !ok {
			t.Fatalf("error is not echo.HTTPError. but %+v", err)
		}

		expectedStatusCode := 404
		if errResp.Code != expectedStatusCode {
			t.Error("status code", expectedStatusCode, "!=", resp.StatusCode)
		}
	})
}

func TestWriter(t *testing.T) {
	t.Run("it writes payload to given file", func(t *testing.T) {
		root := t.TempDir()
		testee := server.Writer(root)
		e := echo.New()

		fileTarGz := try.To(os.ReadFile("./testdata/root.tar.gz")).OrFatal(t)
		md5sum := md5.New()
		md5sum.Write(fileTarGz)
		reader := new(bytes.Buffer)
		reader.Write(fileTarGz)
		ctx, resprec := httptestutil.Post(e, "/", reader)
		ctx.Request().Trailer = http.Header{}
		ctx.Request().Trailer.Add("x-checksum-md5", hex.EncodeToString(md5sum.Sum(nil)))
		if err := testee(ctx); err != nil {
			t.Fatal("fail to POST.", err)
		}
		resp := resprec.Result()
		defer resp.Body.Close()

		expectedStatusCode := http.StatusNoContent
		if resp.StatusCode != expectedStatusCode {
			t.Error("expected", expectedStatusCode, ", but actual ", resp.StatusCode)
		}

		wantRoot := "./testdata/root"
		err := filepath.Walk("./testdata/root", func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			wantStat := try.To(os.Lstat(p)).OrFatal(t)
			if wantStat.IsDir() {
				return nil
			}
			wantRelpath := try.To(filepath.Rel(wantRoot, p)).OrFatal(t)

			gotPath := filepath.Join(root, wantRelpath)
			gotStat := try.To(os.Lstat(gotPath)).OrFatal(t)

			if wantStat.Mode()&os.ModeSymlink != 0 {
				if gotStat.Mode()&os.ModeSymlink == 0 {
					t.Errorf("entry %s is not a symlink", p)
					return nil
				}

				wantLinkname := try.To(os.Readlink(p)).OrFatal(t)
				gotLinkname := try.To(os.Readlink(gotPath)).OrFatal(t)
				if wantLinkname != gotLinkname {
					t.Errorf("symlink unmatch: @%s (expected, actual) = (%s, %s)", p, wantLinkname, gotLinkname)
				}
				return nil
			}

			if wantStat.Mode() != gotStat.Mode() {
				t.Errorf(
					"mode unmatch: @%s (expected, actual) = (%v, %v)",
					p, wantStat.Mode(), gotStat.Mode(),
				)
			}

			wantContent := try.To(os.ReadFile(p)).OrFatal(t)
			gotContent := try.To(os.ReadFile(gotPath)).OrFatal(t)
			if !bytes.Equal(wantContent, gotContent) {
				t.Errorf(
					"file unmatch: @%s\n=== expected ===\n%s\n=== actual ===\n%s",
					wantRelpath, wantContent, gotContent,
				)
			}
			return nil
		})

		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it cause error if checksum is wrong", func(t *testing.T) {
		root := t.TempDir()

		testee := server.Writer(root)
		e := echo.New()

		fileTarGz := try.To(os.ReadFile("./testdata/root.tar.gz")).OrFatal(t)
		md5sum := md5.New()
		md5sum.Write(fileTarGz)

		buf := new(bytes.Buffer)
		buf.Write(fileTarGz)
		ctx, resprec := httptestutil.Post(e, "/", buf)
		ctx.Request().Trailer = http.Header{}
		ctx.Request().Trailer.Add("x-checksum-md5", hex.EncodeToString(md5sum.Sum(nil))+"abc")
		errresp := testee(ctx)
		resp := resprec.Result()
		defer resp.Body.Close()

		if hterr, ok := errresp.(*echo.HTTPError); !ok {
			t.Fatal("server do not return error.")
		} else {
			expectedStatusCode := http.StatusBadRequest
			if hterr.Code != expectedStatusCode {
				t.Error("expected status code = ", expectedStatusCode, "but actual =", hterr)
			}
		}

	})

	t.Run("it cause error if destination directory is not found", func(t *testing.T) {
		root := t.TempDir()

		nowhere := path.Join(root, "nowhere")
		testee := server.Writer(nowhere)
		e := echo.New()

		fileTarGz := try.To(os.ReadFile("./testdata/root.tar.gz")).OrFatal(t)
		md5sum := md5.New()
		md5sum.Write(fileTarGz)

		buf := new(bytes.Buffer)
		buf.Write(fileTarGz)

		ctx, resprec := httptestutil.Post(e, "/", buf)
		ctx.Request().Trailer = http.Header{}
		ctx.Request().Trailer.Add("x-checksum-md5", hex.EncodeToString(md5sum.Sum(nil)))
		errresp := testee(ctx)
		resp := resprec.Result()
		defer resp.Body.Close()

		if hterr, ok := errresp.(*echo.HTTPError); !ok {
			t.Error("server do not return http-error.", errresp)
		} else {
			expectedStatusCode := http.StatusInternalServerError
			if hterr.Code != expectedStatusCode {
				t.Error("expected status code = ", expectedStatusCode, "but actual =", hterr.Code)
			}
		}

		if _, err := os.Stat(nowhere); !os.IsNotExist(err) {
			t.Error("root directory is created, but it should be missing.")
		}
	})

	t.Run("it cause error if destination directory has items", func(t *testing.T) {
		root := t.TempDir()

		nowhere := path.Join(root, "nowhere")

		if file, err := kio.CreateAll(path.Join(nowhere, "file-1"), 0700, 0700); err != nil {
			t.Fatal("fail to create a file for test", err)
		} else {
			defer file.Close()
			try.To(file.Write(b("brabrabra..."))).OrFatal(t)
		}

		filesBefore := []string{}
		filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			filesBefore = append(filesBefore, p)
			return nil
		})

		testee := server.Writer(root)
		e := echo.New()

		filesTarGz := try.To(os.ReadFile("./testdata/root.tar.gz")).OrFatal(t)
		md5sum := md5.New()
		md5sum.Write(filesTarGz)

		buf := new(bytes.Buffer)
		buf.Write(filesTarGz)
		ctx, resprec := httptestutil.Post(e, "/", buf)
		ctx.Request().Trailer = http.Header{}
		ctx.Request().Trailer.Add("x-checksum-md5", hex.EncodeToString(md5sum.Sum(nil)))
		errresp := testee(ctx)
		resp := resprec.Result()
		defer resp.Body.Close()

		if hterr, ok := errresp.(*echo.HTTPError); !ok {
			t.Error("server do not return http-error.", errresp)
		} else {
			expectedStatusCode := http.StatusConflict
			if hterr.Code != expectedStatusCode {
				t.Error("expected status code = ", expectedStatusCode, "but actual =", hterr.Code)
			}
		}

		filesAfter := []string{}
		filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			filesAfter = append(filesAfter, p)
			return nil
		})

		if !cmp.SliceContentEq(filesBefore, filesAfter) {
			t.Errorf("files changed: %v -> %v", filesBefore, filesAfter)
		}
	})
}
