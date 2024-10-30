package server

import (
	"compress/gzip"
	"encoding/hex"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"

	"github.com/labstack/echo/v4"
	apierr "github.com/opst/knitfab/pkg/api-types-binding/errors"
	"github.com/opst/knitfab/pkg/utils/archive"
	kio "github.com/opst/knitfab/pkg/utils/io"
)

func Reader(root string) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()

		resp := c.Response()
		chw := kio.NewMD5Writer(resp.Writer)
		gzw := gzip.NewWriter(chw)

		if fi, err := os.Stat(root); err != nil || !fi.IsDir() {
			resp.Header().Add("Content-Type", "application/json")
			return apierr.NotFound()
		}

		resp.Header().Add("Trailer", "x-checksum-md5")
		resp.Header().Add("Content-Type", "application/tar+gzip")

		prog := archive.GoTar(ctx, root, gzw)
		<-prog.Done()
		if err := prog.Error(); err != nil {
			return err
		}
		gzw.Close()
		resp.Header().Add("x-checksum-md5", hex.EncodeToString(chw.Sum()))
		return nil
	}
}

type breakWalk struct {
	error string
}

func (b breakWalk) Error() string {
	return b.error
}

func Writer(root string) echo.HandlerFunc {
	return func(c echo.Context) error {
		err := filepath.Walk(root, func(p string, info fs.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if p == root {
				return nil
			}
			return breakWalk{}
		})
		switch err.(type) {
		case breakWalk:
			c.Response().Header().Add("Content-Type", "application/json")
			return apierr.NewErrorMessage(http.StatusConflict, "data exists already")
		case nil:
			// nothing to do.
		default:
			if os.IsNotExist(err) {
				return apierr.InternalServerError(err)
			}
			apierr.Fatal("unexpected error", err)
		}

		chr := kio.NewMD5Reader(c.Request().Body)
		gzreader, err := gzip.NewReader(chr)
		if err != nil {
			return apierr.InternalServerError(err)
		}
		defer gzreader.Close()

		prog := archive.GoUntar(c.Request().Context(), gzreader, root)
		<-prog.Done()
		if err := prog.Error(); err != nil {
			return apierr.InternalServerError(err)
		}

		md5hash := c.Request().Trailer.Get("x-checksum-md5")
		if md5hash != "" && md5hash != hex.EncodeToString(chr.Sum()) {
			return apierr.NewErrorMessage(http.StatusBadRequest, "hash is not match.")
		}

		c.Response().WriteHeader(http.StatusNoContent)
		return nil
	}
}
