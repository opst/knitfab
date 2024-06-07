package handlers

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/opst/knitfab/pkg/configs/extras"
)

type Rewriter func(req *url.URL) (*url.URL, error)

var ErrRewrite = errors.New("rewrite error")

func RewriteWith(ep extras.Endpoint) (Rewriter, error) {

	sourcePath := strings.TrimSuffix(ep.Path, "/")

	return func(req *url.URL) (*url.URL, error) {

		dest := ep.ProxyTo
		{
			// taking copy
			d := *dest
			dest = &d
		}
		if p := req.Path; p == sourcePath {
			// its okay. no-op.
		} else if strings.HasPrefix(p, sourcePath) {
			pp := strings.TrimPrefix(p, sourcePath+"/")
			if pp == "" && strings.HasSuffix(p, "/") {
				pp = "/"
			}
			dest = dest.JoinPath(pp)
		} else {
			return nil, fmt.Errorf("%w: path prefix is not match", ErrRewrite)
		}

		dest.Fragment = req.Fragment
		dest.RawQuery = req.RawQuery

		return dest, nil
	}, nil
}

func ExtraAPI(
	e *echo.Echo,
	ex extras.Endpoint,
	proxyFn func(c *echo.Context, url string) error,
) error {

	rew, err := RewriteWith(ex)
	if err != nil {
		return err
	}

	dest := path.Join(ex.Path, "*")
	proxyer := func(c echo.Context) error {
		requrl := c.Request().URL
		dest, err := rew(requrl)
		if err != nil {
			return err
		}
		return proxyFn(&c, dest.String())
	}

	e.GET(dest, proxyer)
	e.POST(dest, proxyer)
	e.PUT(dest, proxyer)
	e.DELETE(dest, proxyer)
	e.PATCH(dest, proxyer)
	e.OPTIONS(dest, proxyer)
	e.HEAD(dest, proxyer)
	e.CONNECT(dest, proxyer)
	e.TRACE(dest, proxyer)

	return nil
}
