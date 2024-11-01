package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/labstack/echo/v4"
	"github.com/opst/knitfab/pkg/utils/slices"
)

type RequestOption func(req *http.Request) *http.Request

func WithContext(ctx context.Context) RequestOption {
	return func(req *http.Request) *http.Request {
		return req.WithContext(ctx)
	}
}

func WithHeader(key string, value string, values ...string) RequestOption {
	return func(req *http.Request) *http.Request {
		req.Header.Add(key, value)
		for _, v := range values {
			req.Header.Add(key, v)
		}
		return req
	}
}

// = WithHeader("Content-Type", ctyp)
func ContentType(ctyp string) RequestOption {
	return WithHeader("Content-Type", ctyp)
}

// = WithHeader("Transfer-Encoding", "chunked")
func Chunked() RequestOption {
	return WithHeader("Transfer-Encoding", "chunked")
}

// add Trailer header and trailer itself.
func WithTrailer(key string, value string, values ...string) RequestOption {
	return func(req *http.Request) *http.Request {
		if _, ok := slices.First(
			req.Header["Trailer"],
			func(t string) bool { return t == key },
		); ok {
			req.Header.Add("Trailer", key)
		}
		if req.Trailer == nil {
			req.Trailer = map[string][]string{}
		}
		req.Trailer.Add(key, value)
		for _, v := range values {
			req.Trailer.Add(key, v)
		}
		return req
	}
}

func Get(e *echo.Echo, target string, reqopts ...RequestOption) (echo.Context, *httptest.ResponseRecorder) {
	req := httptest.NewRequest("GET", target, nil)
	for _, opt := range reqopts {
		req = opt(req)
	}
	resp := httptest.NewRecorder()

	ctx := e.NewContext(req, resp)
	return ctx, resp
}

func Post(e *echo.Echo, target string, data io.Reader, reqopts ...RequestOption) (echo.Context, *httptest.ResponseRecorder) {
	req := httptest.NewRequest("POST", target, data)
	for _, opt := range reqopts {
		req = opt(req)
	}
	resp := httptest.NewRecorder()

	ctx := e.NewContext(req, resp)
	return ctx, resp
}

func Put(e *echo.Echo, target string, data io.Reader, reqopts ...RequestOption) (echo.Context, *httptest.ResponseRecorder) {
	req := httptest.NewRequest("PUT", target, data)
	for _, opt := range reqopts {
		req = opt(req)
	}
	resp := httptest.NewRecorder()

	ctx := e.NewContext(req, resp)
	return ctx, resp
}

func Delete(e *echo.Echo, target string, reqopts ...RequestOption) (echo.Context, *httptest.ResponseRecorder) {
	req := httptest.NewRequest("PUT", target, nil)
	for _, opt := range reqopts {
		req = opt(req)
	}
	resp := httptest.NewRecorder()

	ctx := e.NewContext(req, resp)
	return ctx, resp
}
