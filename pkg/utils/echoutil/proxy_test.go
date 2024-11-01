package echoutil

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	httptestutil "github.com/opst/knitfab/internal/testutils/http"
	"github.com/opst/knitfab/pkg/utils/slices"
)

func is[T comparable](a T) func(b T) bool {
	return func(b T) bool {
		return a == b
	}
}

func TestProxy(t *testing.T) {

	t.Run("when it has chunked endpoint behind, it proxies GET (without body) request and response as they are", func(t *testing.T) {
		trailer := "expires"
		trailerVal := "trailerVal"
		headerKey := "Content-Type"
		headerVal := "text/plain"
		body := []byte("***backend response body***")

		reqHeaderKey := "Content-Type"
		reqHeaderVal := "text/plain"
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			if r.Method != http.MethodGet {
				t.Error("unmatch method.")
			}
			if r.Header.Get(reqHeaderKey) != reqHeaderVal {
				t.Error("unmatch header.")
			}
			w.Header().Add("Transfer-Encoding", "chunked")
			w.Header().Add("Trailer", trailer)
			w.Header().Add(headerKey, headerVal)
			w.WriteHeader(http.StatusOK)
			w.Write(body)
			w.Header().Add(trailer, trailerVal)
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		e := echo.New()
		ctx, respRec := httptestutil.Get(
			e, "/",
			httptestutil.WithHeader(reqHeaderKey, reqHeaderVal),
		)

		// SEND REQUEST
		err := Proxy(&ctx, ts.URL)
		if err != nil {
			t.Error("incorrect result", err.Error())
		}

		// RECEIVE RESPONSE
		response := respRec.Result()

		if response.StatusCode != http.StatusOK {
			t.Error("status code 200 !=", response.StatusCode)
		}

		if _, ok := slices.First(response.Header.Values("Transfer-Encoding"), is("chunked")); !ok {
			t.Error("response is not `Transfer-Encoding: chunked`")
		}

		b, err := io.ReadAll(response.Body)
		if err != nil {
			t.Error(err.Error())
		}

		if string(b) != string(body) {
			t.Errorf("unmatch response body:%s expected:%s", string(b), string(body))
		}

		if response.Header.Get(headerKey) != headerVal {
			t.Error("copy header failed. unmatch header.")
		}

		trVal := response.Trailer.Get(trailer)
		if !strings.EqualFold(trVal, trailerVal) {
			t.Errorf("copy header failed. unmatch trailer:%s, expected:%s\n", trVal, trailerVal)
		}
	})

	t.Run("when it has lengthed endpoint behind, it proxies GET (without body) request and response as they are", func(t *testing.T) {
		trailer := "expires"
		trailerVal := "trailerVal"
		headerKey := "Content-Type"
		headerVal := "text/plain"
		body := []byte("***backend response body***")

		reqHeaderKey := "Content-Type"
		reqHeaderVal := "text/plain"
		// create backend server
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			if r.Method != http.MethodGet {
				t.Error("unmatch method.")
			}
			if r.Header.Get(reqHeaderKey) != reqHeaderVal {
				t.Error("unmatch header.")
			}
			w.Header().Add("Content-Length", fmt.Sprintf("%d", len(body)))
			w.Header().Add("Trailer", trailer)
			w.Header().Add(headerKey, headerVal)
			w.WriteHeader(http.StatusOK)
			w.Write(body)
			w.Header().Add(trailer, trailerVal)
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		e := echo.New()
		ctx, respRec := httptestutil.Get(
			e, "/",
			httptestutil.WithHeader(reqHeaderKey, reqHeaderVal),
		)

		// SEND REQUEST
		err := Proxy(&ctx, ts.URL)
		if err != nil {
			t.Error("incorrect result", err.Error())
		}

		// RECEIVE RESPONSE
		response := respRec.Result()
		if response.StatusCode != http.StatusOK {
			t.Error("status code 200 !=", response.StatusCode)
		}

		if _, ok := slices.First(response.Header.Values("Transfer-Encoding"), is("chunked")); ok {
			t.Error("response is `Transfer-Encoding: chunked`, but it should be with `Content-Length:` header")
		}

		b, err := io.ReadAll(respRec.Body)
		if err != nil {
			t.Error(err.Error())
		}

		if string(b) != string(body) {
			t.Errorf("unmatch response body:%s expected:%s", string(b), string(body))
		}

		if response.Header.Get(headerKey) != headerVal {
			t.Error("copy header failed. unmatch header.")
		}

		trVal := response.Header.Get(trailer)
		if trVal != "" {
			t.Errorf("unexpected trailer is given: `%s: %s`", trailer, trVal)
		}
	})

	t.Run("request GET method failed, when the backend URL is incorrect.", func(t *testing.T) {

		url := "http://example.invalid"

		e := echo.New()
		ctx, respRec := httptestutil.Get(e, "/")

		err := Proxy(&ctx, url)
		if err == nil {
			t.Error("incorrect result", err)
		}

		response := respRec.Result()

		if response.StatusCode != http.StatusInternalServerError {
			t.Errorf("unmatch satuscode:%d, expected:%d\n", ctx.Response().Status, http.StatusInternalServerError)
		}
	})

	t.Run("when it has chunked endpoint behind, it proxies POST (with body) request and response as they are", func(t *testing.T) {
		trailer := "expires"
		trailerVal := "trailerVal"
		headerKey := "Content-Type"
		headerVal := "text/plain"
		body := []byte("***backend response body***")

		reqHeaderKey := "Content-Type"
		reqHeaderVal := "text/plain"
		reqBody := "test data"

		// create backend server
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Error("unmatch method.")
			}
			if r.Header.Get(reqHeaderKey) != reqHeaderVal {
				t.Error("unmatch header.")
			}
			b, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err.Error())
			}
			// compare request body
			if string(b) != string("test data") {
				t.Errorf("unmatch responsebody:%s expected:%s", string(b), reqBody)
			}

			w.Header().Add("Transfer-Encoding", "chunked")
			w.Header().Add("Trailer", trailer)
			w.Header().Add(headerKey, headerVal)
			w.WriteHeader(http.StatusOK)
			w.Write(body)
			w.Header().Add(trailer, trailerVal)
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		// post method
		e := echo.New()

		ctx, respRec := httptestutil.Post(
			e, "/", strings.NewReader(reqBody),
			httptestutil.WithHeader("Content-Length", fmt.Sprintf("%d", len(reqBody))),
			httptestutil.WithHeader(reqHeaderKey, reqHeaderVal),
		)

		// SEND REQUEST
		err := Proxy(&ctx, ts.URL)
		if err != nil {
			t.Error("incorrect result", err.Error())
		}

		// RECEIVE RESPONSE
		response := respRec.Result()

		if response.StatusCode != http.StatusOK {
			t.Error("status code 200 !=", response.StatusCode)
		}

		if _, ok := slices.First(response.Header.Values("Transfer-Encoding"), is("chunked")); !ok {
			t.Error("response is not `Transfer-Encoding: chunked`")
		}

		b, err := io.ReadAll(respRec.Body)
		if err != nil {
			t.Error(err.Error())
		}

		if string(b) != string(body) {
			t.Errorf("unmatch response body:%s expected:%s", string(b), string(body))
		}

		if ctx.Response().Header().Get(headerKey) != headerVal {
			t.Error("copy header failed. unmatch header.")
		}

		trVal := ctx.Response().Header().Get(trailer)
		if !strings.EqualFold(trVal, trailerVal) {
			t.Errorf("unmatch trailer:%s, expected:%s", trVal, trailerVal)
		}
	})

	t.Run("when it has lengthed endpoint behind, it proxies request and response with body as they are", func(t *testing.T) {
		trailer := "expires"
		trailerVal := "trailerVal"
		headerKey := "Content-Type"
		headerVal := "text/plain"
		body := []byte("***backend response body***")

		reqHeaderKey := "Content-Type"
		reqHeaderVal := "text/plain"
		reqBody := "test data"

		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Error("unmatch method.")
			}
			if r.Header.Get(reqHeaderKey) != reqHeaderVal {
				t.Error("unmatch header.")
			}
			b, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err.Error())
			}

			if string(b) != string("test data") {
				t.Errorf("unmatch responsebody:%s expected:%s", string(b), reqBody)
			}

			w.Header().Add("Content-Length", fmt.Sprintf("%d", len(body)))
			w.Header().Add("Trailer", trailer)
			w.Header().Add(headerKey, headerVal)
			w.WriteHeader(http.StatusOK)
			w.Write(body)
			w.Header().Add(trailer, trailerVal)
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		e := echo.New()

		ctx, respRec := httptestutil.Post(
			e, "/", strings.NewReader(reqBody),
			httptestutil.WithHeader("Content-Length", fmt.Sprintf("%d", len(reqBody))),
		)
		ctx.Request().Header.Add(reqHeaderKey, reqHeaderVal)

		// SEND REQUEST
		err := Proxy(&ctx, ts.URL)
		if err != nil {
			t.Error("incorrect result", err.Error())
		}

		response := respRec.Result()

		// check response
		if response.StatusCode != http.StatusOK {
			t.Error("status code 200 !=", response.StatusCode)
		}

		if _, ok := slices.First(response.Header.Values("Transfer-Encoding"), is("chunked")); ok {
			t.Error("response is `Transfer-Encoding: chunked`, but it should be with `Content-Length:` header")
		}

		// read response body
		b, err := io.ReadAll(respRec.Body)
		if err != nil {
			t.Error(err.Error())
		}

		// compare response body
		if string(b) != string(body) {
			t.Errorf("unmatch response body:%s expected:%s", string(b), string(body))
		}

		// compare header
		if ctx.Response().Header().Get(headerKey) != headerVal {
			t.Error("copy header failed. unmatch header.")
		}

		// compare trailer
		trVal := ctx.Response().Header().Get(trailer)
		if trVal != "" {
			t.Errorf("unexpected trailer is given: `%s: %s`", trVal, trailerVal)
		}
	})

	t.Run("when it received chunked request, it proxies request and response as they are", func(t *testing.T) {
		trailer := "expires"
		trailerVal := "trailerVal"
		headerKey := "Content-Type"
		headerVal := "text/plain"
		body := []byte("***backend response body***")

		reqHeaderKey := "Content-Type"
		reqHeaderVal := "text/plain"
		reqBody := "test data"

		// create backend server
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Error("unmatch method.")
			}
			if r.Header.Get(reqHeaderKey) != reqHeaderVal {
				t.Error("unmatch header.")
			}
			b, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err.Error())
			}
			// compare request body
			if string(b) != string("test data") {
				t.Errorf("unmatch responsebody:%s expected:%s", string(b), reqBody)
			}

			// compare trailer
			trVal := r.Trailer.Get(trailer)
			if !strings.EqualFold(trVal, trailerVal) {
				t.Errorf("unmatch trailer:%s, expected:%s", trVal, trailerVal)
			}

			w.Header().Add("Content-Length", fmt.Sprintf("%d", len(body)))
			w.Header().Add(headerKey, headerVal)
			w.WriteHeader(http.StatusOK)
			w.Write(body)
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		// post method
		e := echo.New()

		ctx, respRec := httptestutil.Post(
			e, "/", strings.NewReader(reqBody),
			httptestutil.WithHeader("Transfer-Encoding", "chunked"),
			httptestutil.WithHeader(reqHeaderKey, reqHeaderVal),
			httptestutil.WithTrailer(trailer, trailerVal),
		)

		err := Proxy(&ctx, ts.URL)
		if err != nil {
			t.Error("incorrect result", err.Error())
		}

		// check response
		if ctx.Response().Status != http.StatusOK {
			t.Error("status code 200 !=", ctx.Response().Status)
		}

		// read response body
		_, err = io.ReadAll(respRec.Body)
		if err != nil {
			t.Error(err.Error())
		}
	})

	t.Run("request POST method failed, when the URL is incorrect.", func(t *testing.T) {

		backendApiRoot := "http://example.invalid"

		// post method
		e := echo.New()
		ctx, _ := httptestutil.Post(e, "/", strings.NewReader("test data"))
		err := Proxy(&ctx, backendApiRoot)

		if err == nil {
			t.Error("incorrect result", err)
		}

		if ctx.Response().Status != http.StatusInternalServerError {
			t.Errorf("unmatch satuscode:%d, expected:%d\n", ctx.Response().Status, http.StatusInternalServerError)
		}
	})
}

func TestSendRequest(t *testing.T) {

	t.Run("send GET request and get response from backend normally", func(t *testing.T) {

		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Error("unmatch method.")
			}

			w.WriteHeader(http.StatusOK)
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		req, err := http.NewRequest(http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Error("unexpected error occured. ", err.Error())
		}

		resp, err := sendRquestToBackend(req)

		if err != nil {
			t.Error("unexpected error occured. ", err.Error())
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("unmatch statuscode:%d\n, expected:%d", resp.StatusCode, http.StatusOK)
		}
	})

	t.Run("send POST request and get response from backend normally", func(t *testing.T) {

		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Error("unmatch method.")
			}

			w.WriteHeader(http.StatusOK)
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		req, err := http.NewRequest(http.MethodPost, ts.URL, strings.NewReader("test data"))
		if err != nil {
			t.Error("unexpected error occured. ", err.Error())
		}

		resp, err := sendRquestToBackend(req)
		if err != nil {
			t.Error("unexpected error occured. ", err.Error())
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("unmatch statuscode:%d\n, expected:%d", resp.StatusCode, http.StatusOK)
		}

	})

	t.Run("send GET method faild. when  the URL is incorrect.", func(t *testing.T) {

		url := "http://example.invalid"

		// post method
		req, err := http.NewRequest(http.MethodGet, url, strings.NewReader("test data"))
		if err != nil {
			t.Error("unexpected error occured. ", err.Error())
		}

		resp, err := sendRquestToBackend(req)
		if err == nil {
			t.Error("incorrect result", err)
		}
		if resp != nil {
			t.Error("unexpected responce")
		}

	})

	t.Run("send POST method faild. when  the URL is incorrect.", func(t *testing.T) {

		url := "http://example.invalid"

		// post method
		req, err := http.NewRequest(http.MethodPost, url, strings.NewReader("test data"))
		if err != nil {
			t.Error("unexpected error occured. ", err.Error())
		}

		resp, err := sendRquestToBackend(req)
		if err == nil {
			t.Error("incorrect result", err)
		}
		if resp != nil {
			t.Error("unexpected responce")
		}
	})

}

func TestCreateRequestForBackend(t *testing.T) {
	t.Run("create GET request succsessfully.", func(t *testing.T) {
		headerKey := "Content-Type"
		headerVal := "text/plain"
		backendApiRoot := "http://example.com"

		e := echo.New()
		ctx, _ := httptestutil.Get(e, "/")
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		ctx.Request().Header.Add(headerKey, headerVal)
		req, err := createRequestForBackend(c, backendApiRoot, &ctx)
		if err != nil {
			t.Errorf("unexpected error occured:%s\n", err.Error())
		}

		if req.Header.Get(headerKey) != headerVal {
			t.Errorf("unmatch header val:%s, expected:{%s:%s}", req.Header.Get(headerKey), headerKey, headerVal)
		}

		if req.Method != http.MethodGet {
			t.Errorf("incorrect request method:%s, expected:%s", req.Method, http.MethodGet)
		}
	})

	t.Run("create POST request succsessfully.", func(t *testing.T) {
		headerKey := "Content-Type"
		headerVal := "text/plain"
		body := "test data"
		backendApiRoot := "http://example.com"
		trailer := "Expire"
		trailerVal := "trailerVal"

		e := echo.New()
		ctx, _ := httptestutil.Post(e, "/", strings.NewReader(body))
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		ctx.Request().Header.Add(headerKey, headerVal)
		ctx.Request().Trailer = http.Header{}
		ctx.Request().Trailer.Add(trailer, trailerVal)

		req, err := createRequestForBackend(c, backendApiRoot, &ctx)
		if err != nil {
			t.Errorf("unexpected error occured:%s\n", err.Error())
		}

		if req.Header.Get(headerKey) != headerVal {
			t.Errorf("unmatch header val:%s, expected:{%s:%s}", req.Header.Get(headerKey), headerKey, headerVal)
		}
		if req.Method != http.MethodPost {
			t.Errorf("incorrect request method:%s, expected:%s", req.Method, http.MethodPost)
		}
		// read request body
		b, err := io.ReadAll(req.Body)
		if err != nil {
			t.Error(err.Error())
		}
		// compare request body
		if string(b) != string(body) {
			t.Errorf("unmatch responsebody:%s expected:%s", string(b), string(body))
		}
		// compare trailer
		trVal := req.Trailer.Get(trailer)
		if !strings.EqualFold(trVal, trailerVal) {
			t.Errorf("unmatch trailer:%s, expected:%s", trVal, trailerVal)
		}
	})
}

func TestCopyHeader(t *testing.T) {
	t.Run("the request header should be copy correctly.", func(t *testing.T) {
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "backend handler")
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		e := echo.New()
		ctx, _ := httptestutil.Post(e, "/", strings.NewReader("test data"))

		ctx.Request().Header.Add("Content-Type", "text/plain")
		ctx.Request().Header.Add("Accept-Encoding", "gzip")
		ctx.Request().Header.Add("Accept-Encoding", "compress")
		req, err := http.NewRequest(http.MethodGet, ts.URL, ctx.Request().Body)
		if err != nil {
			t.Error(err.Error())
		}

		CopyHeader(&ctx.Request().Header, &req.Header)

		if ctx.Request().Header.Get("Content-Type") != "text/plain" {
			t.Error("copy header failed. unmatch header.")
		}
		if ctx.Request().Header.Get("Accept-Encoding") != "gzip" {
			t.Error("copy header failed. unmatch header.")
		}

		aceptEncodings := ctx.Request().Header["Accept-Encoding"]
		if len(aceptEncodings) != 2 {
			t.Error("unmatch the number of Accept-Encoding header.")
		}

		acceptEncodeingsExpected := []string{"gzip", "compress"}
		if !reflect.DeepEqual(aceptEncodings, acceptEncodeingsExpected) {
			t.Error("unmatch the value of Accept-Encoding header.")
		}
	})
	t.Run("marked headers are not copied.", func(t *testing.T) {
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "backend handler")
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		e := echo.New()
		ctx, _ := httptestutil.Post(e, "/", strings.NewReader("test data"))

		ctx.Request().Header.Add("Content-Type", "text/plain")
		ctx.Request().Header.Add("Accept-Encoding", "gzip")
		ctx.Request().Header.Add("Accept-Encoding", "compress")
		req, err := http.NewRequest(http.MethodGet, ts.URL, ctx.Request().Body)
		if err != nil {
			t.Error(err.Error())
		}

		CopyHeader(&req.Header, &ctx.Request().Header, "Accept-Encoding")

		if req.Header.Get("Content-Type") != "text/plain" {
			t.Error("copy header failed. unmatch header.")
		}
		if req.Header.Get("Accept-Encoding") != "" {
			t.Error("copy header failed. marked headers are copied.")
		}
	})
}

func TestCopyResponse(t *testing.T) {

	t.Run("response copy correctly. response body is not empty.", func(t *testing.T) {
		trailer := "expires"
		trailerVal := "trailerVal"
		headerKey := "Content-Type"
		headerVal := "text/plain"
		body := []byte("***backend response body***")

		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Log(w, "backend handler")

			w.Header().Add("Trailer", trailer)
			w.Header().Add(headerKey, headerVal)
			w.WriteHeader(http.StatusOK)
			w.Write(body)
			w.Header().Add(trailer, trailerVal)
		})

		// start test server
		ts := httptest.NewServer(h)
		defer ts.Close()

		// create request
		e := echo.New()
		ctx, respRec := httptestutil.Post(e, "/", strings.NewReader("test data"))
		req, err := http.NewRequest(http.MethodPost, ts.URL, ctx.Request().Body)
		if err != nil {
			t.Error(err.Error())
		}

		// send reqest to backend
		client := &http.Client{
			CheckRedirect: nil,
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Error(err.Error())
		}
		defer resp.Body.Close()

		// copy response from backend
		err = CopyResponse(&ctx, resp)
		if err != nil {
			t.Error(err.Error())
		}

		// read response body
		b, err := io.ReadAll(respRec.Body)
		if err != nil {
			t.Error(err.Error())
		}

		// compare response body
		if string(b) != string(body) {
			t.Errorf("unmatch responsebody:%s expected:%s", string(b), string(body))
		}

		// compare header
		if ctx.Response().Header().Get(headerKey) != headerVal {
			t.Error("copy header failed. unmatch header.")
		}

		trVal := ctx.Response().Header().Get(trailer)
		if !strings.EqualFold(trVal, trailerVal) {
			t.Errorf("copy header failed. unmatch trailer:%s, expected:%s", trVal, trailerVal)
		}
	})

	t.Run("response copy correctly. response body length is 0.", func(t *testing.T) {
		headerKey := "Content-Type"
		headerVal := "text/plain"

		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Log(w, "backend handler")

			w.Header().Add(headerKey, headerVal)
			w.WriteHeader(http.StatusOK)
		})

		// start test server
		ts := httptest.NewServer(h)
		defer ts.Close()

		// create request
		e := echo.New()
		ctx, respRec := httptestutil.Post(e, "/", strings.NewReader("test data"))
		req, err := http.NewRequest(http.MethodPost, ts.URL, ctx.Request().Body)
		if err != nil {
			t.Error(err.Error())
		}

		// send reqest to backend
		client := &http.Client{
			CheckRedirect: nil,
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Error(err.Error())
		}
		defer resp.Body.Close()

		// copy response from backend
		err = CopyResponse(&ctx, resp)
		if err != nil {
			t.Error(err.Error())
		}

		// compare response body
		if respRec.Body.Len() != 0 {
			t.Error("response body length should be 0.")
		}

		// compare header
		if ctx.Response().Header().Get(headerKey) != headerVal {
			t.Error("copy header failed. unmatch header.")
		}
	})
}
