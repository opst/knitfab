package echoutil

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	kio "github.com/opst/knitfab/pkg/io"
)

func Proxy(cp *echo.Context, url string) error {
	c := *cp

	req, err := createRequestForBackend(c.Request().Context(), url, cp)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return err
	}

	// send request to backend
	resp, err := sendRquestToBackend(req)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return err
	}
	defer resp.Body.Close()

	// copy response from backgroundAPI to clientresponce
	err = CopyResponse(cp, resp)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return err
	}

	return nil
}

func sendRquestToBackend(req *http.Request) (*http.Response, error) {
	// create client
	client := &http.Client{
		CheckRedirect: nil,
	}
	// send request
	resp, err := client.Do(req)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func createRequestForBackend(ctx context.Context, url string, cp *echo.Context) (*http.Request, error) {
	c := *cp
	req, err := http.NewRequestWithContext(ctx, c.Request().Method, url, c.Request().Body)
	if err != nil {
		return nil, err
	}

	// Copy the headers
	CopyHeader(&req.Header, &c.Request().Header)
	req.Body = c.Request().Body
	if c.Request().Trailer != nil {
		req.Trailer = http.Header{}
		for k, vs := range c.Request().Trailer {
			for _, v := range vs {
				req.Trailer.Add(k, v)
			}
		}
	}

	return req, nil
}

func CopyHeader(dest *http.Header, src *http.Header, except ...string) {
	// convert []string to set
	exc := map[string]interface{}{}

	for _, x := range except {
		exc[strings.ToLower(x)] = nil
	}

	for k, vs := range *src {
		if _, ok := exc[strings.ToLower(k)]; ok {
			// this header marked not to be copied
			continue
		}
		for _, v := range vs {
			dest.Add(k, v)
		}
	}
}

func CopyRequest(
	ctx context.Context,
	dest string,
	src *http.Request,
) (*http.Response, error) {
	client := http.Client{}

	hook := kio.NewTriggerReader(src.Body)
	req, err := http.NewRequestWithContext(
		ctx,
		src.Method,
		dest,
		hook,
	)
	if err != nil {
		return nil, err
	}
	if src.Trailer != nil {
		if req.Trailer == nil {
			req.Trailer = map[string][]string{}
		}
		for tr := range src.Trailer {
			req.Trailer[tr] = nil
		}
	}
	hook.OnEnd(func() {
		CopyHeader(&req.Trailer, &src.Trailer)
	})

	CopyHeader(&req.Header, &src.Header, "host")
	// copy hop-by-hop headers.
	for _, te := range src.TransferEncoding {
		req.Header.Add("Transfer-Encoding", te)
	}
	return client.Do(req)
}

func CopyResponse(cp *echo.Context, resp *http.Response) error {
	c := *cp
	ctx := c.Request().Context()

	dstResp := c.Response()
	dstHeader := dstResp.Header()
	CopyHeader(&dstHeader, &resp.Header)

	// copy hop-by-hop header
	chunked := false
	for _, te := range resp.TransferEncoding {
		dstHeader.Add("Transfer-Encoding", te)
		if strings.ToLower(te) == "chunked" {
			chunked = true
		}
	}
	for trailer := range resp.Trailer {
		dstHeader.Add("Trailer", trailer)
	}

	dstResp.WriteHeader(resp.StatusCode)

	src := kio.NewTriggerReader(resp.Body)
	src.OnEnd(func() {
		trailer := c.Response().Header()
		for k, vs := range resp.Trailer {
			for _, v := range vs {
				trailer.Add(k, v)
			}
		}
	})
	if !chunked {
		_, err := io.Copy(dstResp.Writer, src)
		return err
	}

	buf := make([]byte, 1024*1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := src.Read(buf)
		if err != nil {
			dstResp.Flush()
			if errors.Is(err, io.EOF) {
				_, err := dstResp.Write(buf[:n])
				return err
			}
			return err
		}
		if n == 0 {
			continue
		}

		if _, err := dstResp.Write(buf[:n]); err != nil {
			return err
		}
		dstResp.Flush()
	}
}
