package hook

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Web is a webhook for before/after hooks.
type Web[T any, R any] struct {
	// BeforeURL is a list of URLs to call before processing the value T.
	//
	// The value T is sent as a JSON payload for each URL.
	//
	// If and only if all of the URLs return a 2xx status code, the hook proceeds.
	// Otherwise, the hook fails.
	BeforeURL []*url.URL

	// AfterURL is a list of URLs to call after processing the value T.
	//
	// The value T is sent as a JSON payload for each URL.
	//
	// If and only if all of the URLs return a 2xx status code, the hook proceeds.
	// Otherwise, the hook fails.
	AfterURL []*url.URL

	Merge func(a, b R) R
}

func (w Web[T, R]) sendRequest(url string, payload io.Reader) (R, error) {
	resp, err := http.Post(url, "application/json", payload)
	if err != nil {
		return *new(R), errors.Join(err, ErrHookFailed)
	}
	defer resp.Body.Close()

	if 200 <= resp.StatusCode && resp.StatusCode < 300 {

		if resp.Header.Get("Content-Type") == "application/json" {
			r := new(R)
			if err := json.NewDecoder(resp.Body).Decode(r); err != nil {
				return *r, errors.Join(err, ErrHookFailed)
			}
			return *r, nil
		}

		return *new(R), nil
	}

	ctype := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ctype, "text/") && !(strings.HasPrefix(ctype, "application/") && strings.Contains(ctype, "json")) {
		return *new(R), fmt.Errorf(
			"%w (%s %d, Content-Type: %s)",
			ErrHookFailed, url, resp.StatusCode, ctype,
		)
	}

	body, _ := io.ReadAll(resp.Body)
	return *new(R), fmt.Errorf(
		"%w (%s %d, Content-Type: %s): %s",
		ErrHookFailed, url, resp.StatusCode, ctype, string(body),
	)
}

func (w Web[T, R]) hook(value T, urls []*url.URL) (R, error) {
	buf, err := json.Marshal(value)
	if err != nil {
		return *new(R), err
	}

	if len(urls) == 0 {
		return *new(R), nil
	}

	first, rest := urls[0], urls[1:]

	resp, err := w.sendRequest(first.String(), bytes.NewBuffer(buf))
	if err != nil {
		return *new(R), err
	}

	for _, url := range rest {
		r, err := w.sendRequest(url.String(), bytes.NewBuffer(buf))
		if err != nil {
			return *new(R), err
		}
		resp = w.Merge(resp, r)
	}

	return resp, nil
}

func (w Web[T, R]) Before(value T) (R, error) {
	return w.hook(value, w.BeforeURL)
}

func (w Web[T, R]) After(value T) error {
	_, err := w.hook(value, w.AfterURL)
	return err
}
