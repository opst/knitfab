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
type Web[T any] struct {
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
}

func (w Web[T]) sendRequest(url string, payload io.Reader) error {
	resp, err := http.Post(url, "application/json", payload)
	if err != nil {
		return errors.Join(err, ErrHookFailed)
	}
	defer resp.Body.Close()

	if 200 <= resp.StatusCode && resp.StatusCode < 300 {
		return nil
	}

	ctype := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ctype, "text/") && !(strings.HasPrefix(ctype, "application/") && strings.Contains(ctype, "json")) {
		return fmt.Errorf(
			"%w (%s %d, Content-Type: %s)",
			ErrHookFailed, url, resp.StatusCode, ctype,
		)
	}

	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf(
		"%w (%s %d, Content-Type: %s): %s",
		ErrHookFailed, url, resp.StatusCode, ctype, string(body),
	)
}

func (w Web[T]) hook(value T, urls []*url.URL) error {
	buf, err := json.Marshal(value)
	if err != nil {
		return err
	}

	for _, url := range urls {
		payload := bytes.NewBuffer(buf)
		if err := w.sendRequest(url.String(), payload); err != nil {
			return err
		}
	}

	return nil
}

func (w Web[T]) Before(value T) error {
	return w.hook(value, w.BeforeURL)
}

func (w Web[T]) After(value T) error {
	return w.hook(value, w.AfterURL)
}
