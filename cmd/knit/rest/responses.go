package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	cerr "github.com/opst/knitfab/cmd/knit/errors"
	apiaerr "github.com/opst/knitfab/pkg/api/types/errors"
)

type MessageFor map[StatusCodeRange]string

// unmarshal http response which has json content.
//
// args:
//   - resp: http response to be processed.
//   - v: value which response should be.
//   - messageFor: title of error message for HTTP status code range.
//
// return:
//
//	error if...
//	- can not read response body
//	- response body is not shaped of v
//	- status code is in 4xx or 5xx
func unmarshalJsonResponse[T any](resp *http.Response, v *T, messageFor MessageFor) error {
	scr := StatusCodeRangeOf(resp)
	if scr <= Status2xx {

		if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
			message := fmt.Sprintf("unexpected error: %s (status code = %d)", err.Error(), resp.StatusCode)
			return cerr.NewCuiError(message, cerr.WithCause(err))
		}
		return nil
	}

	message, ok := messageFor[scr]
	if !ok {
		message = scr.String()
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		e := cerr.NewCuiError(
			fmt.Sprintf(
				"%s\ncannot read server message: %s",
				message, err.Error(),
			),
			cerr.WithCause(err),
		)
		return e
	}

	if detail, err := parseErrorMessage(body); err == nil {
		return cerr.NewCuiError(
			message,
			cerr.WithDetail(func(summary string) (string, error) {
				return summary + "\n" + detail, nil
			}),
		)
	}

	return cerr.NewCuiError(
		message,
		cerr.WithDetail(func(summary string) (string, error) {
			return summary + "\n" + string(body), nil
		}),
	)
}

func jsonUnmarshal[T any](buf []byte) (*T, error) {
	ret := new(T)
	if err := json.Unmarshal(buf, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func unmarshalStreamResponse(resp *http.Response, messageFor MessageFor) (io.ReadCloser, error) {
	scr := StatusCodeRangeOf(resp)
	if scr <= Status2xx {
		return resp.Body, nil
	}

	message, ok := messageFor[scr]
	if !ok {
		message = scr.String()
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		e := cerr.NewCuiError(
			fmt.Sprintf(
				"%s\ncannot read server message: %s",
				message, err.Error(),
			),
			cerr.WithCause(err),
		)
		return nil, e
	}

	if detail, err := parseErrorMessage(body); err == nil {
		return nil, cerr.NewCuiError(
			message,
			cerr.WithDetail(func(summary string) (string, error) {
				return summary + "\n" + detail, nil
			}),
		)
	}

	return nil, cerr.NewCuiError(
		message,
		cerr.WithDetail(func(summary string) (string, error) {
			return summary + "\n" + string(body), nil
		}),
	)
}

func unmarshalResponseDiscardingPayload(resp *http.Response, messageFor MessageFor) error {
	rc, err := unmarshalStreamResponse(resp, messageFor)
	if rc != nil {
		io.ReadAll(rc)
		rc.Close()
	}
	return err
}

func parseErrorMessage(body []byte) (string, error) {
	if eresp, err := jsonUnmarshal[apiaerr.ErrorMessage](body); err == nil {
		if detail, err := json.MarshalIndent(eresp, "", "    "); err != nil {
			return "", err
		} else {
			return string(detail), nil
		}
	}

	if msg, err := jsonUnmarshal[struct {
		Message *string `json:"message"`
	}](body); err == nil && msg.Message != nil {
		detail, err := json.MarshalIndent(msg, "", "    ")
		if err != nil {
			return "", err
		}
		return string(detail), nil
	}

	return string(body), nil
}
