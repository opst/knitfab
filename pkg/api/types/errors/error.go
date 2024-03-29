package errors

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
)

type ErrorResponse struct {
	Message ErrorMessage `json:"message"`
}

type ErrorMessage struct {
	Reason string `json:"reason"`
	Advice string `json:"advice,omitempty"`
	See    string `json:"see,omitempty"`
	Cause  error  `json:"-"`
}

func (em *ErrorMessage) UnmarshalJSON(bytes []byte) error {
	f := new(struct {
		Reason *string `json:"reason"`
		Advice *string `json:"advice,omitempty"`
		See    *string `json:"see,omitempty"`
	})
	if err := json.Unmarshal(bytes, f); err != nil {
		return err
	}

	if f.Reason == nil {
		return fmt.Errorf(`required field missing: "reason"`)
	}
	em.Reason = *f.Reason

	if f.Advice != nil {
		em.Advice = *f.Advice
	}

	if f.See != nil {
		em.See = *f.See
	}

	return nil
}

func (e ErrorMessage) String() string {
	lines := []string{e.Reason}
	if e.Advice != "" {
		lines = append(lines, e.Advice)
	}
	if e.Cause != nil {
		lines = append(lines, fmt.Sprint(" caused by:", e.Cause.Error()))
	}
	return strings.Join(lines, "\n")
}

func (e ErrorMessage) Error() string {
	return e.String()
}

func (e ErrorMessage) Unwrap() error {
	return e.Cause
}

type ErrorMessageOption func(in *ErrorMessage) *ErrorMessage

func WithAdvice(advice string) ErrorMessageOption {
	return func(in *ErrorMessage) *ErrorMessage {
		if advice != "" {
			in.Advice = advice
		}
		return in
	}
}

func WithError(err error) ErrorMessageOption {
	return func(in *ErrorMessage) *ErrorMessage {
		if err != nil {
			in.Cause = err
		}
		return in
	}
}

func WithSee(see string) ErrorMessageOption {
	return func(in *ErrorMessage) *ErrorMessage {
		if see != "" {
			in.See = see
		}
		return in
	}
}

func NewErrorMessage(code int, reason string, opts ...ErrorMessageOption) *echo.HTTPError {
	msg := ErrorMessage{Reason: reason}
	for _, opt := range opts {
		msg = *opt(&msg)
	}

	return echo.NewHTTPError(code, msg).SetInternal(msg)
}

func NewErrorAdvice(code int, reason string, advice string) *echo.HTTPError {
	return echo.NewHTTPError(code, ErrorMessage{Reason: reason}, WithAdvice(advice))
}

// cause panic with error message.
//
// This get in echo's standard error handling pipeline (HTTPErrorHandler)
func Fatal(reason string, err error) {
	panic(echo.NewHTTPError(
		http.StatusInternalServerError,
		ErrorMessage{
			Reason: reason,
			Advice: "ask your system admin.",
			Cause:  err,
		},
	),
	)
}

// cause panic with error message.
//
// This get in echo's standard error handling pipeline (HTTPErrorHandler)
func FatalWithAdvice(code int, reason string, advice string, err error) {
	panic(
		echo.NewHTTPError(
			http.StatusInternalServerError,
			ErrorMessage{
				Reason: reason,
				Advice: advice,
				Cause:  err,
			},
		),
	)
}

func ServiceUnavailable(advice string, err error) *echo.HTTPError {
	return NewErrorMessage(
		http.StatusServiceUnavailable,
		"service unavailable temporaly",
		WithAdvice(advice),
		WithError(err),
	)
}

func NotFound() *echo.HTTPError {
	return NewErrorMessage(http.StatusNotFound, "not found")
}

func BadRequest(advice string, err error) *echo.HTTPError {
	return NewErrorMessage(
		http.StatusBadRequest,
		"bad request",
		WithAdvice(advice),
		WithError(err),
	)
}

func Conflict(message string, options ...ErrorMessageOption) *echo.HTTPError {
	return NewErrorMessage(
		http.StatusConflict,
		message,
		options...,
	)
}

func InternalServerError(err error) *echo.HTTPError {
	return NewErrorMessage(
		http.StatusInternalServerError,
		"unexpected error",
		WithError(err),
	)
}
