package errors

import (
	"net/http"

	"github.com/labstack/echo/v4"
	apierr "github.com/opst/knitfab-api-types/errors"
)

type ErrorMessageOption func(in *apierr.ErrorMessage) *apierr.ErrorMessage

func WithAdvice(advice string) ErrorMessageOption {
	return func(in *apierr.ErrorMessage) *apierr.ErrorMessage {
		if advice != "" {
			in.Advice = advice
		}
		return in
	}
}

func WithError(err error) ErrorMessageOption {
	return func(in *apierr.ErrorMessage) *apierr.ErrorMessage {
		if err != nil {
			in.Cause = err
		}
		return in
	}
}

func WithSee(see string) ErrorMessageOption {
	return func(in *apierr.ErrorMessage) *apierr.ErrorMessage {
		if see != "" {
			in.See = see
		}
		return in
	}
}

func NewErrorMessage(code int, reason string, opts ...ErrorMessageOption) *echo.HTTPError {
	msg := apierr.ErrorMessage{Reason: reason}
	for _, opt := range opts {
		msg = *opt(&msg)
	}

	return echo.NewHTTPError(code, msg).SetInternal(msg)
}

func NewErrorAdvice(code int, reason string, advice string) *echo.HTTPError {
	return echo.NewHTTPError(code, apierr.ErrorMessage{Reason: reason}, WithAdvice(advice))
}

// cause panic with error message.
//
// This get in echo's standard error handling pipeline (HTTPErrorHandler)
func Fatal(reason string, err error) {
	panic(echo.NewHTTPError(
		http.StatusInternalServerError,
		apierr.ErrorMessage{
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
			apierr.ErrorMessage{
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

func Unauthorized(message string, err error) *echo.HTTPError {
	return NewErrorMessage(
		http.StatusUnauthorized,
		message,
		WithError(err),
	)
}
