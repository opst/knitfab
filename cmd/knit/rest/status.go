package rest

import (
	"fmt"
	"net/http"
)

type StatusCodeRange int

func (sc StatusCodeRange) String() string {
	switch sc {
	case Status1xx:
		return "informational response"
	case Status2xx:
		return "success"
	case Status3xx:
		return "redirect"
	case Status4xx:
		return "client error"
	case Status5xx:
		return "server error"
	default:
		return fmt.Sprintf("unknown (%d)", sc)
	}
}

func StatusCodeRangeOf(resp *http.Response) StatusCodeRange {
	sc := resp.StatusCode
	if sc < 200 {
		return Status1xx
	}
	if sc < 300 {
		return Status2xx
	}
	if sc < 400 {
		return Status3xx
	}
	if sc < 500 {
		return Status4xx
	}
	if sc < 600 {
		return Status5xx
	}
	return StatusUnknown
}

const (
	StatusUnknown StatusCodeRange = iota
	Status1xx
	Status2xx
	Status3xx
	Status4xx
	Status5xx
)
