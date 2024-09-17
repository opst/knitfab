package errors

import (
	"encoding/json"
	"fmt"
	"strings"
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
