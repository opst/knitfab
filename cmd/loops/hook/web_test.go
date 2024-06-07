package hook_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestWebHook_Before(t *testing.T) {
	type Value struct {
		Content string `json:"content"`
	}

	type When struct {
		value       Value
		statusCode1 int
		statusCode2 int
	}

	type Then struct {
		value Value

		invoked1 bool
		invoked2 bool

		err error
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			handler := func(
				w http.ResponseWriter, r *http.Request, name string, statusCode int,
			) {
				buf := new(bytes.Buffer)
				buf.ReadFrom(r.Body)

				if r.Method != http.MethodPost {
					t.Errorf("%s: unexpected method: %s", name, r.Method)
				}

				var got Value
				err := json.Unmarshal(buf.Bytes(), &got)
				if err != nil {
					t.Fatalf("%s: unexpected error: %v", name, err)
				}
				if got != then.value {
					t.Errorf("%s: Expected: %v, Got: %v", name, then.value, got)
				}

				w.WriteHeader(statusCode)
			}

			invoked1, invoked2 := false, false
			server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				invoked1 = true
				handler(w, r, "server1", when.statusCode1)
			}))
			defer server1.Close()

			server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				invoked2 = true
				handler(w, r, "server2", when.statusCode2)
			}))
			defer server2.Close()

			testee := hook.Web[Value]{BeforeURL: []*url.URL{
				try.To(url.Parse(server1.URL)).OrFatal(t),
				try.To(url.Parse(server2.URL)).OrFatal(t),
			}}
			err := testee.Before(when.value)
			if !errors.Is(err, then.err) {
				t.Errorf("Expected: %v, Got: %v", then.err, err)
			}

			if invoked1 != then.invoked1 {
				t.Errorf("Expected: %v, Got: %v", then.invoked1, invoked1)
			}
			if invoked2 != then.invoked2 {
				t.Errorf("Expected: %v, Got: %v", then.invoked2, invoked2)
			}
		}
	}

	t.Run("Success All", theory(
		When{
			value:       Value{Content: "hello"},
			statusCode1: http.StatusOK,
			statusCode2: http.StatusOK,
		},
		Then{
			value:    Value{Content: "hello"},
			invoked1: true,
			invoked2: true,
			err:      nil,
		},
	))

	t.Run("Fail First", theory(
		When{
			value:       Value{Content: "hello"},
			statusCode1: http.StatusNotFound,
			statusCode2: http.StatusOK,
		},
		Then{
			value:    Value{Content: "hello"},
			invoked1: true,
			invoked2: false,
			err:      hook.ErrHookFailed,
		},
	))

	t.Run("Fail Second", theory(
		When{
			value:       Value{Content: "hello"},
			statusCode1: http.StatusOK,
			statusCode2: http.StatusNotFound,
		},
		Then{
			value:    Value{Content: "hello"},
			invoked1: true,
			invoked2: true,
			err:      hook.ErrHookFailed,
		},
	))
}

func TestHook_Before_Sends_InvalidUrl(t *testing.T) {
	testee := hook.Web[string]{
		BeforeURL: []*url.URL{
			try.To(url.Parse("http://somewhere.invalid")).OrFatal(t),
		},
	}

	err := testee.Before("hello")
	if err == nil {
		t.Fatal("Expected an error")
	}
}

func TestWebHook_After(t *testing.T) {
	type Value struct {
		Content string `json:"content"`
	}

	type When struct {
		value       Value
		statusCode1 int
		statusCode2 int
	}

	type Then struct {
		value Value

		invoked1 bool
		invoked2 bool

		err error
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			handler := func(
				w http.ResponseWriter, r *http.Request, name string, statusCode int,
			) {
				buf := new(bytes.Buffer)
				buf.ReadFrom(r.Body)

				if r.Method != http.MethodPost {
					t.Errorf("%s: unexpected method: %s", name, r.Method)
				}

				var got Value
				err := json.Unmarshal(buf.Bytes(), &got)
				if err != nil {
					t.Fatalf("%s: unexpected error: %v", name, err)
				}
				if got != then.value {
					t.Errorf("%s: Expected: %v, Got: %v", name, then.value, got)
				}

				w.WriteHeader(statusCode)
			}

			invoked1, invoked2 := false, false
			server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				invoked1 = true
				handler(w, r, "server1", when.statusCode1)
			}))
			defer server1.Close()

			server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				invoked2 = true
				handler(w, r, "server2", when.statusCode2)
			}))
			defer server2.Close()

			testee := hook.Web[Value]{AfterURL: []*url.URL{
				try.To(url.Parse(server1.URL)).OrFatal(t),
				try.To(url.Parse(server2.URL)).OrFatal(t),
			}}
			err := testee.After(when.value)
			if !errors.Is(err, then.err) {
				t.Errorf("Expected: %v, Got: %v", then.err, err)
			}

			if invoked1 != then.invoked1 {
				t.Errorf("Expected: %v, Got: %v", then.invoked1, invoked1)
			}
			if invoked2 != then.invoked2 {
				t.Errorf("Expected: %v, Got: %v", then.invoked2, invoked2)
			}
		}
	}

	t.Run("Success All", theory(
		When{
			value:       Value{Content: "hello"},
			statusCode1: http.StatusOK,
			statusCode2: http.StatusOK,
		},
		Then{
			value:    Value{Content: "hello"},
			invoked1: true,
			invoked2: true,
			err:      nil,
		},
	))

	t.Run("Fail First", theory(
		When{
			value:       Value{Content: "hello"},
			statusCode1: http.StatusNotFound,
			statusCode2: http.StatusOK,
		},
		Then{
			value:    Value{Content: "hello"},
			invoked1: true,
			invoked2: false,
			err:      hook.ErrHookFailed,
		},
	))

	t.Run("Fail Second", theory(
		When{
			value:       Value{Content: "hello"},
			statusCode1: http.StatusOK,
			statusCode2: http.StatusNotFound,
		},
		Then{
			value:    Value{Content: "hello"},
			invoked1: true,
			invoked2: true,
			err:      hook.ErrHookFailed,
		},
	))
}

func TestHook_After_Sends_InvalidUrl(t *testing.T) {
	testee := hook.Web[string]{
		AfterURL: []*url.URL{
			try.To(url.Parse("http://somewhere.invalid")).OrFatal(t),
		},
	}

	err := testee.After("hello")
	if err == nil {
		t.Fatal("Expected an error")
	}
}
