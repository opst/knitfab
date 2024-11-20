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
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestWebHook_Before(t *testing.T) {
	type Value struct {
		Content string `json:"content"`
	}

	type Resp struct {
		StatusCode  int
		ContentType string
		Content     string
	}

	type When struct {
		value Value
		resp1 Resp
		resp2 Resp
	}

	type Then struct {
		value Value

		invoked1 bool
		invoked2 bool

		ret map[string]string
		err error
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			handler := func(
				w http.ResponseWriter, r *http.Request, name string, resp Resp,
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

				if resp.ContentType != "" {
					w.Header().Set("Content-Type", resp.ContentType)
				}
				w.WriteHeader(resp.StatusCode)
				if resp.Content != "" {
					w.Write([]byte(resp.Content))
				}
			}

			invoked1, invoked2 := false, false
			server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				invoked1 = true
				handler(w, r, "server1", when.resp1)
			}))
			defer server1.Close()

			server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				invoked2 = true
				handler(w, r, "server2", when.resp2)
			}))
			defer server2.Close()

			testee := hook.Web[Value, map[string]string]{
				BeforeURL: []*url.URL{
					try.To(url.Parse(server1.URL)).OrFatal(t),
					try.To(url.Parse(server2.URL)).OrFatal(t),
				},
				Merge: func(a, b map[string]string) map[string]string {
					ret := make(map[string]string)
					for k, v := range a {
						ret[k] = v
					}
					for k, v := range b {
						ret[k] = v
					}
					return ret
				},
			}
			ret, err := testee.Before(when.value)
			if !errors.Is(err, then.err) {
				t.Errorf("Want: %v, Got: %v", then.err, err)
			}

			if invoked1 != then.invoked1 {
				t.Errorf("Want: %v, Got: %v", then.invoked1, invoked1)
			}
			if invoked2 != then.invoked2 {
				t.Errorf("Want: %v, Got: %v", then.invoked2, invoked2)
			}

			if !cmp.MapEq(ret, then.ret) {
				t.Errorf("Want: %v, Got: %v", then.ret, ret)
			}
		}
	}

	t.Run("Success All", theory(
		When{
			value: Value{Content: "hello"},
			resp1: Resp{StatusCode: http.StatusOK, ContentType: "application/json", Content: `{"a": "1"}`},
			resp2: Resp{StatusCode: http.StatusOK, ContentType: "application/json", Content: `{"b": "2"}`},
		},
		Then{
			value:    Value{Content: "hello"},
			invoked1: true,
			invoked2: true,
			ret:      map[string]string{"a": "1", "b": "2"},
			err:      nil,
		},
	))

	t.Run("Success All (with not json response)", theory(
		When{
			value: Value{Content: "hello"},
			resp1: Resp{StatusCode: http.StatusOK, ContentType: "application/json", Content: `{"a": "1"}`},
			resp2: Resp{StatusCode: http.StatusOK, ContentType: "text/plain", Content: `{"b": "2"}`},
		},
		Then{
			value:    Value{Content: "hello"},
			invoked1: true,
			invoked2: true,
			ret:      map[string]string{"a": "1"}, // only json response is considered
			err:      nil,
		},
	))

	t.Run("Fail First", theory(
		When{
			value: Value{Content: "hello"},
			resp1: Resp{StatusCode: http.StatusNotFound},
			resp2: Resp{StatusCode: http.StatusOK, ContentType: "application/json", Content: `{"b": "2"}`},
		},
		Then{
			value:    Value{Content: "hello"},
			invoked1: true,
			invoked2: false,
			ret:      map[string]string{},
			err:      hook.ErrHookFailed,
		},
	))

	t.Run("Fail Second", theory(
		When{
			value: Value{Content: "hello"},
			resp1: Resp{StatusCode: http.StatusOK, ContentType: "application/json", Content: `{"a": "1"}`},
			resp2: Resp{StatusCode: http.StatusNotFound},
		},
		Then{
			value:    Value{Content: "hello"},
			invoked1: true,
			invoked2: true,
			err:      hook.ErrHookFailed,
			ret:      map[string]string{},
		},
	))
}

func TestHook_Before_Sends_InvalidUrl(t *testing.T) {
	testee := hook.Web[string, struct{}]{
		BeforeURL: []*url.URL{
			try.To(url.Parse("http://somewhere.invalid")).OrFatal(t),
		},
		Merge: func(a, b struct{}) struct{} { return struct{}{} },
	}

	_, err := testee.Before("hello")
	if err == nil {
		t.Fatal("Expected an error")
	}
}

func TestWebHook_After(t *testing.T) {

	type Value struct {
		Content string `json:"content"`
	}

	type Resp struct {
		StatusCode  int
		ContentType string
		Content     string
	}

	type When struct {
		value Value
		resp1 Resp
		resp2 Resp
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
				w http.ResponseWriter, r *http.Request, name string, resp Resp,
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

				if resp.ContentType != "" {
					w.Header().Set("Content-Type", resp.ContentType)
				}
				w.WriteHeader(resp.StatusCode)
				if resp.Content != "" {
					w.Write([]byte(resp.Content))
				}
			}

			invoked1, invoked2 := false, false
			server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				invoked1 = true
				handler(w, r, "server1", when.resp1)
			}))
			defer server1.Close()

			server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				invoked2 = true
				handler(w, r, "server2", when.resp2)
			}))
			defer server2.Close()

			testee := hook.Web[Value, map[string]string]{
				AfterURL: []*url.URL{
					try.To(url.Parse(server1.URL)).OrFatal(t),
					try.To(url.Parse(server2.URL)).OrFatal(t),
				},
				Merge: func(a, b map[string]string) map[string]string {
					ret := make(map[string]string)
					for k, v := range a {
						ret[k] = v
					}
					for k, v := range b {
						ret[k] = v
					}
					return ret
				},
			}
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
			value: Value{Content: "hello"},
			resp1: Resp{StatusCode: http.StatusOK, ContentType: "application/json", Content: `{"a": "1"}`},
			resp2: Resp{StatusCode: http.StatusOK, ContentType: "application/json", Content: `{"b": "2"}`},
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
			value: Value{Content: "hello"},
			resp1: Resp{StatusCode: http.StatusNotFound},
			resp2: Resp{StatusCode: http.StatusOK, ContentType: "application/json", Content: `{"b": "2"}`},
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
			value: Value{Content: "hello"},
			resp1: Resp{StatusCode: http.StatusOK, ContentType: "application/json", Content: `{"a": "1"}`},
			resp2: Resp{StatusCode: http.StatusNotFound},
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
	testee := hook.Web[string, struct{}]{
		AfterURL: []*url.URL{
			try.To(url.Parse("http://somewhere.invalid")).OrFatal(t),
		},
		Merge: func(a, b struct{}) struct{} { return struct{}{} },
	}

	err := testee.After("hello")
	if err == nil {
		t.Fatal("Expected an error")
	}
}
