package extras_test

import (
	"errors"
	"net/url"
	"os"
	"testing"

	"github.com/opst/knitfab/pkg/configs/extras"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestConfig_Load(t *testing.T) {
	type When struct {
		content string
	}
	type Then struct {
		err  error
		want extras.Config
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			dir := t.TempDir()
			file := dir + "/config.json"
			if err := os.WriteFile(file, []byte(when.content), 0644); err != nil {
				t.Fatal(err)
			}

			got, err := extras.Load(file)
			if !errors.Is(err, then.err) {
				t.Errorf("want %v, but got %v", then.err, err)
			}

			if !cmp.SliceContentEqWith(
				got.Endpoints, then.want.Endpoints,
				func(e1, e2 extras.Endpoint) bool {
					return e1.Path == e2.Path && e1.ProxyTo.String() == e2.ProxyTo.String()
				},
			) {
				t.Errorf("want %v, but got %v", then.want, got)
			}

		}
	}

	t.Run("empty endpoints", theory(
		When{content: `endpoints: []`},
		Then{err: nil, want: extras.Config{}},
	))

	t.Run("single", theory(
		When{content: `
endpoints:
  - path: "/"
    proxy_to: "http://example.com"
`},
		Then{
			err: nil,
			want: extras.Config{
				Endpoints: []extras.Endpoint{
					{
						Path:    "/",
						ProxyTo: try.To(url.Parse("http://example.com")).OrFatal(t),
					},
				},
			},
		},
	))

	t.Run("multiple", theory(
		When{content: `
endpoints:
  - path: "/external1"
    proxy_to: "http://example.com:8080/api"
  - path: "/external2"
    proxy_to: "https://example.com:8888/api"
`},
		Then{
			err: nil,
			want: extras.Config{
				Endpoints: []extras.Endpoint{
					{
						Path:    "/external1",
						ProxyTo: try.To(url.Parse("http://example.com:8080/api")).OrFatal(t),
					},
					{
						Path:    "/external2",
						ProxyTo: try.To(url.Parse("https://example.com:8888/api")).OrFatal(t),
					},
				},
			},
		},
	))

	t.Run("relative path", theory(
		When{content: `
endpoints:
  - path: "relative"
    proxy_to: "http://localhost:8080"
`},
		Then{
			err:  extras.ErrInvalidEndpointPath,
			want: extras.Config{},
		},
	))

	t.Run("not clean path", theory(
		When{content: `
endpoints:
  - path: "/not/../clean"
    proxy_to: "http://localhost:8080"
`},
		Then{
			err:  extras.ErrInvalidEndpointPath,
			want: extras.Config{},
		},
	))

	t.Run("relative rediredct_to", theory(
		When{content: `
endpoints:
  - path: "/extra"
    proxy_to: "localhost:8080/path"
`},
		Then{
			err:  extras.ErrInvalidRedirectTo,
			want: extras.Config{},
		},
	))

	t.Run("hostless rediredct_to", theory(
		When{content: `
endpoints:
  - path: "/extra"
    proxy_to: "http://:8080/path"
`},
		Then{
			err:  extras.ErrInvalidRedirectTo,
			want: extras.Config{},
		},
	))
}
