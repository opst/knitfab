package handlers_test

import (
	"net/url"
	"testing"

	"github.com/opst/knitfab/cmd/knitd/handlers"
	"github.com/opst/knitfab/pkg/configs/extras"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestRewriter(t *testing.T) {

	type When struct {
		Endpoint extras.Endpoint
		Url      string
	}

	type Then struct {
		Url string
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			testee := try.To(handlers.RewriteWith(when.Endpoint)).OrFatal(t)

			requrl := try.To(url.Parse(when.Url)).OrFatal(t)
			{
				dest, err := testee(requrl)
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}

				if dest.String() != then.Url {
					t.Fatalf("want %s, but got %s", then.Url, dest.String())
				}
			}
			{
				// test the safety for repeated call
				dest, err := testee(requrl)
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}

				if dest.String() != then.Url {
					t.Fatalf("want %s, but got %s", then.Url, dest.String())
				}
			}
		}
	}

	t.Run("rewrite between no path URLs", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/",
				ProxyTo: try.To(url.Parse("http://example.com")).OrFatal(t),
			},
			Url: "http://localhost.com",
		},
		Then{
			Url: "http://example.com",
		},
	))

	t.Run("rewrite between no path URLs (with port)", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/",
				ProxyTo: try.To(url.Parse("http://example.com:8888")).OrFatal(t),
			},
			Url: "http://localhost.com:8080",
		},
		Then{
			Url: "http://example.com:8888",
		},
	))

	t.Run("rewrite between no path URLs (trailing slash in request)", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/",
				ProxyTo: try.To(url.Parse("http://example.com")).OrFatal(t),
			},
			Url: "http://localhost.com/",
		},
		Then{
			Url: "http://example.com/",
		},
	))

	t.Run("rewrite between no path URLs (trailing slash in destination)", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/",
				ProxyTo: try.To(url.Parse("http://example.com/")).OrFatal(t),
			},
			Url: "http://localhost.com",
		},
		Then{
			Url: "http://example.com/",
		},
	))

	t.Run("rewrite between no path URLs (trailing slash in both)", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/",
				ProxyTo: try.To(url.Parse("http://example.com/")).OrFatal(t),
			},
			Url: "http://localhost.com/",
		},
		Then{
			Url: "http://example.com/",
		},
	))

	t.Run("rewrite between no path URLs with query", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/",
				ProxyTo: try.To(url.Parse("http://example.com")).OrFatal(t),
			},
			Url: "http://localhost.com?query=1",
		},
		Then{
			Url: "http://example.com?query=1",
		},
	))

	t.Run("rewrite between no path URLs with fragment", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/",
				ProxyTo: try.To(url.Parse("http://example.com")).OrFatal(t),
			},
			Url: "http://localhost.com#fragment",
		},
		Then{
			Url: "http://example.com#fragment",
		},
	))

	t.Run("rewrite between no path URLs with query and fragment", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/",
				ProxyTo: try.To(url.Parse("http://example.com")).OrFatal(t),
			},
			Url: "http://localhost.com?query=1#fragment",
		},
		Then{
			Url: "http://example.com?query=1#fragment",
		},
	))

	t.Run("rewrite between path URLs", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api",
				ProxyTo: try.To(url.Parse("http://example.com/api")).OrFatal(t),
			},
			Url: "http://localhost.com/extra-api",
		},
		Then{
			Url: "http://example.com/api",
		},
	))

	t.Run("rewrite between path URLs (trailing slash in request)", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api",
				ProxyTo: try.To(url.Parse("http://example.com/api")).OrFatal(t),
			},
			Url: "http://localhost.com/extra-api/",
		},
		Then{
			Url: "http://example.com/api/",
		},
	))

	t.Run("rewrite between path URLs (trailing slash in endpoint)", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api/",
				ProxyTo: try.To(url.Parse("http://example.com/api")).OrFatal(t),
			},
			Url: "http://localhost.com/extra-api",
		},
		Then{
			Url: "http://example.com/api",
		},
	))

	t.Run("rewrite between path URLs (trailing slash in destination)", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api",
				ProxyTo: try.To(url.Parse("http://example.com/api/")).OrFatal(t),
			},
			Url: "http://localhost.com/extra-api",
		},
		Then{
			Url: "http://example.com/api/",
		},
	))

	t.Run("rewrite between path URLs with query", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api",
				ProxyTo: try.To(url.Parse("http://example.com/api")).OrFatal(t),
			},
			Url: "http://localhost.com/extra-api?query=1",
		},
		Then{
			Url: "http://example.com/api?query=1",
		},
	))

	t.Run("rewrite between path URLs with fragment", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api",
				ProxyTo: try.To(url.Parse("http://example.com/api")).OrFatal(t),
			},
			Url: "http://localhost.com/extra-api#fragment",
		},
		Then{
			Url: "http://example.com/api#fragment",
		},
	))

	t.Run("rewrite between path URLs with query and fragment", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api",
				ProxyTo: try.To(url.Parse("http://example.com/api")).OrFatal(t),
			},
			Url: "http://localhost.com/extra-api?query=1#fragment",
		},
		Then{
			Url: "http://example.com/api?query=1#fragment",
		},
	))

	t.Run("rewrite between sub-path URLs", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api",
				ProxyTo: try.To(url.Parse("http://example.com/root")).OrFatal(t),
			},
			Url: "http://localhost.com/extra-api/sub/path",
		},
		Then{
			Url: "http://example.com/root/sub/path",
		},
	))

	t.Run("rewrite between sub-path URLs with query", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api",
				ProxyTo: try.To(url.Parse("http://example.com/root")).OrFatal(t),
			},
			Url: "http://localhost.com/extra-api/sub/path?query=1",
		},
		Then{
			Url: "http://example.com/root/sub/path?query=1",
		},
	))

	t.Run("rewrite between sub-path URLs with query (with port)", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api",
				ProxyTo: try.To(url.Parse("http://example.com:8888/root")).OrFatal(t),
			},
			Url: "http://localhost.com:8080/extra-api/sub/path?query=1",
		},
		Then{
			Url: "http://example.com:8888/root/sub/path?query=1",
		},
	))

	t.Run("rewrite between sub-path URLs with query (trailing shash in request)", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api",
				ProxyTo: try.To(url.Parse("http://example.com/root")).OrFatal(t),
			},
			Url: "http://localhost.com/extra-api/sub/path/?query=1",
		},
		Then{
			Url: "http://example.com/root/sub/path/?query=1",
		},
	))

	t.Run("rewrite between sub-path URLs with query (trailing shash in destination)", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api",
				ProxyTo: try.To(url.Parse("http://example.com/root/")).OrFatal(t),
			},
			Url: "http://localhost.com/extra-api/sub/path?query=1",
		},
		Then{
			Url: "http://example.com/root/sub/path?query=1",
		},
	))

	t.Run("rewrite between sub-path URLs with fragment", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api",
				ProxyTo: try.To(url.Parse("http://example.com/root")).OrFatal(t),
			},
			Url: "http://localhost.com/extra-api/sub/path#fragment",
		},
		Then{
			Url: "http://example.com/root/sub/path#fragment",
		},
	))

	t.Run("rewrite between sub-path URLs with query and fragment", theory(
		When{
			Endpoint: extras.Endpoint{
				Path:    "/extra-api",
				ProxyTo: try.To(url.Parse("http://example.com/root")).OrFatal(t),
			},
			Url: "http://localhost.com/extra-api/sub/path?query=1#fragment",
		},
		Then{
			Url: "http://example.com/root/sub/path?query=1#fragment",
		},
	))
}
