package frontend_test

import (
	"testing"

	kcf "github.com/opst/knitfab/pkg/configs/frontend"
)

func TestLoadFrontendConfig(t *testing.T) {

	t.Run("it can be created from a config file", func(t *testing.T) {
		result, err := kcf.LoadFrontendConfig("./testdata/config.yaml")

		if err != nil {
			t.Errorf("failed to parse config.: %v", err)
		}
		expectedURI := "postgres://knit-test-pgdb-svc:32555/knit"
		if result.DBURI != expectedURI {
			t.Errorf("unmatch host:%s, expected:%s", result.DBURI, expectedURI)
		}
		expectedBackend := "http://127.0.0.1:8080"
		if result.BackendApiRoot != expectedBackend {
			t.Errorf("unmatch backendapiroot:%s, expected:%s", result.BackendApiRoot, expectedBackend)
		}
		expectedServerPort := "8080"
		if result.ServerPort != expectedServerPort {
			t.Errorf("unmatch serverport:%s, expected:%s", result.ServerPort, expectedServerPort)
		}

	})

}
