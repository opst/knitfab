package cert_test

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/opst/knitfab/pkg/cert"
)

func TestCA(t *testing.T) {
	ca, err := cert.NewCA()
	if err != nil {
		t.Fatalf("failed to get CA: %#v", err)
	}
	cert, err := ca.Certificate(cert.DNSName("localhost"))
	if err != nil {
		t.Fatalf("failed to get Server Cert: %#v", err)
	}
	tlscert, err := cert.TLSCert()
	if err != nil {
		t.Fatalf("failed to get tlscert: %#v (%s)", err, err.Error())
	}

	certpool := x509.NewCertPool()
	if !certpool.AppendCertsFromPEM(ca.PEM()) {
		t.Fatal("failed to add CA into client certpool")
	}

	normalClient := http.Client{}

	trustingClient := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: certpool,
			},
		},
	}

	t.Run("client not trust CA can not send https request", func(t *testing.T) {
		handled := false
		svr := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handled = true
			w.WriteHeader(http.StatusNoContent)
		}))

		svr.TLS = &tls.Config{
			Certificates: []tls.Certificate{*tlscert},
		}

		svr.StartTLS()
		defer svr.Close()

		_url := strings.Split(svr.URL, ":")
		port := _url[len(_url)-1]

		_, err := normalClient.Get(fmt.Sprintf("https://localhost:%s", port))
		if err == nil {
			t.Fatal("request is succeeded to untrusted host.")
		}

		if handled {
			t.Error("request is not handled with server.")
		}
	})

	t.Run("client trust CA can send https request", func(t *testing.T) {
		handled := false
		svr := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handled = true
			w.WriteHeader(http.StatusNoContent)
		}))

		svr.TLS = &tls.Config{
			Certificates: []tls.Certificate{*tlscert},
		}

		svr.StartTLS()
		defer svr.Close()

		url := strings.Split(svr.URL, ":")
		port := url[len(url)-1]

		resp, err := trustingClient.Get(fmt.Sprintf("https://localhost:%s", port))
		if err != nil {
			t.Fatalf("failed to get response: %#v (%s)", err, err.Error())
		}
		if resp.Body != nil {
			resp.Body.Close()
		}

		if !handled {
			t.Error("request is not handled with server.")
		}

		if resp.StatusCode != http.StatusNoContent {
			t.Errorf(
				"unexpected status code: (expected, actual) = (%d, %d)",
				resp.StatusCode, http.StatusNoContent,
			)
		}

	})
}
