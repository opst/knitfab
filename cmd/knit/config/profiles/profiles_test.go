package profiles_test

import (
	_ "embed"
	"encoding/base64"
	"errors"
	"os"
	"testing"

	prof "github.com/opst/knitfab/cmd/knit/config/profiles"
)

func TestConfig(t *testing.T) {
	t.Run("unmarshalling works well", func(t *testing.T) {
		conf, err := prof.Unmarshall([]byte(`
profname:
    apiRoot: "https://api.example.com"
    cert:
        ca: BASE64_ENCODED_CERT
`))
		if err != nil {
			t.Fatalf("failed to unmarshal.: %+v", err)
		}
		prof, ok := conf["profname"]
		if !ok {
			t.Fatal("config has not profile")
		}

		expectedKnitUrl := "https://api.example.com"
		if prof.ApiRoot != expectedKnitUrl {
			t.Errorf("prof.KnitUrl unmatch. (actual, expected) = (%s, %s)", prof.ApiRoot, expectedKnitUrl)
		}

		expectedCACert := "BASE64_ENCODED_CERT"
		if prof.Cert.CA != expectedCACert {
			t.Errorf("prof.CACerts ummatch. (actual, expected) = (%v, %v)", prof.Cert.CA, expectedCACert)
		}
	})

}

func TestKnitProfile(t *testing.T) {

	t.Run("verify profile", func(t *testing.T) {
		temp, err := os.MkdirTemp("", "")
		if err != nil {
			t.Fatalf("failed to create temo dir: %v", err)
		}
		defer os.RemoveAll(temp)

		for name, testcase := range map[string]struct {
			prof      *prof.KnitProfile
			toBeValid error
		}{
			"all value is valid, it is valid": {
				prof: &prof.KnitProfile{
					ApiRoot: "https://api.example.com",
					Cert: prof.KnitCert{
						CA: base64.StdEncoding.EncodeToString(cacertfile),
					},
				},
				toBeValid: nil,
			},
			"no CACerts is ok": {
				prof: &prof.KnitProfile{
					ApiRoot: "https://api.example.com",
					Cert: prof.KnitCert{
						CA: "",
					},
				},
				toBeValid: nil,
			},
			"when knit api url is broken, it is not valid": {
				prof: &prof.KnitProfile{
					ApiRoot: "not url",
					Cert:    prof.KnitCert{},
				},
				toBeValid: prof.ErrProfileInvalid,
			},
			"when CACert contains missing file, it is not valid": {
				prof: &prof.KnitProfile{
					ApiRoot: "https://api.example.com",
					Cert: prof.KnitCert{
						CA: base64.StdEncoding.EncodeToString([]byte("broken cert")),
					},
				},
				toBeValid: prof.ErrProfileInvalid,
			},
		} {
			t.Run(name, func(t *testing.T) {
				if !errors.Is(testcase.prof.Verify(), testcase.toBeValid) {
					t.Errorf(
						"profile verification wrong. toBeValid?(=%v) content = %+v",
						testcase.toBeValid, testcase.prof,
					)
				}
			})
		}

	})

}

//go:embed testdata/ca.crt
var cacertfile []byte
