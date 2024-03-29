package profiles

import (
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"github.com/hectane/go-acl"
	"github.com/opst/knitfab/cmd/knit/config/open"
	yaml "gopkg.in/yaml.v3"
)

var ErrProfileStoreNotFound = errors.New("config file is not found")
var ErrCannotCreateConfig = errors.New("cannot create config file")
var ErrCannotUpdateConfig = errors.New("cannot update config file")
var ErrProfileInvalid = errors.New("knit profile is invalid")

// ProfileStore is a map from profile name to KnitProfile.
type ProfileStore map[string]*KnitProfile

type KnitCert struct {
	// base64 encoded CA certificate
	CA string `yaml:"ca,omitempty"`
}

// KnitProfile is a profile for knit server.
type KnitProfile struct {
	// endpoint of knit server
	ApiRoot string `yaml:"apiRoot"`

	// cert is a certificate for knit server.
	Cert KnitCert `yaml:"cert"`
}

func verifyUrl(s string) bool {
	u, err := url.Parse(s)
	return err == nil && u.IsAbs()
}

func verifyPEM(b64cert string) bool {
	bin, err := base64.StdEncoding.DecodeString(b64cert)
	if err != nil {
		return false
	}
	blk, _ := pem.Decode(bin)
	return blk != nil
}

// Verify KnitProfile
//
// # Return
//
// nil if it is valid. Otherwise, ErrProfileInvalid error.
func (p *KnitProfile) Verify() error {
	if !verifyUrl(p.ApiRoot) {
		return fmt.Errorf("%w: apiRoot is not URL: %s", ErrProfileInvalid, p.ApiRoot)
	}
	if p.Cert.CA != "" && !verifyPEM(p.Cert.CA) {
		return fmt.Errorf("%w: cert.ca is not PEM", ErrProfileInvalid)
	}

	return nil
}

// LoadProfileStore loads profile store from file.
func LoadProfileStore(filepath string) (ProfileStore, error) {
	buf, err := os.ReadFile(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w at %s", ErrProfileStoreNotFound, filepath)
		}
		return nil, err
	}
	return Unmarshall(buf)
}

// Unmarshall profile store from yaml in byte array.
func Unmarshall(buf []byte) (ProfileStore, error) {
	ret := map[string]*KnitProfile{}
	err := yaml.Unmarshal(buf, &ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// Save profile store to file.
func (kc *ProfileStore) Save(path string) error {
	saving := false

	if err := os.MkdirAll(filepath.Dir(path), os.FileMode(0700)); err != nil {
		return err
	}

	bkpath := path + ".backup"
	bk, err := open.NewSafeFile(bkpath)
	if err != nil {
		return err
	}
	defer func() {
		if !saving {
			os.Remove(bkpath)
		}
	}()
	defer bk.Close()

	f, err := os.OpenFile(path, os.O_RDWR, os.FileMode(0600))
	if err == nil {
		// In case of the existing file with loose permissions,
		// enforce permission to 0600.
		if err := acl.Chmod(path, os.FileMode(0600)); err != nil {
			return err
		}
	} else {
		if os.IsPermission(err) {
			return fmt.Errorf(
				"%w, because no permission to write file at %s",
				ErrCannotUpdateConfig, path,
			)
		} else if os.IsNotExist(err) {
			f_, err_ := open.NewSafeFile(path)
			if err_ != nil {
				return fmt.Errorf(
					"%w: cannot create a file at %s",
					ErrCannotCreateConfig, path,
				)
			}
			f = f_
		} else {
			return err
		}
	}
	defer f.Close()

	if err := bk.Truncate(0); err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	if _, err := io.Copy(bk, f); err != nil {
		return err
	}

	saving = true
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	if err := f.Truncate(0); err != nil {
		return err
	}
	buf, err := yaml.Marshal(kc)
	if err != nil {
		return err
	}
	_, err = f.Write(buf)

	if err == nil {
		saving = false
	}
	return err
}
