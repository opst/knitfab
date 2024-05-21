package extras

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"

	"gopkg.in/yaml.v3"
)

type Endpoint struct {
	// Path is the path to be redirected to RedirectRoot.
	//
	// This should be a clean absolute path (start with / and do not contain . or ..).
	Path string

	// ProxyTo is the root URL of the endpoint which receives redirected request.
	//
	// Sub-path in original request is appended to this path.
	ProxyTo *url.URL
}

var ErrInvalidEndpointPath = errors.New("extras: endpoint path is invalid")
var ErrInvalidRedirectTo = errors.New("extras: redirect_to is invalid")

func (e *Endpoint) UnmarshalYAML(node *yaml.Node) error {
	raw := struct {
		Path    string `yaml:"path"`
		ProxyTo string `yaml:"proxy_to"`
	}{}
	if err := node.Decode(&raw); err != nil {
		return err
	}

	if raw.Path == "" {
		return fmt.Errorf("%w: Endpoiny Path is empty", ErrInvalidEndpointPath)
	}
	if !path.IsAbs(raw.Path) {
		return fmt.Errorf("%w: not absolute: %s", ErrInvalidEndpointPath, raw.Path)
	}
	if path.Clean(raw.Path) != raw.Path {
		return fmt.Errorf("%w: not clean: %s", ErrInvalidEndpointPath, raw.Path)
	}

	rr, err := url.Parse(raw.ProxyTo)
	if err != nil {
		return err
	}
	if !rr.IsAbs() {
		return fmt.Errorf("%w: not absolute: %s", ErrInvalidRedirectTo, raw.ProxyTo)
	}
	if rr.Hostname() == "" {
		return fmt.Errorf("%w: no hostname: %s", ErrInvalidRedirectTo, raw.ProxyTo)
	}

	e.Path = raw.Path
	e.ProxyTo = rr
	return nil
}

type Config struct {
	Endpoints []Endpoint
}

func (c *Config) UnmarshalYAML(node *yaml.Node) error {
	raw := struct {
		Endpoints []*Endpoint `yaml:"endpoints,omitempty"`
	}{}
	if err := node.Decode(&raw); err != nil {
		return err
	}

	for _, e := range raw.Endpoints {
		c.Endpoints = append(c.Endpoints, *e)
	}
	return nil
}

// Load loads configuration from the file.
func Load(file string) (Config, error) {
	f, err := os.Open(file)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	cfg := Config{}
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
