package config

import (
	"net/url"
	"os"

	"gopkg.in/yaml.v3"
)

func Load(filename string) (Config, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := yaml.Unmarshal(content, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

type Config struct {
	Lifecycle WebHook `yaml:"lifecycle-hooks,omitempty"`
}

type WebHook struct {
	Before []*url.URL
	After  []*url.URL
}

func (wh *WebHook) UnmarshalYAML(node *yaml.Node) error {
	raw := struct {
		Before []string `yaml:"before"`
		After  []string `yaml:"after"`
	}{}
	if err := node.Decode(&raw); err != nil {
		return err
	}

	wh.Before = make([]*url.URL, len(raw.Before))
	for i, u := range raw.Before {
		parsed, err := url.Parse(u)
		if err != nil {
			return err
		}
		wh.Before[i] = parsed
	}

	wh.After = make([]*url.URL, len(raw.After))
	for i, u := range raw.After {
		parsed, err := url.Parse(u)
		if err != nil {
			return err
		}
		wh.After[i] = parsed
	}
	return nil
}

type Hooks struct {
	Initialize    WebHook `yaml:"initialize,omitempty"`
	RunManagement WebHook `yaml:"run_management,omitempty"`
	Finishing     WebHook `yaml:"finishing,omitempty"`
}
