package frontend

import (
	"os"

	"gopkg.in/yaml.v3"
)

func LoadFrontendConfig(filepath string) (*FrontendConfig, error) {
	content, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	return Unmarshal(content)
}

func Unmarshal(conf []byte) (*FrontendConfig, error) {
	var out FrontendConfig
	err := yaml.Unmarshal(conf, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}
