package env

import (
	"errors"
	"fmt"
	"os"

	apitags "github.com/opst/knitfab-api-types/tags"
	"gopkg.in/yaml.v3"
)

var ErrInvalidTagFormat = errors.New("invalid tag format")

func NewErrInvalidTagFormat(plain string) error {
	return fmt.Errorf("%w: %s", ErrInvalidTagFormat, plain)
}

type KnitEnv struct {
	Tag      []apitags.Tag     `yaml:"tag"`
	Resource map[string]string `yaml:"resource"`
}

func New() *KnitEnv {
	return new(KnitEnv)
}

func (ke *KnitEnv) Tags() []apitags.Tag {
	return ke.Tag
}

func LoadKnitEnv(filepath string) (*KnitEnv, error) {

	env := KnitEnv{}

	content, err := os.ReadFile(filepath)
	if err != nil {
		return &env, nil
	}

	err = yaml.Unmarshal(content, &env)
	if err != nil {
		return nil, err
	}

	return &env, nil
}
