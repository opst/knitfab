package backend

import (
	"os"

	"gopkg.in/yaml.v3"
)

// load knit server config from a file.
//
// args:
//   - filepath: filepath refers a config file.
//
// returns *KnitClusterConfig, error:
//
//	When loading success, returns `(*KnitClusterConfig, nil)`.
//	Otherwise, returns `(nil, error)`.
func LoadBackendConfig(filepath string) (*BackendConfig, error) {
	content, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	return Unmarshal(content)
}

func Unmarshal(conf []byte) (out *BackendConfig, err error) {
	var _out *BackendConfigMarshall
	err = yaml.Unmarshal(conf, &_out)
	if err != nil {
		return nil, err
	}
	out = TrySeal(_out)
	return out, nil
}
