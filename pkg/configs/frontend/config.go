package frontend

type FrontendConfig struct {
	DBURI          string `yaml:"dburi"`
	BackendApiRoot string `yaml:"backendapiroot"`
	ServerPort     string `yaml:"serverport"`
}
