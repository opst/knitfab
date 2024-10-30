package analyzer

// DockerManifest represents the manifest file (/manifest.json) of a Docker image.
type DockerManifest struct {
	// Config is the path to the image configuration file.
	Config string `json:"Config,omitempty"`

	// RepoTags is the list of tags in the image repository.
	RepoTags []string `json:"RepoTags,omitempty"`

	// Other fields are ignored.
}
