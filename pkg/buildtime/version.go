package buildtime

import (
	_ "embed"
	"strings"
)

//go:embed VERSION
var version string

//go:embed revision
var revision string

func init() {
	version = strings.TrimSpace(version)
	revision = strings.TrimSpace(revision)
}

// version string when this knitfab has been built.
func VERSION() string {
	return version
}

func GIT_REVISION() string {
	return revision
}

func VersionString() string {
	return version + " (commit: " + revision + ")"
}
