// Based on `config.config` in https://github.com/opencontainers/image-spec/blob/main/config.md .
package analyzer

import "github.com/opst/knitfab/pkg/utils/cmp"

type RootFs struct {
	Type    string   `json:"type"`
	DiffIds []string `json:"diff_ids"`
}

func (rf RootFs) Equal(other RootFs) bool {
	return rf.Type == other.Type && cmp.SliceEq(rf.DiffIds, other.DiffIds)
}

// Config represents the configuration of a container image.
//
// This is a subset of the spec.
type Config struct {
	Entrypoint []string            `json:"Entrypoint,omitempty"`
	Cmd        []string            `json:"Cmd,omitempty"`
	Volumes    map[string]struct{} `json:"Volumes,omitempty"`
	WorkingDir string              `json:"WorkingDir,omitempty"`
}

func (imc Config) Equal(other Config) bool {
	return cmp.SliceEq(imc.Entrypoint, other.Entrypoint) &&
		cmp.SliceEq(imc.Cmd, other.Cmd) &&
		cmp.MapEq(imc.Volumes, other.Volumes)
}

type Image struct {
	Os           *string `json:"os"`
	Architecture *string `json:"architecture"`
	Config       Config  `json:"config"`
	RootFs       *RootFs `json:"rootfs"`
}

func (cf Image) IsValid() bool {
	return cf.Os != nil && cf.Architecture != nil && cf.RootFs != nil
}

func (cf Image) Equal(other Image) bool {
	return nileq(cf.Os, other.Os) &&
		nileq(cf.Architecture, other.Architecture) &&
		cf.Config.Equal(other.Config) &&
		nileqWith(cf.RootFs, other.RootFs, RootFs.Equal)
}

func nileq[T comparable](a, b *T) bool {
	if a == nil {
		return b == nil
	}
	if b == nil {
		return false
	}
	return *a == *b
}

func nileqWith[T any](a, b *T, pred func(a, b T) bool) bool {
	if a == nil {
		return b == nil
	}
	if b == nil {
		return false
	}
	return pred(*a, *b)
}
