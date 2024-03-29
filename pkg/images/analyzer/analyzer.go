package analyzer

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/utils"
)

func opener(b []byte) tarball.Opener {
	return func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(b)), nil
	}
}

type analyzeOption struct {
	explicitName bool
	tag          string
}

type Option func(*analyzeOption) *analyzeOption

func WithTag(name string) Option {
	return func(o *analyzeOption) *analyzeOption {
		o.tag = name
		o.explicitName = true
		return o
	}
}

type TaggedConfig struct {
	Tag    name.Tag
	Config gcr.Config
}

func (tc *TaggedConfig) Equal(o *TaggedConfig) bool {
	if (tc == nil) || (o == nil) {
		return (tc == nil) && (o == nil)
	}

	healthChackEq := func(a, b *gcr.HealthConfig) bool {
		if (a == nil) || (b == nil) {
			return (a == nil) && (b == nil)
		}

		return cmp.SliceEq(a.Test, b.Test) &&
			a.Interval == b.Interval &&
			a.Timeout == b.Timeout &&
			a.StartPeriod == b.StartPeriod &&
			a.Retries == b.Retries
	}

	configEq := func(a, b gcr.Config) bool {
		return a.AttachStderr == b.AttachStderr &&
			a.AttachStdin == b.AttachStdin &&
			a.AttachStdout == b.AttachStdout &&
			cmp.SliceEq(a.Cmd, b.Cmd) &&
			healthChackEq(a.Healthcheck, b.Healthcheck) &&
			a.Domainname == b.Domainname &&
			cmp.SliceEq(a.Entrypoint, b.Entrypoint) &&
			cmp.SliceEq(a.Env, b.Env) &&
			a.Hostname == b.Hostname &&
			a.Image == b.Image &&
			cmp.MapEq(a.Labels, b.Labels) &&
			cmp.SliceEq(a.OnBuild, b.OnBuild) &&
			a.OpenStdin == b.OpenStdin &&
			a.StdinOnce == b.StdinOnce &&
			a.Tty == b.Tty &&
			a.User == b.User &&
			cmp.MapEq(a.Volumes, b.Volumes) &&
			a.WorkingDir == b.WorkingDir &&
			cmp.MapEq(a.ExposedPorts, b.ExposedPorts) &&
			a.ArgsEscaped == b.ArgsEscaped &&
			a.NetworkDisabled == b.NetworkDisabled &&
			a.MacAddress == b.MacAddress &&
			a.StopSignal == b.StopSignal &&
			cmp.SliceEq(a.Shell, b.Shell)
	}

	return tc.Tag == o.Tag && configEq(tc.Config, o.Config)
}

func Analyze(stream io.Reader, option ...Option) (*TaggedConfig, error) {

	opt := &analyzeOption{}
	for _, o := range option {
		opt = o(opt)
	}

	b, err := io.ReadAll(stream)
	if err != nil {
		return nil, err
	}

	_opener := opener(b)
	m, err := tarball.LoadManifest(_opener)
	if err != nil {
		return nil, err
	}

	tag := opt.tag
	if !opt.explicitName {
		set := false
		for _, d := range m {
			for _, t := range d.RepoTags {
				if !set {
					tag = t
					set = true
					continue
				}
				return nil, fmt.Errorf(
					"%w: %s",
					ErrAmbiguousTag,
					utils.Concat(utils.Map(
						m,
						func(d tarball.Descriptor) []string { return d.RepoTags },
					)),
				)
			}
		}
		if !set {
			return nil, ErrTagNotDetected
		}
	}

	ref, err := name.NewTag(tag)
	if err != nil {
		return nil, err
	}

	img, err := tarball.Image(_opener, &ref)
	if err != nil {
		return nil, err
	}

	cfg, err := img.ConfigFile()
	if err != nil {
		return nil, err
	}

	return &TaggedConfig{Tag: ref, Config: cfg.Config}, nil
}

var ErrAmbiguousTag = errors.New("ambiguous tag")
var ErrTagNotDetected = errors.New("no tags detected")
