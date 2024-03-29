package analyzer_test

import (
	"bytes"
	"errors"
	"testing"

	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	gcrrand "github.com/google/go-containerregistry/pkg/v1/random"
	gcrtarball "github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/images/analyzer"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestAnalyze(t *testing.T) {
	type ConfigPatch struct {
		Entrypooint []string
		Cmd         []string
		Volumes     map[string]struct{}
	}

	type When struct {
		image   map[gcrname.Reference]ConfigPatch
		options []analyzer.Option
	}

	type Then struct {
		tag          gcrname.Reference
		entrypoint   []string
		cmd          []string
		volume       map[string]struct{}
		err          error
		wantAnyError bool
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {

			img := map[gcrname.Reference]gcr.Image{}
			for name, patch := range when.image {
				i := try.To(gcrrand.Image(64, 1)).OrFatal(t)
				c := try.To(i.ConfigFile()).OrFatal(t).Config
				newConfig := gcr.Config{
					AttachStderr:    c.AttachStderr,
					AttachStdin:     c.AttachStdin,
					AttachStdout:    c.AttachStdout,
					Cmd:             patch.Cmd,
					Healthcheck:     c.Healthcheck,
					Domainname:      c.Domainname,
					Entrypoint:      patch.Entrypooint,
					Env:             c.Env,
					Hostname:        c.Hostname,
					Image:           c.Image,
					Labels:          c.Labels,
					OnBuild:         c.OnBuild,
					OpenStdin:       c.OpenStdin,
					StdinOnce:       c.StdinOnce,
					Tty:             c.Tty,
					User:            c.User,
					Volumes:         patch.Volumes,
					WorkingDir:      c.WorkingDir,
					ExposedPorts:    c.ExposedPorts,
					ArgsEscaped:     c.ArgsEscaped,
					NetworkDisabled: c.NetworkDisabled,
					MacAddress:      c.MacAddress,
					StopSignal:      c.StopSignal,
					Shell:           c.Shell,
				}
				img[name] = try.To(mutate.Config(i, newConfig)).OrFatal(t)
			}

			stream := bytes.NewBuffer(nil)
			if err := gcrtarball.MultiRefWrite(img, stream); err != nil {
				t.Fatal(err)
			}

			actual, err := analyzer.Analyze(stream, when.options...)
			if then.wantAnyError {
				if err == nil {
					t.Errorf("expected error, but no error")
				}
			} else if !errors.Is(err, then.err) {
				t.Errorf("expected error: %v, actual: %v", then.err, err)
			}
			if err != nil {
				return
			}

			if then.tag != actual.Tag {
				t.Errorf("tag: expected: %v, actual: %v", then.tag, actual.Tag)
			}
			if !cmp.SliceEq(then.entrypoint, actual.Config.Entrypoint) {
				t.Errorf("entrypoint: expected: %v, actual: %v", then.entrypoint, actual.Config.Entrypoint)
			}
			if !cmp.SliceEq(then.cmd, actual.Config.Cmd) {
				t.Errorf("cmd: expected: %v, actual: %v", then.cmd, actual.Config.Cmd)
			}
			if !cmp.MapEq(then.volume, actual.Config.Volumes) {
				t.Errorf("volume: expected: %v, actual: %v", then.volume, actual.Config.Volumes)
			}
		}
	}

	t.Run("image with single tag", theory(
		When{
			image: map[gcrname.Reference]ConfigPatch{
				gcrname.MustParseReference("repo.invalid/image:tag"): {
					Entrypooint: []string{"/entrypoint"},
					Cmd:         []string{"/cmd"},
					Volumes: map[string]struct{}{
						"/in":  {},
						"/out": {},
					},
				},
			},
		},
		Then{
			tag:        gcrname.MustParseReference("repo.invalid/image:tag"),
			entrypoint: []string{"/entrypoint"},
			cmd:        []string{"/cmd"},
			volume: map[string]struct{}{
				"/in":  {},
				"/out": {},
			},
		},
	))

	t.Run("image with single tag, specifying name", theory(
		When{
			image: map[gcrname.Reference]ConfigPatch{
				gcrname.MustParseReference("repo.invalid/image:tag"): {
					Entrypooint: []string{"/entrypoint"},
					Cmd:         []string{"/cmd"},
					Volumes: map[string]struct{}{
						"/in":  {},
						"/out": {},
					},
				},
			},
			options: []analyzer.Option{
				analyzer.WithTag("repo.invalid/image:tag"),
			},
		},
		Then{
			tag:        gcrname.MustParseReference("repo.invalid/image:tag"),
			entrypoint: []string{"/entrypoint"},
			cmd:        []string{"/cmd"},
			volume: map[string]struct{}{
				"/in":  {},
				"/out": {},
			},
		},
	))

	t.Run("image with multiple tags", theory(
		When{
			image: map[gcrname.Reference]ConfigPatch{
				gcrname.MustParseReference("repo.invalid/image:tag"): {
					Entrypooint: []string{"/entrypoint"},
					Cmd:         []string{"/cmd"},
					Volumes: map[string]struct{}{
						"/in":  {},
						"/out": {},
					},
				},
				gcrname.MustParseReference("repo.invalid/image:tag2"): {
					Entrypooint: []string{"/entrypoint2"},
					Cmd:         []string{"/cmd2"},
					Volumes: map[string]struct{}{
						"/in2":  {},
						"/out2": {},
					},
				},
			},
			options: []analyzer.Option{
				analyzer.WithTag("repo.invalid/image:tag2"),
			},
		},
		Then{
			tag:        gcrname.MustParseReference("repo.invalid/image:tag2"),
			entrypoint: []string{"/entrypoint2"},
			cmd:        []string{"/cmd2"},
			volume: map[string]struct{}{
				"/in2":  {},
				"/out2": {},
			},
		},
	))

	t.Run("image with multiple tags, but not specifying name", theory(
		When{
			image: map[gcrname.Reference]ConfigPatch{
				gcrname.MustParseReference("repo.invalid/image:tag"): {
					Entrypooint: []string{"/entrypoint"},
					Cmd:         []string{"/cmd"},
					Volumes: map[string]struct{}{
						"/in":  {},
						"/out": {},
					},
				},
				gcrname.MustParseReference("repo.invalid/image:tag2"): {
					Entrypooint: []string{"/entrypoint2"},
					Cmd:         []string{"/cmd2"},
					Volumes: map[string]struct{}{
						"/in2":  {},
						"/out2": {},
					},
				},
			},
		},
		Then{
			err: analyzer.ErrAmbiguousTag,
		},
	))

	t.Run("image with multiple tags, specifying wrong name", theory(
		When{
			image: map[gcrname.Reference]ConfigPatch{
				gcrname.MustParseReference("repo.invalid/image:tag"): {
					Entrypooint: []string{"/entrypoint"},
					Cmd:         []string{"/cmd"},
					Volumes: map[string]struct{}{
						"/in":  {},
						"/out": {},
					},
				},
				gcrname.MustParseReference("repo.invalid/image:tag2"): {
					Entrypooint: []string{"/entrypoint2"},
					Cmd:         []string{"/cmd2"},
					Volumes: map[string]struct{}{
						"/in2":  {},
						"/out2": {},
					},
				},
			},
			options: []analyzer.Option{
				analyzer.WithTag("repo.invalid/image:tag3"),
			},
		},
		Then{
			wantAnyError: true,
		},
	))
}
