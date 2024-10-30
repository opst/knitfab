package analyzer_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	gcrrand "github.com/google/go-containerregistry/pkg/v1/random"
	gcrtarball "github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/images/analyzer"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestAnalyze(t *testing.T) {
	type ConfigPatch struct {
		Entrypooint []string
		Cmd         []string
		Volumes     map[string]struct{}
		WorkingDir  string
	}

	type When struct {
		image map[gcrname.Reference]ConfigPatch
	}

	type Then struct {
		want         []analyzer.TaggedConfig
		err          error
		wantAnyError bool
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

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
					WorkingDir:      patch.WorkingDir,
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

			actual, err := analyzer.Analyze(ctx, stream)
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

			if !cmp.SliceContentEqWith(actual, then.want, analyzer.TaggedConfig.Equal) {
				t.Errorf("expected: %v, actual: %v", then.want, actual)
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
					WorkingDir: "/work",
				},
			},
		},
		Then{
			want: []analyzer.TaggedConfig{
				{
					Tags: []string{"repo.invalid/image:tag"},
					Config: analyzer.Config{
						Entrypoint: []string{"/entrypoint"},
						Cmd:        []string{"/cmd"},
						Volumes: map[string]struct{}{
							"/in":  {},
							"/out": {},
						},
						WorkingDir: "/work",
					},
				},
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
					WorkingDir: "/work-1",
				},
				gcrname.MustParseReference("repo.invalid/image:tag2"): {
					Entrypooint: []string{"/entrypoint2"},
					Cmd:         []string{"/cmd2"},
					Volumes: map[string]struct{}{
						"/in2":  {},
						"/out2": {},
					},
					WorkingDir: "/work-2",
				},
			},
		},
		Then{
			want: []analyzer.TaggedConfig{
				{
					Tags: []string{"repo.invalid/image:tag"},
					Config: analyzer.Config{
						Entrypoint: []string{"/entrypoint"},
						Cmd:        []string{"/cmd"},
						Volumes: map[string]struct{}{
							"/in":  {},
							"/out": {},
						},
						WorkingDir: "/work-1",
					},
				},
				{
					Tags: []string{"repo.invalid/image:tag2"},
					Config: analyzer.Config{
						Entrypoint: []string{"/entrypoint2"},
						Cmd:        []string{"/cmd2"},
						Volumes: map[string]struct{}{
							"/in2":  {},
							"/out2": {},
						},
						WorkingDir: "/work-2",
					},
				},
			},
		},
	))

}
