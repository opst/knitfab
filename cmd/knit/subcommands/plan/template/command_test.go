package template_test

import (
	"context"
	"errors"
	"io"
	"log"
	"testing"

	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/opst/knitfab/cmd/knit/env"
	plan_template "github.com/opst/knitfab/cmd/knit/subcommands/plan/template"
	apiplans "github.com/opst/knitfab/pkg/api/types/plans"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/images/analyzer"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestNewPlanFromScratch(t *testing.T) {
	testee := plan_template.FromScratch()

	knitenv := env.KnitEnv{
		Tag: []apitag.Tag{
			{Key: "type", Value: "raw data"},
			{Key: "format", Value: "rgb image"},
		},
		Resource: map[string]string{
			"cpu":    "0.5",
			"memory": "500Mi",
		},
	}

	ctx := context.Background()
	actual := try.To(testee(
		ctx, nil, "repo.invalid/image:tag", knitenv,
	)).OrFatal(t)

	expected := apiplans.PlanSpec{
		Image: apiplans.Image{
			Repository: "repo.invalid/image",
			Tag:        "tag",
		},
		Inputs: []apiplans.Mountpoint{
			{Path: "/in", Tags: knitenv.Tag},
		},
		Outputs: []apiplans.Mountpoint{
			{Path: "/out", Tags: knitenv.Tag},
		},
		Resources: apiplans.Resources{
			"cpu":    resource.MustParse("0.5"),
			"memory": resource.MustParse("500Mi"),
		},
		Log: &apiplans.LogPoint{
			Tags: append(
				[]apitag.Tag{{Key: "type", Value: "log"}},
				knitenv.Tag...,
			),
		},
	}

	if !expected.Equal(&actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

type mockedAnalyzer struct {
	Config analyzer.TaggedConfig
	Err    error
}

func (m mockedAnalyzer) Analyze(s io.Reader, _ ...analyzer.Option) (*analyzer.TaggedConfig, error) {
	return &m.Config, m.Err
}

type namedReader struct {
	name string
}

func (n namedReader) Name() string {
	return n.name
}

func (n namedReader) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func TestNewPlanFromImage(t *testing.T) {

	type When struct {
		analyzer mockedAnalyzer
		env      env.KnitEnv
	}

	type Then struct {
		plan apiplans.PlanSpec
		err  error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			testee := plan_template.FromImage(when.analyzer.Analyze)
			logger := log.New(io.Discard, "", 0)

			ctx := context.Background()
			actual, err := testee(
				ctx,
				logger,
				namedReader{name: "filename"},
				"image:tag",
				when.env,
			)

			if !errors.Is(err, then.err) {
				t.Errorf("expected: %+v, actual: %+v", then.err, err)
			}
			if err != nil {
				return
			}

			if !then.plan.Equal(&actual) {
				t.Errorf(
					"\n===actual===\n%+v\n===expected===\n%+v\n",
					actual, then.plan,
				)
			}
		}
	}

	{
		expectedErr := errors.New("an error")
		t.Run("when the analyzer returns an error, it returns thar error", theory(
			When{
				analyzer: mockedAnalyzer{
					Err: expectedErr,
				},
				env: env.KnitEnv{
					Tag: []apitag.Tag{
						{Key: "type", Value: "raw data"},
						{Key: "format", Value: "rgb image"},
					},
					Resource: map[string]string{
						"cpu":    "0.5",
						"memory": "500Mi",
					},
				},
			},
			Then{
				err: expectedErr,
			},
		))
	}

	{
		t.Run("when the analyzer returns a config, it returns a plan", theory(
			When{
				analyzer: mockedAnalyzer{
					Config: analyzer.TaggedConfig{
						Tag: try.To(
							gcrname.NewTag("image:tag", gcrname.WithDefaultRegistry("")),
						).OrFatal(t),
						Config: gcr.Config{
							WorkingDir: "/work",
							Entrypoint: []string{
								"/entrypoint.sh",
								"in/1",
								"/in/2",
								"3/in",
								"/4/in",
								"...",
								"/out/1",
								"./out/2",
								"/3/out",
								"/4/out",
								"...",
							},
							Cmd: []string{
								"command-a",
								"in/1",
								"./in/5",
								"/in/6",
								"command-b",
								"/7/in",
								"/8/in",
								"/out/1",
								"out/5",
								"/out/6",
								"command-c",
								"/7/out",
								"8/out",
							},
							Volumes: map[string]struct{}{
								"/in/2":   {},
								"/in/9":   {},
								"/in/10":  {},
								"/11/in":  {},
								"/12/in":  {},
								"/cahce":  {},
								"./out/2": {},
								"/out/9":  {},
								"/out/10": {},
								"/11/out": {},
								"/12/out": {},
								"/temp":   {},
							},
						},
					},
				},
				env: env.KnitEnv{
					Tag: []apitag.Tag{
						{Key: "project", Value: "test"},
						{Key: "type", Value: "example"},
					},
					Resource: map[string]string{
						"cpu":    "0.5",
						"memory": "500Mi",
					},
				},
			},
			Then{
				plan: apiplans.PlanSpec{
					Image: apiplans.Image{Repository: "image", Tag: "tag"},
					Inputs: []apiplans.Mountpoint{
						{
							Path: "/work/in/1",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "1"},
							},
						},
						{
							Path: "/in/2",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "2"},
							},
						},
						{
							Path: "/work/3/in",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "3"},
							},
						},
						{
							Path: "/4/in",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "4"},
							},
						},
						{
							Path: "/work/in/5",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "5"},
							},
						},
						{
							Path: "/in/6",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "6"},
							},
						},
						{
							Path: "/7/in",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "7"},
							},
						},
						{
							Path: "/8/in",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "8"},
							},
						},
						{
							Path: "/in/9",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "9"},
							},
						},
						{
							Path: "/in/10",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "10"},
							},
						},
						{
							Path: "/11/in",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "11"},
							},
						},
						{
							Path: "/12/in",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "12"},
							},
						},
					},
					Outputs: []apiplans.Mountpoint{
						{
							Path: "/out/1",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "1"},
							},
						},
						{
							Path: "/work/out/2",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "2"},
							},
						},
						{
							Path: "/3/out",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "3"},
							},
						},
						{
							Path: "/4/out",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "4"},
							},
						},
						{
							Path: "/work/out/5",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "5"},
							},
						},
						{
							Path: "/out/6",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "6"},
							},
						},
						{
							Path: "/7/out",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "7"},
							},
						},
						{
							Path: "/work/8/out",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "8"},
							},
						},
						{
							Path: "/out/9",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "9"},
							},
						},
						{
							Path: "/out/10",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "10"},
							},
						},
						{
							Path: "/11/out",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "11"},
							},
						},
						{
							Path: "/12/out",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "example"},
								{Key: "type", Value: "12"},
							},
						},
					},
					Resources: apiplans.Resources{
						"cpu":    resource.MustParse("0.5"),
						"memory": resource.MustParse("500Mi"),
					},
					Log: &apiplans.LogPoint{
						Tags: []apitag.Tag{
							{Key: "project", Value: "test"},
							{Key: "type", Value: "example"},
							{Key: "type", Value: "log"},
						},
					},
				},
			},
		))

		t.Run("when env has no resource config, it generates as default", theory(
			When{
				analyzer: mockedAnalyzer{
					Config: analyzer.TaggedConfig{
						Tag: try.To(
							gcrname.NewTag("image:tag", gcrname.WithDefaultRegistry("")),
						).OrFatal(t),
						Config: gcr.Config{
							WorkingDir: "/work",
							Entrypoint: []string{
								"/entrypoint.sh",
								"in/1",
							},
							Cmd: []string{
								"command-a",
								"in/1",
								"out/1",
							},
						},
					},
				},
				env: env.KnitEnv{
					Tag: []apitag.Tag{
						{Key: "project", Value: "test"},
					},
				},
			},
			Then{
				plan: apiplans.PlanSpec{
					Image: apiplans.Image{Repository: "image", Tag: "tag"},
					Inputs: []apiplans.Mountpoint{
						{
							Path: "/work/in/1",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "1"},
							},
						},
					},
					Outputs: []apiplans.Mountpoint{
						{
							Path: "/work/out/1",
							Tags: []apitag.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "1"},
							},
						},
					},
					Resources: apiplans.Resources{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("1Gi"),
					},
					Log: &apiplans.LogPoint{
						Tags: []apitag.Tag{
							{Key: "project", Value: "test"},
							{Key: "type", Value: "log"},
						},
					},
				},
			},
		))
	}
}
