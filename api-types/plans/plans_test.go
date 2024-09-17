package plans_test

import (
	"encoding/json"
	"testing"

	"github.com/opst/knitfab-api-types/internal/utils/cmp"
	"github.com/opst/knitfab-api-types/plans"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestImage(t *testing.T) {
	theory := func(expr string, image plans.Image) func(*testing.T) {
		return func(t *testing.T) {
			{
				actual := new(plans.Image)
				if err := actual.Parse(expr); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if *actual != image {
					t.Errorf("unexpected result: Image.Parse(%s) --> %#v", expr, actual)
				}
			}
			{
				type Json struct {
					Image *plans.Image `json:"image"`
				}

				actual, err := json.Marshal(Json{Image: &image})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if string(actual) != `{"image":"`+expr+`"}` {
					t.Errorf("unexpected result: json.Marshal(%#v) --> %s", image, actual)
				}
			}
			{
				type Yaml struct {
					Image *plans.Image `yaml:"image"`
				}

				actual, err := yaml.Marshal(Yaml{Image: &image})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				expected := `image: "` + expr + `"` + "\n"
				if got := string(actual); got != expected {
					t.Errorf("unexpected result: yaml.Marshal(%#v) --> %s", image, actual)
				}
			}
		}
	}

	t.Run("repository and tag", theory("repo:tag", plans.Image{
		Repository: "repo",
		Tag:        "tag",
	}))

	t.Run("registry, repository and tag", theory("registry.invalid/repo:tag", plans.Image{
		Repository: "registry.invalid/repo",
		Tag:        "tag",
	}))

	t.Run("registry /w port and repository and tag", theory("registry.invalid:5000/repo:tag", plans.Image{
		Repository: "registry.invalid:5000/repo",
		Tag:        "tag",
	}))
}

func TestResources(t *testing.T) {
	type Expr struct {
		Yaml string
		Json string
	}
	theory := func(expr Expr, resources plans.Resources) func(*testing.T) {
		return func(t *testing.T) {
			{
				type Json struct {
					Resources plans.Resources `json:"resources"`
				}

				unmarshalled := Json{}
				if err := json.Unmarshal([]byte(expr.Json), &unmarshalled); err != nil {
					t.Fatal(err)
				}
				if !cmp.MapEqual(unmarshalled.Resources, resources) {
					t.Errorf("unexpected result: json.Unmarshal(%s) --> %#v", expr.Json, unmarshalled)
				}

				marshalled, err := json.Marshal(Json{Resources: resources})
				if err != nil {
					t.Fatal(err)
				}
				reunmarshalled := Json{}
				if err := json.Unmarshal(marshalled, &reunmarshalled); err != nil {
					t.Fatal(err)
				}

				if !cmp.MapEqual(reunmarshalled.Resources, resources) {
					t.Errorf("unexpected result: json.Marshal(%#v) --> %s", resources, marshalled)
				}
			}

			{
				type Yaml struct {
					Resources plans.Resources `yaml:"resources"`
				}

				unmarshalled := Yaml{}
				if err := yaml.Unmarshal([]byte(expr.Yaml), &unmarshalled); err != nil {
					t.Fatal(err)
				}
				if !cmp.MapEqual(unmarshalled.Resources, resources) {
					t.Errorf("unexpected result: yaml.Unmarshal(%s) --> %#v", expr.Yaml, unmarshalled)
				}

				marshalled, err := yaml.Marshal(Yaml{Resources: resources})
				if err != nil {
					t.Fatal(err)
				}
				reunmarshalled := Yaml{}
				if err := yaml.Unmarshal(marshalled, &reunmarshalled); err != nil {
					t.Fatal(err)
				}

				if !cmp.MapEqual(reunmarshalled.Resources, resources) {
					t.Errorf("unexpected result: yaml.Marshal(%#v) --> %s", resources, marshalled)
				}
			}
		}
	}

	t.Run("test marshal and unmarshal", theory(
		Expr{
			Yaml: `
resources:
  cpu: 1
  memory: 1Gi
  gpu: "1"
`,
			Json: `
{
  "resources": {
    "cpu": 1,
    "memory": "1Gi",
    "gpu": "1"
  }
}
`,
		},
		plans.Resources{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
			"gpu":    resource.MustParse("1"),
		},
	))
}
