package plans_test

import (
	"encoding/json"
	"testing"

	apiplan "github.com/opst/knitfab/pkg/api/types/plans"
	apitags "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils/try"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestComposeDetail(t *testing.T) {

	for name, testcase := range map[string]struct {
		when kdb.Plan
		then apiplan.Detail
	}{
		"When a plan with log is passed, it should compose a Detail corresponding to the plan.": {
			when: kdb.Plan{
				PlanBody: kdb.PlanBody{
					PlanId: "plan-1", Active: true, Hash: "hash1",
					Image:  &kdb.ImageIdentifier{Image: "image-1", Version: "ver-1"},
					Pseudo: &kdb.PseudoPlanDetail{},
				},
				Inputs: []kdb.MountPoint{
					{
						Id: 1, Path: "C:\\mp1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "key1", Value: "val1"},
						}),
					},
				},
				Outputs: []kdb.MountPoint{
					{
						Id: 2, Path: "C:\\mp2",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "key2", Value: "val2"},
							{Key: "key3", Value: "val3"},
						}),
					},
				},
				Log: &kdb.LogPoint{
					Id: 3,
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "logkey1", Value: "logval1"},
						{Key: "logkey2", Value: "logval2"},
					}),
				},
			},
			then: apiplan.Detail{
				Summary: apiplan.Summary{
					PlanId: "plan-1",
					Image: &apiplan.Image{
						Repository: "image-1",
						Tag:        "ver-1",
					},
				},
				Inputs: []apiplan.Mountpoint{

					{
						Path: "C:\\mp1",
						Tags: []apitags.Tag{
							{Key: "key1", Value: "val1"},
						},
					},
				},
				Outputs: []apiplan.Mountpoint{
					{
						Path: "C:\\mp2",
						Tags: []apitags.Tag{
							{Key: "key2", Value: "val2"},
							{Key: "key3", Value: "val3"},
						},
					},
				},
				Log: &apiplan.LogPoint{
					Tags: []apitags.Tag{
						{Key: "logkey1", Value: "logval1"},
						{Key: "logkey2", Value: "logval2"},
					},
				},
				Active: true,
			},
		},
		"When a plan without log is passed, it should compose a Detail corresponding to the plan.": {
			when: kdb.Plan{
				PlanBody: kdb.PlanBody{
					PlanId: "plan-1", Active: true, Hash: "hash1",
					Image:  &kdb.ImageIdentifier{Image: "image-1", Version: "ver-1"},
					Pseudo: &kdb.PseudoPlanDetail{},
				},
				Inputs: []kdb.MountPoint{
					{
						Id: 1, Path: "C:\\mp1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "key1", Value: "val1"},
						}),
					},
				},
				Outputs: []kdb.MountPoint{
					{
						Id: 2, Path: "C:\\mp2",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "key2", Value: "val2"},
							{Key: "key3", Value: "val3"},
						}),
					},
					{
						Id: 3, Path: "C:\\mp3",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "key4", Value: "val4"},
							{Key: "key5", Value: "val5"},
						}),
					},
				},
			},
			then: apiplan.Detail{
				Summary: apiplan.Summary{
					PlanId: "plan-1", Image: &apiplan.Image{Repository: "image-1", Tag: "ver-1"},
				},
				Inputs: []apiplan.Mountpoint{
					{
						Path: "C:\\mp1",
						Tags: []apitags.Tag{
							{Key: "key1", Value: "val1"},
						},
					},
				},
				Outputs: []apiplan.Mountpoint{
					{
						Path: "C:\\mp2",
						Tags: []apitags.Tag{
							{Key: "key2", Value: "val2"},
							{Key: "key3", Value: "val3"},
						},
					},
					{
						Path: "C:\\mp3",
						Tags: []apitags.Tag{
							{Key: "key4", Value: "val4"},
							{Key: "key5", Value: "val5"},
						},
					},
				},
				Active: true,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual := apiplan.ComposeDetail(testcase.when)
			if !actual.Equal(&testcase.then) {
				t.Fatalf("unexpected result: ComposeDetail(%+v) --> %+v", testcase.then, actual)
			}
		})
	}
}

func TestComposeSummary(t *testing.T) {

	for name, testcase := range map[string]struct {
		when kdb.PlanBody
		then apiplan.Summary
	}{
		"When a non-pseudo plan is passed, it should compose a Summary corresponding to the plan.": {
			when: kdb.PlanBody{
				PlanId: "plan-1",
				Hash:   "###plan-1###",
				Active: true,
				Image:  &kdb.ImageIdentifier{Image: "image-1", Version: "ver-1"},
			},
			then: apiplan.Summary{
				PlanId: "plan-1",
				Image:  &apiplan.Image{Repository: "image-1", Tag: "ver-1"},
			},
		},
		"When a pseudo plan is passed, it should compose a Summary corresponding to the plan.": {
			when: kdb.PlanBody{
				PlanId: "plan-1",
				Hash:   "###plan-1###",
				Active: true,
				Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-plan-name"},
			},
			then: apiplan.Summary{
				PlanId: "plan-1",
				Name:   "pseudo-plan-name",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual := apiplan.ComposeSummary(testcase.when)

			if !actual.Equal(testcase.then) {
				t.Errorf("unexpected result: ComposeSummary(%+v) --> %+v", testcase.then, actual)
			}
		})
	}
}

func TestImage(t *testing.T) {
	theory := func(expr string, image apiplan.Image) func(*testing.T) {
		return func(t *testing.T) {
			{
				actual := new(apiplan.Image)
				if err := actual.Parse(expr); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if *actual != image {
					t.Errorf("unexpected result: Image.Parse(%s) --> %#v", expr, actual)
				}
			}
			{
				type Json struct {
					Image *apiplan.Image `json:"image"`
				}

				actual := try.To(json.Marshal(Json{Image: &image})).OrFatal(t)
				if string(actual) != `{"image":"`+expr+`"}` {
					t.Errorf("unexpected result: json.Marshal(%#v) --> %s", image, actual)
				}
			}
			{
				type Yaml struct {
					Image *apiplan.Image `yaml:"image"`
				}
				actual := string(try.To(yaml.Marshal(Yaml{Image: &image})).OrFatal(t))
				expected := `image: "` + expr + `"` + "\n"
				if actual != expected {
					t.Errorf("unexpected result: yaml.Marshal(%#v) --> %s", image, actual)
				}
			}
		}
	}

	t.Run("repository and tag", theory("repo:tag", apiplan.Image{
		Repository: "repo",
		Tag:        "tag",
	}))

	t.Run("registry, repository and tag", theory("registry.invalid/repo:tag", apiplan.Image{
		Repository: "registry.invalid/repo",
		Tag:        "tag",
	}))

	t.Run("registry /w port and repository and tag", theory("registry.invalid:5000/repo:tag", apiplan.Image{
		Repository: "registry.invalid:5000/repo",
		Tag:        "tag",
	}))
}

func TestResources(t *testing.T) {
	type Expr struct {
		Yaml string
		Json string
	}
	theory := func(expr Expr, resources apiplan.Resources) func(*testing.T) {
		return func(t *testing.T) {
			{
				type Json struct {
					Resources apiplan.Resources `json:"resources"`
				}

				unmarshalled := Json{}
				if err := json.Unmarshal([]byte(expr.Json), &unmarshalled); err != nil {
					t.Fatal(err)
				}
				if !cmp.MapEqWith(unmarshalled.Resources, resources, resource.Quantity.Equal) {
					t.Errorf("unexpected result: json.Unmarshal(%s) --> %#v", expr.Json, unmarshalled)
				}

				marshalled := try.To(json.Marshal(Json{Resources: resources})).OrFatal(t)
				reunmarshalled := Json{}
				if err := json.Unmarshal(marshalled, &reunmarshalled); err != nil {
					t.Fatal(err)
				}

				if !cmp.MapEqWith(reunmarshalled.Resources, resources, resource.Quantity.Equal) {
					t.Errorf("unexpected result: json.Marshal(%#v) --> %s", resources, marshalled)
				}
			}

			{
				type Yaml struct {
					Resources apiplan.Resources `yaml:"resources"`
				}

				unmarshalled := Yaml{}
				if err := yaml.Unmarshal([]byte(expr.Yaml), &unmarshalled); err != nil {
					t.Fatal(err)
				}
				if !cmp.MapEqWith(unmarshalled.Resources, resources, resource.Quantity.Equal) {
					t.Errorf("unexpected result: yaml.Unmarshal(%s) --> %#v", expr.Yaml, unmarshalled)
				}

				marshalled := try.To(yaml.Marshal(Yaml{Resources: resources})).OrFatal(t)
				reunmarshalled := Yaml{}
				if err := yaml.Unmarshal(marshalled, &reunmarshalled); err != nil {
					t.Fatal(err)
				}

				if !cmp.MapEqWith(reunmarshalled.Resources, resources, resource.Quantity.Equal) {
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
		apiplan.Resources{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
			"gpu":    resource.MustParse("1"),
		},
	))
}
