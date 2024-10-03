package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	plans "github.com/opst/knitfab-api-types/plans"
	apitag "github.com/opst/knitfab-api-types/tags"
	handlers "github.com/opst/knitfab/cmd/knitd/handlers"
	httptestutil "github.com/opst/knitfab/internal/testutils/http"
	bindplans "github.com/opst/knitfab/pkg/api-types-binding/plans"
	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	mockdb "github.com/opst/knitfab/pkg/db/mocks"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/logic"
	"github.com/opst/knitfab/pkg/utils/try"
	"k8s.io/apimachinery/pkg/api/resource"
)

func allValidated[T any, V interface{ Validate() (T, error) }](t *testing.T, vs []V) []T {
	return utils.Map(
		vs, func(v V) T { return try.To(v.Validate()).OrFatal(t) },
	)
}

func TestRegisterPlan(t *testing.T) {

	Status := func(statusCode int) func(error) bool {
		return func(err error) bool {
			switch actual := err.(type) {
			case *echo.HTTPError:
				return actual.Code == statusCode
			default:
				return false
			}
		}
	}

	type request struct {
		Options []httptestutil.RequestOption
		Body    string
	}
	type registerResult struct {
		plan *kdb.Plan
		err  error
	}
	type when struct {
		request
		registerResult
	}

	type resultErr struct {
		Match func(error) bool
	}
	type resultSuccess struct {
		StatusCode int
		Header     map[string][]string
		Body       plans.Detail
	}

	type then struct {
		Query   []*kdb.PlanSpec
		Err     *resultErr
		Success *resultSuccess
	}

	for name, testcase := range map[string]struct {
		when
		then
	}{
		"when registering a new plan and succeess, it should response metadata of created plan": {
			when{
				request{
					Options: []httptestutil.RequestOption{
						httptestutil.WithHeader("content-type", "application/json"),
					},
					Body: `{
	"image": "repo.invalid/image-1:0.1.0",
	"active": true,
	"inputs": [
		{"path": "/in/1", "tags": ["type:raw data", "format:rgb image"]}
	],
	"outputs": [
		{"path": "/out/2","tags": ["type:training data", "format:mask"]}
	],
	"log": {"tags": ["type:log", "format:jsonl"]},
	"on_node": {
		"may": ["vram=xlarge"],
		"prefer": ["vram=large", "accelerator=tpu"],
		"must": ["accelerator=gpu"]
	},
	"service_account": "example-service-account",
	"annotations": [
		"annot1=val1",
		"annot2=val2"
	]
}`,
				},
				registerResult{
					plan: &kdb.Plan{
						PlanBody: kdb.PlanBody{
							PlanId: "plan-id-1", Active: true, Hash: "plan-hash",
							Image: &kdb.ImageIdentifier{
								Image: "repo.invalid/image-1", Version: "0.1.0",
							},
							OnNode: []kdb.OnNode{
								{Mode: kdb.MayOnNode, Key: "vram", Value: "xlarge"},
								{Mode: kdb.PreferOnNode, Key: "vram", Value: "large"},
								{Mode: kdb.PreferOnNode, Key: "accelerator", Value: "tpu"},
								{Mode: kdb.MustOnNode, Key: "accelerator", Value: "gpu"},
							},
							Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("1"),
								"memory": resource.MustParse("1Gi"),
							},
							ServiceAccount: "example-service-account",
							Annotations: []kdb.Annotation{
								{Key: "annot1", Value: "val1"},
								{Key: "annot2", Value: "val2"},
							},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 1, Path: "/in/1",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								}),
							},
						},
						Outputs: []kdb.MountPoint{
							{
								Id: 2, Path: "/out/2",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								}),
							},
						},
						Log: &kdb.LogPoint{
							Id: 3,
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "type", Value: "log"},
								{Key: "format", Value: "jsonl"},
							}),
						},
					},
				},
			},
			then{
				Query: allValidated(t, []kdb.PlanParam{
					{
						Image: "repo.invalid/image-1", Version: "0.1.0", Active: true,
						Inputs: []kdb.MountPointParam{
							{
								Path: "/in/1",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								}),
							},
						},
						Outputs: []kdb.MountPointParam{
							{
								Path: "/out/2",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								}),
							},
						},
						Log: &kdb.LogParam{
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "type", Value: "log"},
								{Key: "format", Value: "jsonl"},
							}),
						},
						OnNode: []kdb.OnNode{
							{Mode: kdb.MayOnNode, Key: "vram", Value: "xlarge"},
							{Mode: kdb.PreferOnNode, Key: "vram", Value: "large"},
							{Mode: kdb.PreferOnNode, Key: "accelerator", Value: "tpu"},
							{Mode: kdb.MustOnNode, Key: "accelerator", Value: "gpu"},
						},
						Resources: map[string]resource.Quantity{
							// should be defaulted
							"cpu":    resource.MustParse("1"),
							"memory": resource.MustParse("1Gi"),
						},
						ServiceAccount: "example-service-account",
						Annotations: []kdb.Annotation{
							{Key: "annot1", Value: "val1"},
							{Key: "annot2", Value: "val2"},
						},
					},
				}),
				Success: &resultSuccess{
					StatusCode: http.StatusOK,
					Header: map[string][]string{
						"Content-Type": {"application/json"},
					},
					Body: plans.Detail{
						Summary: plans.Summary{
							PlanId: "plan-id-1",
							Image:  &plans.Image{Repository: "repo.invalid/image-1", Tag: "0.1.0"},
							Annotations: plans.Annotations{
								{Key: "annot1", Value: "val1"},
								{Key: "annot2", Value: "val2"},
							},
						},
						Active: true,
						Inputs: []plans.Mountpoint{
							{
								Path: "/in/1",
								Tags: []apitag.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								},
							},
						},
						Outputs: []plans.Mountpoint{
							{
								Path: "/out/2",
								Tags: []apitag.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								},
							},
						},
						Log: &plans.LogPoint{
							Tags: []apitag.Tag{
								{Key: "type", Value: "log"},
								{Key: "format", Value: "jsonl"},
							},
						},
						OnNode: &plans.OnNode{
							May: []plans.OnSpecLabel{
								{Key: "vram", Value: "xlarge"},
							},
							Prefer: []plans.OnSpecLabel{
								{Key: "vram", Value: "large"},
								{Key: "accelerator", Value: "tpu"},
							},
							Must: []plans.OnSpecLabel{
								{Key: "accelerator", Value: "gpu"},
							},
						},
						Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("1"),
							"memory": resource.MustParse("1Gi"),
						},
						ServiceAccount: "example-service-account",
					},
				},
			},
		},
		"when registering a new plan with resource request, it should use that": {
			when{
				request{
					Options: []httptestutil.RequestOption{
						httptestutil.WithHeader("content-type", "application/json"),
					},
					Body: `{
	"image": "repo.invalid/image-1:0.1.0",
	"active": true,
	"inputs": [
		{"path": "/in/1", "tags": ["type:raw data", "format:rgb image"]}
	],
	"outputs": [
		{"path": "/out/2","tags": ["type:training data", "format:mask"]}
	],
	"log": {"tags": ["type:log", "format:jsonl"]},
	"on_node": {
		"may": ["vram=xlarge"],
		"prefer": ["vram=large", "accelerator=tpu"],
		"must": ["accelerator=gpu"]
	},
	"resources": {
		"cpu": "500m",
		"memory": "128Mi"
	}
}`,
				},
				registerResult{
					plan: &kdb.Plan{
						PlanBody: kdb.PlanBody{
							PlanId: "plan-id-1", Active: true, Hash: "plan-hash",
							Image: &kdb.ImageIdentifier{
								Image: "repo.invalid/image-1", Version: "0.1.0",
							},
							OnNode: []kdb.OnNode{
								{Mode: kdb.MayOnNode, Key: "vram", Value: "xlarge"},
								{Mode: kdb.PreferOnNode, Key: "vram", Value: "large"},
								{Mode: kdb.PreferOnNode, Key: "accelerator", Value: "tpu"},
								{Mode: kdb.MustOnNode, Key: "accelerator", Value: "gpu"},
							},
							Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("500m"),
								"memory": resource.MustParse("128Mi"),
							},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 1, Path: "/in/1",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								}),
							},
						},
						Outputs: []kdb.MountPoint{
							{
								Id: 2, Path: "/out/2",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								}),
							},
						},
						Log: &kdb.LogPoint{
							Id: 3,
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "type", Value: "log"},
								{Key: "format", Value: "jsonl"},
							}),
						},
					},
				},
			},
			then{
				Query: allValidated[*kdb.PlanSpec](t, []kdb.PlanParam{
					{
						Image: "repo.invalid/image-1", Version: "0.1.0", Active: true,
						Inputs: []kdb.MountPointParam{
							{
								Path: "/in/1",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								}),
							},
						},
						Outputs: []kdb.MountPointParam{
							{
								Path: "/out/2",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								}),
							},
						},
						Log: &kdb.LogParam{
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "type", Value: "log"},
								{Key: "format", Value: "jsonl"},
							}),
						},
						OnNode: []kdb.OnNode{
							{Mode: kdb.MayOnNode, Key: "vram", Value: "xlarge"},
							{Mode: kdb.PreferOnNode, Key: "vram", Value: "large"},
							{Mode: kdb.PreferOnNode, Key: "accelerator", Value: "tpu"},
							{Mode: kdb.MustOnNode, Key: "accelerator", Value: "gpu"},
						},
						Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("500m"),
							"memory": resource.MustParse("128Mi"),
						},
					},
				}),
				Success: &resultSuccess{
					StatusCode: http.StatusOK,
					Header: map[string][]string{
						"Content-Type": {"application/json"},
					},
					Body: plans.Detail{
						Summary: plans.Summary{
							PlanId: "plan-id-1",
							Image:  &plans.Image{Repository: "repo.invalid/image-1", Tag: "0.1.0"},
						},
						Active: true,
						Inputs: []plans.Mountpoint{
							{
								Path: "/in/1",
								Tags: []apitag.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								},
							},
						},
						Outputs: []plans.Mountpoint{
							{
								Path: "/out/2",
								Tags: []apitag.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								},
							},
						},
						Log: &plans.LogPoint{
							Tags: []apitag.Tag{
								{Key: "type", Value: "log"},
								{Key: "format", Value: "jsonl"},
							},
						},
						Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("500m"),
							"memory": resource.MustParse("128Mi"),
						},
						OnNode: &plans.OnNode{
							May: []plans.OnSpecLabel{
								{Key: "vram", Value: "xlarge"},
							},
							Prefer: []plans.OnSpecLabel{
								{Key: "vram", Value: "large"},
								{Key: "accelerator", Value: "tpu"},
							},
							Must: []plans.OnSpecLabel{
								{Key: "accelerator", Value: "gpu"},
							},
						},
					},
				},
			},
		},
		"when registering a new inactive plan and succeess, it should response metadata of created plan": {
			when{
				request{
					Options: []httptestutil.RequestOption{
						httptestutil.WithHeader("content-type", "application/json"),
					},
					Body: `{
	"image": "repo.invalid/image-1:0.1.0",
	"active": false,
	"inputs": [
		{"path": "/in/1", "tags": ["type:raw data", "format:rgb image"]}
	],
	"outputs": [
		{"path": "/out/2", "tags": ["type:training data","format:mask"]}
	],
	"log": {"tags": ["type:log", "format:jsonl"]}
}`,
				},
				registerResult{
					plan: &kdb.Plan{
						PlanBody: kdb.PlanBody{
							PlanId: "plan-id-1", Active: false, Hash: "plan-hash",
							Image: &kdb.ImageIdentifier{
								Image: "repo.invalid/image-1", Version: "0.1.0",
							},
							Resources: map[string]resource.Quantity{
								// should be defaulted
								"cpu":    resource.MustParse("1"),
								"memory": resource.MustParse("1Gi"),
							},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 1, Path: "/in/1",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								}),
							},
						},
						Outputs: []kdb.MountPoint{
							{
								Id: 2, Path: "/out/2",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								}),
							},
						},
						Log: &kdb.LogPoint{
							Id: 3,
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "type", Value: "log"},
								{Key: "format", Value: "jsonl"},
							}),
						},
					},
				},
			},
			then{
				Query: allValidated[*kdb.PlanSpec](t, []kdb.PlanParam{
					{
						Image: "repo.invalid/image-1", Version: "0.1.0", Active: false,
						Inputs: []kdb.MountPointParam{
							{
								Path: "/in/1",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								}),
							},
						},
						Outputs: []kdb.MountPointParam{
							{
								Path: "/out/2",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								}),
							},
						},
						Log: &kdb.LogParam{
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "type", Value: "log"},
								{Key: "format", Value: "jsonl"},
							}),
						},
						Resources: map[string]resource.Quantity{
							// should be defaulted
							"cpu":    resource.MustParse("1"),
							"memory": resource.MustParse("1Gi"),
						},
					},
				}),
				Success: &resultSuccess{
					StatusCode: http.StatusOK,
					Header: map[string][]string{
						"Content-Type": {"application/json"},
					},
					Body: plans.Detail{
						Summary: plans.Summary{
							PlanId: "plan-id-1",
							Image:  &plans.Image{Repository: "repo.invalid/image-1", Tag: "0.1.0"},
						},
						Active: false,
						Inputs: []plans.Mountpoint{
							{
								Path: "/in/1",
								Tags: []apitag.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								},
							},
						},
						Outputs: []plans.Mountpoint{
							{
								Path: "/out/2",
								Tags: []apitag.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								},
							},
						},
						Log: &plans.LogPoint{
							Tags: []apitag.Tag{
								{Key: "type", Value: "log"},
								{Key: "format", Value: "jsonl"},
							},
						},
						Resources: map[string]resource.Quantity{
							// should be defaulted
							"cpu":    resource.MustParse("1"),
							"memory": resource.MustParse("1Gi"),
						},
					},
				},
			},
		},
		"when registering a new plan with default activeness and succeess, it should response metadata of created active plan": {
			when{
				request{
					Options: []httptestutil.RequestOption{
						httptestutil.WithHeader("content-type", "application/json"),
					},
					Body: `{
	"image": "repo.invalid/image-1:0.1.0",
	"inputs": [
		{"path": "/in/1", "tags": ["type:raw data", "format:rgb image"]}
	],
	"outputs": [
		{"path": "/out/2", "tags": ["type:training data", "format:mask"]}
	],
	"log": {"tags": ["type:log", "format:jsonl"]}
}`,
				},
				registerResult{
					plan: &kdb.Plan{
						PlanBody: kdb.PlanBody{
							PlanId: "plan-id-1", Active: true, Hash: "plan-hash",
							Image: &kdb.ImageIdentifier{
								Image: "repo.invalid/image-1", Version: "0.1.0",
							},
							Resources: map[string]resource.Quantity{
								// should be defaulted
								"cpu":    resource.MustParse("1"),
								"memory": resource.MustParse("1Gi"),
							},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 1, Path: "/in/1",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								}),
							},
						},
						Outputs: []kdb.MountPoint{
							{
								Id: 2, Path: "/out/2",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								}),
							},
						},
						Log: &kdb.LogPoint{
							Id: 3,
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "type", Value: "log"},
								{Key: "format", Value: "jsonl"},
							}),
						},
					},
				},
			},
			then{
				Query: allValidated[*kdb.PlanSpec](t, []kdb.PlanParam{
					{
						Image: "repo.invalid/image-1", Version: "0.1.0", Active: true,
						Inputs: []kdb.MountPointParam{
							{
								Path: "/in/1",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								}),
							},
						},
						Outputs: []kdb.MountPointParam{
							{
								Path: "/out/2",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								}),
							},
						},
						Log: &kdb.LogParam{
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "type", Value: "log"},
								{Key: "format", Value: "jsonl"},
							}),
						},
						Resources: map[string]resource.Quantity{
							// should be defaulted
							"cpu":    resource.MustParse("1"),
							"memory": resource.MustParse("1Gi"),
						},
					},
				}),
				Success: &resultSuccess{
					StatusCode: http.StatusOK,
					Header: map[string][]string{
						"Content-Type": {"application/json"},
					},
					Body: plans.Detail{
						Summary: plans.Summary{
							PlanId: "plan-id-1",
							Image:  &plans.Image{Repository: "repo.invalid/image-1", Tag: "0.1.0"},
						},
						Active: true,
						Inputs: []plans.Mountpoint{
							{
								Path: "/in/1",
								Tags: []apitag.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								},
							},
						},
						Outputs: []plans.Mountpoint{
							{
								Path: "/out/2",
								Tags: []apitag.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								},
							},
						},
						Log: &plans.LogPoint{
							Tags: []apitag.Tag{
								{Key: "type", Value: "log"},
								{Key: "format", Value: "jsonl"},
							},
						},
						Resources: map[string]resource.Quantity{
							// should be defaulted
							"cpu":    resource.MustParse("1"),
							"memory": resource.MustParse("1Gi"),
						},
					},
				},
			},
		},
		"when registering a new plan without log and succeess, it should response metadata of created plan": {
			when{
				request{
					Options: []httptestutil.RequestOption{
						httptestutil.WithHeader("content-type", "application/json"),
					},
					Body: `{
	"image": "repo.invalid/image-1:0.1.0",
	"active": true,
	"inputs": [{"path": "/in/1", "tags": ["type:raw data", "format:rgb image"]}],
	"outputs": [{"path": "/out/2", "tags": ["type: training data", "format:mask"]}]
}`,
				},
				registerResult{
					plan: &kdb.Plan{
						PlanBody: kdb.PlanBody{
							PlanId: "plan-id-1", Active: true, Hash: "plan-hash",
							Image: &kdb.ImageIdentifier{
								Image: "repo.invalid/image-1", Version: "0.1.0",
							},
							Resources: map[string]resource.Quantity{
								// should be defaulted
								"cpu":    resource.MustParse("1"),
								"memory": resource.MustParse("1Gi"),
							},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 1, Path: "/in/1",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								}),
							},
						},
						Outputs: []kdb.MountPoint{
							{
								Id: 2, Path: "/out/2",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								}),
							},
						},
					},
				},
			},
			then{
				Query: allValidated[*kdb.PlanSpec](t, []kdb.PlanParam{
					{
						Image: "repo.invalid/image-1", Version: "0.1.0", Active: true,
						Inputs: []kdb.MountPointParam{
							{
								Path: "/in/1",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								}),
							},
						},
						Outputs: []kdb.MountPointParam{
							{
								Path: "/out/2",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								}),
							},
						},
						Resources: map[string]resource.Quantity{
							// should be defaulted
							"cpu":    resource.MustParse("1"),
							"memory": resource.MustParse("1Gi"),
						},
					},
				}),
				Success: &resultSuccess{
					StatusCode: http.StatusOK,
					Header: map[string][]string{
						"Content-Type": {"application/json"},
					},
					Body: plans.Detail{
						Summary: plans.Summary{
							PlanId: "plan-id-1",
							Image:  &plans.Image{Repository: "repo.invalid/image-1", Tag: "0.1.0"},
						},
						Active: true,
						Inputs: []plans.Mountpoint{
							{
								Path: "/in/1",
								Tags: []apitag.Tag{
									{Key: "type", Value: "raw data"},
									{Key: "format", Value: "rgb image"},
								},
							},
						},
						Outputs: []plans.Mountpoint{
							{
								Path: "/out/2",
								Tags: []apitag.Tag{
									{Key: "type", Value: "training data"},
									{Key: "format", Value: "mask"},
								},
							},
						},
						Resources: map[string]resource.Quantity{
							// should be defaulted
							"cpu":    resource.MustParse("1"),
							"memory": resource.MustParse("1Gi"),
						},
					},
				},
			},
		},
		"when receiving with non-json content-type (implicit), it should response with 400": {
			when{
				request{
					// no Content-Type header.
					Body: `{
"image": "repo.invalid/image-1:0.1.0",
"inputs": [{"path": "/in/1", "tags": ["type:raw data", "format:rgb image"]}],
"outputs": [{"path": "/out/2", "tags": ["type:training data", "format:mask"]}],
"log": {"tags": ["type:log", "format:jsonl"]}
}`,
				},
				registerResult{
					plan: nil, err: errors.New("fake error"),
				},
			},
			then{
				Query: []*kdb.PlanSpec{}, // empty.
				Err: &resultErr{
					Match: Status(http.StatusBadRequest),
				},
			},
		},
		"when receiving non-json content-type (explicit), it should response with 400": {
			when{
				request{
					Options: []httptestutil.RequestOption{httptestutil.WithHeader("content-type", "text/plain")},
					Body: `{
"image": "repo.invalid/image-1:0.1.0",
"inputs": [{"path": "/in/1", "tags": ["type:raw data", "format:rgb image"]}],
"outputs": [{"path": "/out/2", "tags": ["type:training data", "format:mask"]}],
"log": {"tags": ["type:log", "format:jsonl"]}
}`,
				},
				registerResult{
					plan: nil, err: errors.New("fake error"),
				},
			},
			then{
				Query: []*kdb.PlanSpec{}, // empty.
				Err: &resultErr{
					Match: Status(http.StatusBadRequest),
				},
			},
		},
	} {
		t.Run(name,
			func(t *testing.T) {
				when, then := testcase.when, testcase.then

				mockPlan := mockdb.NewPlanInteraface()
				mockPlan.Impl.Register = func(ctx context.Context, ps *kdb.PlanSpec) (string, error) {
					return when.registerResult.plan.PlanId, when.registerResult.err
				}

				mockPlan.Impl.Get = func(ctx context.Context, s []string) (map[string]*kdb.Plan, error) {
					if when.registerResult.err != nil {
						t.Fatal("unexpected call!")
					}

					plan := when.registerResult.plan
					return map[string]*kdb.Plan{plan.PlanId: plan}, nil
				}

				testee := handlers.PlanRegisterHandler(mockPlan)

				e := echo.New()
				c, respRec := httptestutil.Post(
					e, "/api/plan", bytes.NewBuffer([]byte(when.request.Body)),
					when.request.Options...,
				)
				err := testee(c)

				if !cmp.SliceEqWith(then.Query, mockPlan.Calls.Register, (*kdb.PlanSpec).Equal) {
					t.Errorf(
						"unmatch:\n- actual   : %+v\n- expected : %+v",
						utils.DerefOf(mockPlan.Calls.Register), then.Query,
					)
				}

				if then.Err != nil {
					if !then.Err.Match(err) {
						t.Errorf("unexpected type error: %+v", err)
					}
					return
				}

				if err != nil {
					t.Fatalf("unexpected error: %+v", err)
				}

				if respRec.Result().StatusCode != then.Success.StatusCode {
					t.Errorf(
						"unexpected status code: (actual, expected) = (%d, %d)",
						respRec.Result().StatusCode, then.Success.StatusCode,
					)
				}

				{
					actual := respRec.Header()
					if !cmp.MapLeqWith(actual, then.Success.Header, func(a, b []string) bool {
						return cmp.SliceContentEq(a, b)
					}) {
						t.Errorf(
							"not enough header:\n- actual   : %+v\n- expected : %+v",
							actual, then.Success.Header,
						)
					}
				}

				{
					actual := plans.Detail{}
					if err := json.NewDecoder(respRec.Body).Decode(&actual); err != nil {
						t.Errorf("parse error: %+v", err)
					} else if !then.Success.Body.Equal(actual) {
						t.Errorf(
							"response body not match:\n- actual   : %+v\n- expected : %+v",
							actual, then.Success.Body,
						)
					}
				}
			})
	}

	for errorName, testcase := range map[string]struct {
		when error
		then int
	}{
		"ErrInvalidPlan":     {when: kdb.ErrInvalidPlan, then: http.StatusBadRequest},
		"ErrConflictingPlan": {when: kdb.ErrConflictingPlan, then: http.StatusConflict},

		"unexpected error": {when: errors.New("unexpected error"), then: http.StatusInternalServerError},
	} {
		testname := fmt.Sprintf(
			"when PlanInterface.Register returns %s, it should response %d",
			errorName, testcase.then,
		)
		t.Run(testname, func(t *testing.T) {
			when, then := testcase.when, testcase.then

			mockPlan := mockdb.NewPlanInteraface()
			mockPlan.Impl.Register = func(ctx context.Context, ps *kdb.PlanSpec) (string, error) {
				return "", when
			}
			testee := handlers.PlanRegisterHandler(mockPlan)

			e := echo.New()
			c, _ := httptestutil.Post(
				e, "/api/plan", bytes.NewBuffer([]byte(`{
	"image": "repo.invalid/image-1", "version": "0.1.0", "active": true,
	"inputs": [{"path": "/in/1", "tags": ["type:raw data"]}],
	"outputs": [{"path": "/out/2", "tags": ["type:training data"]}],
	"log": {"tags": ["type:log"]}
}`)),
				httptestutil.WithHeader("content-type", "application/json"),
			)
			err := testee(c)
			if !Status(then)(err) {
				t.Errorf("unexpected type error: %+v", err)
			}
		})
	}

	for condition, testcase := range map[string]struct {
		when string
		then int
	}{
		"has no image name": {
			when: `{
	"image": ":0.1.0", "active": true,
	"inputs": [{"path": "/in/1", "tags": ["type:raw data"]}],
	"outputs": [{"path": "/out/2", "tags": ["type:training data"]}],
	"log": { "tags": ["type:log"] }
}`,
			then: http.StatusBadRequest,
		},
		"has no image version": {
			when: `{
	"image": "repo.invalid/image-1", "active": true,
	"inputs1": [{"path": "/in/1", "tags": ["type:raw data"]}],
	"outputs": [{"path": "/out/2", "tags": ["type:training data"]}],
	"log": { "tags": ["type:log"] }
}`,
			then: http.StatusBadRequest,
		},
		"has no input": {
			when: `{
	"image": "repo.invalid/image-1:0.1.0", "active": true,
	"outputs": [{"path": "/out/2", "tags": ["type:training data"]}],
	"log": { "tags": ["type:log"] }
}`,
			then: http.StatusBadRequest,
		},
		"has an input without tags": {
			when: `{
	"image": "repo.invalid/image-1:0.1.0", "active": true,
	"inputs": [
		{"path": "/in/1", "tags": ["type:raw data"]},
		{"path": "/in/2", "tags": []}
	],
	"outputs": [{"path": "/out/2", "tags": ["type:training data"]}],
	"log": { "tags": ["type:log"] }
}`,
			then: http.StatusBadRequest,
		},
		"has an input with knit#transient": {
			when: `{
	"image": "repo.invalid/image-1:0.1.0", "active": true,
	"inputs": [{"path": "/in/1", "tags": ["type:raw data", "knit#transient:anything"]}],
	"outputs": [{"path": "/out/2", "tags": ["type:training data"]}],
	"log": { "tags": ["type:log"] }
}`,
			then: http.StatusBadRequest,
		},
		"has an output with system tag": {
			when: `{
	"image": "repo.invalid/image-1:0.1.0", "active": true,
	"inputs": [{"path": "/in/1", "tags": ["type:raw data"]}],
	"outputs": [{"path": "/out/2", "tags": ["type:training data", "knit#id:something"]}],
	"log": { "tags": ["type:log"] }
}`,
			then: http.StatusBadRequest,
		},
		"has a log with system tag": {
			when: `{
	"image": "repo.invalid/image-1", "version": "0.1.0", "active": true,
	"inputs": [{"path": "/in/1", "tags": ["type:raw data"]}],
	"outputs": [{"path": "/out/2", "tags": ["type:training data"]}],
	"log": { "tags": ["type:log", "knit#id:something"] }
}`,
			then: http.StatusBadRequest,
		},
		"has overlapping mountpoints": {
			when: `{
	"image": "repo.invalid/image-1:0.1.0", "active": true,
	"inputs": [{"path": "/path/1", "tags": ["type:raw data"]}],
	"outputs": [{"path": "/path/1/out", "tags": ["type:training data"]}],
	"log": { "tags": ["type:log", "knit#id:something"] }
}`,
			then: http.StatusBadRequest,
		},
		"has mountpoint with non-absolute path": {
			when: `{
	"image": "repo.invalid/image-1:0.1.0", "active": true,
	"inputs": [{"path": "/in/../1", "tags": ["type:raw data"]}],
	"outputs": [{"path": "/out", "tags": ["type:training data"]}],
	"log": { "tags": ["type:log", "knit#id:something"] }
}`,
			then: http.StatusBadRequest,
		},
		"has mountpoint with relative path": {
			when: `{
	"image": "repo.invalid/image-1:0.1.0", "active": true,
	"inputs": [{"path": "/in/1", "tags": ["type:raw data"]}],
	"outputs": [{"path": "./out", "tags": ["type:training data"]}],
	"log": { "tags": ["type:log", "knit#id: something"] }
}`,
			then: http.StatusBadRequest,
		},
		"is not json": {
			when: `// this is not json`,
			then: http.StatusBadRequest,
		},
	} {
		testname := fmt.Sprintf("when it receives request that %s, it responses %d", condition, testcase.then)
		t.Run(testname, func(t *testing.T) {
			when, then := testcase.when, testcase.then

			mockPlan := mockdb.NewPlanInteraface()
			mockPlan.Impl.Register = func(ctx context.Context, ps *kdb.PlanSpec) (string, error) {
				return "", errors.New("should not be reached")
			}
			testee := handlers.PlanRegisterHandler(mockPlan)

			e := echo.New()
			c, _ := httptestutil.Post(
				e, "/api/plan", bytes.NewBuffer([]byte(when)),
				httptestutil.WithHeader("content-type", "application/json"),
			)
			err := testee(c)

			if 0 < mockPlan.Calls.Register.Times() {
				t.Errorf("PlanInterface.Register: unexpectedly called")
			}

			if !Status(then)(err) {
				t.Errorf("unexpected status code: %s (expected: %d)", err, then)
			}
		})
	}
}

func TestFind(t *testing.T) {

	type when struct {
		request     string
		queryResult []*kdb.Plan
		err         error
	}

	type then struct {
		callResult  mockdb.PlanFindArgs
		contentType string
		isErr       bool
		statusCode  int
		body        []plans.Detail
	}

	for name, testcase := range map[string]struct {
		when
		then
	}{
		"When PlanInterface.Find returns query result, it should convert it to JSON format": {
			when: when{
				request: "/api/plans?",
				queryResult: []*kdb.Plan{
					{
						PlanBody: kdb.PlanBody{
							PlanId: "plan-1", Active: true, Hash: "hash-1",
							Image: &kdb.ImageIdentifier{Image: "image-1", Version: "ver-1"},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 1, Path: "path-1",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "key-1", Value: "val-1"}, {Key: "key-2", Value: "val-2"}},
								),
							},
						},
						Outputs: []kdb.MountPoint{
							{
								Id: 2, Path: "path-2",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "key-1", Value: "val-3"}, {Key: "key-2", Value: "val-4"},
								}),
							},
						},
					},
					{
						PlanBody: kdb.PlanBody{
							PlanId: "plan-2", Active: false, Hash: "hash-2",
							Image: &kdb.ImageIdentifier{Image: "image-2", Version: "ver-2"},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 3, Path: "path-3",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "key-3", Value: "val-1"}, {Key: "key-4", Value: "val-2"}},
								),
							},
						},
						Log: &kdb.LogPoint{
							Id: 4,
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "key-3", Value: "val-3"}, {Key: "key-4", Value: "val-4"},
							}),
						},
					},
				},
				err: nil,
			},
			then: then{
				callResult: mockdb.PlanFindArgs{
					Active:   logic.Indeterminate,
					ImageVer: kdb.ImageIdentifier{Image: "", Version: ""},
					InTag:    []kdb.Tag{},
					OutTag:   []kdb.Tag{},
				},
				contentType: "application/json", isErr: false, statusCode: http.StatusOK,
				body: []plans.Detail{
					{
						Summary: plans.Summary{
							PlanId: "plan-1",
							Image:  &plans.Image{Repository: "image-1", Tag: "ver-1"},
						},
						Inputs: []plans.Mountpoint{
							{
								Path: "path-1",
								Tags: []apitag.Tag{{Key: "key-1", Value: "val-1"}, {Key: "key-2", Value: "val-2"}},
							},
						},
						Outputs: []plans.Mountpoint{
							{
								Path: "path-2",
								Tags: []apitag.Tag{{Key: "key-1", Value: "val-3"}, {Key: "key-2", Value: "val-4"}},
							},
						},
						Log: nil, Active: true,
					},
					{
						Summary: plans.Summary{
							PlanId: "plan-2",
							Image:  &plans.Image{Repository: "image-2", Tag: "ver-2"},
						},
						Inputs: []plans.Mountpoint{
							{
								Path: "path-3",
								Tags: []apitag.Tag{{Key: "key-3", Value: "val-1"}, {Key: "key-4", Value: "val-2"}},
							},
						},
						Log: &plans.LogPoint{
							Tags: []apitag.Tag{{Key: "key-3", Value: "val-3"}, {Key: "key-4", Value: "val-4"}},
						},
						Active: false,
					},
				},
			},
		},
		"When PlanInterface.Find returns empty result, it returns an empty response without error": {
			when: when{request: "/api/plans?", queryResult: []*kdb.Plan{}, err: nil},
			then: then{
				callResult: mockdb.PlanFindArgs{
					Active:   logic.Indeterminate,
					ImageVer: kdb.ImageIdentifier{Image: "", Version: ""},
					InTag:    []kdb.Tag{},
					OutTag:   []kdb.Tag{},
				},
				contentType: "application/json", isErr: false, statusCode: http.StatusOK, body: []plans.Detail{}},
		},
		"when PlanInterface.Find cause error, it returns internal server error.": {
			when: when{request: "/api/plans?", queryResult: []*kdb.Plan{}, err: errors.New("dummy error")},
			then: then{contentType: "application/json", isErr: true, statusCode: http.StatusInternalServerError, body: []plans.Detail{}},
		},
		"When it receives query parameter, it converts it properly and passed it to PlanInterface.Find.": {
			when: when{
				request:     "/api/plans?active=true&image=image-1:ver-1&in_tag=key-1:val-1&in_tag=key-2:val-2&out_tag=key-3:val-3&out_tag=key-4:val-4",
				queryResult: []*kdb.Plan{}, err: nil,
			},
			then: then{
				callResult: mockdb.PlanFindArgs{
					Active:   logic.True,
					ImageVer: kdb.ImageIdentifier{Image: "image-1", Version: "ver-1"},
					InTag:    []kdb.Tag{{Key: "key-1", Value: "val-1"}, {Key: "key-2", Value: "val-2"}},
					OutTag:   []kdb.Tag{{Key: "key-3", Value: "val-3"}, {Key: "key-4", Value: "val-4"}},
				},
				contentType: "application/json", isErr: false, statusCode: http.StatusOK, body: []plans.Detail{},
			},
		},
		"When active is specified incorrectly in the query, it responses Bad Request.": {
			when: when{
				request:     "/api/plans?active=hoge&image=image-1:ver-1&in_tag=key-1:val-1&in_tag=key-2:val-2&out_tag=key-3:val-3&in_tag=key-4:val-4",
				queryResult: []*kdb.Plan{}, err: nil,
			},
			then: then{
				callResult:  mockdb.PlanFindArgs{},
				contentType: "application/json", isErr: true, statusCode: http.StatusBadRequest, body: []plans.Detail{},
			},
		},
		"When active is not specified in the query, it calls PlanInterface.Find with active: Indeterminate.": {
			when: when{
				request:     "/api/plans?image=image-1:ver-1&in_tag=key-1:val-1&in_tag=key-2:val-2&out_tag=key-3:val-3&out_tag=key-4:val-4",
				queryResult: []*kdb.Plan{}, err: nil,
			},
			then: then{
				callResult: mockdb.PlanFindArgs{
					Active:   logic.Indeterminate,
					ImageVer: kdb.ImageIdentifier{Image: "image-1", Version: "ver-1"},
					InTag:    []kdb.Tag{{Key: "key-1", Value: "val-1"}, {Key: "key-2", Value: "val-2"}},
					OutTag:   []kdb.Tag{{Key: "key-3", Value: "val-3"}, {Key: "key-4", Value: "val-4"}},
				},
				contentType: "application/json", isErr: false, statusCode: http.StatusOK, body: []plans.Detail{},
			},
		},
		"When version is not specified in the query,  it calls PlanInterface.Find with image only.": {
			when: when{
				request:     "/api/plans?image=image-1&in_tag=key-1:val-1&in_tag=key-2:val-2&out_tag=key-3:val-3&out_tag=key-4:val-4",
				queryResult: []*kdb.Plan{}, err: nil,
			},
			then: then{
				callResult: mockdb.PlanFindArgs{
					Active:   logic.Indeterminate,
					ImageVer: kdb.ImageIdentifier{Image: "image-1", Version: ""},
					InTag:    []kdb.Tag{{Key: "key-1", Value: "val-1"}, {Key: "key-2", Value: "val-2"}},
					OutTag:   []kdb.Tag{{Key: "key-3", Value: "val-3"}, {Key: "key-4", Value: "val-4"}},
				},
				contentType: "application/json", isErr: false, statusCode: http.StatusOK, body: []plans.Detail{},
			},
		},
		"When both image and version are not specified in the query, it calls PlanInterface.Find both with an empty character.": {
			when: when{
				request:     "/api/plans?in_tag=key-1:val-1&in_tag=key-2:val-2&out_tag=key-3:val-3&out_tag=key-4:val-4",
				queryResult: []*kdb.Plan{}, err: nil,
			},
			then: then{
				callResult: mockdb.PlanFindArgs{
					Active:   logic.Indeterminate,
					ImageVer: kdb.ImageIdentifier{Image: "", Version: ""},
					InTag:    []kdb.Tag{{Key: "key-1", Value: "val-1"}, {Key: "key-2", Value: "val-2"}},
					OutTag:   []kdb.Tag{{Key: "key-3", Value: "val-3"}, {Key: "key-4", Value: "val-4"}},
				},
				contentType: "application/json", isErr: false, statusCode: http.StatusOK, body: []plans.Detail{},
			},
		},
		"When the query has version but does not have image, it responses Bad Request.": {
			when: when{
				request:     "/api/plans?image=:ver-1&in_tag=key-1:val-1&in_tag=key-2:val-2&out_tag=key-3:val-3&out_tag=key-4:val-4",
				queryResult: []*kdb.Plan{}, err: nil,
			},
			then: then{
				callResult:  mockdb.PlanFindArgs{},
				contentType: "application/json", isErr: true, statusCode: http.StatusBadRequest, body: []plans.Detail{},
			},
		},
		"When in_tag are not specified in the query, it calls PlanInterface.Find with empty InTag.": {
			when: when{
				request:     "/api/plans?out_tag=key-3:val-3&out_tag=key-4:val-4",
				queryResult: []*kdb.Plan{}, err: nil,
			},
			then: then{
				callResult: mockdb.PlanFindArgs{
					Active:   logic.Indeterminate,
					ImageVer: kdb.ImageIdentifier{Image: "", Version: ""},
					InTag:    []kdb.Tag{},
					OutTag:   []kdb.Tag{{Key: "key-3", Value: "val-3"}, {Key: "key-4", Value: "val-4"}},
				},
				contentType: "application/json", isErr: false, statusCode: http.StatusOK, body: []plans.Detail{},
			},
		},
		"When out_tag are not specified in the query, it calls PlanInterface.Find with empty OutTag.": {
			when: when{
				request:     "/api/plans?",
				queryResult: []*kdb.Plan{}, err: nil,
			},
			then: then{
				callResult: mockdb.PlanFindArgs{
					Active:   logic.Indeterminate,
					ImageVer: kdb.ImageIdentifier{Image: "", Version: ""},
					InTag:    []kdb.Tag{},
					OutTag:   []kdb.Tag{},
				},
				contentType: "application/json", isErr: false, statusCode: http.StatusOK, body: []plans.Detail{},
			},
		},
		"When tag without delimiter is specified, it responses Bad Request.": {
			when: when{
				request:     "/api/plans?out_tag=key-3&out_tag=val-4",
				queryResult: []*kdb.Plan{}, err: nil,
			},
			then: then{
				callResult:  mockdb.PlanFindArgs{},
				contentType: "application/json", isErr: true, statusCode: http.StatusBadRequest, body: []plans.Detail{},
			},
		},
		"when empty tag is specified in the query, it calls PlanInterface.Find without error": {
			when: when{
				request:     "/api/plans?out_tag=:val-3&out_tag=key-4:",
				queryResult: []*kdb.Plan{}, err: nil,
			},
			then: then{
				callResult: mockdb.PlanFindArgs{
					Active:   logic.Indeterminate,
					ImageVer: kdb.ImageIdentifier{Image: "", Version: ""},
					InTag:    []kdb.Tag{},
					OutTag:   []kdb.Tag{{Key: "", Value: "val-3"}, {Key: "key-4", Value: ""}},
				},
				contentType: "application/json", isErr: false, statusCode: http.StatusOK, body: []plans.Detail{},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {

			mockPlan := mockdb.NewPlanInteraface()
			mockPlan.Impl.Find = func(ctx context.Context, active logic.Ternary, imageVer kdb.ImageIdentifier, inTag []kdb.Tag, outTag []kdb.Tag) ([]string, error) {
				planIds := utils.Map(testcase.when.queryResult, func(p *kdb.Plan) string { return p.PlanId })
				return planIds, testcase.when.err
			}

			mockPlan.Impl.Get = func(ctx context.Context, s []string) (map[string]*kdb.Plan, error) {
				expectedPlanIds := utils.Map(testcase.when.queryResult, func(p *kdb.Plan) string { return p.PlanId })
				if !cmp.SliceContentEq(s, expectedPlanIds) {
					t.Errorf("unmatch: planIds: (actual, expected) = (%v, %v)", s, expectedPlanIds)
				}
				resp := map[string]*kdb.Plan{}
				for _, p := range testcase.when.queryResult {
					resp[p.PlanId] = p
				}
				return resp, testcase.when.err
			}

			e := echo.New()
			c, respRec := httptestutil.Get(
				e, testcase.when.request,
			)

			testee := handlers.FindPlanHandler(mockPlan)

			err := testee(c)

			actualContentType := strings.Split(respRec.Result().Header.Get("Content-Type"), ";")[0]
			if actualContentType != testcase.then.contentType {
				t.Errorf("Content-Type: %s != %s", actualContentType, testcase.then.contentType)
			}

			if testcase.then.isErr {
				if err == nil {
					t.Fatalf("response is not illegal. error is nothing")
				}
				var echoErr *echo.HTTPError
				if !errors.As(err, &echoErr) {
					t.Fatalf("error is not echo.HTTPError. acutal = %#v", err)
				}
				if echoErr.Code != testcase.then.statusCode {
					t.Fatalf("unmatch error code:%d, expeced:%d", echoErr.Code, testcase.then.statusCode)
				}
			} else {
				if err != nil {
					t.Fatalf("response is not illegal. error = %v", err)
				}

				if mockPlan.Calls.Find.Times() != 1 {
					t.Error("Find did not call correctly")
				}

				if !(testcase.then.callResult.Equal(&mockPlan.Calls.Find[0])) {
					t.Errorf("Find did not call with correct args. (actual, expected) = \n(%#v, \n%#v)",
						mockPlan.Calls.Find[0], testcase.then.callResult)
				}

				actualResponse := []plans.Detail{}

				err = json.Unmarshal(respRec.Body.Bytes(), &actualResponse)
				if err != nil {
					t.Errorf("response is not illegal. error = %v", err)
				}

				actualStatusCode := respRec.Result().StatusCode
				if actualStatusCode != testcase.then.statusCode {
					t.Errorf("status code %d != %d", actualStatusCode, testcase.then.statusCode)
				}
				if !cmp.SliceEqWith(actualResponse, testcase.then.body, plans.Detail.Equal) {
					t.Errorf(
						"data does not match. (actual, expected) = \n(%v, \n%v)",
						actualResponse, testcase.then,
					)
				}
			}
		})
	}

}

func TestGetPlanHandler(t *testing.T) {

	type when struct {
		planId      string
		queryResult []*kdb.Plan
		err         error
	}

	type then struct {
		contentType string
		isErr       bool
		statusCode  int
		body        plans.Detail
	}

	for name, testcase := range map[string]struct {
		when
		then
	}{
		"When PlanInterface.Get returns query result, it should convert it to JSON format": {
			when: when{
				planId: "plan-1",
				queryResult: []*kdb.Plan{
					{
						PlanBody: kdb.PlanBody{
							PlanId: "plan-1", Active: true, Hash: "hash-1",
							Image:  &kdb.ImageIdentifier{Image: "image-1", Version: "ver-1"},
							Pseudo: &kdb.PseudoPlanDetail{},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 1, Path: "path-1",
								Tags: kdb.NewTagSet([]kdb.Tag{{Key: "key-1", Value: "val-1"}, {Key: "key-2", Value: "val-2"}}),
							},
						},
						Outputs: []kdb.MountPoint{
							{
								Id: 2, Path: "path-2",
								Tags: kdb.NewTagSet([]kdb.Tag{{Key: "key-1", Value: "val-3"}, {Key: "key-2", Value: "val-4"}}),
							},
						},
					},
				},
				err: nil,
			},
			then: then{
				contentType: "application/json",
				isErr:       false,
				statusCode:  http.StatusOK,
				body: plans.Detail{
					Summary: plans.Summary{
						PlanId: "plan-1",
						Image:  &plans.Image{Repository: "image-1", Tag: "ver-1"},
					},
					Inputs: []plans.Mountpoint{
						{
							Path: "path-1",
							Tags: []apitag.Tag{{Key: "key-1", Value: "val-1"}, {Key: "key-2", Value: "val-2"}},
						},
					},
					Outputs: []plans.Mountpoint{
						{
							Path: "path-2",
							Tags: []apitag.Tag{{Key: "key-1", Value: "val-3"}, {Key: "key-2", Value: "val-4"}},
						},
					},
					Log: nil, Active: true,
				},
			},
		},
		"When PlanInterface.Get does not have queried plan, it returns 404 not found error.": {
			when: when{
				planId:      "do-not-care",
				queryResult: nil,
				err:         nil,
			},
			then: then{
				contentType: "application/json",
				isErr:       true,
				statusCode:  http.StatusNotFound,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {

			mockPlan := mockdb.NewPlanInteraface()
			mockPlan.Impl.Get = func(ctx context.Context, planId []string) (map[string]*kdb.Plan, error) {
				return utils.ToMap(testcase.when.queryResult, func(p *kdb.Plan) string { return p.PlanId }), testcase.when.err
			}

			e := echo.New()
			c, respRec := httptestutil.Get(e, "/api/plans/"+testcase.when.planId)
			c.SetParamNames("planId")
			c.SetParamValues(testcase.when.planId)

			testee := handlers.GetPlanHandler(mockPlan)

			err := testee(c)

			actualContentType := strings.Split(respRec.Result().Header.Get("Content-Type"), ";")[0]
			if actualContentType != testcase.then.contentType {
				t.Errorf("Content-Type: %s != %s", actualContentType, testcase.then.contentType)
			}

			if testcase.then.isErr {
				if err == nil {
					t.Fatalf("response is not illegal. error is nothing")
				}
				var echoErr *echo.HTTPError
				if !errors.As(err, &echoErr) {
					t.Fatalf("error is not echo.HTTPError. acutal = %#v", err)
				}
				if echoErr.Code != testcase.then.statusCode {
					t.Fatalf("unmatch error code:%d, expeced:%d", echoErr.Code, testcase.then.statusCode)
				}
			} else {
				if err != nil {
					t.Fatalf("response is not illegal. error = %v", err)
				}

				actualResponse := plans.Detail{}

				err = json.Unmarshal(respRec.Body.Bytes(), &actualResponse)
				if err != nil {
					t.Errorf("response is not illegal. error = %v", err)
				}

				actualStatusCode := respRec.Result().StatusCode
				if actualStatusCode != testcase.then.statusCode {
					t.Errorf("status code %d != %d", actualStatusCode, testcase.then.statusCode)
				}
				if !actualResponse.Equal(testcase.then.body) {
					t.Errorf(
						"data does not match. (actual, expected) = \n(%v, \n%v)",
						actualResponse, testcase.then.body,
					)
				}
			}
		})
	}
	t.Run("it passes planId in path parameter to PlanInterface", func(t *testing.T) {

		mockPlan := mockdb.NewPlanInteraface()
		mockPlan.Impl.Get = func(ctx context.Context, planId []string) (map[string]*kdb.Plan, error) {
			return nil, nil
		}

		planId := "test-plan-id"

		e := echo.New()
		c, _ := httptestutil.Get(e, "/api/plans/"+planId)
		c.SetPath("/plans/:planId")
		c.SetParamNames("planId")
		c.SetParamValues(planId)

		testee := handlers.GetPlanHandler(mockPlan)

		testee(c)

		if !cmp.SliceEqWith(mockPlan.Calls.Get, [][]string{{planId}}, cmp.SliceContentEq[string]) {
			t.Errorf("unmatch: query parameter: (actual, expected) = \n(%#v, \n%#v)",
				mockPlan.Calls.Get, []string{planId})
		}
	})
}

func TestActivatePlan(t *testing.T) {
	type when struct {
		planId      string
		request     string
		isActive    bool
		queryResult []*kdb.Plan
		err         error
	}

	type then struct {
		contentType string
		shouldError bool
		statusCode  int
		body        plans.Detail
	}

	for name, testcase := range map[string]struct {
		when
		then
	}{
		"[Activate]When PlanInterface.Activate returns query result, it should convert it to JSON format": {
			when: when{
				planId:   "plan-1",
				request:  "/api/plan/plan-1/active",
				isActive: true,
				queryResult: []*kdb.Plan{
					{
						PlanBody: kdb.PlanBody{
							PlanId: "plan-1", Active: true, Hash: "hash-1",
							Image:  &kdb.ImageIdentifier{Image: "image-1", Version: "ver-1"},
							Pseudo: &kdb.PseudoPlanDetail{},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 1, Path: "path-1",
								Tags: kdb.NewTagSet([]kdb.Tag{{Key: "key-1", Value: "val-1"}, {Key: "key-2", Value: "val-2"}}),
							},
						},
						Outputs: []kdb.MountPoint{
							{
								Id: 2, Path: "path-2",
								Tags: kdb.NewTagSet([]kdb.Tag{{Key: "key-1", Value: "val-3"}, {Key: "key-2", Value: "val-4"}}),
							},
						},
					},
				},
				err: nil,
			},
			then: then{
				contentType: "application/json", shouldError: false, statusCode: http.StatusOK,
				body: plans.Detail{
					Summary: plans.Summary{
						PlanId: "plan-1",
						Image:  &plans.Image{Repository: "image-1", Tag: "ver-1"},
					},
					Inputs: []plans.Mountpoint{
						{
							Path: "path-1",
							Tags: []apitag.Tag{{Key: "key-1", Value: "val-1"}, {Key: "key-2", Value: "val-2"}},
						},
					},
					Outputs: []plans.Mountpoint{
						{
							Path: "path-2",
							Tags: []apitag.Tag{{Key: "key-1", Value: "val-3"}, {Key: "key-2", Value: "val-4"}},
						},
					},
					Log: nil, Active: true,
				},
			},
		},
		"[Activate]When PlanInterface.Activate returns Missing result, it returns 404 not found error.": {
			when: when{
				request:     "/api/plan/plan-1/active",
				planId:      "plan-1",
				isActive:    true,
				queryResult: []*kdb.Plan{},
				err:         kdb.ErrMissing,
			},
			then: then{
				contentType: "application/json",
				shouldError: true,
				statusCode:  http.StatusNotFound,
			},
		},
		"[Deactivate]When PlanInterface.Activate returns query result, it should convert it to JSON format": {
			when: when{
				request:  "/api/plan/plan-1/active",
				planId:   "plan-1",
				isActive: false,
				queryResult: []*kdb.Plan{
					{
						PlanBody: kdb.PlanBody{
							PlanId: "plan-1", Active: false, Hash: "hash-1",
							Image:  &kdb.ImageIdentifier{Image: "image-1", Version: "ver-1"},
							Pseudo: &kdb.PseudoPlanDetail{},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 1, Path: "path-1",
								Tags: kdb.NewTagSet([]kdb.Tag{{Key: "key-1", Value: "val-1"}, {Key: "key-2", Value: "val-2"}}),
							},
						},
						Outputs: []kdb.MountPoint{
							{
								Id: 2, Path: "path-2",
								Tags: kdb.NewTagSet([]kdb.Tag{{Key: "key-1", Value: "val-3"}, {Key: "key-2", Value: "val-4"}}),
							},
						},
					},
				},
				err: nil,
			},
			then: then{
				contentType: "application/json", shouldError: false, statusCode: http.StatusOK,
				body: plans.Detail{
					Summary: plans.Summary{
						PlanId: "plan-1",
						Image:  &plans.Image{Repository: "image-1", Tag: "ver-1"},
					},
					Inputs: []plans.Mountpoint{
						{
							Path: "path-1",
							Tags: []apitag.Tag{{Key: "key-1", Value: "val-1"}, {Key: "key-2", Value: "val-2"}},
						},
					},
					Outputs: []plans.Mountpoint{
						{
							Path: "path-2",
							Tags: []apitag.Tag{{Key: "key-1", Value: "val-3"}, {Key: "key-2", Value: "val-4"}},
						},
					},
					Log: nil, Active: false,
				},
			},
		},
		"[Deactivate]When PlanInterface.Activate returns Missing result, it returns 404 not found error.": {
			when: when{
				request:     "/api/plan/plan-1/active",
				planId:      "plan-1",
				isActive:    false,
				queryResult: []*kdb.Plan{},
				err:         kdb.ErrMissing,
			},
			then: then{
				contentType: "application/json",
				shouldError: true,
				statusCode:  http.StatusNotFound,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {

			mockPlan := mockdb.NewPlanInteraface()
			mockPlan.Impl.Activate = func(ctx context.Context, planId string, isActive bool) error {
				return testcase.when.err
			}
			mockPlan.Impl.Get = func(ctx context.Context, planId []string) (map[string]*kdb.Plan, error) {
				plans := utils.ToMap(testcase.when.queryResult, func(p *kdb.Plan) string { return p.PlanId })
				return plans, testcase.when.err
			}

			e := echo.New()

			var c echo.Context
			var respRec *httptest.ResponseRecorder

			if testcase.when.isActive {
				// When activate plan, PUT requset body is not required
				c, respRec = httptestutil.Put(
					e, testcase.when.request, bytes.NewReader([]byte("")),
				)
			} else {
				c, respRec = httptestutil.Delete(
					e, testcase.when.request,
				)
			}
			c.SetParamNames("planId")
			c.SetParamValues(testcase.when.planId)

			testee := handlers.PutPlanForActivate(mockPlan, testcase.when.isActive)
			err := testee(c)

			if testcase.then.shouldError {
				if err == nil {
					t.Fatalf("response should be error, but not")
				}
				var echoErr *echo.HTTPError
				if !errors.As(err, &echoErr) {
					t.Fatalf("error is not echo.HTTPError. acutal = %#v", err)
				}
				if echoErr.Code != testcase.then.statusCode {
					t.Fatalf("unmatch error code:%d, expeced:%d", echoErr.Code, testcase.then.statusCode)
				}
			} else {
				if err != nil {
					t.Fatalf("response is not illegal. error = %v", err)
				}

				actualContentType := strings.Split(respRec.Result().Header.Get("Content-Type"), ";")[0]
				if actualContentType != testcase.then.contentType {
					t.Errorf("Content-Type: %s != %s", actualContentType, testcase.then.contentType)
				}

				const expectedCallTimes uint = 1
				if mockPlan.Calls.Activate.Times() != expectedCallTimes {
					t.Errorf("Activate did not call correctly. actual: %d, expected: %d", mockPlan.Calls.Find.Times(), expectedCallTimes)
				}

				actualResponse := plans.Detail{}

				err = json.Unmarshal(respRec.Body.Bytes(), &actualResponse)
				if err != nil {
					t.Errorf("response is not illegal. error = %v", err)
				}

				actualStatusCode := respRec.Result().StatusCode
				if actualStatusCode != testcase.then.statusCode {
					t.Errorf("status code %d != %d", actualStatusCode, testcase.then.statusCode)
				}
				if !actualResponse.Equal(testcase.then.body) {
					t.Errorf(
						"data does not match. (actual, expected) = \n(%+v, \n%+v)",
						actualResponse, testcase.then.body,
					)
				}
			}
		})
	}
}

func TestPutPlanResource(t *testing.T) {
	type When struct {
		planId      string
		request     string
		contentType string

		queryResult             []*kdb.Plan
		getError                error
		setResourceLimitError   error
		unsetResourceLimitError error
	}

	type Then struct {
		contentType string

		setResource   map[string]resource.Quantity
		unsetResource []string

		shouldError bool
		statusCode  int
		body        plans.Detail
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			mockPlan := mockdb.NewPlanInteraface()
			mockPlan.Impl.Get = func(ctx context.Context, planId []string) (map[string]*kdb.Plan, error) {
				plans := utils.ToMap(when.queryResult, func(p *kdb.Plan) string { return p.PlanId })
				return plans, when.getError
			}
			mockPlan.Impl.SetResourceLimit = func(ctx context.Context, planId string, resources map[string]resource.Quantity) error {
				if planId != when.planId {
					t.Errorf("unmatch: planId: (actual, expected) = (%s, %s)", planId, when.planId)
				}
				if !cmp.MapEqWith(resources, then.setResource, resource.Quantity.Equal) {
					t.Errorf("unmatch: resources: (actual, expected) = (%v, %v)", resources, then.setResource)
				}
				return when.setResourceLimitError
			}
			mockPlan.Impl.UnsetResourceLimit = func(ctx context.Context, planId string, resourceTypes []string) error {
				if planId != when.planId {
					t.Errorf("unmatch: planId: (actual, expected) = (%s, %s)", planId, when.planId)
				}
				if !cmp.SliceEq(resourceTypes, then.unsetResource) {
					t.Errorf("unmatch: resourceTypes: (actual, expected) = (%v, %v)", resourceTypes, then.unsetResource)
				}
				return when.unsetResourceLimitError
			}

			e := echo.New()
			c, respRec := httptestutil.Put(
				e, "/api/plan/:planId/resources", bytes.NewReader([]byte(when.request)),
				httptestutil.WithHeader("content-type", when.contentType),
			)
			c.SetParamNames("planId")
			c.SetParamValues(when.planId)

			testee := handlers.PutPlanResource(mockPlan, "planId")
			err := testee(c)

			if then.shouldError {
				if err == nil {
					t.Fatalf("response should be error, but not")
				}
				echoErr := new(echo.HTTPError)
				if !errors.As(err, &echoErr) {
					t.Fatalf("error is not echo.HTTPError. acutal = %#v", err)
				}
				if echoErr.Code != then.statusCode {
					t.Fatalf("unmatch error code:%d, expeced:%d", echoErr.Code, then.statusCode)
				}
				return
			}

			if err != nil {
				t.Fatalf("response is not illegal. error = %v", err)
			}

			actualContentType := strings.Split(respRec.Result().Header.Get("Content-Type"), ";")[0]
			if actualContentType != then.contentType {
				t.Errorf("Content-Type: %s != %s", actualContentType, then.contentType)
			}

			actualResponse := plans.Detail{}

			err = json.Unmarshal(respRec.Body.Bytes(), &actualResponse)
			if err != nil {
				t.Errorf("response is not illegal. error = %v", err)
			}

			actualStatusCode := respRec.Result().StatusCode
			if actualStatusCode != then.statusCode {
				t.Errorf("status code %d != %d", actualStatusCode, then.statusCode)
			}
			if !actualResponse.Equal(then.body) {
				t.Errorf(
					"data does not match. (actual, expected) = \n(%+v, \n%+v)",
					actualResponse, then.body,
				)
			}
		}
	}

	planResult := kdb.Plan{
		PlanBody: kdb.PlanBody{
			PlanId: "plan-1", Active: true, Hash: "hash-1",
			Image: &kdb.ImageIdentifier{Image: "image-1", Version: "ver-1"},
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("100m"),
				"memory": resource.MustParse("100Mi"),
			},
		},
		Inputs: []kdb.MountPoint{
			{
				Id: 1, Path: "/in/1",
				Tags: kdb.NewTagSet([]kdb.Tag{{Key: "key-1", Value: "val-1"}, {Key: "key-2", Value: "val-2"}}),
			},
		},
		Outputs: []kdb.MountPoint{
			{
				Id: 2, Path: "/out/1",
				Tags: kdb.NewTagSet([]kdb.Tag{{Key: "key-1", Value: "val-3"}, {Key: "key-2", Value: "val-4"}}),
			},
		},
		Log: &kdb.LogPoint{
			Tags: kdb.NewTagSet([]kdb.Tag{{Key: "key-1", Value: "val-4"}, {Key: "key-2", Value: "val-3"}}),
		},
	}

	t.Run("When request has Set, it should set resource limit to the plan", theory(
		When{
			planId: "plan-1",
			request: string(try.To(
				json.Marshal(plans.ResourceLimitChange{
					Set: map[string]resource.Quantity{
						"cpu":    resource.MustParse("100m"),
						"memory": resource.MustParse("100Mi"),
					},
					Unset: []string{},
				}),
			).OrFatal(t)),
			contentType: "application/json",

			queryResult:             []*kdb.Plan{&planResult},
			getError:                nil,
			setResourceLimitError:   nil,
			unsetResourceLimitError: nil,
		},
		Then{
			contentType: "application/json",
			setResource: map[string]resource.Quantity{
				"cpu":    resource.MustParse("100m"),
				"memory": resource.MustParse("100Mi"),
			},
			unsetResource: []string{},
			shouldError:   false,
			statusCode:    http.StatusOK,
			body:          bindplans.ComposeDetail(planResult),
		},
	))

	t.Run("When request has Unset, it should unset resource limit to the plan", theory(
		When{
			planId: "plan-1",
			request: string(try.To(
				json.Marshal(plans.ResourceLimitChange{
					Set:   map[string]resource.Quantity{},
					Unset: []string{"cpu", "memory"},
				}),
			).OrFatal(t)),
			contentType: "application/json",

			queryResult:             []*kdb.Plan{&planResult},
			getError:                nil,
			setResourceLimitError:   nil,
			unsetResourceLimitError: nil,
		},
		Then{
			contentType:   "application/json",
			setResource:   map[string]resource.Quantity{},
			unsetResource: []string{"cpu", "memory"},
			shouldError:   false,
			statusCode:    http.StatusOK,
			body:          bindplans.ComposeDetail(planResult),
		},
	))

	t.Run("When request has both Set and Unset, it should set and unset resource limit to the plan", theory(
		When{
			planId: "plan-1",
			request: string(try.To(
				json.Marshal(plans.ResourceLimitChange{
					Set:   map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
					Unset: []string{"memory"},
				}),
			).OrFatal(t)),
			contentType: "application/json",

			queryResult:             []*kdb.Plan{&planResult},
			getError:                nil,
			setResourceLimitError:   nil,
			unsetResourceLimitError: nil,
		},
		Then{
			contentType:   "application/json",
			setResource:   map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
			unsetResource: []string{"memory"},
			shouldError:   false,
			statusCode:    http.StatusOK,
			body:          bindplans.ComposeDetail(planResult),
		},
	))

	t.Run("When SetResource return Missing, it should return 404 not found error", theory(
		When{
			planId: "plan-1",
			request: string(try.To(
				json.Marshal(plans.ResourceLimitChange{
					Set:   map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
					Unset: []string{"memory"},
				}),
			).OrFatal(t)),
			contentType: "application/json",

			queryResult:             []*kdb.Plan{},
			getError:                nil,
			setResourceLimitError:   kdb.ErrMissing,
			unsetResourceLimitError: nil,
		},
		Then{
			contentType: "application/json",
			setResource: map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
			shouldError: true,
			statusCode:  http.StatusNotFound,
		},
	))

	t.Run("When SetResource return other error, it should return 500 internal server error", theory(
		When{
			planId: "plan-1",
			request: string(try.To(
				json.Marshal(plans.ResourceLimitChange{
					Set:   map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
					Unset: []string{"memory"},
				}),
			).OrFatal(t)),
			contentType: "application/json",

			queryResult:             []*kdb.Plan{&planResult},
			getError:                nil,
			setResourceLimitError:   errors.New("dummy error"),
			unsetResourceLimitError: nil,
		},
		Then{
			contentType: "application/json",
			setResource: map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
			shouldError: true,
			statusCode:  http.StatusInternalServerError,
		},
	))

	t.Run("When UnsetResource return Missing, it should return 404 not found error", theory(
		When{
			planId: "plan-1",
			request: string(try.To(
				json.Marshal(plans.ResourceLimitChange{
					Set:   map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
					Unset: []string{"cpu", "memory"},
				}),
			).OrFatal(t)),
			contentType: "application/json",

			queryResult:             []*kdb.Plan{&planResult},
			getError:                nil,
			setResourceLimitError:   nil,
			unsetResourceLimitError: kdb.ErrMissing,
		},
		Then{
			contentType:   "application/json",
			setResource:   map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
			unsetResource: []string{"cpu", "memory"},
			shouldError:   true,
			statusCode:    http.StatusNotFound,
		},
	))

	t.Run("When UnsetResource return other error, it should return 500 internal server error", theory(
		When{
			planId: "plan-1",
			request: string(try.To(
				json.Marshal(plans.ResourceLimitChange{
					Set:   map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
					Unset: []string{"cpu", "memory"},
				}),
			).OrFatal(t)),
			contentType: "application/json",

			queryResult:             []*kdb.Plan{&planResult},
			getError:                nil,
			setResourceLimitError:   nil,
			unsetResourceLimitError: errors.New("dummy error"),
		},
		Then{
			contentType:   "application/json",
			setResource:   map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
			unsetResource: []string{"cpu", "memory"},
			shouldError:   true,
			statusCode:    http.StatusInternalServerError,
		},
	))

	t.Run("When Get return Missing, it should return 404 not found error", theory(
		When{
			planId: "plan-1",
			request: string(try.To(
				json.Marshal(plans.ResourceLimitChange{
					Set:   map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
					Unset: []string{"cpu", "memory"},
				}),
			).OrFatal(t)),
			contentType: "application/json",
			getError:    kdb.ErrMissing,
		},
		Then{
			contentType:   "application/json",
			setResource:   map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
			unsetResource: []string{"cpu", "memory"},
			shouldError:   true,
			statusCode:    http.StatusNotFound,
		},
	))

	t.Run("When Get return other error, it should return 500 internal server error", theory(
		When{
			planId: "plan-1",
			request: string(try.To(
				json.Marshal(plans.ResourceLimitChange{
					Set:   map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
					Unset: []string{"cpu", "memory"},
				}),
			).OrFatal(t)),
			contentType: "application/json",
			getError:    errors.New("dummy error"),
		},
		Then{
			contentType:   "application/json",
			setResource:   map[string]resource.Quantity{"cpu": resource.MustParse("100m")},
			unsetResource: []string{"cpu", "memory"},
			shouldError:   true,
			statusCode:    http.StatusInternalServerError,
		},
	))

	t.Run("When request has invalid JSON, it should return 400 bad request error", theory(
		When{
			planId:  "plan-1",
			request: "invalid-json",
		},
		Then{
			contentType: "application/json",
			shouldError: true,
			statusCode:  http.StatusBadRequest,
		},
	))
}

func TestPutAnnotations(t *testing.T) {
	type When struct {
		planId      string
		request     string
		contentType string

		queryResult            map[string]*kdb.Plan
		getError               error
		updateAnnotationsError error
	}

	type Then struct {
		requestedDelta kdb.AnnotationDelta

		contentType string
		statusCode  int
		body        plans.Detail
		wantError   bool
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			mockPlan := mockdb.NewPlanInteraface()

			mockPlan.Impl.UpdateAnnotations = func(ctx context.Context, planId string, annotations kdb.AnnotationDelta) error {
				if planId != when.planId {
					t.Errorf("unmatch: planId: (actual, expected) = (%s, %s)", planId, when.planId)
				}

				if !cmp.SliceContentEq(annotations.Add, then.requestedDelta.Add) {
					t.Errorf("unmatch: Add: (actual, expected) = (%v, %v)", annotations.Add, then.requestedDelta.Add)
				}

				if !cmp.SliceContentEq(annotations.Remove, then.requestedDelta.Remove) {
					t.Errorf("unmatch: Remove: (actual, expected) = (%v, %v)", annotations.Remove, then.requestedDelta.Remove)
				}

				return when.updateAnnotationsError
			}

			mockPlan.Impl.Get = func(ctx context.Context, planId []string) (map[string]*kdb.Plan, error) {
				return when.queryResult, when.getError
			}

			e := echo.New()
			c, resprec := httptestutil.Put(
				e, "/api/plan/:planId/annotations", bytes.NewReader([]byte(when.request)),
				httptestutil.WithHeader("content-type", when.contentType),
			)
			c.SetParamNames("planId")
			c.SetParamValues(when.planId)

			testee := handlers.PutPlanAnnotations(mockPlan, "planId")
			err := testee(c)

			if err != nil {
				if !then.wantError {
					t.Fatalf("response should be error, but not")
				}
			} else {
				if then.wantError {
					t.Fatalf("response should not be error, but not")
				}
			}

			actualContentType := strings.Split(c.Response().Header().Get("Content-Type"), ";")[0]
			if actualContentType != then.contentType {
				t.Errorf("unexpected content type: %s (want %s)", actualContentType, then.contentType)
			}

			if herr := new(echo.HTTPError); errors.As(err, &herr) {
				if herr.Code != then.statusCode {
					t.Errorf("unexpected status code: %d", herr.Code)
				}
			} else {
				actualStatusCode := c.Response().Status
				if actualStatusCode != then.statusCode {
					t.Errorf("unexpected status code: %d (want %d)", actualStatusCode, then.statusCode)
				}
			}

			if !then.wantError {
				actualResponse := plans.Detail{}
				err = json.Unmarshal(resprec.Body.Bytes(), &actualResponse)
				if err != nil {
					t.Errorf("failed to unmarshal response: %v", err)
				}

				if !actualResponse.Equal(then.body) {
					t.Errorf("unexpected response: %v (want %v)", actualResponse, then.body)
				}
			}
		}
	}

	t.Run("When request has valid JSON, it should update annotations", theory(
		When{
			planId: "plan-1",
			request: string(try.To(
				json.Marshal(plans.AnnotationChange{
					Add:    plans.Annotations{{Key: "annot-1", Value: "val-1"}},
					Remove: plans.Annotations{{Key: "annot-2", Value: "val-2"}},
				}),
			).OrFatal(t)),
			contentType: "application/json",
			queryResult: map[string]*kdb.Plan{
				"plan-1": {
					PlanBody: kdb.PlanBody{
						PlanId: "plan-1", Active: true, Hash: "hash-1",
						Image: &kdb.ImageIdentifier{Image: "image-1", Version: "ver-1"},
						Annotations: []kdb.Annotation{
							{Key: "annot-1", Value: "val-1"},
							{Key: "annot-2", Value: "val-2"},
						},
					},
					Inputs: []kdb.MountPoint{
						{
							Id: 1, Path: "/in/1",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "key-1", Value: "val-1"},
								{Key: "key-2", Value: "val-2"},
							}),
						},
					},
					Outputs: []kdb.MountPoint{
						{
							Id: 2, Path: "/out/1",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "key-1", Value: "val-3"},
								{Key: "key-3", Value: "val-4"},
							}),
						},
					},
				},
			},
		},
		Then{
			requestedDelta: kdb.AnnotationDelta{
				Add:    []kdb.Annotation{{Key: "annot-1", Value: "val-1"}},
				Remove: []kdb.Annotation{{Key: "annot-2", Value: "val-2"}},
			},
			contentType: "application/json",
			statusCode:  http.StatusOK,
			body: plans.Detail{
				Summary: plans.Summary{
					PlanId: "plan-1",
					Image:  &plans.Image{Repository: "image-1", Tag: "ver-1"},
					Annotations: plans.Annotations{
						{Key: "annot-1", Value: "val-1"},
						{Key: "annot-2", Value: "val-2"},
					},
				},
				Active: true,
				Inputs: []plans.Mountpoint{
					{
						Path: "/in/1",
						Tags: []apitag.Tag{
							{Key: "key-1", Value: "val-1"},
							{Key: "key-2", Value: "val-2"},
						},
					},
				},
				Outputs: []plans.Mountpoint{
					{
						Path: "/out/1",
						Tags: []apitag.Tag{
							{Key: "key-1", Value: "val-3"},
							{Key: "key-3", Value: "val-4"},
						},
					},
				},
			},
		},
	))

	t.Run("When request has invalid JSON, it should return 400 bad request error", theory(
		When{
			planId:      "plan-1",
			request:     "invalid-json",
			contentType: "application/json",
		},
		Then{
			statusCode: http.StatusBadRequest,
			wantError:  true,
		},
	))

	t.Run("When UpdateAnnotations return Missing, it should return 404 not found error", theory(
		When{
			planId: "plan-1",
			request: string(try.To(
				json.Marshal(plans.AnnotationChange{
					Add:    plans.Annotations{{Key: "annot-1", Value: "val-1"}},
					Remove: plans.Annotations{{Key: "annot-2", Value: "val-2"}},
				}),
			).OrFatal(t)),
			contentType:            "application/json",
			queryResult:            map[string]*kdb.Plan{},
			updateAnnotationsError: kdb.ErrMissing,
		},
		Then{
			requestedDelta: kdb.AnnotationDelta{
				Add:    []kdb.Annotation{{Key: "annot-1", Value: "val-1"}},
				Remove: []kdb.Annotation{{Key: "annot-2", Value: "val-2"}},
			},
			statusCode: http.StatusNotFound,
			wantError:  true,
		},
	))

	t.Run("When UpdateAnnotations return other error, it should return 500 internal server error", theory(
		When{
			planId: "plan-1",
			request: string(try.To(
				json.Marshal(plans.AnnotationChange{
					Add:    plans.Annotations{{Key: "annot-1", Value: "val-1"}},
					Remove: plans.Annotations{{Key: "annot-2", Value: "val-2"}},
				}),
			).OrFatal(t)),
			contentType:            "application/json",
			queryResult:            map[string]*kdb.Plan{},
			updateAnnotationsError: errors.New("dummy error"),
		},
		Then{
			requestedDelta: kdb.AnnotationDelta{
				Add:    []kdb.Annotation{{Key: "annot-1", Value: "val-1"}},
				Remove: []kdb.Annotation{{Key: "annot-2", Value: "val-2"}},
			},
			statusCode: http.StatusInternalServerError,
			wantError:  true,
		},
	))

	t.Run("When Get return error, it should return 404 not found error", theory(
		When{
			planId: "plan-1",
			request: string(try.To(
				json.Marshal(plans.AnnotationChange{
					Add:    plans.Annotations{{Key: "annot-1", Value: "val-1"}},
					Remove: plans.Annotations{{Key: "annot-2", Value: "val-2"}},
				}),
			).OrFatal(t)),
			contentType: "application/json",
			getError:    errors.New("dummy error"),
		},
		Then{
			requestedDelta: kdb.AnnotationDelta{
				Add:    []kdb.Annotation{{Key: "annot-1", Value: "val-1"}},
				Remove: []kdb.Annotation{{Key: "annot-2", Value: "val-2"}},
			},
			statusCode: http.StatusInternalServerError,
			wantError:  true,
		},
	))
}

func TestPutPlanServiceAccount(t *testing.T) {
	type When struct {
		planId         string
		serviceAccount string
		contentType    string

		queryResult               map[string]*kdb.Plan
		getError                  error
		updateServiceAccountError error
	}

	type Then struct {
		contentType string
		statusCode  int
		body        plans.Detail
		wantError   bool
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			mockPlan := mockdb.NewPlanInteraface()

			mockPlan.Impl.SetServiceAccount = func(ctx context.Context, planId string, sa string) error {
				if planId != when.planId {
					t.Errorf("unmatch: planId: (actual, expected) = (%s, %s)", planId, when.planId)
				}
				if sa != when.serviceAccount {
					t.Errorf("unmatch: serviceAccount: (actual, expected) = (%s, %s)", sa, when.serviceAccount)
				}
				return when.updateServiceAccountError
			}

			mockPlan.Impl.Get = func(ctx context.Context, planId []string) (map[string]*kdb.Plan, error) {
				return when.queryResult, when.getError
			}

			e := echo.New()
			payload := plans.SetServiceAccount{ServiceAccount: when.serviceAccount}
			c, resprec := httptestutil.Put(
				e, "/api/plan/:planId/serviceaccount",
				bytes.NewReader(try.To(json.Marshal(payload)).OrFatal(t)),
				httptestutil.WithHeader("content-type", when.contentType),
			)
			c.SetParamNames("planId")
			c.SetParamValues(when.planId)

			testee := handlers.PutPlanServiceAccount(mockPlan, "planId")
			err := testee(c)

			if err != nil {
				if !then.wantError {
					t.Fatalf("response should not be error, but not")
				}
			} else {
				if then.wantError {
					t.Fatalf("response should be error, but not")
				}
			}

			actualContentType := strings.Split(c.Response().Header().Get("Content-Type"), ";")[0]
			if actualContentType != then.contentType {
				t.Errorf("unexpected content type: %s (want %s)", actualContentType, then.contentType)
			}

			if herr := new(echo.HTTPError); errors.As(err, &herr) {
				if herr.Code != then.statusCode {
					t.Errorf("unexpected status code: %d", herr.Code)
				}
			} else {
				actualStatusCode := c.Response().Status
				if actualStatusCode != then.statusCode {
					t.Errorf("unexpected status code: %d (want %d)", actualStatusCode, then.statusCode)
				}
			}

			if !then.wantError {
				actualResponse := plans.Detail{}
				err = json.Unmarshal(resprec.Body.Bytes(), &actualResponse)
				if err != nil {
					t.Errorf("failed to unmarshal response: %v", err)
				}

				if !actualResponse.Equal(then.body) {
					t.Errorf("unexpected response: %v (want %v)", actualResponse, then.body)
				}
			}
		}
	}

	t.Run("When requested planId exists, it should set service account", theory(
		When{
			planId:         "plan-1",
			serviceAccount: "service-account-1",
			contentType:    "application/json",

			queryResult: map[string]*kdb.Plan{
				"plan-1": {
					PlanBody: kdb.PlanBody{
						PlanId: "plan-1", Active: true, Hash: "hash-1",
						Image:          &kdb.ImageIdentifier{Image: "image-1", Version: "ver-1"},
						ServiceAccount: "service-account-1",
					},
					Inputs: []kdb.MountPoint{
						{
							Id: 1, Path: "/in/1",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "key-1", Value: "val-1"},
							}),
						},
					},
					Outputs: []kdb.MountPoint{
						{
							Id: 2, Path: "/out/1",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "key-1", Value: "val-3"},
							}),
						},
					},
				},
			},
		},
		Then{
			contentType: "application/json",
			statusCode:  http.StatusOK,
			body: plans.Detail{
				Summary: plans.Summary{
					PlanId: "plan-1",
					Image:  &plans.Image{Repository: "image-1", Tag: "ver-1"},
				},
				Active:         true,
				ServiceAccount: "service-account-1",
				Inputs: []plans.Mountpoint{
					{
						Path: "/in/1",
						Tags: []apitag.Tag{
							{Key: "key-1", Value: "val-1"},
						},
					},
				},
				Outputs: []plans.Mountpoint{
					{
						Path: "/out/1",
						Tags: []apitag.Tag{
							{Key: "key-1", Value: "val-3"},
						},
					},
				},
			},
		},
	))

	t.Run("When requested planId does not exist, it should return 404 not found error", theory(
		When{
			planId:         "plan-1",
			serviceAccount: "service-account-1",
			contentType:    "application/json",

			queryResult: map[string]*kdb.Plan{},
			getError:    nil,

			updateServiceAccountError: kdb.ErrMissing,
		},
		Then{
			statusCode: http.StatusNotFound,
			wantError:  true,
		},
	))

	t.Run("When UpdateServiceAccount return other error, it should return 500 internal server error", theory(
		When{
			planId:         "plan-1",
			serviceAccount: "service-account-1",
			contentType:    "application/json",

			queryResult: map[string]*kdb.Plan{},

			updateServiceAccountError: errors.New("dummy error"),
		},

		Then{
			statusCode: http.StatusInternalServerError,
			wantError:  true,
		},
	))

	t.Run("When request has invalid JSON, it should return 400 bad request error", theory(
		When{
			planId:         "plan-1",
			serviceAccount: "service-account-1",
			contentType:    "text/plain",
		},
		Then{
			statusCode: http.StatusBadRequest,
			wantError:  true,
		},
	))

	t.Run("When Get return error, it should return 500 internal server error", theory(
		When{
			planId:         "plan-1",
			serviceAccount: "service-account-1",
			contentType:    "application/json",

			getError: errors.New("dummy error"),
		},
		Then{
			statusCode: http.StatusInternalServerError,
			wantError:  true,
		},
	))
}

func TestDeletePlanServiceAccount(t *testing.T) {
	type When struct {
		planId string

		queryResult               map[string]*kdb.Plan
		getError                  error
		updateServiceAccountError error
	}

	type Then struct {
		contentType string
		statusCode  int
		body        plans.Detail
		wantError   bool
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			mockPlan := mockdb.NewPlanInteraface()

			mockPlan.Impl.UnsetServiceAccount = func(ctx context.Context, planId string) error {
				if planId != when.planId {
					t.Errorf("unmatch: planId: (actual, expected) = (%s, %s)", planId, when.planId)
				}
				return when.updateServiceAccountError
			}

			mockPlan.Impl.Get = func(ctx context.Context, planId []string) (map[string]*kdb.Plan, error) {
				return when.queryResult, when.getError
			}

			e := echo.New()
			c, resprec := httptestutil.Delete(
				e, "/api/plan/:planId/serviceaccount",
			)
			c.SetParamNames("planId")
			c.SetParamValues(when.planId)

			testee := handlers.DeletePlanServiceAccount(mockPlan, "planId")
			err := testee(c)

			if err != nil {
				if !then.wantError {
					t.Fatalf("response should not be error, but not")
				}
			} else {
				if then.wantError {
					t.Fatalf("response should be error, but not")
				}
			}

			actualContentType := strings.Split(c.Response().Header().Get("Content-Type"), ";")[0]
			if actualContentType != then.contentType {
				t.Errorf("unexpected content type: %s (want %s)", actualContentType, then.contentType)
			}

			if herr := new(echo.HTTPError); errors.As(err, &herr) {
				if herr.Code != then.statusCode {
					t.Errorf("unexpected status code: %d", herr.Code)
				}
			} else {
				actualStatusCode := c.Response().Status
				if actualStatusCode != then.statusCode {
					t.Errorf("unexpected status code: %d (want %d)", actualStatusCode, then.statusCode)
				}
			}

			if !then.wantError {
				actualResponse := plans.Detail{}
				err = json.Unmarshal(resprec.Body.Bytes(), &actualResponse)
				if err != nil {
					t.Errorf("failed to unmarshal response: %v", err)
				}

				if !actualResponse.Equal(then.body) {
					t.Errorf("unexpected response: %+v (want %+v)", actualResponse, then.body)
				}
			}
		}
	}

	t.Run("When requested planId exists, it should unset service account", theory(
		When{
			planId: "plan-1",

			queryResult: map[string]*kdb.Plan{
				"plan-1": {
					PlanBody: kdb.PlanBody{
						PlanId: "plan-1", Active: true, Hash: "hash-1",
						Image: &kdb.ImageIdentifier{Image: "image-1", Version: "ver-1"},
					},
					Inputs: []kdb.MountPoint{
						{
							Id: 1, Path: "/in/1",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "key-1", Value: "val-1"},
							}),
						},
					},
					Outputs: []kdb.MountPoint{
						{
							Id: 2, Path: "/out/1",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "key-1", Value: "val-3"},
							}),
						},
					},
				},
			},
		},
		Then{
			contentType: "application/json",
			statusCode:  http.StatusOK,
			body: plans.Detail{
				Summary: plans.Summary{
					PlanId: "plan-1",
					Image:  &plans.Image{Repository: "image-1", Tag: "ver-1"},
				},
				Active: true,
				Inputs: []plans.Mountpoint{
					{
						Path: "/in/1",
						Tags: []apitag.Tag{
							{Key: "key-1", Value: "val-1"},
						},
					},
				},
				Outputs: []plans.Mountpoint{
					{
						Path: "/out/1",
						Tags: []apitag.Tag{
							{Key: "key-1", Value: "val-3"},
						},
					},
				},
			},
		},
	))

	t.Run("When requested planId does not exist, it should return 404 not found error", theory(
		When{
			planId: "plan-1",

			queryResult: map[string]*kdb.Plan{},
			getError:    nil,

			updateServiceAccountError: kdb.ErrMissing,
		},
		Then{
			statusCode: http.StatusNotFound,
			wantError:  true,
		},
	))

	t.Run("When UnsetServiceAccount return other error, it should return 500 internal server error", theory(
		When{
			planId: "plan-1",

			queryResult:               map[string]*kdb.Plan{},
			updateServiceAccountError: errors.New("dummy error"),
		},
		Then{
			statusCode: http.StatusInternalServerError,
			wantError:  true,
		},
	))

	t.Run("When Get return error, it should return 500 internal server error", theory(
		When{
			planId:   "plan-1",
			getError: errors.New("dummy error"),
		},
		Then{
			statusCode: http.StatusInternalServerError,
			wantError:  true,
		},
	))
}
