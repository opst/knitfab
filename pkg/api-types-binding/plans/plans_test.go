package plans_test

import (
	"testing"

	apiplans "github.com/opst/knitfab-api-types/plans"
	apitags "github.com/opst/knitfab-api-types/tags"
	bindplan "github.com/opst/knitfab/pkg/api-types-binding/plans"
	"github.com/opst/knitfab/pkg/domain"
)

func TestComposeDetail(t *testing.T) {

	for name, testcase := range map[string]struct {
		when domain.Plan
		then apiplans.Detail
	}{
		"When a plan with log is passed, it should compose a Detail corresponding to the plan.": {
			when: domain.Plan{
				PlanBody: domain.PlanBody{
					PlanId: "plan-1", Active: true, Hash: "hash1",
					Image:          &domain.ImageIdentifier{Image: "image-1", Version: "ver-1"},
					Pseudo:         &domain.PseudoPlanDetail{},
					Entrypoint:     []string{"python", "main.py"},
					Args:           []string{"--arg1", "val1", "--arg2", "val2"},
					ServiceAccount: "service-account-name",
					Annotations: []domain.Annotation{
						{Key: "anno1", Value: "val1"},
						{Key: "anno2", Value: "val2"},
					},
				},
				Inputs: []domain.Input{
					{
						MountPoint: domain.MountPoint{
							Id: 1, Path: "/in/mp1",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "key1", Value: "val1"},
							}),
						},
						Upstreams: []domain.PlanUpstream{
							{
								PlanBody: domain.PlanBody{
									PlanId:     "upstream-plan-1",
									Image:      &domain.ImageIdentifier{Image: "upstream-image-1", Version: "upstream-ver-1"},
									Entrypoint: []string{"python", "upstream-main.py"},
									Args:       []string{"--uparg1", "upval1", "--uparg2", "upval2"},
									Annotations: []domain.Annotation{
										{Key: "upanno1", Value: "upval1"},
										{Key: "upanno2", Value: "upval2"},
									},
								},
								Mountpoint: &domain.MountPoint{
									Id: 11, Path: "/upstream1/mp1",
									Tags: domain.NewTagSet([]domain.Tag{
										{Key: "key1", Value: "val1"},
										{Key: "upkey1", Value: "upval1"},
									}),
								},
							},
							{
								PlanBody: domain.PlanBody{
									PlanId: "upstream-plan-2",
									Image:  &domain.ImageIdentifier{Image: "upstream-image-2", Version: "upstream-ver-2"},
								},
								Log: &domain.LogPoint{
									Id: 12,
									Tags: domain.NewTagSet([]domain.Tag{
										{Key: "logkey1", Value: "logval1"},
										{Key: "logkey2", Value: "logval2"},
									}),
								},
							},
						},
					},
				},
				Outputs: []domain.Output{
					{
						MountPoint: domain.MountPoint{
							Id: 2, Path: "/out/mp2",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "key2", Value: "val2"},
								{Key: "key3", Value: "val3"},
							}),
						},
						Downstreams: []domain.PlanDownstream{
							{
								PlanBody: domain.PlanBody{
									PlanId:     "downstream-plan-1",
									Image:      &domain.ImageIdentifier{Image: "downstream-image-1", Version: "downstream-ver-1"},
									Entrypoint: []string{"python", "downstream-main.py"},
									Args:       []string{"--downarg1", "downval1", "--downarg2", "downval2"},
									Annotations: []domain.Annotation{
										{Key: "downanno1", Value: "downval1"},
										{Key: "downanno2", Value: "downval2"},
									},
								},
								Mountpoint: domain.MountPoint{
									Id: 21, Path: "/downstream1/mp1",
									Tags: domain.NewTagSet([]domain.Tag{
										{Key: "key1", Value: "val1"},
									}),
								},
							},
							{
								PlanBody: domain.PlanBody{
									PlanId: "downstream-plan-2",
									Image:  &domain.ImageIdentifier{Image: "downstream-image-2", Version: "downstream-ver-2"},
								},
								Mountpoint: domain.MountPoint{
									Id: 22, Path: "/downstream2/mp1",
									Tags: domain.NewTagSet([]domain.Tag{
										{Key: "key2", Value: "val2"},
									}),
								},
							},
						},
					},
				},
				Log: &domain.LogPoint{
					Id: 3,
					Tags: domain.NewTagSet([]domain.Tag{
						{Key: "logkey1", Value: "logval1"},
						{Key: "logkey2", Value: "logval2"},
					}),
					Downstreams: []domain.PlanDownstream{
						{
							PlanBody: domain.PlanBody{
								PlanId:     "log-downstream-plan-1",
								Image:      &domain.ImageIdentifier{Image: "log-downstream-image-1", Version: "log-downstream-ver-1"},
								Entrypoint: []string{"python", "log-downstream-main.py"},
								Args:       []string{"--log-downarg1", "log-downval1", "--log-downarg2", "log-downval2"},
								Annotations: []domain.Annotation{
									{Key: "log-downanno1", Value: "log-downval1"},
									{Key: "log-downanno2", Value: "log-downval2"},
								},
							},
							Mountpoint: domain.MountPoint{
								Id: 31, Path: "/log-downstream1/mp1",
								Tags: domain.NewTagSet([]domain.Tag{
									{Key: "logkey1", Value: "logval1"},
								}),
							},
						},
					},
				},
			},
			then: apiplans.Detail{
				Summary: apiplans.Summary{
					PlanId: "plan-1",
					Image: &apiplans.Image{
						Repository: "image-1",
						Tag:        "ver-1",
					},
					Entrypoint: []string{"python", "main.py"},
					Args:       []string{"--arg1", "val1", "--arg2", "val2"},
					Annotations: apiplans.Annotations{
						{Key: "anno1", Value: "val1"},
						{Key: "anno2", Value: "val2"},
					},
				},
				Inputs: []apiplans.Input{

					{
						Mountpoint: apiplans.Mountpoint{
							Path: "/in/mp1",
							Tags: []apitags.Tag{
								{Key: "key1", Value: "val1"},
							},
						},
						Upstreams: []apiplans.Upstream{
							{
								Summary: apiplans.Summary{
									PlanId: "upstream-plan-1",
									Image: &apiplans.Image{
										Repository: "upstream-image-1",
										Tag:        "upstream-ver-1",
									},
									Entrypoint: []string{"python", "upstream-main.py"},
									Args:       []string{"--uparg1", "upval1", "--uparg2", "upval2"},
									Annotations: apiplans.Annotations{
										{Key: "upanno1", Value: "upval1"},
										{Key: "upanno2", Value: "upval2"},
									},
								},
								Mountpoint: &apiplans.Mountpoint{
									Path: "/upstream1/mp1",
									Tags: []apitags.Tag{
										{Key: "key1", Value: "val1"},
										{Key: "upkey1", Value: "upval1"},
									},
								},
							},
							{
								Summary: apiplans.Summary{
									PlanId: "upstream-plan-2",
									Image: &apiplans.Image{
										Repository: "upstream-image-2",
										Tag:        "upstream-ver-2",
									},
								},
								Log: &apiplans.LogPoint{
									Tags: []apitags.Tag{
										{Key: "logkey1", Value: "logval1"},
										{Key: "logkey2", Value: "logval2"},
									},
								},
							},
						},
					},
				},
				Outputs: []apiplans.Output{
					{
						Mountpoint: apiplans.Mountpoint{
							Path: "/out/mp2",
							Tags: []apitags.Tag{
								{Key: "key2", Value: "val2"},
								{Key: "key3", Value: "val3"},
							},
						},
						Downstreams: []apiplans.Downstream{
							{
								Summary: apiplans.Summary{
									PlanId: "downstream-plan-1",
									Image: &apiplans.Image{
										Repository: "downstream-image-1",
										Tag:        "downstream-ver-1",
									},
									Entrypoint: []string{"python", "downstream-main.py"},
									Args:       []string{"--downarg1", "downval1", "--downarg2", "downval2"},
									Annotations: apiplans.Annotations{
										{Key: "downanno1", Value: "downval1"},
										{Key: "downanno2", Value: "downval2"},
									},
								},
								Mountpoint: apiplans.Mountpoint{
									Path: "/downstream1/mp1",
									Tags: []apitags.Tag{
										{Key: "key1", Value: "val1"},
									},
								},
							},
							{
								Summary: apiplans.Summary{
									PlanId: "downstream-plan-2",
									Image: &apiplans.Image{
										Repository: "downstream-image-2",
										Tag:        "downstream-ver-2",
									},
								},
								Mountpoint: apiplans.Mountpoint{
									Path: "/downstream2/mp1",
									Tags: []apitags.Tag{
										{Key: "key2", Value: "val2"},
									},
								},
							},
						},
					},
				},
				Log: &apiplans.Log{
					LogPoint: apiplans.LogPoint{
						Tags: []apitags.Tag{
							{Key: "logkey1", Value: "logval1"},
							{Key: "logkey2", Value: "logval2"},
						},
					},
					Downstreams: []apiplans.Downstream{
						{
							Summary: apiplans.Summary{
								PlanId: "log-downstream-plan-1",
								Image: &apiplans.Image{
									Repository: "log-downstream-image-1",
									Tag:        "log-downstream-ver-1",
								},
								Entrypoint: []string{"python", "log-downstream-main.py"},
								Args:       []string{"--log-downarg1", "log-downval1", "--log-downarg2", "log-downval2"},
								Annotations: apiplans.Annotations{
									{Key: "log-downanno1", Value: "log-downval1"},
									{Key: "log-downanno2", Value: "log-downval2"},
								},
							},
							Mountpoint: apiplans.Mountpoint{
								Path: "/log-downstream1/mp1",
								Tags: []apitags.Tag{
									{Key: "logkey1", Value: "logval1"},
								},
							},
						},
					},
				},
				Active:         true,
				ServiceAccount: "service-account-name",
			},
		},
		"When a plan without log is passed, it should compose a Detail corresponding to the plan.": {
			when: domain.Plan{
				PlanBody: domain.PlanBody{
					PlanId: "plan-1", Active: true, Hash: "hash1",
					Image:  &domain.ImageIdentifier{Image: "image-1", Version: "ver-1"},
					Pseudo: &domain.PseudoPlanDetail{},
				},
				Inputs: []domain.Input{
					{
						MountPoint: domain.MountPoint{
							Id: 1, Path: "/in/mp1",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "key1", Value: "val1"},
							}),
						},
					},
				},
				Outputs: []domain.Output{
					{
						MountPoint: domain.MountPoint{
							Id: 2, Path: "/out/mp2",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "key2", Value: "val2"},
								{Key: "key3", Value: "val3"},
							}),
						},
					},
					{
						MountPoint: domain.MountPoint{
							Id: 3, Path: "/out/mp3",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "key4", Value: "val4"},
								{Key: "key5", Value: "val5"},
							}),
						},
					},
				},
			},
			then: apiplans.Detail{
				Summary: apiplans.Summary{
					PlanId: "plan-1", Image: &apiplans.Image{Repository: "image-1", Tag: "ver-1"},
				},
				Inputs: []apiplans.Input{
					{
						Mountpoint: apiplans.Mountpoint{
							Path: "/in/mp1",
							Tags: []apitags.Tag{
								{Key: "key1", Value: "val1"},
							},
						},
						Upstreams: []apiplans.Upstream{},
					},
				},
				Outputs: []apiplans.Output{
					{
						Mountpoint: apiplans.Mountpoint{
							Path: "/out/mp2",
							Tags: []apitags.Tag{
								{Key: "key2", Value: "val2"},
								{Key: "key3", Value: "val3"},
							},
						},
						Downstreams: []apiplans.Downstream{},
					},
					{
						Mountpoint: apiplans.Mountpoint{
							Path: "/out/mp3",
							Tags: []apitags.Tag{
								{Key: "key4", Value: "val4"},
								{Key: "key5", Value: "val5"},
							},
						},
						Downstreams: []apiplans.Downstream{},
					},
				},
				Active: true,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual := bindplan.ComposeDetail(testcase.when)
			if !actual.Equal(testcase.then) {
				t.Fatalf("unexpected result: ComposeDetail(%+v) --> %+v", testcase.then, actual)
			}
		})
	}
}

func TestComposeSummary(t *testing.T) {

	for name, testcase := range map[string]struct {
		when domain.PlanBody
		then apiplans.Summary
	}{
		"When a non-pseudo plan is passed, it should compose a Summary corresponding to the plan.": {
			when: domain.PlanBody{
				PlanId: "plan-1",
				Hash:   "###plan-1###",
				Active: true,
				Image:  &domain.ImageIdentifier{Image: "image-1", Version: "ver-1"},
			},
			then: apiplans.Summary{
				PlanId: "plan-1",
				Image:  &apiplans.Image{Repository: "image-1", Tag: "ver-1"},
			},
		},
		"When a pseudo plan is passed, it should compose a Summary corresponding to the plan.": {
			when: domain.PlanBody{
				PlanId: "plan-1",
				Hash:   "###plan-1###",
				Active: true,
				Pseudo: &domain.PseudoPlanDetail{Name: "pseudo-plan-name"},
			},
			then: apiplans.Summary{
				PlanId: "plan-1",
				Name:   "pseudo-plan-name",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual := bindplan.ComposeSummary(testcase.when)

			if !actual.Equal(testcase.then) {
				t.Errorf("unexpected result: ComposeSummary(%+v) --> %+v", testcase.then, actual)
			}
		})
	}
}
