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
				Inputs: []domain.MountPoint{
					{
						Id: 1, Path: "C:\\mp1",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "key1", Value: "val1"},
						}),
					},
				},
				Outputs: []domain.MountPoint{
					{
						Id: 2, Path: "C:\\mp2",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "key2", Value: "val2"},
							{Key: "key3", Value: "val3"},
						}),
					},
				},
				Log: &domain.LogPoint{
					Id: 3,
					Tags: domain.NewTagSet([]domain.Tag{
						{Key: "logkey1", Value: "logval1"},
						{Key: "logkey2", Value: "logval2"},
					}),
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
				Inputs: []apiplans.Mountpoint{

					{
						Path: "C:\\mp1",
						Tags: []apitags.Tag{
							{Key: "key1", Value: "val1"},
						},
					},
				},
				Outputs: []apiplans.Mountpoint{
					{
						Path: "C:\\mp2",
						Tags: []apitags.Tag{
							{Key: "key2", Value: "val2"},
							{Key: "key3", Value: "val3"},
						},
					},
				},
				Log: &apiplans.LogPoint{
					Tags: []apitags.Tag{
						{Key: "logkey1", Value: "logval1"},
						{Key: "logkey2", Value: "logval2"},
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
				Inputs: []domain.MountPoint{
					{
						Id: 1, Path: "C:\\mp1",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "key1", Value: "val1"},
						}),
					},
				},
				Outputs: []domain.MountPoint{
					{
						Id: 2, Path: "C:\\mp2",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "key2", Value: "val2"},
							{Key: "key3", Value: "val3"},
						}),
					},
					{
						Id: 3, Path: "C:\\mp3",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "key4", Value: "val4"},
							{Key: "key5", Value: "val5"},
						}),
					},
				},
			},
			then: apiplans.Detail{
				Summary: apiplans.Summary{
					PlanId: "plan-1", Image: &apiplans.Image{Repository: "image-1", Tag: "ver-1"},
				},
				Inputs: []apiplans.Mountpoint{
					{
						Path: "C:\\mp1",
						Tags: []apitags.Tag{
							{Key: "key1", Value: "val1"},
						},
					},
				},
				Outputs: []apiplans.Mountpoint{
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
