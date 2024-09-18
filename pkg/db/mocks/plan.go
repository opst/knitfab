package mocks

import (
	"context"
	"errors"

	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/logic"
	"k8s.io/apimachinery/pkg/api/resource"
)

type PlanFindArgs struct {
	Active   logic.Ternary
	ImageVer kdb.ImageIdentifier
	InTag    []kdb.Tag
	OutTag   []kdb.Tag
}

func (s *PlanFindArgs) Equal(d *PlanFindArgs) bool {

	if !cmp.SliceContentEqWith(utils.RefOf(s.InTag), utils.RefOf(d.InTag), (*kdb.Tag).Equal) {
		return false
	}
	if !cmp.SliceContentEqWith(utils.RefOf(s.OutTag), utils.RefOf(d.OutTag), (*kdb.Tag).Equal) {
		return false
	}

	return s.Active == d.Active && s.ImageVer == d.ImageVer
}

type PlanInterface struct {
	Impl struct {
		Get                func(context.Context, []string) (map[string]*kdb.Plan, error)
		Register           func(context.Context, *kdb.PlanSpec) (string, error)
		Activate           func(context.Context, string, bool) error
		SetResourceLimit   func(context.Context, string, map[string]resource.Quantity) error
		UnsetResourceLimit func(context.Context, string, []string) error
		Find               func(context.Context, logic.Ternary, kdb.ImageIdentifier, []kdb.Tag, []kdb.Tag) ([]string, error)
	}
	Calls struct {
		Get      CallLog[[]string]
		Register CallLog[*kdb.PlanSpec]
		Activate CallLog[string]
		Find     CallLog[PlanFindArgs]
	}
}

var _ kdb.PlanInterface = &PlanInterface{}

func NewPlanInteraface() *PlanInterface {
	return &PlanInterface{}
}

func (m *PlanInterface) Get(ctx context.Context, knitIds []string) (map[string]*kdb.Plan, error) {
	m.Calls.Get = append(m.Calls.Get, knitIds)
	if m.Impl.Get != nil {
		return m.Impl.Get(ctx, knitIds)
	}

	panic(errors.New("should not be called"))
}

func (m *PlanInterface) Register(ctx context.Context, spec *kdb.PlanSpec) (string, error) {
	m.Calls.Register = append(m.Calls.Register, spec)
	if m.Impl.Register != nil {
		return m.Impl.Register(ctx, spec)
	}

	panic(errors.New("should not be called"))
}

func (m *PlanInterface) Activate(ctx context.Context, planId string, isActive bool) error {
	m.Calls.Activate = append(m.Calls.Activate, planId)
	if m.Impl.Activate != nil {
		return m.Impl.Activate(ctx, planId, isActive)
	}

	panic(errors.New("should not be called"))
}

func (m *PlanInterface) SetResourceLimit(ctx context.Context, planId string, limits map[string]resource.Quantity) error {
	if m.Impl.SetResourceLimit != nil {
		return m.Impl.SetResourceLimit(ctx, planId, limits)
	}

	panic(errors.New("should not be called"))
}

func (m *PlanInterface) UnsetResourceLimit(ctx context.Context, planId string, resources []string) error {
	if m.Impl.UnsetResourceLimit != nil {
		return m.Impl.UnsetResourceLimit(ctx, planId, resources)
	}

	panic(errors.New("should not be called"))
}

func (m *PlanInterface) Find(ctx context.Context, active logic.Ternary, imageVer kdb.ImageIdentifier, inTag []kdb.Tag, outTag []kdb.Tag) ([]string, error) {
	m.Calls.Find = append(m.Calls.Find, PlanFindArgs{
		Active: active,
		ImageVer: kdb.ImageIdentifier{
			Image:   imageVer.Image,
			Version: imageVer.Version,
		},
		InTag:  inTag,
		OutTag: outTag,
	})
	if m.Impl.Find != nil {
		return m.Impl.Find(ctx, active, imageVer, inTag, outTag)
	}

	panic(errors.New("should not be called"))
}
