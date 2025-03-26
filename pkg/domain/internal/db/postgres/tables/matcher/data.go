package matcher

import (
	"fmt"
	"time"

	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
)

type Data struct {
	KnitId   Matcher[string]
	PlanId   Matcher[string]
	RunId    Matcher[string]
	OutputId Matcher[int]
}

func (r Data) Match(actual tables.Data) bool {
	return r.KnitId.Match(actual.KnitId) &&
		r.PlanId.Match(actual.PlanId) &&
		r.RunId.Match(actual.RunId) &&
		r.OutputId.Match(actual.OutputId)
}

func (r Data) String() string {
	return fmt.Sprintf(
		"{knitId:%s planId:%s runId:%s outputId:%s}",
		r.KnitId, r.PlanId, r.RunId, r.OutputId,
	)
}

func (d Data) Format(s fmt.State, _ rune) {
	fmt.Fprint(s, d.String())
}

type VolumeRef struct {
	KnitId    Matcher[string]
	VolumeRef Matcher[string]
}

func (vr VolumeRef) Match(actual tables.VolumeRef) bool {
	return vr.KnitId.Match(actual.KnitId) &&
		vr.VolumeRef.Match(actual.VolumeRef)
}

func (vr VolumeRef) String() string {
	return fmt.Sprintf(
		"{KnitId:%s VolumeRef:%s}",
		vr.KnitId, vr.VolumeRef,
	)
}

func (vr VolumeRef) Format(s fmt.State, _ rune) {
	fmt.Fprint(s, vr.String())
}

type DataAgentMatcher struct {
	Name                  Matcher[string]
	Mode                  Matcher[string]
	KnitId                Matcher[string]
	LifecycleSuspendUntil Matcher[time.Time]
}

func (dam DataAgentMatcher) Match(da tables.DataAgent) bool {
	return dam.Name.Match(da.Name) &&
		dam.Mode.Match(da.Mode) &&
		dam.KnitId.Match(da.KnitId) &&
		dam.LifecycleSuspendUntil.Match(da.LifecycleSuspendUntil)
}

func (dam DataAgentMatcher) String() string {
	return fmt.Sprintf(
		"{Name:%s Mode:%s KnitId:%s LifecycleSuspendUntil:%s}",
		dam.Name, dam.Mode, dam.KnitId, dam.LifecycleSuspendUntil,
	)
}

func (dam DataAgentMatcher) Format(s fmt.State, _ rune) {
	fmt.Fprint(s, dam.String())
}
