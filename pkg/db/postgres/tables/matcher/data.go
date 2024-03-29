package matcher

import (
	"fmt"
	"time"

	"github.com/opst/knitfab/pkg/db/postgres/tables"
)

type Data struct {
	KnitId    Matcher[string]
	VolumeRef Matcher[string]
	PlanId    Matcher[string]
	RunId     Matcher[string]
	OutputId  Matcher[int]
}

func (r Data) Match(actual tables.Data) bool {
	return r.KnitId.Match(actual.KnitId) &&
		r.VolumeRef.Match(actual.VolumeRef) &&
		r.PlanId.Match(actual.PlanId) &&
		r.RunId.Match(actual.RunId) &&
		r.OutputId.Match(actual.OutputId)
}

func (r Data) String() string {
	return fmt.Sprintf(
		"{knitId:%s volumeRef:%s planId:%s runId:%s outputId:%s}",
		r.KnitId, r.VolumeRef, r.PlanId, r.RunId, r.OutputId,
	)
}

func (d Data) Format(s fmt.State, _ rune) {
	fmt.Fprint(s, d.String())
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
