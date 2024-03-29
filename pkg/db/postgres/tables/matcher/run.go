package matcher

import (
	"fmt"
	"time"

	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
)

type Run struct {
	RunId                 Matcher[string]
	PlanId                Matcher[string]
	Status                Matcher[kdb.KnitRunStatus]
	LifecycleSuspendUntil Matcher[time.Time]
	UpdatedAt             Matcher[time.Time]
}

func (r Run) Match(actual tables.Run) bool {
	return r.RunId.Match(actual.RunId) &&
		r.PlanId.Match(actual.PlanId) &&
		r.Status.Match(actual.Status) &&
		r.LifecycleSuspendUntil.Match(actual.LifecycleSuspendUntil) &&
		r.UpdatedAt.Match(actual.UpdatedAt)
}

func (r Run) String() string {
	return fmt.Sprintf(
		"{RunId:%s PlanId:%s Status:%s LifecycleSuspendUntil:%s UpdatedAt:%s}",
		r.RunId, r.PlanId, r.Status, r.LifecycleSuspendUntil, r.UpdatedAt,
	)
}

func (r Run) Format(s fmt.State, _ rune) {
	fmt.Fprint(s, r.String())
}
