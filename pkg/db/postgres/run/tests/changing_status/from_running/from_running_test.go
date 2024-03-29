package from_running_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	. "github.com/opst/knitfab/pkg/db/postgres/run/tests/changing_status/internal"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/try"
)

func Test_ChangingStatus_FromRunning(t *testing.T) {

	type Expectation struct {
		Statuses  []kdb.KnitRunStatus
		Assertion Assertion
	}

	ctx := context.Background()
	poolBroaker := testenv.NewPoolBroaker(ctx, t)
	pool := poolBroaker.GetPool(ctx, t)

	conn := try.To(pool.Acquire(ctx)).OrFatal(t)
	defer conn.Release()

	given := Testdata(
		t,
		try.To(th.PGNow(ctx, conn)).OrFatal(t),
	)

	nth := 0
	for _, testcase := range []struct {
		// (when cursor given,) the run should be subject of state changing.
		when []When

		// try to change picked run's status with them
		change []Expectation
	}{
		{
			when: []When{
				{
					Target: given.ExpectedRun[th.Padding36("run@pseudo-running-1")],
					Cursor: kdb.RunCursor{
						Status:     []kdb.KnitRunStatus{kdb.Running},
						Pseudo:     []kdb.PseudoPlanName{PseudoActive, PseudoInactive},
						PseudoOnly: true,
						Head:       th.Padding36("run@pseudo-running-X"),
						Debounce:   time.Hour,
					},
				},
				{
					Target: given.ExpectedRun[th.Padding36("run@pseudo-running-2")],
					Cursor: kdb.RunCursor{
						Status:     []kdb.KnitRunStatus{kdb.Running},
						Pseudo:     []kdb.PseudoPlanName{PseudoActive, PseudoInactive},
						PseudoOnly: true,
						Head:       th.Padding36("run@pseudo-running-1"),
						Debounce:   time.Hour,
					},
				},
				{
					Target: given.ExpectedRun[th.Padding36("run@image-running-1")],
					Cursor: kdb.RunCursor{
						Status:   []kdb.KnitRunStatus{kdb.Running},
						Head:     th.Padding36("run@image-running-X"),
						Debounce: time.Hour,
					},
				},
			},
			change: []Expectation{
				{
					Statuses: []kdb.KnitRunStatus{
						kdb.Running, kdb.Aborting, kdb.Completing,
					},
					Assertion: CanBeChanged,
				},
				{
					Statuses: []kdb.KnitRunStatus{
						kdb.Waiting, kdb.Deactivated, kdb.Ready, kdb.Starting,
						kdb.Failed, kdb.Done, kdb.Invalidated,
					},
					Assertion: ShouldNotBeChanged,
				},
			},
		},
	} {
		for _, target := range testcase.when {
			for _, change := range testcase.change {
				for _, status := range change.Statuses {
					knitIds, _ := utils.Group(
						utils.Map(
							utils.Concat(target.Target.Inputs, target.Target.Outputs),
							func(a kdb.Assignment) string { return a.KnitDataBody.KnitId },
						),
						func(s string) bool { return len(s) != 0 },
					)
					if log := target.Target.Log; log != nil && log.KnitDataBody.KnitId != "" {
						knitIds = append(knitIds, log.KnitDataBody.KnitId)
					}

					nth += 1
					t.Run(fmt.Sprintf("#%d", nth), func(t *testing.T) {
						change.Assertion(
							context.Background(), t, poolBroaker,
							[]tables.Operation{given.Plans, given.Runs},
							When{
								Target: target.Target,
								Cursor: target.Cursor,
							},
							Then{
								NewStatus:         status,
								RunIdsToBeLocked:  []string{target.Target.RunBody.Id},
								KnitIdsToBeLocked: knitIds,
							},
						)
					})
				}
			}
		}
	}
}
