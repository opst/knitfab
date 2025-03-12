package testcases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	pgerrcode "github.com/jackc/pgerrcode"
	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	. "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	kpgnom "github.com/opst/knitfab/pkg/domain/nomination/db/postgres"
	ds "github.com/opst/knitfab/pkg/domain/nomination/db/postgres/tests/nominate_data/internal/dataset"
	"github.com/opst/knitfab/pkg/utils/cmp"
	fn "github.com/opst/knitfab/pkg/utils/function"
	"github.com/opst/knitfab/pkg/utils/try"
)

type Testcase struct {
	given tables.Operation
	when  string // knit ids to be nominated
	then  []tables.Nomination
}

// Theory is a test helper function that generates a test function for each test case.
//
// Before run this test, you need to prepare a database with the internal/dataset package.:
func Theory(testcase Testcase, rootTx kpool.Tx, pool kpool.Pool) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		tx := try.To(rootTx.Begin(ctx)).OrFatal(t)
		defer tx.Rollback(ctx)
		if err := testcase.given.ApplyWithConn(ctx, tx); err != nil {
			t.Fatal(err)
		}

		testee := kpgnom.DefaultNominator()
		{
			wtx := proxy.WrapTx(tx)
			wtx.Events().Query.After(func() {
				BeginFuncToRollback(ctx, pool, fn.Void[error](func(tx kpool.Tx) {
					if _, err := tx.Exec(ctx, `lock table "nomination" in ROW EXCLUSIVE mode nowait`); err == nil {
						t.Errorf("nomination is not locked")
					} else if pgerr := new(pgconn.PgError); !errors.As(err, &pgerr) || pgerr.Code != pgerrcode.LockNotAvailable {
						t.Errorf(
							"unexpected error: expected error code is %s, but %s",
							pgerrcode.LockNotAvailable, err,
						)
					}
				}))
			})

			_tx := try.To(wtx.Begin(ctx)).OrFatal(t)
			defer _tx.Rollback(ctx)
			if err := testee.NominateData(ctx, _tx, []string{testcase.when}); err != nil {
				t.Fatal(err)
			}
			if err := _tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		}

		actual := try.To(scanner.New[tables.Nomination]().QueryAll(
			ctx, tx, `table "nomination"`,
		)).OrFatal(t)

		if !cmp.SliceContentEq(actual, testcase.then) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expeted===\n%+v", actual, testcase.then)
		}
	}
}

func GenearteTestcasesFor(nonDone domain.KnitRunStatus) map[string]Testcase {
	return map[string]Testcase{
		"it should not nominate data with {TAGSET_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   Padding36("knit-target"),
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								UserTag:   ds.TAGSET_1,
							},
						},
					},
				},
			},
			when: Padding36("knit-target"),
			then: ds.GivenDatabase.Nomination,
		},
		"it renominates data with {TAGSET_1} comes run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   Padding36("knit-target"),
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								UserTag:   ds.TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: Padding36("knit-target"), InputId: 1_00_00_01, Updated: false},
					{KnitId: Padding36("knit-target"), InputId: 1_02_02_02, Updated: false},
					{KnitId: Padding36("knit-target"), InputId: 1_02_00_02, Updated: true},
				},
			},
			when: Padding36("knit-target"),
			then: ds.GivenDatabase.Nomination,
		},
		"it should not nominate data with {DAY_2, TAGSET_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   Padding36("knit-target"),
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								Timestamp: &ds.DAY_2, UserTag: ds.TAGSET_1,
							},
						},
					},
				},
			},
			when: Padding36("knit-target"),
			then: ds.GivenDatabase.Nomination,
		},
		"it renominates data with {DAY_2, TAGSET_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   Padding36("knit-target"),
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								Timestamp: &ds.DAY_2, UserTag: ds.TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: Padding36("knit-target"), InputId: 1_00_00_01, Updated: false},
					{KnitId: Padding36("knit-target"), InputId: 1_02_02_02, Updated: false},
					{KnitId: Padding36("knit-target"), InputId: 1_02_00_02, Updated: true},
				},
			},
			when: Padding36("knit-target"),
			then: ds.GivenDatabase.Nomination,
		},
		"it should not nominate data with {DAY_1, TAGSET_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   Padding36("knit-target"),
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								Timestamp: &ds.DAY_1, UserTag: ds.TAGSET_1,
							},
						},
					},
				},
			},
			when: Padding36("knit-target"),
			then: ds.GivenDatabase.Nomination,
		},
		"it renominates data with {DAY_1, TAGSET_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   Padding36("knit-target"),
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								Timestamp: &ds.DAY_1, UserTag: ds.TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: Padding36("knit-target"), InputId: 1_00_01_00, Updated: false},
					{KnitId: Padding36("knit-target"), InputId: 1_02_02_02, Updated: true},
					{KnitId: Padding36("knit-target"), InputId: 1_02_00_02, Updated: false},
				},
			},
			when: Padding36("knit-target"),
			then: ds.GivenDatabase.Nomination,
		},
		"it should not nominate data with {KNITID_2, DAY_1, TAGSET_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   ds.KNITID_2,
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								Timestamp: &ds.DAY_1, UserTag: ds.TAGSET_1,
							},
						},
					},
				},
			},
			when: ds.KNITID_2,
			then: ds.GivenDatabase.Nomination,
		},
		"it renominates data with {KNITID_2, DAY_1, TAGSET_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   ds.KNITID_2,
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								Timestamp: &ds.DAY_1, UserTag: ds.TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: ds.KNITID_2, InputId: 1_00_00_01, Updated: false},
					{KnitId: ds.KNITID_2, InputId: 1_02_02_02, Updated: true},
					{KnitId: ds.KNITID_2, InputId: 1_02_00_02, Updated: false},
				},
			},
			when: ds.KNITID_2,
			then: ds.GivenDatabase.Nomination,
		},
		"it should not nominate data with {KNITID_1, DAY_1, TAGSET_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   ds.KNITID_1,
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								Timestamp: &ds.DAY_1, UserTag: ds.TAGSET_1,
							},
						},
					},
				},
			},
			when: ds.KNITID_1,
			then: ds.GivenDatabase.Nomination,
		},
		"it renominates data with {KNITID_1, DAY_1, TAGSET_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   ds.KNITID_1,
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								Timestamp: &ds.DAY_1, UserTag: ds.TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: ds.KNITID_1, InputId: 1_00_01_00, Updated: false},
					{KnitId: ds.KNITID_1, InputId: 1_01_00_00, Updated: false},
					{KnitId: ds.KNITID_1, InputId: 1_00_01_01, Updated: true},
					{KnitId: ds.KNITID_1, InputId: 1_01_00_01, Updated: true},
					{KnitId: ds.KNITID_1, InputId: 1_02_02_02, Updated: false},
					{KnitId: ds.KNITID_1, InputId: 1_02_00_02, Updated: true},
				},
			},
			when: ds.KNITID_1,
			then: ds.GivenDatabase.Nomination,
		},
		"it should not nominate data with {KNITID_1, DAY_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   ds.KNITID_1,
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								Timestamp: &ds.DAY_1, UserTag: ds.TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: ds.KNITID_1, InputId: 1_01_00_00, Updated: true},
					{KnitId: ds.KNITID_1, InputId: 1_02_02_00, Updated: true},
				},
			},
			when: ds.KNITID_1,
			then: ds.GivenDatabase.Nomination,
		},
		"it nominates data with {KNITID_1, DAY_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   ds.KNITID_1,
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								Timestamp: &ds.DAY_1, UserTag: ds.TAGSET_1,
							},
						},
					},
				},
			},
			when: ds.KNITID_1,
			then: ds.GivenDatabase.Nomination,
		},
		"it rerenominates data with {KNITID_1, DAY_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   ds.KNITID_1,
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								Timestamp: &ds.DAY_1, UserTag: ds.TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: ds.KNITID_1, InputId: 1_00_01_00, Updated: false},
					{KnitId: ds.KNITID_1, InputId: 1_00_02_00, Updated: false},
					{KnitId: ds.KNITID_1, InputId: 1_02_02_00, Updated: true},
				},
			},
			when: ds.KNITID_1,
			then: ds.GivenDatabase.Nomination,
		},
		"it nominates data with {KNITID_1, TAGSET_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   ds.KNITID_1,
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								UserTag:   ds.TAGSET_1,
							},
						},
					},
				},
			},
			when: ds.KNITID_1,
			then: ds.GivenDatabase.Nomination,
		},
		"it renominates data with {KNITID_1, TAGSET_1} comes from run in " + string(nonDone): {
			given: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							Status: nonDone,
							RunId:  Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							UpdatedAt: ds.DAY_1.Add(-24 * time.Hour),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   ds.KNITID_1,
								OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("plan-pseudo"),
							}: {
								VolumeRef: "#vol",
								UserTag:   ds.TAGSET_1,
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: ds.KNITID_1, InputId: 1_00_00_01, Updated: false},
					{KnitId: ds.KNITID_1, InputId: 1_02_00_02, Updated: false},
				},
			},
			when: ds.KNITID_1,
			then: ds.GivenDatabase.Nomination,
		},
	}
}
