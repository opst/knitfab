package nomintate_input_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	pgerrcode "github.com/jackc/pgerrcode"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	. "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	kpgnom "github.com/opst/knitfab/pkg/domain/nomination/db/postgres"
	"github.com/opst/knitfab/pkg/utils/cmp"
	fn "github.com/opst/knitfab/pkg/utils/function"
	"github.com/opst/knitfab/pkg/utils/slices"
	kstr "github.com/opst/knitfab/pkg/utils/strings"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestNominator_NominateInput(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	oldTimestamp := try.To(rfctime.ParseRFC3339DateTime(
		"2022-11-12T13:14:15.678+09:00",
	)).OrFatal(t).Time()

	newTimestamp := try.To(rfctime.ParseRFC3339DateTime(
		"2022-11-13T14:15:16.678+09:00",
	)).OrFatal(t).Time()

	tagsetAll := []domain.Tag{
		{Key: "tag-a", Value: "a-value"},
		{Key: "tag-b", Value: "b-value"},
		{Key: "tag-c", Value: "c-value"},
	}
	tagsetAandB := tagsetAll[:2]
	tagsetBandC := tagsetAll[1:]
	tagsetAandC := []domain.Tag{tagsetAll[0], tagsetAll[2]}

	plan := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: Padding36("pseudo"), Active: true, Hash: Padding64("#plan-pseudo")},
			{PlanId: Padding36("plan-pre"), Active: true, Hash: Padding64("#plan-pre")},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: Padding36("pseudo"), Name: "pseudo"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 1010, PlanId: Padding36("pseudo"), Path: "/out"}: {},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			{InputId: 1, PlanId: Padding36("plan-pre"), Path: "/in/tags_a-only"}: {
				UserTag: tagsetAll[:1],
			},
			{InputId: 2, PlanId: Padding36("plan-pre"), Path: "/in/timestamp_old"}: {
				Timestamp: []time.Time{oldTimestamp},
			},
			{InputId: 3, PlanId: Padding36("plan-pre"), Path: "/in/knitid_x"}: {
				KnitId: []string{Padding36("knitid_x")},
			},
		},
		Nomination: []tables.Nomination{
			{InputId: 1, KnitId: Padding36("knit_a-and-b_old_done"), Updated: true},
			{InputId: 1, KnitId: Padding36("knit_a-and-b_new_done"), Updated: true},
			{InputId: 2, KnitId: Padding36("knit_a-and-b_old_done"), Updated: true},
			{InputId: 2, KnitId: Padding36("knit_a-and-b_new_done"), Updated: true},
		},
	}

	// generating data and its upstream
	for tagcode, tag := range map[string][]domain.Tag{
		"a-and-b": tagsetAandB,
		"b-and-c": tagsetBandC,
		// a-and-c: no such data.
		"no-tags": {},
	} {
		for timecode, timestamp := range map[string]*time.Time{
			"old":     &oldTimestamp,
			"new":     &newTimestamp,
			"no-time": nil,
		} {
			for _, status := range []domain.KnitRunStatus{
				// knit#transient: processing
				domain.Deactivated, domain.Waiting, domain.Ready, domain.Starting, domain.Running, domain.Aborting, domain.Completing,

				// no knit#transient
				domain.Done,

				// knit#transient: failed
				domain.Failed, domain.Invalidated,
			} {
				// status in id are cut off after the 4th letter,
				// to maintain ids shorter than 36 chars.
				runid := Padding36(fmt.Sprintf(
					"run_%s_%s_%s", tagcode, timecode, status[:4],
				))
				knitid := Padding36(fmt.Sprintf(
					"knit_%s_%s_%s", tagcode, timecode, status[:4],
				))
				step := tables.Step{
					Run: tables.Run{
						RunId: runid, Status: status, PlanId: Padding36("pseudo"),
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime("2022-10-11T12:13:14.567+09:00")).OrFatal(t).Time(),
					},
					Outcomes: map[tables.Data]tables.DataAttibutes{
						{
							KnitId: knitid,
							RunId:  runid, OutputId: 1010, PlanId: Padding36("pseudo"),
						}: {
							VolumeRef: Padding64("#" + knitid),
							UserTag:   tag,
							Timestamp: timestamp,
						},
					},
				}
				plan.Steps = append(plan.Steps, step)
			}
		}
	}

	for name, testcase := range map[string]struct {
		given tables.Operation
		when  []int // input id
		then  []tables.Nomination
	}{
		"if input has no tags, it should not nominate the input": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input has user tags but not match anything, it should not nominate the input": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag: tagsetAandC,
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input has user tags and knit id but not match anything, it should not nominate the input": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag: tagsetAandB,
						KnitId:  []string{Padding36("knit_b-and-c_new_done")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input has knit id but not match anything, it should not nominate the input": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("no-such-knit-id")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input has timestamp but not match anything, it should not nominate the input": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						Timestamp: []time.Time{newTimestamp.Add(240 * time.Hour)},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input has user tags but no data have such, it should not nominate the input": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag: []domain.Tag{{Key: "unexpected", Value: "tag!"}},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Deactivated data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_deac")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Waiting data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_wait")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Ready data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_read")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Starting data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_star")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Running data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_runn")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Aborting data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_abor")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Completing data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_comp")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Failed data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_fail")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input is pinned to Invalidated data, it should not nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_new_inva")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: plan.Nomination,
		},
		"if input matches in user tag, it should nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag: tagsetAll[:1], // a only
					},
				},
			},
			when: []int{1_00_00_00},
			then: slices.Concat(
				slices.Map(
					kstr.SprintMany(
						"knit_a-and-b_%s_done",
						[]any{"old", "new", "no-time"},
					),
					func(knitId string) tables.Nomination {
						return tables.Nomination{InputId: 1_00_00_00, KnitId: Padding36(knitId), Updated: true}
					},
				),
				plan.Nomination,
			),
		},
		"if input matches in timestamp, it should nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						Timestamp: []time.Time{newTimestamp},
					},
				},
			},
			when: []int{1_00_00_00},
			then: slices.Concat(
				slices.Map(
					kstr.SprintMany(
						"knit_%s_new_done",
						[]any{"a-and-b", "b-and-c", "no-tags"},
					),
					func(knitId string) tables.Nomination {
						return tables.Nomination{InputId: 1_00_00_00, KnitId: Padding36(knitId), Updated: true}
					},
				),
				plan.Nomination,
			),
		},
		"if input matches in knitid, it should nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						KnitId: []string{Padding36("knit_a-and-b_no-time_done")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: slices.Concat(
				[]tables.Nomination{
					{InputId: 1_00_00_00, KnitId: Padding36("knit_a-and-b_no-time_done"), Updated: true},
				},
				plan.Nomination,
			),
		},
		"if input matches in knitid and timestamp, it should nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						Timestamp: []time.Time{newTimestamp},
						KnitId:    []string{Padding36("knit_b-and-c_new_done")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: slices.Concat(
				[]tables.Nomination{{InputId: 1_00_00_00, KnitId: Padding36("knit_b-and-c_new_done"), Updated: true}},
				plan.Nomination,
			),
		},
		"if input matches in knitid and user tag, it should nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag: tagsetAll[1:2], // b
						KnitId:  []string{Padding36("knit_b-and-c_new_done")},
					},
				},
			},
			when: []int{1_00_00_00},
			then: slices.Concat(
				[]tables.Nomination{{InputId: 1_00_00_00, KnitId: Padding36("knit_b-and-c_new_done"), Updated: true}},
				plan.Nomination,
			),
		},
		"if input matches in timestamp and user tag, it should nominate": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag:   tagsetAll[1:2], // b
						Timestamp: []time.Time{newTimestamp},
					},
				},
			},
			when: []int{1_00_00_00},
			then: slices.Concat(
				slices.Map(
					kstr.SprintMany(
						"knit_%s_new_done",
						[]any{"a-and-b", "b-and-c"},
					),
					func(knitId string) tables.Nomination {
						return tables.Nomination{InputId: 1_00_00_00, KnitId: Padding36(knitId), Updated: true}
					},
				),
				plan.Nomination,
			),
		},
		"if inputs are passed, it should perform nomination for each of them": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: Padding36("plan"), Active: true, Hash: "hash"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan"), Image: "repo.invalid/image", Version: "v0.1"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1_00_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag:   tagsetAll[:1], // a
						Timestamp: []time.Time{newTimestamp},
					},
					{InputId: 1_01_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag:   tagsetAll[2:], // c
						Timestamp: []time.Time{oldTimestamp},
					},
					{InputId: 1_02_00_00, PlanId: Padding36("plan"), Path: "/in/1"}: {
						UserTag: tagsetAll[1:2], // b
					},
				},
			},
			when: []int{1_00_00_00, 1_01_00_00}, // not nominating 1_02_00_00
			then: slices.Concat(
				[]tables.Nomination{
					{InputId: 1_00_00_00, KnitId: Padding36("knit_a-and-b_new_done"), Updated: true},
					{InputId: 1_01_00_00, KnitId: Padding36("knit_b-and-c_old_done"), Updated: true},
					// not nominated input does not appear here.
				},
				plan.Nomination,
			),
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pool := poolBroaker.GetPool(ctx, t)
			if err := plan.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if err := testcase.given.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			wpool := proxy.Wrap(pool)
			wpool.Events().Events().Query.After(func() {
				BeginFuncToRollback(ctx, pool, fn.Void[error](func(tx kpool.Tx) {
					if _, err := tx.Exec(ctx, `lock table "nomination" in ROW EXCLUSIVE mode nowait;`); err == nil {
						t.Errorf("nomination is not locked")
					} else if pgerr := new(pgconn.PgError); !errors.As(err, &pgerr) || pgerr.Code != pgerrcode.LockNotAvailable {
						t.Errorf(
							"unexpected error: expected error code is %s, but %s",
							pgerrcode.LockNotAvailable, err,
						)
					}
				}))
			})

			tx := try.To(wpool.Begin(ctx)).OrFatal(t)
			defer tx.Rollback(ctx)
			testee := kpgnom.DefaultNominator()
			if err := testee.NominateMountpoints(ctx, tx, testcase.when); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}

			conn := try.To(pool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			actual := try.To(scanner.New[tables.Nomination]().QueryAll(
				ctx, conn, `table "nomination"`,
			)).OrFatal(t)

			if !cmp.SliceContentEq(actual, testcase.then) {
				t.Errorf("unmatch\n===actual===\n%+v\n===expected===\n%+v",
					actual, testcase.then,
				)
			}

		})
	}

}
