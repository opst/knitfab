package drop_data_test

import (
	"context"
	"errors"
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
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestDropData(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	TIMESTAMP := try.To(rfctime.ParseRFC3339DateTime(
		"2022-11-13T14:15:16.678+09:00",
	)).OrFatal(t).Time()

	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: Padding36("pseudo"), Active: true, Hash: "hash"},
			{PlanId: Padding36("plan-1"), Active: true, Hash: "hash"},
			{PlanId: Padding36("plan-2"), Active: true, Hash: "hash"},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: Padding36("pseudo"), Name: "upload"},
		},
		PlanImage: []tables.PlanImage{
			{PlanId: Padding36("plan-1"), Image: "repo.invalid/image", Version: "v0.1"},
			{PlanId: Padding36("plan-2"), Image: "repo.invalid/image", Version: "v0.1"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 1, PlanId: Padding36("pseudo"), Path: "/out"}: {},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			{InputId: 100, PlanId: Padding36("plan-1"), Path: "/in/1"}: {
				KnitId: []string{Padding36("knit-1")},
			},
			{InputId: 101, PlanId: Padding36("plan-1"), Path: "/in/2"}: {
				UserTag: []domain.Tag{{Key: "tagkey", Value: "tagval"}},
			},
			{InputId: 102, PlanId: Padding36("plan-1"), Path: "/in/3"}: {
				Timestamp: []time.Time{TIMESTAMP},
			},

			{InputId: 200, PlanId: Padding36("plan-2"), Path: "/in/1"}: {
				KnitId: []string{Padding36("knit-1")},
			},
			{InputId: 201, PlanId: Padding36("plan-2"), Path: "/in/2"}: {
				UserTag: []domain.Tag{{Key: "tagkey", Value: "tagval"}},
			},
			{InputId: 202, PlanId: Padding36("plan-2"), Path: "/in/3"}: {
				Timestamp: []time.Time{TIMESTAMP},
			},
		},
		Steps: []tables.Step{
			{
				Run: tables.Run{
					RunId: Padding36("run-1"), PlanId: Padding36("pseudo"), Status: domain.Done, UpdatedAt: TIMESTAMP,
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:   Padding36("knit-1"),
						OutputId: 1, RunId: Padding36("run-1"), PlanId: Padding36("pseudo"),
					}: {
						VolumeRef: "vol-1",
					},
				},
			},
			{
				Run: tables.Run{
					RunId: Padding36("run-2"), PlanId: Padding36("pseudo"), Status: domain.Done, UpdatedAt: TIMESTAMP,
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:   Padding36("knit-2"),
						OutputId: 1, RunId: Padding36("run-2"), PlanId: Padding36("pseudo"),
					}: {
						VolumeRef: "vol-2",
						Timestamp: &TIMESTAMP,
					},
				},
			},
			{
				Run: tables.Run{
					RunId: Padding36("run-3"), PlanId: Padding36("pseudo"), Status: domain.Done, UpdatedAt: TIMESTAMP,
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:   Padding36("knit-3"),
						OutputId: 1, RunId: Padding36("run-3"), PlanId: Padding36("pseudo"),
					}: {
						VolumeRef: "vol-3",
						UserTag:   []domain.Tag{{Key: "tagkey", Value: "tagval"}},
					},
				},
			},
			{
				Run: tables.Run{
					RunId: Padding36("run-4"), PlanId: Padding36("pseudo"), Status: domain.Done, UpdatedAt: TIMESTAMP,
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:   Padding36("knit-4"),
						OutputId: 1, RunId: Padding36("run-4"), PlanId: Padding36("pseudo"),
					}: {
						VolumeRef: "vol-4",
						Timestamp: &TIMESTAMP,
						UserTag:   []domain.Tag{{Key: "tagkey", Value: "tagval"}},
					},
				},
			},
		},
		Nomination: []tables.Nomination{
			{KnitId: Padding36("knit-1"), InputId: 102, Updated: true},
			{KnitId: Padding36("knit-1"), InputId: 200, Updated: true},
			{KnitId: Padding36("knit-2"), InputId: 100, Updated: false},
			{KnitId: Padding36("knit-2"), InputId: 201, Updated: false},
			{KnitId: Padding36("knit-3"), InputId: 101, Updated: true},
			{KnitId: Padding36("knit-3"), InputId: 202, Updated: true},
		},
	}

	for name, testcase := range map[string]struct {
		when []string
		then []tables.Nomination
	}{
		`when dropping not existing knit id, it does nothing`: {
			when: []string{Padding36("no-such-knit-id")},
			then: given.Nomination,
		},
		`when dropping zero knit ids, it does nothing`: {
			when: []string{},
			then: given.Nomination,
		},
		`when dropping knit id which is not nominated, it does nothing`: {
			when: []string{Padding36("knit-4")},
			then: given.Nomination,
		},
		`when dropping single knit id, it remove nominations related that`: {
			when: []string{Padding36("knit-1")},
			then: []tables.Nomination{
				{KnitId: Padding36("knit-2"), InputId: 100, Updated: false},
				{KnitId: Padding36("knit-2"), InputId: 201, Updated: false},
				{KnitId: Padding36("knit-3"), InputId: 101, Updated: true},
				{KnitId: Padding36("knit-3"), InputId: 202, Updated: true},
			},
		},
		`when dropping knit ids, it remove nominations related that`: {
			when: []string{Padding36("knit-1"), Padding36("knit-3")},
			then: []tables.Nomination{
				{KnitId: Padding36("knit-2"), InputId: 100, Updated: false},
				{KnitId: Padding36("knit-2"), InputId: 201, Updated: false},
			},
		},
		`when dropping knit ids containing non-existing, it remove nominations related that`: {
			when: []string{Padding36("knit-1"), Padding36("no-such-knit-id"), Padding36("knit-3")},
			then: []tables.Nomination{
				{KnitId: Padding36("knit-2"), InputId: 100, Updated: false},
				{KnitId: Padding36("knit-2"), InputId: 201, Updated: false},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pgpool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			wpool := proxy.Wrap(pgpool)
			wpool.Events().Query.After(func() {
				BeginFuncToRollback(ctx, pgpool, fn.Void[error](func(tx kpool.Tx) {
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

			if err := testee.DropData(ctx, tx, testcase.when); err != nil {
				t.Fatal(err)
			}

			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}

			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			actual := try.To(scanner.New[tables.Nomination]().QueryAll(
				ctx, conn, `table "nomination"`,
			)).OrFatal(t)
			if !cmp.SliceContentEq(actual, testcase.then) {
				t.Errorf(
					"unmatch\n===actual===\n%+v\n===expected===\n%+v",
					actual, testcase.then,
				)
			}

		})
	}
}
