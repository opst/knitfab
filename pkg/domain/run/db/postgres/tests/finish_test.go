package tests_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgconn"
	pgerrcode "github.com/jackc/pgerrcode"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/domain"
	kerr "github.com/opst/knitfab/pkg/domain/errors"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	kpgnommock "github.com/opst/knitfab/pkg/domain/nomination/db/mock"
	kpgrun "github.com/opst/knitfab/pkg/domain/run/db/postgres"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/function"
	"github.com/opst/knitfab/pkg/utils/slices"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestRun_Finish(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	initialState := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: th.Padding36("pseudo-plan-1"), Active: true, Hash: th.Padding64("knit#upload")},
			{PlanId: th.Padding36("pseudo-plan-2"), Active: false, Hash: th.Padding64("knit#deactivated")},
			{PlanId: th.Padding36("plan-deactivated"), Active: true, Hash: th.Padding64("plan/deactivated")},
			{PlanId: th.Padding36("plan-waiting"), Active: true, Hash: th.Padding64("plan/waiting")},
			{PlanId: th.Padding36("plan-ready"), Active: true, Hash: th.Padding64("plan/ready")},
			{PlanId: th.Padding36("plan-starting"), Active: true, Hash: th.Padding64("plan/starting")},
			{PlanId: th.Padding36("plan-running"), Active: true, Hash: th.Padding64("plan/running")},
			{PlanId: th.Padding36("plan-aborting"), Active: true, Hash: th.Padding64("plan/aboring")},
			{PlanId: th.Padding36("plan-completing"), Active: true, Hash: th.Padding64("plan/completing")},
			{PlanId: th.Padding36("plan-failed"), Active: true, Hash: th.Padding64("plan/failed")},
			{PlanId: th.Padding36("plan-done"), Active: true, Hash: th.Padding64("plan/done")},
			{PlanId: th.Padding36("plan-invalidated"), Active: true, Hash: th.Padding64("plan/invalidated")},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: th.Padding36("pseudo-plan-1"), Name: "knit#uploaded"},
			{PlanId: th.Padding36("pseudo-plan-2"), Name: "knit#deactivated"},
		},
		PlanImage: []tables.PlanImage{
			{PlanId: th.Padding36("plan-deactivated"), Image: "repo.invalid/image", Version: "v0.1"},
			{PlanId: th.Padding36("plan-waiting"), Image: "repo.invalid/image", Version: "v0.2"},
			{PlanId: th.Padding36("plan-ready"), Image: "repo.invalid/image", Version: "v0.3"},
			{PlanId: th.Padding36("plan-starting"), Image: "repo.invalid/image", Version: "v0.4"},
			{PlanId: th.Padding36("plan-running"), Image: "repo.invalid/image", Version: "v0.5"},
			{PlanId: th.Padding36("plan-aborting"), Image: "repo.invalid/image", Version: "v0.6"},
			{PlanId: th.Padding36("plan-completing"), Image: "repo.invalid/image", Version: "v0.7"},
			{PlanId: th.Padding36("plan-failed"), Image: "repo.invalid/image", Version: "v0.8"},
			{PlanId: th.Padding36("plan-done"), Image: "repo.invalid/image", Version: "v0.9"},
			{PlanId: th.Padding36("plan-invalidated"), Image: "repo.invalid/image", Version: "v0.10"},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			{InputId: 1_100, PlanId: th.Padding36("plan-deactivated"), Path: "/in"}:  {},
			{InputId: 2_100, PlanId: th.Padding36("plan-waiting"), Path: "/in"}:      {},
			{InputId: 3_100, PlanId: th.Padding36("plan-ready"), Path: "/in"}:        {},
			{InputId: 4_100, PlanId: th.Padding36("plan-starting"), Path: "/in"}:     {},
			{InputId: 5_100, PlanId: th.Padding36("plan-running"), Path: "/in"}:      {},
			{InputId: 6_100, PlanId: th.Padding36("plan-aborting"), Path: "/in"}:     {},
			{InputId: 7_100, PlanId: th.Padding36("plan-completing"), Path: "/in"}:   {},
			{InputId: 8_100, PlanId: th.Padding36("plan-failed"), Path: "/in"}:       {},
			{InputId: 9_100, PlanId: th.Padding36("plan-done"), Path: "/in"}:         {},
			{InputId: 10_100, PlanId: th.Padding36("plan-invalidated"), Path: "/in"}: {},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 99_010, PlanId: th.Padding36("pseudo-plan-1"), Path: "/out"}:    {},
			{OutputId: 99_020, PlanId: th.Padding36("pseudo-plan-2"), Path: "/out"}:    {},
			{OutputId: 1_010, PlanId: th.Padding36("plan-deactivated"), Path: "/out"}:  {},
			{OutputId: 2_010, PlanId: th.Padding36("plan-waiting"), Path: "/out"}:      {},
			{OutputId: 3_010, PlanId: th.Padding36("plan-ready"), Path: "/out"}:        {},
			{OutputId: 4_010, PlanId: th.Padding36("plan-starting"), Path: "/out"}:     {},
			{OutputId: 5_010, PlanId: th.Padding36("plan-running"), Path: "/out"}:      {},
			{OutputId: 6_010, PlanId: th.Padding36("plan-aborting"), Path: "/out"}:     {},
			{OutputId: 7_010, PlanId: th.Padding36("plan-completing"), Path: "/out"}:   {},
			{OutputId: 8_010, PlanId: th.Padding36("plan-failed"), Path: "/out"}:       {},
			{OutputId: 9_010, PlanId: th.Padding36("plan-done"), Path: "/out"}:         {},
			{OutputId: 10_010, PlanId: th.Padding36("plan-invalidated"), Path: "/out"}: {},
		},
		Steps: []tables.Step{
			// pseudo plans
			{
				Run: tables.Run{
					RunId:  th.Padding36("deactivated.pseudo-plan-2"),
					PlanId: th.Padding36("pseudo-plan-2"),
					Status: domain.Deactivated,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.deactivated.pseudo-plan-2"),
						VolumeRef: "pvc/data.deactivated.pseudo-plan-2",
						OutputId:  99_020,
						RunId:     th.Padding36("deactivated.pseudo-plan-2"),
						PlanId:    th.Padding36("pseudo-plan-2"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("waiting.pseudo-plan-1"),
					PlanId: th.Padding36("pseudo-plan-1"),
					Status: domain.Waiting,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.waiting.pseudo-plan-1"),
						VolumeRef: "pvc/data.waiting.pseudo-plan-1",
						OutputId:  99_010,
						RunId:     th.Padding36("waiting.pseudo-plan-1"),
						PlanId:    th.Padding36("pseudo-plan-1"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("ready.pseudo-plan-1"),
					PlanId: th.Padding36("pseudo-plan-1"),
					Status: domain.Ready,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.ready.pseudo-plan-1"),
						VolumeRef: "pvc/data.ready.pseudo-plan-1",
						OutputId:  99_010,
						RunId:     th.Padding36("ready.pseudo-plan-1"),
						PlanId:    th.Padding36("pseudo-plan-1"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("starting.pseudo-plan-1"),
					PlanId: th.Padding36("pseudo-plan-1"),
					Status: domain.Starting,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.starting.pseudo-plan-1"),
						VolumeRef: "pvc/data.starting.pseudo-plan-1",
						OutputId:  99_010,
						RunId:     th.Padding36("starting.pseudo-plan-1"),
						PlanId:    th.Padding36("pseudo-plan-1"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("running.pseudo-plan-1"),
					PlanId: th.Padding36("pseudo-plan-1"),
					Status: domain.Running,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.running.pseudo-plan-1"),
						VolumeRef: "pvc/data.running.pseudo-plan-1",
						OutputId:  99_010,
						RunId:     th.Padding36("running.pseudo-plan-1"),
						PlanId:    th.Padding36("pseudo-plan-1"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("aborting.pseudo-plan-1"),
					PlanId: th.Padding36("pseudo-plan-1"),
					Status: domain.Aborting,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.aborting.pseudo-plan-1"),
						VolumeRef: "pvc/data.aborting.pseudo-plan-1",
						OutputId:  99_010,
						RunId:     th.Padding36("aborting.pseudo-plan-1"),
						PlanId:    th.Padding36("pseudo-plan-1"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("completing.pseudo-plan-1"),
					PlanId: th.Padding36("pseudo-plan-1"),
					Status: domain.Completing,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.completing.pseudo-plan-1"),
						VolumeRef: "pvc/data.completing.pseudo-plan-1",
						OutputId:  99_010,
						RunId:     th.Padding36("completing.pseudo-plan-1"),
						PlanId:    th.Padding36("pseudo-plan-1"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("failed.pseudo-plan-1"),
					PlanId: th.Padding36("pseudo-plan-1"),
					Status: domain.Failed,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.failed.pseudo-plan-1"),
						VolumeRef: "pvc/data.failed.pseudo-plan-1",
						OutputId:  99_010,
						RunId:     th.Padding36("failed.pseudo-plan-1"),
						PlanId:    th.Padding36("pseudo-plan-1"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("done.pseudo-plan-1"),
					PlanId: th.Padding36("pseudo-plan-1"),
					Status: domain.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.done.pseudo-plan-1"),
						VolumeRef: "pvc/data.done.pseudo-plan-1",
						OutputId:  99_010,
						RunId:     th.Padding36("done.pseudo-plan-1"),
						PlanId:    th.Padding36("pseudo-plan-1"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("invalidated.pseudo-plan-1"),
					PlanId: th.Padding36("pseudo-plan-1"),
					Status: domain.Invalidated,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.invalidated.pseudo-plan-1"),
						VolumeRef: "pvc/data.invalidated.pseudo-plan-1",
						OutputId:  99_010,
						RunId:     th.Padding36("invalidated.pseudo-plan-1"),
						PlanId:    th.Padding36("pseudo-plan-1"),
					}: {},
				},
			},
			// plans with image
			{
				Run: tables.Run{
					RunId:  th.Padding36("run.plan-deactivated"),
					PlanId: th.Padding36("plan-deactivated"),
					Status: domain.Deactivated,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("data.done.pseudo-plan-1"),
						InputId: 1_100,
						RunId:   th.Padding36("run.plan-deactivated"),
						PlanId:  th.Padding36("plan-deactivated"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.plan-deactivated"),
						VolumeRef: "pvc/data.plan-deactivated",
						OutputId:  1_010,
						RunId:     th.Padding36("run.plan-deactivated"),
						PlanId:    th.Padding36("plan-deactivated"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("run.plan-waiting"),
					PlanId: th.Padding36("plan-waiting"),
					Status: domain.Waiting,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("data.done.pseudo-plan-1"),
						InputId: 2_100,
						RunId:   th.Padding36("run.plan-waiting"),
						PlanId:  th.Padding36("plan-waiting"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.plan-waiting"),
						VolumeRef: "pvc/data.plan-waiting",
						OutputId:  2_010,
						RunId:     th.Padding36("run.plan-waiting"),
						PlanId:    th.Padding36("plan-waiting"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("run.plan-ready"),
					PlanId: th.Padding36("plan-ready"),
					Status: domain.Ready,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("data.done.pseudo-plan-1"),
						InputId: 3_100,
						RunId:   th.Padding36("run.plan-ready"),
						PlanId:  th.Padding36("plan-ready"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.plan-ready"),
						VolumeRef: "pvc/data.plan-ready",
						OutputId:  3_010,
						RunId:     th.Padding36("run.plan-ready"),
						PlanId:    th.Padding36("plan-ready"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("run.plan-starting"),
					PlanId: th.Padding36("plan-starting"),
					Status: domain.Starting,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("data.done.pseudo-plan-1"),
						InputId: 4_100,
						RunId:   th.Padding36("run.plan-starting"),
						PlanId:  th.Padding36("plan-starting"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.plan-starting"),
						VolumeRef: "pvc/data.plan-starting",
						OutputId:  4_010,
						RunId:     th.Padding36("run.plan-starting"),
						PlanId:    th.Padding36("plan-starting"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("run.plan-running"),
					PlanId: th.Padding36("plan-running"),
					Status: domain.Running,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("data.done.pseudo-plan-1"),
						InputId: 5_100,
						RunId:   th.Padding36("run.plan-running"),
						PlanId:  th.Padding36("plan-running"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.plan-running"),
						VolumeRef: "pvc/data.plan-running",
						OutputId:  5_010,
						RunId:     th.Padding36("run.plan-running"),
						PlanId:    th.Padding36("plan-running"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("run.plan-aborting"),
					PlanId: th.Padding36("plan-aborting"),
					Status: domain.Aborting,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("data.done.pseudo-plan-1"),
						InputId: 6_100,
						RunId:   th.Padding36("run.plan-aborting"),
						PlanId:  th.Padding36("plan-aborting"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.plan-aborting"),
						VolumeRef: "pvc/data.plan-aborting",
						OutputId:  6_010,
						RunId:     th.Padding36("run.plan-aborting"),
						PlanId:    th.Padding36("plan-aborting"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("run.plan-completing"),
					PlanId: th.Padding36("plan-completing"),
					Status: domain.Completing,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("data.done.pseudo-plan-1"),
						InputId: 7_100,
						RunId:   th.Padding36("run.plan-completing"),
						PlanId:  th.Padding36("plan-completing"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.plan-completing"),
						VolumeRef: "pvc/data.plan-completing",
						OutputId:  7_010,
						RunId:     th.Padding36("run.plan-completing"),
						PlanId:    th.Padding36("plan-completing"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("run.plan-failed"),
					PlanId: th.Padding36("plan-failed"),
					Status: domain.Failed,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("data.done.pseudo-plan-1"),
						InputId: 8_100,
						RunId:   th.Padding36("run.plan-failed"),
						PlanId:  th.Padding36("plan-failed"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.plan-failed"),
						VolumeRef: "pvc/data.plan-failed",
						OutputId:  8_010,
						RunId:     th.Padding36("run.plan-failed"),
						PlanId:    th.Padding36("plan-failed"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("run.plan-done"),
					PlanId: th.Padding36("plan-done"),
					Status: domain.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("data.done.pseudo-plan-1"),
						InputId: 9_100,
						RunId:   th.Padding36("run.plan-done"),
						PlanId:  th.Padding36("plan-done"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.plan-done"),
						VolumeRef: "pvc/data.plan-done",
						OutputId:  9_010,
						RunId:     th.Padding36("run.plan-done"),
						PlanId:    th.Padding36("plan-done"),
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:  th.Padding36("run.plan-invalidated"),
					PlanId: th.Padding36("plan-invalidated"),
					Status: domain.Invalidated,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-11-12T13:14:15.678+00:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("data.invalidated.pseudo-plan-1"),
						InputId: 10_100,
						RunId:   th.Padding36("run.plan-invalidated"),
						PlanId:  th.Padding36("plan-invalidated"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data.plan-invalidated"),
						VolumeRef: "pvc/data.plan-invalidated",
						OutputId:  10_010,
						RunId:     th.Padding36("run.plan-invalidated"),
						PlanId:    th.Padding36("plan-invalidated"),
					}: {},
				},
			},
		},
	}

	t.Run("it should cause ErrMissing when finishing nonexisting run", func(t *testing.T) {
		ctx := context.Background()
		pgpool := poolBroaker.GetPool(ctx, t)
		if err := initialState.Apply(ctx, pgpool); err != nil {
			t.Fatal(err)
		}
		runId := th.Padding36("there-are-no-such-run")
		wpool := proxy.Wrap(pgpool)

		conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		// we do not need any nominator nor naming convention.
		// it should make an error before use them.
		testee := kpgrun.New(wpool)

		if err := testee.Finish(ctx, runId); !errors.Is(err, kerr.ErrMissing) {
			t.Errorf(`unexpected error: (actual, expected) = (%+v, %+v)`, err, kerr.ErrMissing)
		}
	})

	t.Run("runs should be finished if", func(t *testing.T) {
		type expectation struct {
			status           domain.KnitRunStatus
			nominatedKnitIds []string
		}

		for name, testcase := range map[string]struct {
			runId    string
			expected expectation
		}{
			"the run is completing (pseudo plan)": {
				runId: th.Padding36("completing.pseudo-plan-1"),
				expected: expectation{
					status: domain.Done,
					nominatedKnitIds: []string{
						th.Padding36("data.completing.pseudo-plan-1"),
					},
				},
			},
			"the run is aborting (pseudo plan)": {
				runId: th.Padding36("aborting.pseudo-plan-1"),
				expected: expectation{
					status:           domain.Failed,
					nominatedKnitIds: []string{}, // empty
				},
			},
			"the run is completing (image plan)": {
				runId: th.Padding36("run.plan-completing"),
				expected: expectation{
					status: domain.Done,
					nominatedKnitIds: []string{
						th.Padding36("data.plan-completing"),
					},
				},
			},
			"the run is aborting (image plan)": {
				runId: th.Padding36("run.plan-aborting"),
				expected: expectation{
					status:           domain.Failed,
					nominatedKnitIds: []string{}, //empty
				},
			},
		} {
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				pgpool := poolBroaker.GetPool(ctx, t)
				if err := initialState.Apply(ctx, pgpool); err != nil {
					t.Fatal(err)
				}

				wpool := proxy.Wrap(pgpool)
				wpool.Events().Query.After(func() {
					th.BeginFuncToRollback(ctx, pgpool, function.Void[error](func(tx kpool.Tx) {
						var notUse string
						if err := tx.QueryRow(
							ctx,
							`select "run_id" from "run" where "run_id" = $1 for update nowait`,
							testcase.runId,
						).Scan(&notUse); err == nil {
							t.Errorf("unexpected run are found (should be locked, but not). run_id = %s", testcase.runId)
						} else if pgerr := new(pgconn.PgError); !errors.As(err, &pgerr) || pgerr.Code != pgerrcode.LockNotAvailable {
							t.Errorf(
								"unexpected error: expected error code is %s, but %s",
								pgerrcode.LockNotAvailable, err,
							)
						}
					}))
				})

				nom := kpgnommock.New(t)
				nom.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
					return nil
				}
				// check with DB
				conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()

				testee := kpgrun.New(wpool, kpgrun.WithNominator(nom))
				// no naming convention is needed. data & worker should not be requested here.

				before := try.To(th.PGNow(ctx, conn)).OrFatal(t)
				err := testee.Finish(ctx, testcase.runId)
				after := try.To(th.PGNow(ctx, conn)).OrFatal(t)
				if err != nil {
					t.Fatal(err)
				}
				// Is the run's status set properly?
				{
					actual := try.To(scanner.New[tables.Run]().QueryAll(
						ctx, conn,
						`select * from "run" where "run_id" = $1`,
						testcase.runId,
					)).OrFatal(t)
					if _, ng := slices.First(
						actual,
						func(a tables.Run) bool {
							return a.Status != testcase.expected.status ||
								a.UpdatedAt.Before(before) ||
								a.UpdatedAt.After(after)
						},
					); ng {
						t.Errorf(
							"unmatch run\n===actual===\n%+v\n===expected===\nstatus %+v\nupdated_at: %s..%s",
							actual, testcase.expected.status, before, after,
						)
					}

				}

				// Is nominator.NominateData called?
				{
					if !cmp.SliceContentEq(
						slices.Concat(nom.Calls.NominateData...),
						testcase.expected.nominatedKnitIds,
					) {
						t.Errorf(
							"unmatch: invocation of Nominator.NominateData\n===actual===\n%+v\n===expected===\n%+v",
							nom.Calls.NominateData, testcase.expected.nominatedKnitIds,
						)
					}
				}
			})
		}
	})

	t.Run("runs should not be finished if", func(t *testing.T) {

		for name, testcase := range map[string]struct {
			runId string
		}{
			"the run is deactivated (pseudo plan)": {
				runId: th.Padding36("deactivated.pseudo-plan-2"),
			},
			"the run is waiting (pseudo plan)": {
				runId: th.Padding36("waiting.pseudo-plan-1"),
			},
			"the run is ready (pseudo plan)": {
				runId: th.Padding36("ready.pseudo-plan-1"),
			},
			"the run is starting (pseudo plan)": {
				runId: th.Padding36("starting.pseudo-plan-1"),
			},
			"the run is running (pseudo plan)": {
				runId: th.Padding36("running.pseudo-plan-1"),
			},
			"the run is done (pseudo plan)": {
				runId: th.Padding36("done.pseudo-plan-1"),
			},
			"the run is failed (pseudo plan)": {
				runId: th.Padding36("failed.pseudo-plan-1"),
			},
			"the run is invalidated (pseudo plan)": {
				runId: th.Padding36("invalidated.pseudo-plan-1"),
			},
			"the run is deactivated (image plan)": {
				runId: th.Padding36("run.plan-deactivated"),
			},
			"the run is waiting (image plan)": {
				runId: th.Padding36("run.plan-waiting"),
			},
			"the run is ready (image plan)": {
				runId: th.Padding36("run.plan-ready"),
			},
			"the run is starting (image plan)": {
				runId: th.Padding36("run.plan-starting"),
			},
			"the run is running (image plan)": {
				runId: th.Padding36("run.plan-running"),
			},
			"the run is done (image plan)": {
				runId: th.Padding36("run.plan-done"),
			},
			"the run is failed (image plan)": {
				runId: th.Padding36("run.plan-failed"),
			},
			"the run is invalidated (image plan)": {
				runId: th.Padding36("run.plan-invalidated"),
			},
		} {
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				pgpool := poolBroaker.GetPool(ctx, t)
				if err := initialState.Apply(ctx, pgpool); err != nil {
					t.Fatal(err)
				}

				wpool := proxy.Wrap(pgpool)
				wpool.Events().Query.After(func() {
					th.BeginFuncToRollback(ctx, pgpool, function.Void[error](func(tx kpool.Tx) {
						var notUse string
						if err := tx.QueryRow(
							ctx,
							`select "run_id" from "run" where "run_id" = $1 for update nowait`,
							testcase.runId,
						).Scan(&notUse); err == nil {
							t.Errorf("unexpected run are found (should be locked, but not). run_id = %s", testcase.runId)
						} else if pgerr := new(pgconn.PgError); !errors.As(err, &pgerr) || pgerr.Code != pgerrcode.LockNotAvailable {
							t.Errorf(
								"unexpected error: expected error code is %s, but %s",
								pgerrcode.LockNotAvailable, err,
							)
						}
					}))
				})

				nom := kpgnommock.New(t)
				nom.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
					return nil
				}
				// check with DB
				conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()

				beforeFinish := try.To(scanner.New[tables.Run]().
					QueryAll(
						ctx, conn,
						`select * from "run" where "run_id" = $1`,
						testcase.runId,
					),
				).OrFatal(t)
				testee := kpgrun.New(wpool, kpgrun.WithNominator(nom))
				// no naming convention is needed. data & worker should not be requested here.

				err := testee.Finish(ctx, testcase.runId)
				if !errors.Is(err, domain.ErrInvalidRunStateChanging) {
					t.Errorf("unexpected error: %+v", err)
				}
				// Is the run changed?
				{
					actual := try.To(scanner.New[tables.Run]().QueryAll(
						ctx, conn,
						`select * from "run" where "run_id" = $1`,
						testcase.runId,
					)).OrFatal(t)
					if !cmp.SliceContentEqWith(
						actual, beforeFinish,
						func(a, b tables.Run) bool { return a.Equal(&b) },
					) {
						t.Errorf(
							"unmatch run\n===actual===\n%+v\n===expected===\n%+v",
							actual, beforeFinish,
						)
					}
				}

				// Is nominator.NominateData called?
				{
					if !cmp.SliceContentEq(
						slices.Concat(nom.Calls.NominateData...),
						[]string{},
					) {
						t.Errorf(
							"unmatch: invocation of Nominator.NominateData is not empty\n===actual===\n%+v",
							nom.Calls.NominateData,
						)
					}
				}
			})
		}
	})

}
