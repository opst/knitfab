package tests_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	nom_mock "github.com/opst/knitfab/pkg/db/postgres/nominator/mock"
	"github.com/opst/knitfab/pkg/db/postgres/pool"
	"github.com/opst/knitfab/pkg/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/db/postgres/run"
	"github.com/opst/knitfab/pkg/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	ptr "github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestRun_Delete(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	UPLOADED_AT := try.To(rfctime.ParseRFC3339DateTime(
		"2022-11-12T13:14:15.789+09:00",
	)).OrFatal(t).Time()

	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: th.Padding36("plan-pseudo"), Active: true, Hash: th.Padding64("#plan-pseudo")},
			{PlanId: th.Padding36("plan-deactivated"), Active: true, Hash: th.Padding64("#plan-deactivated")},
			{PlanId: th.Padding36("plan-waiting"), Active: true, Hash: th.Padding64("#plan-waiting")},
			{PlanId: th.Padding36("plan-ready"), Active: true, Hash: th.Padding64("#plan-ready")},
			{PlanId: th.Padding36("plan-starting"), Active: true, Hash: th.Padding64("#plan-starting")},
			{PlanId: th.Padding36("plan-running"), Active: true, Hash: th.Padding64("#plan-running")},
			{PlanId: th.Padding36("plan-aborting"), Active: true, Hash: th.Padding64("#plan-aborting")},
			{PlanId: th.Padding36("plan-completing"), Active: true, Hash: th.Padding64("#plan-completing")},
			{PlanId: th.Padding36("plan-failed"), Active: true, Hash: th.Padding64("#plan-failed")},
			{PlanId: th.Padding36("plan-done"), Active: true, Hash: th.Padding64("#plan-done")},
			{PlanId: th.Padding36("plan-invalidated"), Active: true, Hash: th.Padding64("#plan-invalidated")},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: th.Padding36("plan-pseudo"), Name: "knit#uploaded"},
		},
		PlanImage: []tables.PlanImage{
			{PlanId: th.Padding36("plan-deactivated"), Image: "repo.invalid/deactivated", Version: "v1.1"},
			{PlanId: th.Padding36("plan-waiting"), Image: "repo.invalid/waiting", Version: "v1.1"},
			{PlanId: th.Padding36("plan-ready"), Image: "repo.invalid/ready", Version: "v1.1"},
			{PlanId: th.Padding36("plan-starting"), Image: "repo.invalid/starting", Version: "v1.1"},
			{PlanId: th.Padding36("plan-running"), Image: "repo.invalid/running", Version: "v1.1"},
			{PlanId: th.Padding36("plan-aborting"), Image: "repo.invalid/aborting", Version: "v1.1"},
			{PlanId: th.Padding36("plan-completing"), Image: "repo.invalid/completing", Version: "v1.1"},
			{PlanId: th.Padding36("plan-failed"), Image: "repo.invalid/failed", Version: "v1.1"},
			{PlanId: th.Padding36("plan-done"), Image: "repo.invalid/done", Version: "v1.1"},
			{PlanId: th.Padding36("plan-invalidated"), Image: "repo.invalid/invalidated", Version: "v1.1"},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			{InputId: 2_100, PlanId: th.Padding36("plan-deactivated"), Path: "/in/1"}:  {},
			{InputId: 3_100, PlanId: th.Padding36("plan-waiting"), Path: "/in/1"}:      {},
			{InputId: 4_100, PlanId: th.Padding36("plan-ready"), Path: "/in/1"}:        {},
			{InputId: 5_100, PlanId: th.Padding36("plan-starting"), Path: "/in/1"}:     {},
			{InputId: 6_100, PlanId: th.Padding36("plan-running"), Path: "/in/1"}:      {},
			{InputId: 7_100, PlanId: th.Padding36("plan-aborting"), Path: "/in/1"}:     {},
			{InputId: 8_100, PlanId: th.Padding36("plan-completing"), Path: "/in/1"}:   {},
			{InputId: 9_100, PlanId: th.Padding36("plan-failed"), Path: "/in/1"}:       {},
			{InputId: 10_100, PlanId: th.Padding36("plan-done"), Path: "/in/1"}:        {},
			{InputId: 11_100, PlanId: th.Padding36("plan-invalidated"), Path: "/in/1"}: {},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 1_010, PlanId: th.Padding36("plan-pseudo"), Path: "/out/1"}:       {},
			{OutputId: 2_010, PlanId: th.Padding36("plan-deactivated"), Path: "/out/1"}:  {},
			{OutputId: 3_010, PlanId: th.Padding36("plan-waiting"), Path: "/out/1"}:      {},
			{OutputId: 4_010, PlanId: th.Padding36("plan-ready"), Path: "/out/1"}:        {},
			{OutputId: 5_010, PlanId: th.Padding36("plan-starting"), Path: "/out/1"}:     {},
			{OutputId: 6_010, PlanId: th.Padding36("plan-running"), Path: "/out/1"}:      {},
			{OutputId: 7_010, PlanId: th.Padding36("plan-aborting"), Path: "/out/1"}:     {},
			{OutputId: 8_010, PlanId: th.Padding36("plan-completing"), Path: "/out/1"}:   {},
			{OutputId: 9_010, PlanId: th.Padding36("plan-failed"), Path: "/out/1"}:       {},
			{OutputId: 10_010, PlanId: th.Padding36("plan-done"), Path: "/out/1"}:        {},
			{OutputId: 11_010, PlanId: th.Padding36("plan-invalidated"), Path: "/out/1"}: {},
		},
		Steps: []tables.Step{
			// gen1
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen1/deactivated"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    kdb.Deactivated,
					UpdatedAt: UPLOADED_AT,
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen1/waiting"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    kdb.Waiting,
					UpdatedAt: UPLOADED_AT.Add(1 * time.Hour),
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen1/ready"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    kdb.Ready,
					UpdatedAt: UPLOADED_AT.Add(2 * time.Hour),
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen1/starting"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    kdb.Starting,
					UpdatedAt: UPLOADED_AT.Add(3 * time.Hour),
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen1/running"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    kdb.Running,
					UpdatedAt: UPLOADED_AT.Add(4 * time.Hour),
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen1/aborting"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    kdb.Aborting,
					UpdatedAt: UPLOADED_AT.Add(5 * time.Hour),
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen1/completing"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    kdb.Completing,
					UpdatedAt: UPLOADED_AT.Add(6 * time.Hour),
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen1/failed"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    kdb.Failed,
					UpdatedAt: UPLOADED_AT.Add(7 * time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("gen1/failed/:out/1"),
						VolumeRef: "pvc-gen1-failed-out-1",
						OutputId:  1_010,
						RunId:     th.Padding36("gen1/failed"),
						PlanId:    th.Padding36("plan-pseudo"),
					}: {
						UserTag:   []kdb.Tag{{Key: "key", Value: "value"}},
						Timestamp: ptr.Ref(UPLOADED_AT.Add(7*time.Hour + 1*time.Minute)),
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen1/failed"),
					ExitCode: 1,
					Message:  "reason",
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen1/done-leaf"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    kdb.Done,
					UpdatedAt: UPLOADED_AT.Add(8 * time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("gen1/done-leaf/:out/1"),
						VolumeRef: "pvc-gen1-done-leaf-out-1",
						OutputId:  1_010,
						RunId:     th.Padding36("gen1/done-leaf"),
						PlanId:    th.Padding36("plan-pseudo"),
					}: {
						UserTag:   []kdb.Tag{{Key: "key", Value: "value"}},
						Timestamp: ptr.Ref(UPLOADED_AT.Add(8*time.Hour + 1*time.Minute)),
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen1/done-leaf"),
					ExitCode: 0,
					Message:  "reason",
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen1/done-leaf-with-invalidated"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    kdb.Done,
					UpdatedAt: UPLOADED_AT.Add(8 * time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("gen1/done-leaf-with-invalidated/:out/1"),
						VolumeRef: "pvc-gen1-done-leaf-with-invalidated-out-1",
						OutputId:  1_010,
						RunId:     th.Padding36("gen1/done-leaf-with-invalidated"),
						PlanId:    th.Padding36("plan-pseudo"),
					}: {
						UserTag:   []kdb.Tag{{Key: "key", Value: "value"}},
						Timestamp: ptr.Ref(UPLOADED_AT.Add(8*time.Hour + 1*time.Minute)),
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen1/done-leaf-with-invalidated"),
					ExitCode: 0,
					Message:  "reason",
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen1/done-protected"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    kdb.Done,
					UpdatedAt: UPLOADED_AT.Add(9 * time.Hour),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("gen1/done-protected/:out/1"),
						VolumeRef: "pvc-gen1-done-protected-out-1",
						OutputId:  1_010,
						RunId:     th.Padding36("gen1/done-protected"),
						PlanId:    th.Padding36("plan-pseudo"),
					}: {
						UserTag:   []kdb.Tag{{Key: "key", Value: "value"}},
						Timestamp: ptr.Ref(UPLOADED_AT.Add(9*time.Hour + 1*time.Minute)),
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen1/done-protected"),
					ExitCode: 0,
					Message:  "reason",
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen1/invalidated"),
					PlanId:    th.Padding36("plan-pseudo"),
					Status:    kdb.Invalidated,
					UpdatedAt: UPLOADED_AT.Add(10 * time.Hour),
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen1/invalidated"),
					ExitCode: 0,
					Message:  "reason",
				},
			},

			// gen2
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen2/deactivated"),
					PlanId:    th.Padding36("plan-deactivated"),
					Status:    kdb.Deactivated,
					UpdatedAt: UPLOADED_AT,
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen1/done-protected/:out/1"),
						InputId: 2_100,
						RunId:   th.Padding36("gen2/deactivated"),
						PlanId:  th.Padding36("plan-deactivated"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen2/waiting"),
					PlanId:    th.Padding36("plan-waiting"),
					Status:    kdb.Waiting,
					UpdatedAt: UPLOADED_AT.Add(1 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen1/done-protected/:out/1"),
						InputId: 3_100,
						RunId:   th.Padding36("gen2/waiting"),
						PlanId:  th.Padding36("plan-waiting"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen2/ready"),
					PlanId:    th.Padding36("plan-ready"),
					Status:    kdb.Ready,
					UpdatedAt: UPLOADED_AT.Add(2 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen1/done-protected/:out/1"),
						InputId: 4_100,
						RunId:   th.Padding36("gen2/ready"),
						PlanId:  th.Padding36("plan-ready"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen2/starting"),
					PlanId:    th.Padding36("plan-starting"),
					Status:    kdb.Starting,
					UpdatedAt: UPLOADED_AT.Add(3 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen1/done-protected/:out/1"),
						InputId: 5_100,
						RunId:   th.Padding36("gen2/starting"),
						PlanId:  th.Padding36("plan-starting"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen2/running"),
					PlanId:    th.Padding36("plan-running"),
					Status:    kdb.Running,
					UpdatedAt: UPLOADED_AT.Add(4 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen1/done-protected/:out/1"),
						InputId: 6_100,
						RunId:   th.Padding36("gen2/running"),
						PlanId:  th.Padding36("plan-running"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen2/aborting"),
					PlanId:    th.Padding36("plan-aborting"),
					Status:    kdb.Aborting,
					UpdatedAt: UPLOADED_AT.Add(5 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen1/done-protected/:out/1"),
						InputId: 7_100,
						RunId:   th.Padding36("gen2/aborting"),
						PlanId:  th.Padding36("plan-aborting"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen2/completing"),
					PlanId:    th.Padding36("plan-completing"),
					Status:    kdb.Completing,
					UpdatedAt: UPLOADED_AT.Add(6 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen1/done-protected/:out/1"),
						InputId: 8_100,
						RunId:   th.Padding36("gen2/completing"),
						PlanId:  th.Padding36("plan-completing"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen2/failed"),
					PlanId:    th.Padding36("plan-failed"),
					Status:    kdb.Failed,
					UpdatedAt: UPLOADED_AT.Add(7 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen1/done-protected/:out/1"),
						InputId: 9_100,
						RunId:   th.Padding36("gen2/failed"),
						PlanId:  th.Padding36("plan-failed"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("gen2/failed/:out/1"),
						VolumeRef: "pvc-gen2-failed-out-1",
						OutputId:  9_010,
						RunId:     th.Padding36("gen2/failed"),
						PlanId:    th.Padding36("plan-failed"),
					}: {
						UserTag:   []kdb.Tag{{Key: "key", Value: "value"}},
						Timestamp: ptr.Ref(UPLOADED_AT.Add(7*time.Hour + 1*time.Minute)),
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen2/failed"),
					ExitCode: 1,
					Message:  "reason",
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen2/done-leaf"),
					PlanId:    th.Padding36("plan-done"),
					Status:    kdb.Done,
					UpdatedAt: UPLOADED_AT.Add(8 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen1/done-protected/:out/1"),
						InputId: 10_100,
						RunId:   th.Padding36("gen2/done-leaf"),
						PlanId:  th.Padding36("plan-done"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("gen2/done-leaf/:out/1"),
						VolumeRef: "pvc-gen2-done-leaf-out-1",
						OutputId:  10_010,
						RunId:     th.Padding36("gen2/done-leaf"),
						PlanId:    th.Padding36("plan-done"),
					}: {
						UserTag:   []kdb.Tag{{Key: "key", Value: "value"}},
						Timestamp: ptr.Ref(UPLOADED_AT.Add(8*time.Hour + 1*time.Minute)),
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen2/done-leaf"),
					ExitCode: 0,
					Message:  "reason",
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen2/done-leaf-with-invalidated"),
					PlanId:    th.Padding36("plan-invalidated"),
					Status:    kdb.Done,
					UpdatedAt: UPLOADED_AT.Add(8 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen1/done-protected/:out/1"),
						InputId: 11_100,
						RunId:   th.Padding36("gen2/done-leaf-with-invalidated"),
						PlanId:  th.Padding36("plan-invalidated"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("gen2/done-leaf-with-invalidated/:out/1"),
						VolumeRef: "pvc-gen2-done-leaf-with-invalidated-out-1",
						OutputId:  11_010,
						RunId:     th.Padding36("gen2/done-leaf-with-invalidated"),
						PlanId:    th.Padding36("plan-invalidated"),
					}: {
						UserTag:   []kdb.Tag{{Key: "key", Value: "value"}},
						Timestamp: ptr.Ref(UPLOADED_AT.Add(8*time.Hour + 1*time.Minute)),
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen2/done-leaf-with-invalidated"),
					ExitCode: 0,
					Message:  "reason",
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen2/done-protected"),
					PlanId:    th.Padding36("plan-done"),
					Status:    kdb.Done,
					UpdatedAt: UPLOADED_AT.Add(9 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen1/done-protected/:out/1"),
						InputId: 10_100,
						RunId:   th.Padding36("gen2/done-protected"),
						PlanId:  th.Padding36("plan-done"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("gen2/done-protected/:out/1"),
						VolumeRef: "pvc-gen2-done-protected-out-1",
						OutputId:  10_010,
						RunId:     th.Padding36("gen2/done-protected"),
						PlanId:    th.Padding36("plan-done"),
					}: {
						UserTag:   []kdb.Tag{{Key: "key", Value: "value"}},
						Timestamp: ptr.Ref(UPLOADED_AT.Add(9*time.Hour + 1*time.Minute)),
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen2/done-protected"),
					ExitCode: 0,
					Message:  "reason",
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen2/invalidated"),
					PlanId:    th.Padding36("plan-invalidated"),
					Status:    kdb.Invalidated,
					UpdatedAt: UPLOADED_AT.Add(10 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen1/done-leaf-with-invalidated/:out/1"),
						InputId: 11_100,
						RunId:   th.Padding36("gen2/invalidated"),
						PlanId:  th.Padding36("plan-invalidated"),
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen2/invalidated"),
					ExitCode: 0,
					Message:  "reason",
				},
			},

			//  gen3
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen3/deactivated"),
					PlanId:    th.Padding36("plan-deactivated"),
					Status:    kdb.Deactivated,
					UpdatedAt: UPLOADED_AT,
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen2/done-protected/:out/1"),
						InputId: 2_100,
						RunId:   th.Padding36("gen3/deactivated"),
						PlanId:  th.Padding36("plan-deactivated"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen3/waiting"),
					PlanId:    th.Padding36("plan-waiting"),
					Status:    kdb.Waiting,
					UpdatedAt: UPLOADED_AT.Add(1 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen2/done-protected/:out/1"),
						InputId: 3_100,
						RunId:   th.Padding36("gen3/waiting"),
						PlanId:  th.Padding36("plan-waiting"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen3/ready"),
					PlanId:    th.Padding36("plan-ready"),
					Status:    kdb.Ready,
					UpdatedAt: UPLOADED_AT.Add(2 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen2/done-protected/:out/1"),
						InputId: 4_100,
						RunId:   th.Padding36("gen3/ready"),
						PlanId:  th.Padding36("plan-ready"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen3/starting"),
					PlanId:    th.Padding36("plan-starting"),
					Status:    kdb.Starting,
					UpdatedAt: UPLOADED_AT.Add(3 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen2/done-protected/:out/1"),
						InputId: 5_100,
						RunId:   th.Padding36("gen3/starting"),
						PlanId:  th.Padding36("plan-starting"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen3/running"),
					PlanId:    th.Padding36("plan-running"),
					Status:    kdb.Running,
					UpdatedAt: UPLOADED_AT.Add(4 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen2/done-protected/:out/1"),
						InputId: 6_100,
						RunId:   th.Padding36("gen3/running"),
						PlanId:  th.Padding36("plan-running"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen3/aborting"),
					PlanId:    th.Padding36("plan-aborting"),
					Status:    kdb.Aborting,
					UpdatedAt: UPLOADED_AT.Add(5 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen2/done-protected/:out/1"),
						InputId: 7_100,
						RunId:   th.Padding36("gen3/aborting"),
						PlanId:  th.Padding36("plan-aborting"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen3/completing"),
					PlanId:    th.Padding36("plan-completing"),
					Status:    kdb.Completing,
					UpdatedAt: UPLOADED_AT.Add(6 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen2/done-protected/:out/1"),
						InputId: 8_100,
						RunId:   th.Padding36("gen3/completing"),
						PlanId:  th.Padding36("plan-completing"),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen3/failed"),
					PlanId:    th.Padding36("plan-failed"),
					Status:    kdb.Failed,
					UpdatedAt: UPLOADED_AT.Add(7 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen2/done-protected/:out/1"),
						InputId: 9_100,
						RunId:   th.Padding36("gen3/failed"),
						PlanId:  th.Padding36("plan-failed"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("gen3/failed/:out/1"),
						VolumeRef: "pvc-gen3-failed-out-1",
						OutputId:  9_010,
						RunId:     th.Padding36("gen3/failed"),
						PlanId:    th.Padding36("plan-failed"),
					}: {
						UserTag:   []kdb.Tag{{Key: "key", Value: "value"}},
						Timestamp: ptr.Ref(UPLOADED_AT.Add(7*time.Hour + 1*time.Minute)),
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen3/failed"),
					ExitCode: 1,
					Message:  "reason",
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen3/done-leaf"),
					PlanId:    th.Padding36("plan-done"),
					Status:    kdb.Done,
					UpdatedAt: UPLOADED_AT.Add(8 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen2/done-protected/:out/1"),
						InputId: 10_100,
						RunId:   th.Padding36("gen3/done-leaf"),
						PlanId:  th.Padding36("plan-done"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("gen3/done-leaf/:out/1"),
						VolumeRef: "pvc-gen3-done-leaf-out-1",
						OutputId:  10_010,
						RunId:     th.Padding36("gen3/done-leaf"),
						PlanId:    th.Padding36("plan-done"),
					}: {
						UserTag:   []kdb.Tag{{Key: "key", Value: "value"}},
						Timestamp: ptr.Ref(UPLOADED_AT.Add(8*time.Hour + 1*time.Minute)),
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen3/done-leaf"),
					ExitCode: 0,
					Message:  "reason",
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen3/done-with-dataagent"),
					PlanId:    th.Padding36("plan-done"),
					Status:    kdb.Done,
					UpdatedAt: UPLOADED_AT.Add(8 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen2/done-protected/:out/1"),
						InputId: 10_100,
						RunId:   th.Padding36("gen3/done-with-dataagent"),
						PlanId:  th.Padding36("plan-done"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("gen3/done-with-dataagent/:out/1"),
						VolumeRef: "pvc-gen3-done-with-dataagent-out-1",
						OutputId:  10_010,
						RunId:     th.Padding36("gen3/done-with-dataagent"),
						PlanId:    th.Padding36("plan-done"),
					}: {
						UserTag:   []kdb.Tag{{Key: "key", Value: "value"}},
						Timestamp: ptr.Ref(UPLOADED_AT.Add(8*time.Hour + 1*time.Minute)),
						Agent: []tables.DataAgent{
							{
								Mode:                  string(kdb.DataAgentRead),
								Name:                  "agent::gen3/done-with-dataagent",
								KnitId:                th.Padding36("gen3/done-with-dataagent/:out/1"),
								LifecycleSuspendUntil: UPLOADED_AT.Add(8*time.Hour + 1*time.Minute),
							},
						},
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen3/done-with-dataagent"),
					ExitCode: 0,
					Message:  "reason",
				},
			},
			{
				Run: tables.Run{
					RunId:     th.Padding36("gen3/invalidated"),
					PlanId:    th.Padding36("plan-invalidated"),
					Status:    kdb.Invalidated,
					UpdatedAt: UPLOADED_AT.Add(10 * time.Hour),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("gen2/done-leaf-with-invalidated/:out/1"),
						InputId: 11_100,
						RunId:   th.Padding36("gen3/invalidated"),
						PlanId:  th.Padding36("plan-invalidated"),
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("gen3/invalidated"),
					ExitCode: 0,
					Message:  "reason",
				},
			},
		},
	}

	type When struct {
		RunId                  string
		NominatorDropDataError error
	}
	type Then struct {
		WantNominatorDropData           []string
		RunIdsShouldBeDeletedAdditional []string
		ReasonNotDeleted                error
	}

	shouldBeTruncated := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			p := poolBroaker.GetPool(ctx, t)
			if err := given.Apply(ctx, p); err != nil {
				t.Fatal(err)
			}
			conn := try.To(p.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			wrapped := proxy.Wrap(p)
			wrapped.Events().Events().Query.After(func() {
				lockedRunIds := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`
						with "all" as (
							select "run_id" from "run"
						),
						"unlocked" as (
							select "run_id" from "run" for update skip locked
						)
						select "run_id" from "all"
						except
						select "run_id" from "unlocked"
						`,
					),
				).OrFatal(t)

				toBeLocked := []string{when.RunId}
				toBeLocked = append(toBeLocked, then.RunIdsShouldBeDeletedAdditional...)
				if !cmp.SliceContentEq(toBeLocked, lockedRunIds) {
					t.Errorf("unexpected locked run ids: %v", lockedRunIds)
				}
			})

			nom := nom_mock.New(t)
			nom.Impl.DropData = func(_ context.Context, _ pool.Tx, knitIds []string) error {
				return nil
			}

			testee := run.New(wrapped, run.WithNominator(nom))

			before := try.To(th.PGNow(ctx, conn)).OrFatal(t)
			if err := testee.Delete(ctx, when.RunId); err != nil {
				t.Fatal(err)
			}
			after := try.To(th.PGNow(ctx, conn)).OrFatal(t)

			{
				got := []string{}
				for _, dd := range nom.Calls.DropData {
					got = append(got, dd...)
				}

				if !cmp.SliceContentEq(then.WantNominatorDropData, got) {
					t.Errorf("unexpected nominator drop data: %v", got)
				}
			}

			{
				got := try.To(
					scanner.New[tables.Run]().QueryAll(
						ctx, conn,
						`select * from "run" where "run_id" = $1`,
						when.RunId,
					),
				).OrFatal(t)
				if len(got) != 1 {
					t.Errorf("unexpected run: %v", got)
				} else {
					g := got[0]
					if g.Status != kdb.Invalidated {
						t.Errorf("unexpected run status: %v", g.Status)
					}

					if g.UpdatedAt.Before(before) || g.UpdatedAt.After(after) {
						t.Errorf(
							"unexpected updated at: %v (should between %s and %s)",
							g.UpdatedAt, before, after,
						)
					}
				}
			}

			{
				got := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "knit_id" from "data" where "run_id" = $1`,
						when.RunId,
					),
				).OrFatal(t)
				if len(got) != 0 {
					t.Errorf("unexpected data: %v", got)
				}
			}

			{
				got := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "run_id" from "run_exit" where "run_id" = $1`,
						when.RunId,
					),
				).OrFatal(t)
				if len(got) != 0 {
					t.Errorf("unexpected run exit: %v", got)
				}
			}

			{
				got := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "run_id" from "run" where "run_id" = any($1)`,
						then.RunIdsShouldBeDeletedAdditional,
					),
				).OrFatal(t)
				if len(got) != 0 {
					t.Errorf("unexpected run: %v", got)
				}
			}
		}
	}

	t.Run("gen2/deactivated", shouldBeTruncated(
		When{RunId: th.Padding36("gen2/deactivated")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen2/waiting", shouldBeTruncated(
		When{RunId: th.Padding36("gen2/waiting")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen2/failed", shouldBeTruncated(
		When{RunId: th.Padding36("gen2/failed")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen2/failed/:out/1"),
			},
		},
	))

	t.Run("gen2/done-leaf", shouldBeTruncated(
		When{RunId: th.Padding36("gen2/done-leaf")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen2/done-leaf/:out/1"),
			},
		},
	))

	t.Run("gen2/done-leaf-with-invalidated", shouldBeTruncated(
		When{RunId: th.Padding36("gen2/done-leaf-with-invalidated")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen2/done-leaf-with-invalidated/:out/1"),
			},
			RunIdsShouldBeDeletedAdditional: []string{
				th.Padding36("gen3/invalidated"),
			},
		},
	))

	t.Run("gen3/deactivated", shouldBeTruncated(
		When{RunId: th.Padding36("gen3/deactivated")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen3/waiting", shouldBeTruncated(
		When{RunId: th.Padding36("gen3/waiting")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen3/failed", shouldBeTruncated(
		When{RunId: th.Padding36("gen3/failed")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen3/failed/:out/1"),
			},
		},
	))

	t.Run("gen3/done-leaf", shouldBeTruncated(
		When{RunId: th.Padding36("gen3/done-leaf")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen3/done-leaf/:out/1"),
			},
		},
	))

	shouldBeDeleted := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			p := poolBroaker.GetPool(ctx, t)
			if err := given.Apply(ctx, p); err != nil {
				t.Fatal(err)
			}
			conn := try.To(p.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			wrapped := proxy.Wrap(p)
			wrapped.Events().Events().Query.After(func() {
				unlockedRunIds := try.To(
					// in this case, target run may be deleted.
					// so, we should check it is not-not locked.
					scanner.New[string]().QueryAll(
						ctx, conn,
						`
						select "run_id" from "run"
						where "run_id" = $1
						for update skip locked
						`,
						when.RunId,
					),
				).OrFatal(t)

				if len(unlockedRunIds) != 0 {
					t.Errorf("unexpected locked run ids: %v", unlockedRunIds)
				}
			})

			nom := nom_mock.New(t)
			nom.Impl.DropData = func(_ context.Context, _ pool.Tx, knitIds []string) error {
				return nil
			}

			testee := run.New(wrapped, run.WithNominator(nom))

			if err := testee.Delete(ctx, when.RunId); err != nil {
				t.Fatal(err)
			}

			{
				got := []string{}
				for _, dd := range nom.Calls.DropData {
					got = append(got, dd...)
				}

				if !cmp.SliceContentEq(then.WantNominatorDropData, got) {
					t.Errorf("unexpected nominator drop data: %v", got)
				}
			}

			{
				runIdsToBeDeleted := []string{when.RunId}
				runIdsToBeDeleted = append(runIdsToBeDeleted, then.RunIdsShouldBeDeletedAdditional...)
				got := try.To(
					scanner.New[tables.Run]().QueryAll(
						ctx, conn,
						`select * from "run" where "run_id" = any($1)`,
						runIdsToBeDeleted,
					),
				).OrFatal(t)
				if len(got) != 0 {
					t.Errorf("unexpected run: %v", got)
				}
			}
		}
	}

	t.Run("gen1/deactivated", shouldBeDeleted(
		When{RunId: th.Padding36("gen1/deactivated")},
		Then{},
	))

	t.Run("gen1/waiting", shouldBeDeleted(
		When{RunId: th.Padding36("gen1/waiting")},
		Then{},
	))

	t.Run("gen1/failed", shouldBeDeleted(
		When{RunId: th.Padding36("gen1/failed")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen1/failed/:out/1"),
			},
		},
	))

	t.Run("gen1/done-leaf", shouldBeDeleted(
		When{RunId: th.Padding36("gen1/done-leaf")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen1/done-leaf/:out/1"),
			},
		},
	))

	t.Run("gen1/done-leaf-with-invalidated", shouldBeDeleted(
		When{RunId: th.Padding36("gen1/done-leaf-with-invalidated")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen1/done-leaf-with-invalidated/:out/1"),
			},
			RunIdsShouldBeDeletedAdditional: []string{
				th.Padding36("gen2/invalidated"),
			},
		},
	))

	shouldBeProtected := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			p := poolBroaker.GetPool(ctx, t)
			if err := given.Apply(ctx, p); err != nil {
				t.Fatal(err)
			}
			conn := try.To(p.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			wrapped := proxy.Wrap(p)
			wrapped.Events().Events().Query.After(func() {
				lockedRunIds := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`
						with "all" as (
							select "run_id" from "run"
						),
						"unlocked" as (
							select "run_id" from "run" for update skip locked
						)
						select "run_id" from "all"
						except
						select "run_id" from "unlocked"
						`,
					),
				).OrFatal(t)

				if !cmp.SliceContentEq([]string{when.RunId}, lockedRunIds) {
					t.Errorf("unexpected locked run ids: %v", lockedRunIds)
				}
			})

			nom := nom_mock.New(t)
			nom.Impl.DropData = func(_ context.Context, _ pool.Tx, knitIds []string) error {
				return nil
			}

			beforeRuns := try.To(
				scanner.New[string]().QueryAll(
					ctx, conn,
					`select "run_id" from "run"`,
				),
			).OrFatal(t)

			beforeData := try.To(
				scanner.New[string]().QueryAll(
					ctx, conn,
					`select "knit_id" from "data"`,
				),
			).OrFatal(t)

			testee := run.New(wrapped, run.WithNominator(nom))

			if err := testee.Delete(ctx, when.RunId); !errors.Is(err, then.ReasonNotDeleted) {
				t.Errorf("unexpected error: %v", err)
			}

			{
				got := []string{}
				for _, dd := range nom.Calls.DropData {
					got = append(got, dd...)
				}

				if len(got) != 0 {
					t.Errorf("unexpected nominator drop data: %v", got)
				}
			}

			{
				afterRuns := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn, `select "run_id" from "run"`,
					),
				).OrFatal(t)

				if !cmp.SliceContentEq(beforeRuns, afterRuns) {
					t.Errorf("unexpected runs: %v", afterRuns)
				}
			}

			{
				afterData := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn, `select "knit_id" from "data"`,
					),
				).OrFatal(t)

				if !cmp.SliceContentEq(beforeData, afterData) {
					t.Errorf("unexpected data: %v", afterData)
				}
			}
		}
	}

	t.Run("gen1/ready", shouldBeProtected(
		When{RunId: th.Padding36("gen1/ready")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen1/starting", shouldBeProtected(
		When{RunId: th.Padding36("gen1/starting")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen1/running", shouldBeProtected(
		When{RunId: th.Padding36("gen1/running")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen1/aborting", shouldBeProtected(
		When{RunId: th.Padding36("gen1/aborting")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen1/completing", shouldBeProtected(
		When{RunId: th.Padding36("gen1/completing")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen1/done-protected", shouldBeProtected(
		When{RunId: th.Padding36("gen1/done-protected")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen2/ready", shouldBeProtected(
		When{RunId: th.Padding36("gen2/ready")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen2/starting", shouldBeProtected(
		When{RunId: th.Padding36("gen2/starting")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen2/running", shouldBeProtected(
		When{RunId: th.Padding36("gen2/running")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen2/aborting", shouldBeProtected(
		When{RunId: th.Padding36("gen2/aborting")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen2/completing", shouldBeProtected(
		When{RunId: th.Padding36("gen2/completing")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen2/done-protected", shouldBeProtected(
		When{RunId: th.Padding36("gen2/done-protected")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen3/ready", shouldBeProtected(
		When{RunId: th.Padding36("gen3/ready")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen3/starting", shouldBeProtected(
		When{RunId: th.Padding36("gen3/starting")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen3/running", shouldBeProtected(
		When{RunId: th.Padding36("gen3/running")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen3/aborting", shouldBeProtected(
		When{RunId: th.Padding36("gen3/aborting")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen3/completing", shouldBeProtected(
		When{RunId: th.Padding36("gen3/completing")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen3/done-with-dataagent", shouldBeProtected(
		When{RunId: th.Padding36("gen3/done-with-dataagent")},
		Then{ReasonNotDeleted: kdb.ErrRunIsProtected},
	))

	t.Run("gen1/invalidated", shouldBeProtected(
		When{RunId: th.Padding36("gen1/invalidated")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen1/invalidated/:out/1"),
			},
			ReasonNotDeleted: kdb.ErrMissing,
		},
	))

	t.Run("gen2/invalidated", shouldBeProtected(
		When{RunId: th.Padding36("gen2/invalidated")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen2/invalidated/:out/1"),
			},
			ReasonNotDeleted: kdb.ErrMissing,
		},
	))

	t.Run("gen3/invalidated", shouldBeProtected(
		When{RunId: th.Padding36("gen3/invalidated")},
		Then{
			WantNominatorDropData: []string{
				th.Padding36("gen3/invalidated/:out/1"),
			},
			ReasonNotDeleted: kdb.ErrMissing,
		},
	))

	shouldError := func(when When, _ Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			p := poolBroaker.GetPool(ctx, t)
			if err := given.Apply(ctx, p); err != nil {
				t.Fatal(err)
			}
			conn := try.To(p.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			wrapped := proxy.Wrap(p)
			nom := nom_mock.New(t)
			nom.Impl.DropData = func(_ context.Context, _ pool.Tx, knitIds []string) error {
				return when.NominatorDropDataError
			}

			runBefore := try.To(
				scanner.New[string]().QueryAll(
					ctx, conn,
					`select "run_id" from "run"`,
				),
			).OrFatal(t)

			dataBefore := try.To(
				scanner.New[string]().QueryAll(
					ctx, conn,
					`select "knit_id" from "data"`,
				),
			).OrFatal(t)

			testee := run.New(wrapped, run.WithNominator(nom))

			if err := testee.Delete(ctx, when.RunId); !errors.Is(err, when.NominatorDropDataError) {
				t.Errorf("unexpected error: %v", err)
			}
			{
				runAfter := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "run_id" from "run"`,
					),
				).OrFatal(t)

				if !cmp.SliceContentEq(runBefore, runAfter) {
					t.Errorf("unexpected runs: %v", runAfter)
				}
			}

			{
				dataAfter := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "knit_id" from "data"`,
					),
				).OrFatal(t)

				if !cmp.SliceContentEq(dataBefore, dataAfter) {
					t.Errorf("unexpected data: %v", dataAfter)
				}
			}
		}
	}

	t.Run("when nominator cause error during deleting gen1/done-leaf", shouldError(
		When{
			RunId:                  th.Padding36("gen1/done-leaf"),
			NominatorDropDataError: errors.New("nominator cause error"),
		},
		Then{},
	))

	t.Run("when nominator cause error during deleting gen2/done-leaf-with-invalidated", shouldError(
		When{
			RunId:                  th.Padding36("gen2/done-leaf-with-invalidated"),
			NominatorDropDataError: errors.New("nominator cause error"),
		},
		Then{},
	))

	t.Run("when nominator cause error during deleting gen3/failed", shouldError(
		When{
			RunId:                  th.Padding36("gen3/failed"),
			NominatorDropDataError: errors.New("nominator cause error"),
		},
		Then{},
	))
}
