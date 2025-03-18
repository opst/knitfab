package dataset

import (
	"time"

	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils/pointer"
)

var GivenDatabase = tables.Operation{
	Plan: []tables.Plan{
		{
			PlanId: th.Padding36("plan-1-pseudo"),
			Active: true, Hash: th.Padding36("#plan-1-pseudo"),
		},
		{
			PlanId: th.Padding36("plan-2-image"),
			Active: true, Hash: th.Padding36("#plan-2-image"),
		},
		{
			PlanId: th.Padding36("plan-3-image"),
			Active: true, Hash: th.Padding36("#plan-3-image"),
		},
	},
	PlanPseudo: []tables.PlanPseudo{
		{
			PlanId: th.Padding36("plan-1-pseudo"),
			Name:   "pseudo",
		},
	},
	PlanImage: []tables.PlanImage{
		{
			PlanId: th.Padding36("plan-2-image"),
			Image:  "image", Version: "v1.2",
		},
		{
			PlanId: th.Padding36("plan-3-image"),
			Image:  "image", Version: "v1.3",
		},
	},
	Inputs: map[tables.Input]tables.InputAttr{
		{
			PlanId:  th.Padding36("plan-2-image"),
			InputId: 2_100, Path: "/in/1",
		}: {},
		{
			PlanId:  th.Padding36("plan-3-image"),
			InputId: 3_100, Path: "/in/1",
		}: {},
	},
	Outputs: map[tables.Output]tables.OutputAttr{
		{
			PlanId:   th.Padding36("plan-1-pseudo"),
			OutputId: 1_010, Path: "/out/1",
		}: {},
		{
			PlanId:   th.Padding36("plan-2-image"),
			OutputId: 2_010, Path: "/out/1",
		}: {},
		{
			PlanId:   th.Padding36("plan-3-image"),
			OutputId: 3_010, Path: "/out/1",
		}: {},
	},
	Steps: []tables.Step{
		{
			Run: tables.Run{
				RunId:                 th.Padding36("plan-1-pseudo/run-deactivated"),
				PlanId:                th.Padding36("plan-1-pseudo"),
				Status:                domain.Deactivated,
				UpdatedAt:             time.Now().Add(-time.Hour),
				LifecycleSuspendUntil: time.Now().Add(-time.Hour),
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-1-pseudo/run-deactivated/out/1"),
					RunId:    th.Padding36("plan-1-pseudo/run-deactivated"),
					PlanId:   th.Padding36("plan-1-pseudo"),
					OutputId: 1_010,
				}: {
					VolumeRef: "plan-1/run-dectivted/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:                 th.Padding36("plan-1-pseudo/run-waiting"),
				PlanId:                th.Padding36("plan-1-pseudo"),
				Status:                domain.Waiting,
				UpdatedAt:             time.Now().Add(-time.Hour),
				LifecycleSuspendUntil: time.Now().Add(-time.Hour),
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-1-pseudo/run-waiting/out/1"),
					RunId:    th.Padding36("plan-1-pseudo/run-waiting"),
					PlanId:   th.Padding36("plan-1-pseudo"),
					OutputId: 1_010,
				}: {
					VolumeRef: "plan-1/run-waiting/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:                 th.Padding36("plan-1-pseudo/run-ready"),
				PlanId:                th.Padding36("plan-1-pseudo"),
				Status:                domain.Ready,
				UpdatedAt:             time.Now().Add(-time.Hour),
				LifecycleSuspendUntil: time.Now().Add(-time.Hour),
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-1-pseudo/run-ready/out/1"),
					RunId:    th.Padding36("plan-1-pseudo/run-ready"),
					PlanId:   th.Padding36("plan-1-pseudo"),
					OutputId: 1_010,
				}: {
					VolumeRef: "plan-1/run-ready/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:                 th.Padding36("plan-1-pseudo/run-starting"),
				PlanId:                th.Padding36("plan-1-pseudo"),
				Status:                domain.Starting,
				UpdatedAt:             time.Now().Add(-time.Hour),
				LifecycleSuspendUntil: time.Now().Add(-time.Hour),
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-1-pseudo/run-starting/out/1"),
					RunId:    th.Padding36("plan-1-pseudo/run-starting"),
					PlanId:   th.Padding36("plan-1-pseudo"),
					OutputId: 1_010,
				}: {
					VolumeRef: "plan-1/run-starting/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:                 th.Padding36("plan-1-pseudo/run-running"),
				PlanId:                th.Padding36("plan-1-pseudo"),
				Status:                domain.Running,
				UpdatedAt:             time.Now().Add(-time.Hour),
				LifecycleSuspendUntil: time.Now().Add(-time.Hour),
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-1-pseudo/run-running/out/1"),
					RunId:    th.Padding36("plan-1-pseudo/run-running"),
					PlanId:   th.Padding36("plan-1-pseudo"),
					OutputId: 1_010,
				}: {
					VolumeRef: "plan-1/run-running/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:                 th.Padding36("plan-1-pseudo/run-completing"),
				PlanId:                th.Padding36("plan-1-pseudo"),
				Status:                domain.Completing,
				UpdatedAt:             time.Now().Add(-time.Hour),
				LifecycleSuspendUntil: time.Now().Add(-time.Hour),
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-1-pseudo/run-completing/out/1"),
					RunId:    th.Padding36("plan-1-pseudo/run-completing"),
					PlanId:   th.Padding36("plan-1-pseudo"),
					OutputId: 1_010,
				}: {
					VolumeRef: "plan-1/run-completing/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:                 th.Padding36("plan-1-pseudo/run-aborting"),
				PlanId:                th.Padding36("plan-1-pseudo"),
				Status:                domain.Aborting,
				UpdatedAt:             time.Now().Add(-time.Hour),
				LifecycleSuspendUntil: time.Now().Add(-time.Hour),
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-1-pseudo/run-aborting/out/1"),
					RunId:    th.Padding36("plan-1-pseudo/run-aborting"),
					PlanId:   th.Padding36("plan-1-pseudo"),
					OutputId: 1_010,
				}: {
					VolumeRef: "plan-1/run-aborting/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:                 th.Padding36("plan-1-pseudo/run-done"),
				PlanId:                th.Padding36("plan-1-pseudo"),
				Status:                domain.Done,
				UpdatedAt:             time.Now().Add(-time.Hour),
				LifecycleSuspendUntil: time.Now().Add(-time.Hour),
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-1-pseudo/run-done/out/1"),
					RunId:    th.Padding36("plan-1-pseudo/run-done"),
					PlanId:   th.Padding36("plan-1-pseudo"),
					OutputId: 1_010,
				}: {
					VolumeRef: "plan-1/run-done/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:                 th.Padding36("plan-1-pseudo/run-done-leaf"),
				PlanId:                th.Padding36("plan-1-pseudo"),
				Status:                domain.Done,
				UpdatedAt:             time.Now().Add(-time.Hour),
				LifecycleSuspendUntil: time.Now().Add(-time.Hour),
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-1-pseudo/run-done-leaf/out/1"),
					RunId:    th.Padding36("plan-1-pseudo/run-done-leaf"),
					PlanId:   th.Padding36("plan-1-pseudo"),
					OutputId: 1_010,
				}: {
					VolumeRef: "plan-1/run-done-leaf/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:                 th.Padding36("plan-1-pseudo/run-failed"),
				PlanId:                th.Padding36("plan-1-pseudo"),
				Status:                domain.Failed,
				UpdatedAt:             time.Now().Add(-time.Hour),
				LifecycleSuspendUntil: time.Now().Add(-time.Hour),
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-1-pseudo/run-failed/out/1"),
					RunId:    th.Padding36("plan-1-pseudo/run-failed"),
					PlanId:   th.Padding36("plan-1-pseudo"),
					OutputId: 1_010,
				}: {
					VolumeRef: "plan-1/run-failed/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:                 th.Padding36("plan-1-pseudo/run-invalidated"),
				PlanId:                th.Padding36("plan-1-pseudo"),
				Status:                domain.Invalidated,
				UpdatedAt:             time.Now().Add(-time.Hour),
				LifecycleSuspendUntil: time.Now().Add(-time.Hour),
			},
		},

		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-2-image/run-deactivated"),
				PlanId:    th.Padding36("plan-2-image"),
				Status:    domain.Deactivated,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
					RunId:   th.Padding36("plan-2-image/run-deactivated"),
					PlanId:  th.Padding36("plan-2-image"),
					InputId: 2_100,
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-2-image/run-deactivated/out/1"),
					RunId:    th.Padding36("plan-2-image/run-deactivated"),
					PlanId:   th.Padding36("plan-2-image"),
					OutputId: 2_010,
				}: {
					VolumeRef: "plan-2/run-deactivated/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-2-image/run-waiting"),
				PlanId:    th.Padding36("plan-2-image"),
				Status:    domain.Waiting,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
					RunId:   th.Padding36("plan-2-image/run-waiting"),
					PlanId:  th.Padding36("plan-2-image"),
					InputId: 2_100,
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-2-image/run-waiting/out/1"),
					RunId:    th.Padding36("plan-2-image/run-waiting"),
					PlanId:   th.Padding36("plan-2-image"),
					OutputId: 2_010,
				}: {
					VolumeRef: "plan-2/run-waiting/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-2-image/run-ready"),
				PlanId:    th.Padding36("plan-2-image"),
				Status:    domain.Ready,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
					RunId:   th.Padding36("plan-2-image/run-ready"),
					PlanId:  th.Padding36("plan-2-image"),
					InputId: 2_100,
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-2-image/run-ready/out/1"),
					RunId:    th.Padding36("plan-2-image/run-ready"),
					PlanId:   th.Padding36("plan-2-image"),
					OutputId: 2_010,
				}: {
					VolumeRef: "plan-2/run-ready/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-2-image/run-starting"),
				PlanId:    th.Padding36("plan-2-image"),
				Status:    domain.Starting,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
					RunId:   th.Padding36("plan-2-image/run-starting"),
					PlanId:  th.Padding36("plan-2-image"),
					InputId: 2_100,
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-2-image/run-starting/out/1"),
					RunId:    th.Padding36("plan-2-image/run-starting"),
					PlanId:   th.Padding36("plan-2-image"),
					OutputId: 2_010,
				}: {
					VolumeRef: "plan-2/run-starting/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-2-image/run-running"),
				PlanId:    th.Padding36("plan-2-image"),
				Status:    domain.Running,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
					RunId:   th.Padding36("plan-2-image/run-running"),
					PlanId:  th.Padding36("plan-2-image"),
					InputId: 2_100,
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-2-image/run-running/out/1"),
					RunId:    th.Padding36("plan-2-image/run-running"),
					PlanId:   th.Padding36("plan-2-image"),
					OutputId: 2_010,
				}: {
					VolumeRef: "plan-2/run-running/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-2-image/run-completing"),
				PlanId:    th.Padding36("plan-2-image"),
				Status:    domain.Completing,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
					RunId:   th.Padding36("plan-2-image/run-completing"),
					PlanId:  th.Padding36("plan-2-image"),
					InputId: 2_100,
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-2-image/run-completing/out/1"),
					RunId:    th.Padding36("plan-2-image/run-completing"),
					PlanId:   th.Padding36("plan-2-image"),
					OutputId: 2_010,
				}: {
					VolumeRef: "plan-2/run-completing/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-2-image/run-aborting"),
				PlanId:    th.Padding36("plan-2-image"),
				Status:    domain.Aborting,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
					RunId:   th.Padding36("plan-2-image/run-aborting"),
					PlanId:  th.Padding36("plan-2-image"),
					InputId: 2_100,
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-2-image/run-aborting/out/1"),
					RunId:    th.Padding36("plan-2-image/run-aborting"),
					PlanId:   th.Padding36("plan-2-image"),
					OutputId: 2_010,
				}: {
					VolumeRef: "plan-2/run-aborting/out/1",
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-2-image/run-done"),
				PlanId:    th.Padding36("plan-2-image"),
				Status:    domain.Done,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Exit: &tables.RunExit{
				RunId:    th.Padding36("plan-2-image/run-done"),
				ExitCode: 0,
				Message:  "done",
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
					RunId:   th.Padding36("plan-2-image/run-done"),
					PlanId:  th.Padding36("plan-2-image"),
					InputId: 2_100,
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-2-image/run-done/out/1"),
					RunId:    th.Padding36("plan-2-image/run-done"),
					PlanId:   th.Padding36("plan-2-image"),
					OutputId: 2_010,
				}: {
					VolumeRef: "plan-2/run-done/out/1",
					Timestamp: pointer.Ref(time.Now()),
					UserTag:   []domain.Tag{{Key: "key", Value: "value"}},
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-2-image/run-done-purged"),
				PlanId:    th.Padding36("plan-2-image"),
				Status:    domain.Done,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Exit: &tables.RunExit{
				RunId:    th.Padding36("plan-2-image/run-done-purged"),
				ExitCode: 0,
				Message:  "done",
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
					RunId:   th.Padding36("plan-2-image/run-done-purged"),
					PlanId:  th.Padding36("plan-2-image"),
					InputId: 2_100,
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-2-image/run-done-purged/out/1"),
					RunId:    th.Padding36("plan-2-image/run-done"),
					PlanId:   th.Padding36("plan-2-image"),
					OutputId: 2_010,
				}: {
					Timestamp: pointer.Ref(time.Now()),
					UserTag:   []domain.Tag{{Key: "key", Value: "value"}},
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-2-image/run-failed"),
				PlanId:    th.Padding36("plan-2-image"),
				Status:    domain.Failed,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Exit: &tables.RunExit{
				RunId:    th.Padding36("plan-2-image/run-failed"),
				ExitCode: 2,
				Message:  "Error",
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
					RunId:   th.Padding36("plan-2-image/run-failed"),
					PlanId:  th.Padding36("plan-2-image"),
					InputId: 2_100,
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-2-image/run-failed/out/1"),
					RunId:    th.Padding36("plan-2-image/run-failed"),
					PlanId:   th.Padding36("plan-2-image"),
					OutputId: 2_010,
				}: {
					VolumeRef: "plan-2/run-failed/out/1",
					Timestamp: pointer.Ref(time.Now()),
					UserTag:   []domain.Tag{{Key: "key", Value: "value"}},
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-2-image/run-invalidated"),
				PlanId:    th.Padding36("plan-2-image"),
				Status:    domain.Invalidated,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Exit: &tables.RunExit{
				RunId:    th.Padding36("plan-2-image/run-invalidated"),
				ExitCode: 0,
				Message:  "done",
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-1-pseudo/run-done/out/1"),
					RunId:   th.Padding36("plan-2-image/run-invalidated"),
					PlanId:  th.Padding36("plan-2-image"),
					InputId: 2_100,
				},
			},
		},

		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-3-image/run-done-leaf"),
				PlanId:    th.Padding36("plan-3-image"),
				Status:    domain.Done,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Exit: &tables.RunExit{
				RunId:    th.Padding36("plan-3-image/run-done-leaf"),
				ExitCode: 0,
				Message:  "done",
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-2-image/run-done/out/1"),
					RunId:   th.Padding36("plan-3-image/run-done-leaf"),
					PlanId:  th.Padding36("plan-3-image"),
					InputId: 3_100,
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-3-image/run-done-leaf/out/1"),
					RunId:    th.Padding36("plan-3-image/run-done-leaf"),
					PlanId:   th.Padding36("plan-3-image"),
					OutputId: 3_010,
				}: {
					VolumeRef: "plan-3/run-done-leaf/out/1",
					Timestamp: pointer.Ref(time.Now()),
					UserTag:   []domain.Tag{{Key: "key", Value: "value"}},
				},
			},
		},
		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-3-image/run-done"),
				PlanId:    th.Padding36("plan-3-image"),
				Status:    domain.Done,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Exit: &tables.RunExit{
				RunId:    th.Padding36("plan-3-image/run-done"),
				ExitCode: 0,
				Message:  "done",
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-2-image/run-done/out/1"),
					RunId:   th.Padding36("plan-3-image/run-done"),
					PlanId:  th.Padding36("plan-3-image"),
					InputId: 3_100,
				},
			},
			Outcomes: map[tables.Data]tables.DataAttibutes{
				{
					KnitId:   th.Padding36("plan-3-image/run-done/out/1"),
					RunId:    th.Padding36("plan-3-image/run-done"),
					PlanId:   th.Padding36("plan-3-image"),
					OutputId: 3_010,
				}: {
					VolumeRef: "plan-3/run-done/out/1",
					Timestamp: pointer.Ref(time.Now()),
					UserTag:   []domain.Tag{{Key: "key", Value: "value"}},
				},
			},
		},

		{
			Run: tables.Run{
				RunId:     th.Padding36("plan-3-image/run-invalidated"),
				PlanId:    th.Padding36("plan-3-image"),
				Status:    domain.Invalidated,
				UpdatedAt: time.Now().Add(-time.Hour),
			},
			Exit: &tables.RunExit{
				RunId:    th.Padding36("plan-3-image/run-invalidated"),
				ExitCode: 0,
				Message:  "done",
			},
			Assign: []tables.Assign{
				{
					KnitId:  th.Padding36("plan-3-image/run-done/out/1"),
					RunId:   th.Padding36("plan-3-image/run-invalidated"),
					PlanId:  th.Padding36("plan-3-image"),
					InputId: 3_100,
				},
			},
		},
	},
}
