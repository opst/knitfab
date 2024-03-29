package internal

import (
	"testing"
	"time"

	kdb "github.com/opst/knitfab/pkg/db"
	th "github.com/opst/knitfab/pkg/db/postgres/testhelpers"

	"github.com/opst/knitfab/pkg/db/postgres/tables"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/try"
)

type Given struct {
	Plans       tables.Operation
	Runs        tables.Operation
	ExpectedRun map[string]kdb.Run
}

var (
	PseudoActive   = kdb.PseudoPlanName("pseudo-active")
	PseudoInactive = kdb.PseudoPlanName("pseudo-inactive")
)

func Testdata(t *testing.T, NOW time.Time) Given {
	plans := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active")},
			{PlanId: th.Padding36("plan-pseudo-inactive"), Active: false, Hash: th.Padding64("#pseudo-inactive")},
			{PlanId: th.Padding36("plan-image-waiting"), Active: true, Hash: th.Padding64("#image-waiting")},
			{PlanId: th.Padding36("plan-image-ready"), Active: true, Hash: th.Padding64("#image-ready")},
			{PlanId: th.Padding36("plan-image-starting"), Active: true, Hash: th.Padding64("#image-starting")},
			{PlanId: th.Padding36("plan-image-running"), Active: true, Hash: th.Padding64("#image-running")},
			{PlanId: th.Padding36("plan-image-aborting"), Active: true, Hash: th.Padding64("#image-aborting")},
			{PlanId: th.Padding36("plan-image-completing"), Active: true, Hash: th.Padding64("#image-completing")},
			{PlanId: th.Padding36("plan-image-failed"), Active: true, Hash: th.Padding64("#image-failed")},
			{PlanId: th.Padding36("plan-image-done"), Active: true, Hash: th.Padding64("#image-done")},
			{PlanId: th.Padding36("plan-image-invalidated"), Active: true, Hash: th.Padding64("#image-invalidated")},
			{PlanId: th.Padding36("plan-image-deactivated"), Active: false, Hash: th.Padding64("#image-deactivated")},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: th.Padding36("plan-pseudo-active"), Name: PseudoActive.String()},
			{PlanId: th.Padding36("plan-pseudo-inactive"), Name: PseudoInactive.String()},
		},
		PlanImage: []tables.PlanImage{
			{PlanId: th.Padding36("plan-image-waiting"), Image: "repo.invalid/example", Version: "waiting"},
			{PlanId: th.Padding36("plan-image-ready"), Image: "repo.invalid/example", Version: "ready"},
			{PlanId: th.Padding36("plan-image-starting"), Image: "repo.invalid/example", Version: "starting"},
			{PlanId: th.Padding36("plan-image-running"), Image: "repo.invalid/example", Version: "running"},
			{PlanId: th.Padding36("plan-image-aborting"), Image: "repo.invalid/example", Version: "aborting"},
			{PlanId: th.Padding36("plan-image-completing"), Image: "repo.invalid/example", Version: "completing"},
			{PlanId: th.Padding36("plan-image-failed"), Image: "repo.invalid/example", Version: "failed"},
			{PlanId: th.Padding36("plan-image-done"), Image: "repo.invalid/example", Version: "done"},
			{PlanId: th.Padding36("plan-image-invalidated"), Image: "repo.invalid/example", Version: "invalidated"},
			{PlanId: th.Padding36("plan-image-deactivated"), Image: "repo.invalid/example", Version: "deactivated"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 101_010, PlanId: th.Padding36("plan-pseudo-active"), Path: "/out"}:   {},
			{OutputId: 102_010, PlanId: th.Padding36("plan-pseudo-inactive"), Path: "/out"}: {},

			{OutputId: 201_010, PlanId: th.Padding36("plan-image-waiting"), Path: "/out"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "waiting"}, {Key: "mode", Value: "out"}},
			},
			{OutputId: 201_001, PlanId: th.Padding36("plan-image-waiting"), Path: "/log"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "waiting"}, {Key: "mode", Value: "log"}},
				IsLog:   true,
			},

			{OutputId: 202_010, PlanId: th.Padding36("plan-image-ready"), Path: "/out"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "ready"}, {Key: "mode", Value: "out"}},
			},
			{OutputId: 202_001, PlanId: th.Padding36("plan-image-ready"), Path: "/log"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "ready"}, {Key: "mode", Value: "log"}},
				IsLog:   true,
			},

			{OutputId: 203_010, PlanId: th.Padding36("plan-image-starting"), Path: "/out"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "starting"}, {Key: "mode", Value: "out"}},
			},

			{OutputId: 204_010, PlanId: th.Padding36("plan-image-running"), Path: "/out"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "running"}, {Key: "mode", Value: "out"}},
			},

			{OutputId: 205_001, PlanId: th.Padding36("plan-image-aborting"), Path: "/log"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "aborting"}, {Key: "mode", Value: "log"}},
				IsLog:   true,
			},

			{OutputId: 206_001, PlanId: th.Padding36("plan-image-completing"), Path: "/log"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "completing"}, {Key: "mode", Value: "log"}},
				IsLog:   true,
			},

			{OutputId: 207_010, PlanId: th.Padding36("plan-image-failed"), Path: "/out"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "failed"}, {Key: "mode", Value: "out"}},
			},
			{OutputId: 207_001, PlanId: th.Padding36("plan-image-failed"), Path: "/log"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "failed"}, {Key: "mode", Value: "log"}},
				IsLog:   true,
			},

			{OutputId: 208_010, PlanId: th.Padding36("plan-image-done"), Path: "/out"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "done"}, {Key: "mode", Value: "out"}},
			},
			{OutputId: 208_001, PlanId: th.Padding36("plan-image-done"), Path: "/log"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "done"}, {Key: "mode", Value: "log"}},
				IsLog:   true,
			},

			{OutputId: 209_010, PlanId: th.Padding36("plan-image-invalidated"), Path: "/out"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "invalidated"}, {Key: "mode", Value: "out"}},
			},
			{OutputId: 209_001, PlanId: th.Padding36("plan-image-invalidated"), Path: "log"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "invalidated"}, {Key: "mode", Value: "log"}},
				IsLog:   true,
			},

			{OutputId: 210_001, PlanId: th.Padding36("plan-image-deactivated"), Path: "/log"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "deactivated"}, {Key: "mode", Value: "log"}},
				IsLog:   true,
			},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			{InputId: 201_100, PlanId: th.Padding36("plan-image-waiting"), Path: "/in"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "waiting"}, {Key: "mode", Value: "in"}},
				KnitId:  []string{th.Padding36("knit@pseudo-done-1")},
			},
			{InputId: 202_100, PlanId: th.Padding36("plan-image-ready"), Path: "/in"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "ready"}, {Key: "mode", Value: "in"}},
				Timestamp: []time.Time{
					try.To(rfctime.ParseRFC3339DateTime("2021-10-11T11:12:13+09:00")).OrFatal(t).Time(),
				},
			},
			{InputId: 203_100, PlanId: th.Padding36("plan-image-starting"), Path: "/in"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "starting"}, {Key: "mode", Value: "in"}},
				KnitId:  []string{th.Padding36("knit@pseudo-done-1")},
				Timestamp: []time.Time{
					try.To(rfctime.ParseRFC3339DateTime("2021-10-11T11:12:13+09:00")).OrFatal(t).Time(),
				},
			},
			{InputId: 204_100, PlanId: th.Padding36("plan-image-running"), Path: "/in"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "running"}, {Key: "mode", Value: "in"}},
			},
			{InputId: 205_100, PlanId: th.Padding36("plan-image-aborting"), Path: "/in"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "aborting"}, {Key: "mode", Value: "in"}},
				KnitId:  []string{th.Padding36("knit@pseudo-done-2")},
			},
			{InputId: 206_100, PlanId: th.Padding36("plan-image-completing"), Path: "/in"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "completeing"}, {Key: "mode", Value: "in"}},
			},
			{InputId: 207_100, PlanId: th.Padding36("plan-image-failed"), Path: "/in"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "failed"}, {Key: "mode", Value: "in"}},
				KnitId:  []string{th.Padding36("knit@pseudo-done-1")},
				Timestamp: []time.Time{
					try.To(rfctime.ParseRFC3339DateTime("2021-10-11T11:12:13+09:00")).OrFatal(t).Time(),
				},
			},
			{InputId: 208_100, PlanId: th.Padding36("plan-image-done"), Path: "/in"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "done"}, {Key: "mode", Value: "in"}},
			},
			{InputId: 209_100, PlanId: th.Padding36("plan-image-invalidated"), Path: "/in"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "invalidated"}, {Key: "mode", Value: "in"}},
				KnitId:  []string{th.Padding36("knit@pseudo-done-2")},
			},
			{InputId: 210_100, PlanId: th.Padding36("plan-image-deactivated"), Path: "/in"}: {
				UserTag: []kdb.Tag{{Key: "type", Value: "deactivated"}, {Key: "mode", Value: "in"}},
				Timestamp: []time.Time{
					try.To(rfctime.ParseRFC3339DateTime("2021-10-11T11:12:13+09:00")).OrFatal(t).Time(),
				},
			},
		},
	}

	runs := tables.Operation{
		Steps: []tables.Step{
			//
			// pseudo runs
			//
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-waiting-1"),
					Status:                kdb.Waiting,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-waiting-1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-waiting-1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-waiting-1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-waiting+1"),
					Status:                kdb.Waiting,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-waiting+1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-waiting+1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-waiting+1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-ready-1"),
					Status:                kdb.Ready,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-ready-1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-ready-1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-ready-1",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "ready"},
							{Key: "trigger", Value: "user upload"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-ready+1"),
					Status:                kdb.Ready,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-ready+1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-ready+1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-ready+1",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "ready"},
							{Key: "trigger", Value: "user upload"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-starting-1"),
					PlanId:                th.Padding36("plan-pseudo-active"),
					Status:                kdb.Starting,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-starting-1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-starting-1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-starting-1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-starting+1"),
					PlanId:                th.Padding36("plan-pseudo-active"),
					Status:                kdb.Starting,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-starting+1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-starting+1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-starting+1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-running-1"),
					Status:                kdb.Running,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-running-1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-running-1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-running-1",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "running"},
							{Key: "trigger", Value: "user upload"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-running+1"),
					Status:                kdb.Running,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-running+1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-running+1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-running+1",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "running"},
							{Key: "trigger", Value: "user upload"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-aborting-1"),
					Status:                kdb.Aborting,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-aborting-1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-aborting-1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-aborting-1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-aborting+1"),
					Status:                kdb.Aborting,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-aborting+1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-aborting+1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-aborting+1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-completing-1"),
					Status:                kdb.Completing,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-completing-1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-completing-1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-completing-1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-completing+1"),
					Status:                kdb.Completing,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-completing+1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-completing+1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-completing+1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-failed-1"),
					Status:                kdb.Failed,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-failed-1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-failed-1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-failed-1",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "failed"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-12T13:14:21.679+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-failed+1"),
					Status:                kdb.Failed,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-failed+1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-failed+1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-failed+1",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "failed"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-12T13:14:21.679+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-done-1"),
					Status:                kdb.Done,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-done-1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-done-1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-done-1",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "done"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-12T13:14:22.679+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-done+1"),
					Status:                kdb.Done,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-done+1"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-done+1"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-done+1",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "done"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-12T13:14:22.679+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-invalidated-1"),
					Status:                kdb.Invalidated,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{}, // empty
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-invalidated+1"),
					Status:                kdb.Invalidated,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{}, // empty
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-deactivated-1"),
					Status:                kdb.Deactivated,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-inactive"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-deactivated-1"),
						OutputId:  102_010,
						RunId:     th.Padding36("run@pseudo-deactivated-1"),
						PlanId:    th.Padding36("plan-pseudo-inactive"),
						VolumeRef: "*pseudo-deactivated-1",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-deactivated+1"),
					Status:                kdb.Deactivated,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-inactive"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-deactivated+1"),
						OutputId:  102_010,
						RunId:     th.Padding36("run@pseudo-deactivated+1"),
						PlanId:    th.Padding36("plan-pseudo-inactive"),
						VolumeRef: "*pseudo-deactivated+1",
					}: {},
				},
			},

			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-waiting-2"),
					Status:                kdb.Waiting,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-waiting-2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-waiting-2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-waiting-2",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "invalidated"},
							{Key: "reason", Value: "waiting for large storage"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-waiting+2"),
					Status:                kdb.Waiting,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-waiting+2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-waiting+2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-waiting+2",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "invalidated"},
							{Key: "reason", Value: "waiting for large storage"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-ready-2"),
					Status:                kdb.Ready,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-ready-2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-ready-2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-ready-2",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "ready"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-ready+2"),
					Status:                kdb.Ready,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-ready+2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-ready+2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-ready+2",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "ready"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-starting-2"),
					Status:                kdb.Starting,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-starting-2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-starting-2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-starting-2",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-starting+2"),
					Status:                kdb.Starting,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-starting+2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-starting+2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-starting+2",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-running-2"),
					Status:                kdb.Running,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-running-2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-running-2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-running-2",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "running"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-running+2"),
					Status:                kdb.Running,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-running+2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-running+2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-running+2",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "running"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-aborting-2"),
					Status:                kdb.Aborting,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-aborting-2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-aborting-2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-aborting-2",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-aborting+2"),
					Status:                kdb.Aborting,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
					PlanId:                th.Padding36("plan-pseudo-active"),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-aborting+2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-aborting+2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-aborting+2",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-completing-2"),
					PlanId:                th.Padding36("plan-pseudo-active"),
					Status:                kdb.Completing,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-completing-2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-completing-2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-completing-2",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "completing"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-completing+2"),
					PlanId:                th.Padding36("plan-pseudo-active"),
					Status:                kdb.Completing,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-completing+2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-completing+2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-completing+2",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "completing"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-failed-2"),
					PlanId:                th.Padding36("plan-pseudo-active"),
					Status:                kdb.Failed,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-failed-2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-failed-2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-failed-2",
					}: {
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-12T13:15:21.679+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-failed+2"),
					PlanId:                th.Padding36("plan-pseudo-active"),
					Status:                kdb.Failed,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-failed+2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-failed+2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-failed+2",
					}: {
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-12T13:15:21.679+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-done-2"),
					PlanId:                th.Padding36("plan-pseudo-active"),
					Status:                kdb.Done,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-done-2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-done-2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-done-2",
					}: {
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-12T13:15:22.679+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-done+2"),
					PlanId:                th.Padding36("plan-pseudo-active"),
					Status:                kdb.Done,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-done+2"),
						OutputId:  101_010,
						RunId:     th.Padding36("run@pseudo-done+2"),
						PlanId:    th.Padding36("plan-pseudo-active"),
						VolumeRef: "*pseudo-done+2",
					}: {
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-12T13:15:22.679+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-invalidated-2"),
					PlanId:                th.Padding36("plan-pseudo-active"),
					Status:                kdb.Invalidated,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{}, // empty
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-invalidated+2"),
					PlanId:                th.Padding36("plan-pseudo-active"),
					Status:                kdb.Invalidated,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{}, // empty
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-deactivated-2"),
					PlanId:                th.Padding36("plan-pseudo-inactive"),
					Status:                kdb.Deactivated,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-deactivated-2"),
						OutputId:  102_010,
						RunId:     th.Padding36("run@pseudo-deactivated-2"),
						PlanId:    th.Padding36("plan-pseudo-inactive"),
						VolumeRef: "*pseudo-deactivated-2",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@pseudo-deactivated+2"),
					PlanId:                th.Padding36("plan-pseudo-inactive"),
					Status:                kdb.Deactivated,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@pseudo-deactivated+2"),
						OutputId:  102_010,
						RunId:     th.Padding36("run@pseudo-deactivated+2"),
						PlanId:    th.Padding36("plan-pseudo-inactive"),
						VolumeRef: "*pseudo-deactivated+2",
					}: {},
				},
			},
			//
			// image based runs
			//
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-waiting-1"),
					PlanId:                th.Padding36("plan-image-waiting"),
					Status:                kdb.Waiting,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done-1"),
						InputId: 201_100,
						RunId:   th.Padding36("run@image-waiting-1"),
						PlanId:  th.Padding36("plan-image-waiting"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-waiting-1:/out"),
						OutputId:  201_010,
						RunId:     th.Padding36("run@image-waiting-1"),
						PlanId:    th.Padding36("plan-image-waiting"),
						VolumeRef: "*image-waiting-1:/out",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "waiting"},
						},
					},
					{
						KnitId:    th.Padding36("knit@image-waiting-1:/log"),
						OutputId:  201_001,
						RunId:     th.Padding36("run@image-waiting-1"),
						PlanId:    th.Padding36("plan-image-waiting"),
						VolumeRef: "*image-waiting-1:/log",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "waiting"},
							{Key: "format", Value: "text/plain"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-waiting+1"),
					PlanId:                th.Padding36("plan-image-waiting"),
					Status:                kdb.Waiting,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done+1"),
						InputId: 201_100,
						RunId:   th.Padding36("run@image-waiting+1"),
						PlanId:  th.Padding36("plan-image-waiting"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-waiting+1:/out"),
						OutputId:  201_010,
						RunId:     th.Padding36("run@image-waiting+1"),
						PlanId:    th.Padding36("plan-image-waiting"),
						VolumeRef: "*image-waiting+1:/out",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "waiting"},
						},
					},
					{
						KnitId:    th.Padding36("knit@image-waiting+1:/log"),
						OutputId:  201_001,
						RunId:     th.Padding36("run@image-waiting+1"),
						PlanId:    th.Padding36("plan-image-waiting"),
						VolumeRef: "*image-waiting+1:/log",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "waiting"},
							{Key: "format", Value: "text/plain"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-ready-1"),
					PlanId:                th.Padding36("plan-image-ready"),
					Status:                kdb.Ready,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done-1"),
						InputId: 202_100,
						RunId:   th.Padding36("run@image-ready-1"),
						PlanId:  th.Padding36("plan-image-ready"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-ready-1:/out"),
						RunId:     th.Padding36("run@image-ready-1"),
						OutputId:  202_010,
						PlanId:    th.Padding36("plan-image-ready"),
						VolumeRef: "*image-ready-1:/out",
					}: {
						UserTag: []kdb.Tag{}, // empty
					},
					{
						KnitId:    th.Padding36("knit@image-ready-1:/log"),
						RunId:     th.Padding36("run@image-ready-1"),
						OutputId:  202_001,
						PlanId:    th.Padding36("plan-image-ready"),
						VolumeRef: "*image-ready-1:/log",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "ready"},
							{Key: "type", Value: "entry-per-line"},
							{Key: "mode", Value: "log"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-ready+1"),
					PlanId:                th.Padding36("plan-image-ready"),
					Status:                kdb.Ready,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done+1"),
						InputId: 202_100,
						RunId:   th.Padding36("run@image-ready+1"),
						PlanId:  th.Padding36("plan-image-ready"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-ready+1:/out"),
						RunId:     th.Padding36("run@image-ready+1"),
						OutputId:  202_010,
						PlanId:    th.Padding36("plan-image-ready"),
						VolumeRef: "*image-ready+1:/out",
					}: {
						UserTag: []kdb.Tag{}, // empty
					},
					{
						KnitId:    th.Padding36("knit@image-ready+1:/log"),
						RunId:     th.Padding36("run@image-ready+1"),
						OutputId:  202_001,
						PlanId:    th.Padding36("plan-image-ready"),
						VolumeRef: "*image-ready+1:/log",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "ready"},
							{Key: "type", Value: "entry-per-line"},
							{Key: "mode", Value: "log"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-starting-1"),
					PlanId:                th.Padding36("plan-image-starting"),
					Status:                kdb.Starting,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done-1"),
						InputId: 203_100,
						RunId:   th.Padding36("run@image-starting-1"),
						PlanId:  th.Padding36("plan-image-starting"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-starting-1:/out"),
						RunId:     th.Padding36("run@image-starting-1"),
						OutputId:  203_010,
						PlanId:    th.Padding36("plan-image-starting"),
						VolumeRef: "*image-starting-1:/out",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-starting+1"),
					PlanId:                th.Padding36("plan-image-starting"),
					Status:                kdb.Starting,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done+1"),
						InputId: 203_100,
						RunId:   th.Padding36("run@image-starting+1"),
						PlanId:  th.Padding36("plan-image-starting"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-starting+1:/out"),
						RunId:     th.Padding36("run@image-starting+1"),
						OutputId:  203_010,
						PlanId:    th.Padding36("plan-image-starting"),
						VolumeRef: "*image-starting+1:/out",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-running-1"),
					PlanId:                th.Padding36("plan-image-running"),
					Status:                kdb.Running,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done-1"),
						InputId: 204_100,
						RunId:   th.Padding36("run@image-running-1"),
						PlanId:  th.Padding36("plan-image-running"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-running-1:/out"),
						OutputId:  204_010,
						RunId:     th.Padding36("run@image-running-1"),
						PlanId:    th.Padding36("plan-image-running"),
						VolumeRef: "*image-running-1:/out",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "running"},
							{Key: "mode", Value: "out"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-running+1"),
					PlanId:                th.Padding36("plan-image-running"),
					Status:                kdb.Running,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done+1"),
						InputId: 204_100,
						RunId:   th.Padding36("run@image-running+1"),
						PlanId:  th.Padding36("plan-image-running"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-running+1:/out"),
						OutputId:  204_010,
						RunId:     th.Padding36("run@image-running+1"),
						PlanId:    th.Padding36("plan-image-running"),
						VolumeRef: "*image-running+1:/out",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "running"},
							{Key: "mode", Value: "out"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-aborting-1"),
					PlanId:                th.Padding36("plan-image-aborting"),
					Status:                kdb.Aborting,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done-1"),
						InputId: 205_100,
						RunId:   th.Padding36("run@image-aborting-1"),
						PlanId:  th.Padding36("plan-image-aborting"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-aborting-1:/log"),
						RunId:     th.Padding36("run@image-aborting-1"),
						OutputId:  205_001,
						PlanId:    th.Padding36("plan-image-aborting"),
						VolumeRef: "*image-aborting-1:/log",
					}: {
						UserTag: []kdb.Tag{}, // empty
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-aborting+1"),
					PlanId:                th.Padding36("plan-image-aborting"),
					Status:                kdb.Aborting,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done+1"),
						InputId: 205_100,
						RunId:   th.Padding36("run@image-aborting+1"),
						PlanId:  th.Padding36("plan-image-aborting"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-aborting+1:/log"),
						RunId:     th.Padding36("run@image-aborting+1"),
						OutputId:  205_001,
						PlanId:    th.Padding36("plan-image-aborting"),
						VolumeRef: "*image-aborting+1:/log",
					}: {
						UserTag: []kdb.Tag{}, // empty
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-completing-1"),
					PlanId:                th.Padding36("plan-image-completing"),
					Status:                kdb.Completing,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done-1"),
						InputId: 206_100,
						RunId:   th.Padding36("run@image-completing-1"),
						PlanId:  th.Padding36("plan-image-completing"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-completing-1:/log"),
						OutputId:  206_001,
						RunId:     th.Padding36("run@image-completing-1"),
						PlanId:    th.Padding36("plan-image-completing"),
						VolumeRef: "*image-completing-1:/log",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "completing"},
							{Key: "mode", Value: "log"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-completing+1"),
					PlanId:                th.Padding36("plan-image-completing"),
					Status:                kdb.Completing,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done+1"),
						InputId: 206_100,
						RunId:   th.Padding36("run@image-completing+1"),
						PlanId:  th.Padding36("plan-image-completing"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-completing+1:/log"),
						OutputId:  206_001,
						RunId:     th.Padding36("run@image-completing+1"),
						PlanId:    th.Padding36("plan-image-completing"),
						VolumeRef: "*image-completing+1:/log",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "completing"},
							{Key: "mode", Value: "log"},
						},
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-failed-1"),
					PlanId:                th.Padding36("plan-image-failed"),
					Status:                kdb.Failed,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done-1"),
						InputId: 207_100,
						RunId:   th.Padding36("run@image-failed-1"),
						PlanId:  th.Padding36("plan-image-failed"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-failed-1:/out"),
						OutputId:  207_010,
						RunId:     th.Padding36("run@image-failed-1"),
						PlanId:    th.Padding36("plan-image-failed"),
						VolumeRef: "*image-failed-:/out",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "failed"},
							{Key: "mode", Value: "out"},
							{Key: "reason", Value: "scripting error"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-13T14:14:21.679+09:00",
						)).OrFatal(t).Time()),
					},
					{
						KnitId:    th.Padding36("knit@image-failed-1:/log"),
						OutputId:  207_001,
						RunId:     th.Padding36("run@image-failed-1"),
						PlanId:    th.Padding36("plan-image-failed"),
						VolumeRef: "*image-failed-:/log",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "failed"},
							{Key: "mode", Value: "log"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-13T14:14:21.680+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-failed+1"),
					PlanId:                th.Padding36("plan-image-failed"),
					Status:                kdb.Failed,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done+1"),
						InputId: 207_100,
						RunId:   th.Padding36("run@image-failed+1"),
						PlanId:  th.Padding36("plan-image-failed"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-failed+1:/out"),
						OutputId:  207_010,
						RunId:     th.Padding36("run@image-failed+1"),
						PlanId:    th.Padding36("plan-image-failed"),
						VolumeRef: "*image-failed+:/out",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "failed"},
							{Key: "mode", Value: "out"},
							{Key: "reason", Value: "scripting error"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-13T14:14:21.679+09:00",
						)).OrFatal(t).Time()),
					},
					{
						KnitId:    th.Padding36("knit@image-failed+1:/log"),
						OutputId:  207_001,
						RunId:     th.Padding36("run@image-failed+1"),
						PlanId:    th.Padding36("plan-image-failed"),
						VolumeRef: "*image-failed+:/log",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "failed"},
							{Key: "mode", Value: "log"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-13T14:14:21.680+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-done-1"),
					PlanId:                th.Padding36("plan-image-done"),
					Status:                kdb.Done,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done-1"),
						InputId: 208_100,
						RunId:   th.Padding36("run@image-done-1"),
						PlanId:  th.Padding36("plan-image-done"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-done-1:/out"),
						OutputId:  208_010,
						RunId:     th.Padding36("run@image-done-1"),
						PlanId:    th.Padding36("plan-image-done"),
						VolumeRef: "*image-done-:/out",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "model-parameter"},
							{Key: "mode", Value: "out"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-13T14:14:22.679+09:00",
						)).OrFatal(t).Time()),
					},
					{
						KnitId:    th.Padding36("knit@image-done-1:/log"),
						OutputId:  208_001,
						RunId:     th.Padding36("run@image-done-1"),
						PlanId:    th.Padding36("plan-image-done"),
						VolumeRef: "*image-done-:/log",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "application/jsonl"},
							{Key: "mode", Value: "log"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-13T14:14:22.680+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-done+1"),
					PlanId:                th.Padding36("plan-image-done"),
					Status:                kdb.Done,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done+1"),
						InputId: 208_100,
						RunId:   th.Padding36("run@image-done+1"),
						PlanId:  th.Padding36("plan-image-done"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-done+1:/out"),
						OutputId:  208_010,
						RunId:     th.Padding36("run@image-done+1"),
						PlanId:    th.Padding36("plan-image-done"),
						VolumeRef: "*image-done+:/out",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "model-parameter"},
							{Key: "mode", Value: "out"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-13T14:14:22.679+09:00",
						)).OrFatal(t).Time()),
					},
					{
						KnitId:    th.Padding36("knit@image-done+1:/log"),
						OutputId:  208_001,
						RunId:     th.Padding36("run@image-done+1"),
						PlanId:    th.Padding36("plan-image-done"),
						VolumeRef: "*image-done+:/log",
					}: {
						UserTag: []kdb.Tag{
							{Key: "type", Value: "application/jsonl"},
							{Key: "mode", Value: "log"},
						},
						Timestamp: pointer.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-11-13T14:14:22.680+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-invalidated-1"),
					PlanId:                th.Padding36("plan-image-invalidated"),
					Status:                kdb.Invalidated,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done-1"),
						InputId: 209_100,
						RunId:   th.Padding36("run@image-invalidated-1"),
						PlanId:  th.Padding36("plan-image-invalidated"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-invalidated+1"),
					PlanId:                th.Padding36("plan-image-invalidated"),
					Status:                kdb.Invalidated,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done+1"),
						InputId: 209_100,
						RunId:   th.Padding36("run@image-invalidated+1"),
						PlanId:  th.Padding36("plan-image-invalidated"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-deactivated-1"),
					PlanId:                th.Padding36("plan-image-deactivated"),
					Status:                kdb.Deactivated,
					LifecycleSuspendUntil: NOW.Add(-time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done-1"),
						InputId: 210_100,
						RunId:   th.Padding36("run@image-deactivated-1"),
						PlanId:  th.Padding36("plan-image-deactivated"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-deactivated-1:/log"),
						OutputId:  210_001,
						RunId:     th.Padding36("run@image-deactivated-1"),
						PlanId:    th.Padding36("plan-image-deactivated"),
						VolumeRef: "*image-deactivated-1:/log",
					}: {},
				},
			},
			{
				Run: tables.Run{
					RunId:                 th.Padding36("run@image-deactivated+1"),
					PlanId:                th.Padding36("plan-image-deactivated"),
					Status:                kdb.Deactivated,
					LifecycleSuspendUntil: NOW.Add(+time.Hour).UTC(),
					UpdatedAt:             NOW.Add(-2 * time.Hour).UTC(),
				},
				Assign: []tables.Assign{
					{
						KnitId:  th.Padding36("knit@pseudo-done+1"),
						InputId: 210_100,
						RunId:   th.Padding36("run@image-deactivated+1"),
						PlanId:  th.Padding36("plan-image-deactivated"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("knit@image-deactivated+1:/log"),
						OutputId:  210_001,
						RunId:     th.Padding36("run@image-deactivated+1"),
						PlanId:    th.Padding36("plan-image-deactivated"),
						VolumeRef: "*image-deactivated+1:/log",
					}: {},
				},
			},
		},
	}
	expectedRunsForPseudoPlans := []kdb.Run{
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-waiting-1"),
				Status:    kdb.Waiting,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-waiting-1"),
						VolumeRef: "*pseudo-waiting-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-waiting-1")},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-ready-1"),
				Status:    kdb.Ready,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-ready-1"),
						VolumeRef: "*pseudo-ready-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-ready-1")},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
							{Key: "type", Value: "ready"},
							{Key: "trigger", Value: "user upload"},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-starting-1"),
				Status:    kdb.Starting,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-starting-1"),
						VolumeRef: "*pseudo-starting-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-starting-1")},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-running-1"),
				Status:    kdb.Running,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-running-1"),
						VolumeRef: "*pseudo-running-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-running-1")},
							{Key: "type", Value: "running"},
							{Key: "trigger", Value: "user upload"},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-aborting-1"),
				Status:    kdb.Aborting,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-aborting-1"),
						VolumeRef: "*pseudo-aborting-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-aborting-1")},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-completing-1"),
				Status:    kdb.Completing,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-completing-1"),
						VolumeRef: "*pseudo-completing-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-completing-1")},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-failed-1"),
				Status:    kdb.Failed,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-failed-1"),
						VolumeRef: "*pseudo-failed-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-failed-1")},
							{Key: "type", Value: "failed"},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T13:14:21.679+09:00"},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientFailed},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-done-1"),
				Status:    kdb.Done,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-done-1"),
						VolumeRef: "*pseudo-done-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-1")},
							{Key: "type", Value: "done"},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T13:14:22.679+09:00"},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-invalidated-1"),
				Status:    kdb.Invalidated,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-deactivated-1"),
				Status:    kdb.Deactivated,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-inactive"), Active: false, Hash: th.Padding64("#pseudo-inactive"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-inactive"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 102_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-deactivated-1"),
						VolumeRef: "*pseudo-deactivated-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-deactivated-1")},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-waiting-2"),
				Status:    kdb.Waiting,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-waiting-2"),
						VolumeRef: "*pseudo-waiting-2",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-waiting-2")},
							{Key: "type", Value: "invalidated"},
							{Key: "reason", Value: "waiting for large storage"},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-ready-2"),
				Status:    kdb.Ready,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-ready-2"),
						VolumeRef: "*pseudo-ready-2",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-ready-2")},
							{Key: "type", Value: "ready"},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-starting-2"),
				Status:    kdb.Starting,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-starting-2"),
						VolumeRef: "*pseudo-starting-2",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-starting-2")},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-running-2"),
				Status:    kdb.Running,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-running-2"),
						VolumeRef: "*pseudo-running-2",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-running-2")},
							{Key: "type", Value: "running"},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-aborting-2"),
				Status:    kdb.Aborting,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-aborting-2"),
						VolumeRef: "*pseudo-aborting-2",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-aborting-2")},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-completing-2"),
				Status:    kdb.Completing,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-completing-2"),
						VolumeRef: "*pseudo-completing-2",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-completing-2")},
							{Key: "type", Value: "completing"},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-failed-2"),
				Status:    kdb.Failed,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-failed-2"),
						VolumeRef: "*pseudo-failed-2",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-failed-2")},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T13:15:21.679+09:00"},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientFailed},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-done-2"),
				Status:    kdb.Done,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-done-2"),
						VolumeRef: "*pseudo-done-2",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-2")},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T13:15:22.679+09:00"},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-invalidated-2"),
				Status:    kdb.Invalidated,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-active"), Active: true, Hash: th.Padding64("#pseudo-active"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-active"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 101_010, Path: "/out"},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@pseudo-deactivated-2"),
				Status:    kdb.Deactivated,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-pseudo-inactive"), Active: false, Hash: th.Padding64("#pseudo-inactive"),
					Pseudo: &kdb.PseudoPlanDetail{Name: "pseudo-inactive"},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{Id: 102_010, Path: "/out"},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-deactivated-2"),
						VolumeRef: "*pseudo-deactivated-2",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-deactivated-2")},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
	}
	expectedRunsBasedOnImage := []kdb.Run{
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@image-waiting-1"),
				Status:    kdb.Waiting,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-image-waiting"), Active: true, Hash: th.Padding64("#image-waiting"),
					Image: &kdb.ImageIdentifier{Image: "repo.invalid/example", Version: "waiting"},
				},
			},
			Inputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 201_100, Path: "/in",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "waiting"}, {Key: "mode", Value: "in"},
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-1")},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-done-1"),
						VolumeRef: "*pseudo-done-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-1")},
							{Key: "type", Value: "done"},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T13:14:22.679+09:00"},
						}),
					},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 201_010, Path: "/out",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "waiting"},
							{Key: "mode", Value: "out"},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@image-waiting-1:/out"),
						VolumeRef: "*image-waiting-1:/out",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@image-waiting-1:/out")},
							{Key: "type", Value: "waiting"},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
			Log: &kdb.Log{
				Id: 201_001,
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "type", Value: "waiting"},
					{Key: "mode", Value: "log"},
				}),
				KnitDataBody: kdb.KnitDataBody{
					KnitId:    th.Padding36("knit@image-waiting-1:/log"),
					VolumeRef: "*image-waiting-1:/log",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitId, Value: th.Padding36("knit@image-waiting-1:/log")},
						{Key: "type", Value: "waiting"},
						{Key: "format", Value: "text/plain"},
						{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
					}),
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@image-ready-1"),
				Status:    kdb.Ready,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-image-ready"), Active: true, Hash: th.Padding64("#image-ready"),
					Image: &kdb.ImageIdentifier{Image: "repo.invalid/example", Version: "ready"},
				},
			},
			Inputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 202_100, Path: "/in",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "ready"},
							{Key: "mode", Value: "in"},
							{Key: kdb.KeyKnitTimestamp, Value: "2021-10-11T11:12:13+09:00"},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-done-1"),
						VolumeRef: "*pseudo-done-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-1")},
							{Key: "type", Value: "done"},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T13:14:22.679+09:00"},
						}),
					},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 202_010, Path: "/out",
						Tags: kdb.NewTagSet([]kdb.Tag{{
							Key: "type", Value: "ready"},
							{Key: "mode", Value: "out"}},
						),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@image-ready-1:/out"),
						VolumeRef: "*image-ready-1:/out",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@image-ready-1:/out")},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
			Log: &kdb.Log{
				Id: 202_001,
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "type", Value: "ready"},
					{Key: "mode", Value: "log"},
				}),
				KnitDataBody: kdb.KnitDataBody{
					KnitId:    th.Padding36("knit@image-ready-1:/log"),
					VolumeRef: "*image-ready-1:/log",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitId, Value: th.Padding36("knit@image-ready-1:/log")},
						{Key: "type", Value: "ready"},
						{Key: "type", Value: "entry-per-line"},
						{Key: "mode", Value: "log"},
						{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
					}),
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@image-starting-1"),
				Status:    kdb.Starting,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-image-starting"), Active: true, Hash: th.Padding64("#image-starting"),
					Image: &kdb.ImageIdentifier{Image: "repo.invalid/example", Version: "starting"},
				},
			},
			Inputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 203_100, Path: "/in",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "starting"},
							{Key: "mode", Value: "in"},
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-1")},
							{Key: kdb.KeyKnitTimestamp, Value: "2021-10-11T11:12:13+09:00"},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-done-1"),
						VolumeRef: "*pseudo-done-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-1")},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T13:14:22.679+09:00"},
							{Key: "type", Value: "done"},
						}),
					},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 203_010, Path: "/out",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "starting"},
							{Key: "mode", Value: "out"},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@image-starting-1:/out"),
						VolumeRef: "*image-starting-1:/out",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@image-starting-1:/out")},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@image-running-1"),
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				Status:    kdb.Running,
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-image-running"), Active: true, Hash: th.Padding64("#image-running"),
					Image: &kdb.ImageIdentifier{Image: "repo.invalid/example", Version: "running"},
				},
			},
			Inputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 204_100, Path: "/in",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "running"},
							{Key: "mode", Value: "in"},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-done-1"),
						VolumeRef: "*pseudo-done-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-1")},
							{Key: "type", Value: "done"},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T13:14:22.679+09:00"},
						}),
					},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 204_010, Path: "/out",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "running"},
							{Key: "mode", Value: "out"},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@image-running-1:/out"),
						VolumeRef: "*image-running-1:/out",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@image-running-1:/out")},
							{Key: "type", Value: "running"},
							{Key: "mode", Value: "out"},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						}),
					},
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@image-aborting-1"),
				Status:    kdb.Aborting,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-image-aborting"), Active: true, Hash: th.Padding64("#image-aborting"),
					Image: &kdb.ImageIdentifier{Image: "repo.invalid/example", Version: "aborting"},
				},
			},
			Inputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 205_100, Path: "/in",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "aborting"},
							{Key: "mode", Value: "in"},
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-2")},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-done-1"),
						VolumeRef: "*pseudo-done-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-1")},
							{Key: "type", Value: "done"},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T13:14:22.679+09:00"},
						}),
					},
				},
			},
			Log: &kdb.Log{
				Id: 205_001,
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "type", Value: "aborting"},
					{Key: "mode", Value: "log"},
				}),
				KnitDataBody: kdb.KnitDataBody{
					KnitId:    th.Padding36("knit@image-aborting-1:/log"),
					VolumeRef: "*image-aborting-1:/log",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitId, Value: th.Padding36("knit@image-aborting-1:/log")},
						{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
					}),
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@image-completing-1"),
				Status:    kdb.Completing,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-image-completing"), Active: true, Hash: th.Padding64("#image-completing"),
					Image: &kdb.ImageIdentifier{Image: "repo.invalid/example", Version: "completing"},
				},
			},
			Inputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 206_100, Path: "/in",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "completeing"},
							{Key: "mode", Value: "in"},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-done-1"),
						VolumeRef: "*pseudo-done-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-1")},
							{Key: "type", Value: "done"},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T13:14:22.679+09:00"},
						}),
					},
				},
			},
			Log: &kdb.Log{
				Id: 206_001,
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "type", Value: "completing"},
					{Key: "mode", Value: "log"},
				}),
				KnitDataBody: kdb.KnitDataBody{
					KnitId:    th.Padding36("knit@image-completing-1:/log"),
					VolumeRef: "*image-completing-1:/log",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitId, Value: th.Padding36("knit@image-completing-1:/log")},
						{Key: "type", Value: "completing"},
						{Key: "mode", Value: "log"},
						{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
					}),
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@image-failed-1"),
				Status:    kdb.Failed,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-image-failed"), Active: true, Hash: th.Padding64("#image-failed"),
					Image: &kdb.ImageIdentifier{Image: "repo.invalid/example", Version: "failed"},
				},
			},
			Inputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 207_100, Path: "/in",
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-done-1"),
						VolumeRef: "*pseudo-done-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-1")},
							{Key: "type", Value: "done"},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-13T13:14:22.679+09:00"},
						}),
					},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 207_010, Path: "/out",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "failed"},
							{Key: "mode", Value: "out"},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@image-failed-1:/out"),
						VolumeRef: "*image-failed-1:/out",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@image-failed-1:/out")},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-13T14:14:21.679+09:00"},
							{Key: "type", Value: "failed"},
							{Key: "mode", Value: "out"},
							{Key: "reason", Value: "scripting error"},
							{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientFailed},
						}),
					},
				},
			},
			Log: &kdb.Log{
				Id: 207_001,
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "type", Value: "failed"},
					{Key: "mode", Value: "log"},
				}),
				KnitDataBody: kdb.KnitDataBody{
					KnitId:    th.Padding36("knit@image-failed-1:/log"),
					VolumeRef: "*image-failed-1:/log",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitId, Value: th.Padding36("knit@image-failed-1:/log")},
						{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T14:14:21.680+09:00"},
						{Key: "type", Value: "failed"},
						{Key: "mode", Value: "log"},
					}),
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@image-done-1"),
				Status:    kdb.Done,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-image-done"), Active: true, Hash: th.Padding64("#image-done"),
					Image: &kdb.ImageIdentifier{Image: "repo.invalid/example", Version: "done"},
				},
			},
			Inputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 208_100, Path: "/in",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "done"},
							{Key: "mode", Value: "in"},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-done-1"),
						VolumeRef: "*pseudo-done-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-1")},
							{Key: "type", Value: "done"},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T13:14:22.679+09:00"},
						}),
					},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 208_010, Path: "/out",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "done"},
							{Key: "mode", Value: "out"},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@image-done-1:/out"),
						VolumeRef: "*image-done-1:/out",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@image-done-1:/out")},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-13T14:14:22.679+09:00"},
							{Key: "type", Value: "model-parameter"},
							{Key: "mode", Value: "out"},
						}),
					},
				},
			},
			Log: &kdb.Log{
				Id: 208_001,
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "type", Value: "done"},
					{Key: "mode", Value: "log"},
				}),
				KnitDataBody: kdb.KnitDataBody{
					KnitId:    th.Padding36("knit@image-done-1:/log"),
					VolumeRef: "*image-done-1:/log",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitId, Value: th.Padding36("knit@image-done-1:/log")},
						{Key: kdb.KeyKnitTimestamp, Value: "2022-11-13T14:14:22.680+09:00"},
						{Key: "type", Value: "application/jsonl"},
						{Key: "mode", Value: "log"},
					}),
				},
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@image-invalidated-1"),
				Status:    kdb.Invalidated,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-image-invalidated"), Active: true, Hash: th.Padding64("#image-done"),
					Image: &kdb.ImageIdentifier{Image: "repo.invalid/example", Version: "invalidated"},
				},
			},
			Inputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 209_100, Path: "/in",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "invalidated"},
							{Key: "mode", Value: "in"},
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-2")},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-done-1"),
						VolumeRef: "*pseudo-done-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-1")},
							{Key: "type", Value: "done"},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T13:14:22.679+09:00"},
						}),
					},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 209_010, Path: "/out",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "invalidated"},
							{Key: "mode", Value: "out"},
						}),
					},
				},
			},
			Log: &kdb.Log{
				Id: 209_001,
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "type", Value: "invalidated"},
					{Key: "mode", Value: "log"},
				}),
			},
		},
		{
			RunBody: kdb.RunBody{
				Id:        th.Padding36("run@image-deactivated-1"),
				Status:    kdb.Deactivated,
				UpdatedAt: NOW.Add(-2 * time.Hour).UTC(),
				PlanBody: kdb.PlanBody{
					PlanId: th.Padding36("plan-image-deactivated"), Active: false, Hash: th.Padding64("#image-deactivated"),
					Image: &kdb.ImageIdentifier{Image: "repo.invalid/example", Version: "deactivated"},
				},
			},
			Inputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{
						Id: 210_100, Path: "/in",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "deactivated"},
							{Key: "mode", Value: "in"},
							{Key: kdb.KeyKnitTimestamp, Value: "2021-10-11T11:12:13+09:00"},
						}),
					},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    th.Padding36("knit@pseudo-done-1"),
						VolumeRef: "*pseudo-done-1",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: kdb.KeyKnitId, Value: th.Padding36("knit@pseudo-done-1")},
							{Key: "type", Value: "done"},
							{Key: kdb.KeyKnitTimestamp, Value: "2022-11-12T13:14:22.679+09:00"},
						}),
					},
				},
			},
			Log: &kdb.Log{
				Id: 210_001,
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "type", Value: "deactivated"},
					{Key: "mode", Value: "log"},
				}),
				KnitDataBody: kdb.KnitDataBody{
					KnitId:    th.Padding36("knit@image-deactivated-1:/log"),
					VolumeRef: "*image-deactivated-1:/log",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitId, Value: th.Padding36("knit@image-deactivated-1:/log")},
						{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
					}),
				},
			},
		},
	}
	return Given{
		Plans: plans,
		Runs:  runs,
		ExpectedRun: utils.ToMap(
			utils.Concat(expectedRunsForPseudoPlans, expectedRunsBasedOnImage),
			func(r kdb.Run) string { return r.Id },
		),
	}
}
