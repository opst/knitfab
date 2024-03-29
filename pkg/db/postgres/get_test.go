package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	kpgdata "github.com/opst/knitfab/pkg/db/postgres/data"
	"github.com/opst/knitfab/pkg/db/postgres/marshal"
	kpgplan "github.com/opst/knitfab/pkg/db/postgres/plan"
	"github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	kpgrun "github.com/opst/knitfab/pkg/db/postgres/run"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	. "github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/maps"
	ptr "github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/try"
	"k8s.io/apimachinery/pkg/api/resource"
)

// testing {Interface}.Get methods in scenario.
func Test_Get(t *testing.T) {
	ctx := context.Background()
	poolBroaker := testenv.NewPoolBroaker(ctx, t)
	pgpool := poolBroaker.GetPool(ctx, t) // share database state between chapters.

	type expectation struct {
		data []kdb.KnitData
		plan []kdb.Plan
		run  []kdb.Run
	}

	// chapter of scenario.
	type chapter struct {
		// chapter title
		title string

		// change happened at the chapter
		operation tables.Operation

		// model representations (so, this is source of expectations)
		//
		// (mapping: knitId -> KnitData)
		expectation expectation
	}

	START_AT := try.To(rfctime.ParseRFC3339DateTime("2022-01-01T00:00:00+00:00")).OrFatal(t).Time()

	// ordering matters. they run top to bottom.
	for nth, chap := range []chapter{
		{ // chapter 1: databsae has no data (note: comment trailing open-brace line to summary the block. It may help you also after collapsing)
			"database has no data",
			tables.Operation{ // (no runs)
				Plan: []tables.Plan{
					{PlanId: Padding36("plan/ch1#1:uploaded"), Hash: Padding64("#plan/ch1#1:uploaded"), Active: true},
					//                  ^^^^   ^ ^ ^^^^^^^^
					//                  type   | |  mnemonic
					//                        chapter & number in chapter
				},
				PlanPseudo: []tables.PlanPseudo{
					{PlanId: Padding36("plan/ch1#1:uploaded"), Name: string(kdb.Uploaded)},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{
						OutputId: 9_01_01_010,
						//        ^ ^^ ^^ ^^^
						//        A BB CC DEF
						//        A: prefix (reserved)
						//        B: chapter
						//        C: #plan in chapter
						//        D,E,F: #in, #out, #log in run
						Path:   "/out",
						PlanId: Padding36("plan/ch1#1:uploaded"),
					}: {},
				},
			},
			expectation{
				plan: []kdb.Plan{
					{
						PlanBody: kdb.PlanBody{
							PlanId: Padding36("plan/ch1#1:uploaded"), Hash: Padding64("#plan/ch1#1:uploaded"), Active: true,
							Pseudo: &kdb.PseudoPlanDetail{Name: kdb.Uploaded},
						},
						Outputs: []kdb.MountPoint{
							{Id: 9_01_01_010, Path: "/out"},
						},
					},
				},
			}, // no data
		},
		{ // chapter 2: uploading new training data
			"uploading new training data",
			tables.Operation{
				Steps: []tables.Step{
					{ // {} -> run/ch2#1:plan/ch1#1 (running) -> {data/ch2#1:run/ch2#1/out}
						Run: tables.Run{
							RunId: Padding36("run/ch2#1:plan/ch1#1"), PlanId: Padding36("plan/ch1#1:uploaded"),
							Status:    kdb.Running,
							UpdatedAt: START_AT.Add(10*time.Second + 100*time.Millisecond),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    Padding36("data/ch2#1:run/ch2#1/out"),
								VolumeRef: "pvc/data/ch2#1",
								OutputId:  9_01_01_010,
								RunId:     Padding36("run/ch2#1:plan/ch1#1"),
								PlanId:    Padding36("plan/ch1#1:uploaded"),
							}: {},
						},
					},
					{ // {} -> run/ch2#2:plan/ch1#1 (failed) -> {data/ch2#2:run/ch2#2/out}
						Run: tables.Run{
							RunId: Padding36("run/ch2#2:plan/ch1#1"), PlanId: Padding36("plan/ch1#1:uploaded"),
							Status:    kdb.Failed,
							UpdatedAt: START_AT.Add(10*time.Second + 200*time.Millisecond),
						},
						Exit: &tables.RunExit{
							RunId:    Padding36("run/ch2#2:plan/ch1#1"),
							ExitCode: 1,
							Message:  "failed",
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    Padding36("data/ch2#2:run/ch2#2/out"),
								VolumeRef: "pvc/data/ch2#2",
								OutputId:  9_01_01_010,
								RunId:     Padding36("run/ch2#2:plan/ch1#1"),
								PlanId:    Padding36("plan/ch1#1:uploaded"),
							}: {
								Timestamp: ptr.Ref(START_AT.Add(10*time.Second + 250*time.Millisecond)),
							},
						},
					},
					{ // {} -> run/ch2#3:plan/ch1#1 (done) -> {data/ch2#3:run/ch2#3/out}
						Run: tables.Run{
							RunId: Padding36("run/ch2#3:plan/ch1#1"), PlanId: Padding36("plan/ch1#1:uploaded"),
							Status:    kdb.Done,
							UpdatedAt: START_AT.Add(10*time.Second + 300*time.Millisecond),
						},
						Exit: &tables.RunExit{
							RunId:    Padding36("run/ch2#3:plan/ch1#1"),
							ExitCode: 0,
							Message:  "succeeded",
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    Padding36("data/ch2#3:run/ch2#3/out"),
								VolumeRef: "pvc/data/ch2#3",
								OutputId:  9_01_01_010,
								RunId:     Padding36("run/ch2#3:plan/ch1#1"),
								PlanId:    Padding36("plan/ch1#1:uploaded"),
							}: {
								UserTag: []kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "training-data"},
								},
								Timestamp: ptr.Ref(START_AT.Add(10*time.Second + 350*time.Millisecond)),
							},
						},
					},
				},
			},
			expectation{
				run: []kdb.Run{
					{
						RunBody: kdb.RunBody{
							Id: Padding36("run/ch2#1:plan/ch1#1"), Status: kdb.Running,
							UpdatedAt: START_AT.Add(10*time.Second + 100*time.Millisecond),
							PlanBody: kdb.PlanBody{
								PlanId: Padding36("plan/ch1#1:uploaded"), Active: true,
								Hash:   Padding64("#plan/ch1#1:uploaded"),
								Pseudo: &kdb.PseudoPlanDetail{Name: kdb.Uploaded},
							},
						},
						Outputs: []kdb.Assignment{
							{
								MountPoint: kdb.MountPoint{
									Id: 9_01_01_010, Path: "/out",
								},
								KnitDataBody: kdb.KnitDataBody{
									KnitId:    Padding36("data/ch2#1:run/ch2#1/out"),
									VolumeRef: "pvc/data/ch2#1",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: kdb.KeyKnitId, Value: Padding36("data/ch2#1:run/ch2#1/out")},
										{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
									}),
								},
							},
						},
					},
				},
				data: []kdb.KnitData{
					{
						KnitDataBody: kdb.KnitDataBody{
							KnitId:    Padding36("data/ch2#1:run/ch2#1/out"),
							VolumeRef: "pvc/data/ch2#1",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: kdb.KeyKnitId, Value: Padding36("data/ch2#1:run/ch2#1/out")},
								{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
							}),
						},
						Upsteram: kdb.Dependency{
							RunBody: kdb.RunBody{
								Id: Padding36("run/ch2#1:plan/ch1#1"), Status: kdb.Running,
								UpdatedAt: START_AT.Add(10*time.Second + 100*time.Millisecond),
								PlanBody: kdb.PlanBody{
									PlanId: Padding36("plan/ch1#1:uploaded"), Active: true,
									Hash:   Padding64("#plan/ch1#1:uploaded"),
									Pseudo: &kdb.PseudoPlanDetail{Name: kdb.Uploaded},
								},
							},
							MountPoint: kdb.MountPoint{
								Id: 9_01_01_010, Path: "/out",
							},
						},
					},
					{
						KnitDataBody: kdb.KnitDataBody{
							KnitId:    Padding36("data/ch2#2:run/ch2#2/out"),
							VolumeRef: "pvc/data/ch2#2",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: kdb.KeyKnitId, Value: Padding36("data/ch2#2:run/ch2#2/out")},
								{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientFailed},
								{
									Key:   kdb.KeyKnitTimestamp,
									Value: rfctime.RFC3339(START_AT.Add(10*time.Second + 250*time.Millisecond)).String(),
								},
							}),
						},
						Upsteram: kdb.Dependency{
							RunBody: kdb.RunBody{
								Id: Padding36("run/ch2#2:plan/ch1#1"), Status: kdb.Failed,
								UpdatedAt: START_AT.Add(10*time.Second + 200*time.Millisecond),
								PlanBody: kdb.PlanBody{
									PlanId: Padding36("plan/ch1#1:uploaded"), Active: true,
									Hash:   Padding64("#plan/ch1#1:uploaded"),
									Pseudo: &kdb.PseudoPlanDetail{Name: kdb.Uploaded},
								},
								Exit: &kdb.RunExit{
									Code:    1,
									Message: "failed",
								},
							},
							MountPoint: kdb.MountPoint{
								Id: 9_01_01_010, Path: "/out",
							},
						},
					},
					{
						KnitDataBody: kdb.KnitDataBody{
							KnitId:    Padding36("data/ch2#3:run/ch2#3/out"),
							VolumeRef: "pvc/data/ch2#3",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "project", Value: "testing"},
								{Key: "type", Value: "training-data"},
								{Key: kdb.KeyKnitId, Value: Padding36("data/ch2#3:run/ch2#3/out")},
								{
									Key:   kdb.KeyKnitTimestamp,
									Value: rfctime.RFC3339(START_AT.Add(10*time.Second + 350*time.Millisecond)).String(),
								},
							}),
						},
						Upsteram: kdb.Dependency{
							RunBody: kdb.RunBody{
								Id: Padding36("run/ch2#3:plan/ch1#1"), Status: kdb.Done,
								UpdatedAt: START_AT.Add(10*time.Second + 300*time.Millisecond),
								PlanBody: kdb.PlanBody{
									PlanId: Padding36("plan/ch1#1:uploaded"), Active: true,
									Hash:   Padding64("#plan/ch1#1:uploaded"),
									Pseudo: &kdb.PseudoPlanDetail{Name: kdb.Uploaded},
								},
								Exit: &kdb.RunExit{
									Code:    0,
									Message: "succeeded",
								},
							},
							MountPoint: kdb.MountPoint{
								Id: 9_01_01_010, Path: "/out",
							},
						},
					},
				},
			},
		},
		{ // chapter 3: training
			"training",
			tables.Operation{
				Plan: []tables.Plan{
					{
						PlanId: Padding36("plan/ch3#1:trainer"), Active: true,
						Hash: Padding64("#plan/ch3#1:trainer"),
					},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: Padding36("plan/ch3#1:trainer"), Image: "repo.invalid/trainer", Version: "ch3#1"},
				},
				PlanResources: []tables.PlanResource{
					{PlanId: Padding36("plan/ch3#1:trainer"), Type: "gpu", Value: marshal.ResourceQuantity(resource.MustParse("1"))},
					{PlanId: Padding36("plan/ch3#1:trainer"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("0.5"))},
					{PlanId: Padding36("plan/ch3#1:trainer"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("1Gi"))},
				},
				OnNode: []tables.PlanOnNode{
					{
						PlanId: Padding36("plan/ch3#1:trainer"), Mode: kdb.MustOnNode,
						Key: "accelerator", Value: "gpu",
					},
					{
						PlanId: Padding36("plan/ch3#1:trainer"), Mode: kdb.PreferOnNode,
						Key: "accelerator", Value: "high-power",
					},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{
						PlanId: Padding36("plan/ch3#1:trainer"), Path: "/in",
						InputId: 9_03_01_100,
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "training-data"},
						},
					},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{
						PlanId: Padding36("plan/ch3#1:trainer"), Path: "/out/1",
						OutputId: 9_03_01_010,
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "model"},
							{Key: "task", Value: "encode"},
						},
					},
					{
						PlanId: Padding36("plan/ch3#1:trainer"), Path: "/out/2",
						OutputId: 9_03_01_020,
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "model"},
							{Key: "task", Value: "decode"},
						},
					},
					{
						PlanId: Padding36("plan/ch3#1:trainer"), Path: "/out/3",
						OutputId: 9_03_01_030,
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "validation-stats"},
						},
					},
					{
						PlanId: Padding36("plan/ch3#1:trainer"), Path: "/log",
						OutputId: 9_03_01_001,
					}: {
						IsLog: true,
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "log"},
						},
					},
				},
				Steps: []tables.Step{
					{ // {data/ch2#3:run/ch2#3/out} -> run/ch3#1:plan/ch3#1 -> {data/ch3#[1..4]:run/ch3#1{/out/[1..3],/log}}
						Run: tables.Run{
							RunId: Padding36("run/ch3#1:plan/ch3#1"), Status: kdb.Done,
							PlanId:    Padding36("plan/ch3#1:trainer"),
							UpdatedAt: START_AT.Add(30*time.Second + 100*time.Millisecond),
						},
						Exit: &tables.RunExit{
							RunId:    Padding36("run/ch3#1:plan/ch3#1"),
							ExitCode: 0,
							Message:  "succeeded",
						},
						Assign: []tables.Assign{
							{
								KnitId:  Padding36("data/ch2#3:run/ch2#3/out"),
								InputId: 9_03_01_100,
								RunId:   Padding36("run/ch3#1:plan/ch3#1"),
								PlanId:  Padding36("plan/ch3#1:trainer"),
							},
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: Padding36("data/ch3#1:run/ch3#1/out/1"), VolumeRef: "#data/ch3#1",
								OutputId: 9_03_01_010,
								RunId:    Padding36("run/ch3#1:plan/ch3#1"),
								PlanId:   Padding36("plan/ch3#1:trainer"),
							}: {
								UserTag: []kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "model"},
									{Key: "task", Value: "encode"},
									{Key: "model-version", Value: "1"},
								},
								Timestamp: ptr.Ref(START_AT.Add(30*time.Second + 101*time.Millisecond)),
							},
							{
								KnitId: Padding36("data/ch3#2:run/ch3#1/out/2"), VolumeRef: "#data/ch3#2",
								OutputId: 9_03_01_020,
								RunId:    Padding36("run/ch3#1:plan/ch3#1"),
								PlanId:   Padding36("plan/ch3#1:trainer"),
							}: {
								UserTag: []kdb.Tag{

									{Key: "project", Value: "testing"},
									{Key: "type", Value: "model"},
									{Key: "task", Value: "decode"},
									{Key: "model-version", Value: "1"},
								},
								Timestamp: ptr.Ref(START_AT.Add(30*time.Second + 102*time.Millisecond)),
							},
							{
								KnitId: Padding36("data/ch3#3:run/ch3#1/out/3"), VolumeRef: "#data/ch3#3",
								OutputId: 9_03_01_030,
								RunId:    Padding36("run/ch3#1:plan/ch3#1"),
								PlanId:   Padding36("plan/ch3#1:trainer"),
							}: {
								UserTag: []kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "validation-stats"},
								},
								Timestamp: ptr.Ref(START_AT.Add(30*time.Second + 103*time.Millisecond)),
							},
							{
								KnitId: Padding36("data/ch3#4:run/ch3#1/log"), VolumeRef: "#data/ch3#4",
								OutputId: 9_03_01_001,
								RunId:    Padding36("run/ch3#1:plan/ch3#1"),
								PlanId:   Padding36("plan/ch3#1:trainer"),
							}: {
								UserTag: []kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "log"},
								},
								Timestamp: ptr.Ref(START_AT.Add(30*time.Second + 104*time.Millisecond)),
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: Padding36("data/ch2#3:run/ch2#3/out"), InputId: 9_03_01_100},
				},
			},
			expectation{
				plan: []kdb.Plan{
					{
						PlanBody: kdb.PlanBody{
							PlanId: Padding36("plan/ch3#1:trainer"), Active: true,
							Hash:  Padding64("#plan/ch3#1:trainer"),
							Image: &kdb.ImageIdentifier{Image: "repo.invalid/trainer", Version: "ch3#1"},
							OnNode: []kdb.OnNode{
								{Mode: kdb.MustOnNode, Key: "accelerator", Value: "gpu"},
								{Mode: kdb.PreferOnNode, Key: "accelerator", Value: "high-power"},
							},
							Resources: map[string]resource.Quantity{
								"gpu":    resource.MustParse("1"),
								"cpu":    resource.MustParse("0.5"),
								"memory": resource.MustParse("1Gi"),
							},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 9_03_01_100, Path: "/in",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "training-data"},
								}),
							},
						},
						Outputs: []kdb.MountPoint{
							{
								Id: 9_03_01_010, Path: "/out/1",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "model"},
									{Key: "task", Value: "encode"},
								}),
							},
							{
								Id: 9_03_01_020, Path: "/out/2",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "model"},
									{Key: "task", Value: "decode"},
								}),
							},
							{
								Id: 9_03_01_030, Path: "/out/3",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "validation-stats"},
								}),
							},
						},
						Log: &kdb.LogPoint{
							Id: 9_03_01_001,
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "project", Value: "testing"},
								{Key: "type", Value: "log"},
							}),
						},
					},
				},
				run: []kdb.Run{
					{
						RunBody: kdb.RunBody{
							Id: Padding36("run/ch3#1:plan/ch3#1"), Status: kdb.Done,
							Exit: &kdb.RunExit{
								Code:    0,
								Message: "succeeded",
							},
							UpdatedAt: START_AT.Add(30*time.Second + 100*time.Millisecond),
							PlanBody: kdb.PlanBody{
								PlanId: Padding36("plan/ch3#1:trainer"), Active: true,
								Hash:  Padding64("#plan/ch3#1:trainer"),
								Image: &kdb.ImageIdentifier{Image: "repo.invalid/trainer", Version: "ch3#1"},
								OnNode: []kdb.OnNode{
									{Mode: kdb.MustOnNode, Key: "accelerator", Value: "gpu"},
									{Mode: kdb.PreferOnNode, Key: "accelerator", Value: "high-power"},
								},
								Resources: map[string]resource.Quantity{
									"gpu":    resource.MustParse("1"),
									"cpu":    resource.MustParse("0.5"),
									"memory": resource.MustParse("1Gi"),
								},
							},
						},
						Inputs: []kdb.Assignment{
							{
								MountPoint: kdb.MountPoint{
									Id: 9_03_01_100, Path: "/in",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "training-data"},
									}),
								},
								KnitDataBody: kdb.KnitDataBody{
									KnitId:    Padding36("data/ch2#3:run/ch2#3/out"),
									VolumeRef: "pvc/data/ch2#3",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "training-data"},
										{Key: kdb.KeyKnitId, Value: Padding36("data/ch2#3:run/ch2#3/out")},
										{
											Key:   kdb.KeyKnitTimestamp,
											Value: rfctime.RFC3339(START_AT.Add(10*time.Second + 350*time.Millisecond)).String(),
										},
									}),
								},
							},
						},
						Outputs: []kdb.Assignment{
							{
								MountPoint: kdb.MountPoint{
									Id: 9_03_01_010, Path: "/out/1",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "model"},
										{Key: "task", Value: "encode"},
									}),
								},
								KnitDataBody: kdb.KnitDataBody{
									KnitId: Padding36("data/ch3#1:run/ch3#1/out/1"), VolumeRef: "#data/ch3#1",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "model"},
										{Key: "task", Value: "encode"},
										{Key: "model-version", Value: "1"},
										{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#1:run/ch3#1/out/1")},
										{
											Key:   kdb.KeyKnitTimestamp,
											Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 101*time.Millisecond)).String(),
										},
									}),
								},
							},
							{
								MountPoint: kdb.MountPoint{
									Id: 9_03_01_020, Path: "/out/2",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "model"},
										{Key: "task", Value: "decode"},
									}),
								},
								KnitDataBody: kdb.KnitDataBody{
									KnitId: Padding36("data/ch3#2:run/ch3#1/out/2"), VolumeRef: "#data/ch3#2",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "model"},
										{Key: "task", Value: "decode"},
										{Key: "model-version", Value: "1"},
										{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#2:run/ch3#1/out/2")},
										{
											Key:   kdb.KeyKnitTimestamp,
											Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 102*time.Millisecond)).String(),
										},
									}),
								},
							},
							{
								MountPoint: kdb.MountPoint{
									Id: 9_03_01_030, Path: "/out/3",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "validation-stats"},
									}),
								},
								KnitDataBody: kdb.KnitDataBody{
									KnitId: Padding36("data/ch3#3:run/ch3#1/out/3"), VolumeRef: "#data/ch3#3",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "validation-stats"},
										{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#3:run/ch3#1/out/3")},
										{
											Key:   kdb.KeyKnitTimestamp,
											Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 103*time.Millisecond)).String(),
										},
									}),
								},
							},
						},
						Log: &kdb.Log{
							Id: 9_03_01_001,
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "project", Value: "testing"},
								{Key: "type", Value: "log"},
							}),
							KnitDataBody: kdb.KnitDataBody{
								KnitId: Padding36("data/ch3#4:run/ch3#1/log"), VolumeRef: "#data/ch3#4",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "log"},
									{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#4:run/ch3#1/log")},
									{
										Key:   kdb.KeyKnitTimestamp,
										Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 104*time.Millisecond)).String(),
									},
								}),
							},
						},
					},
				},
				data: []kdb.KnitData{
					{
						KnitDataBody: kdb.KnitDataBody{
							KnitId:    Padding36("data/ch2#3:run/ch2#3/out"),
							VolumeRef: "pvc/data/ch2#3",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "project", Value: "testing"},
								{Key: "type", Value: "training-data"},
								{Key: kdb.KeyKnitId, Value: Padding36("data/ch2#3:run/ch2#3/out")},
								{
									Key:   kdb.KeyKnitTimestamp,
									Value: rfctime.RFC3339(START_AT.Add(10*time.Second + 350*time.Millisecond)).String(),
								},
							}),
						},
						Upsteram: kdb.Dependency{
							RunBody: kdb.RunBody{
								Id: Padding36("run/ch2#3:plan/ch1#1"), Status: kdb.Done,
								UpdatedAt: START_AT.Add(10*time.Second + 300*time.Millisecond),
								PlanBody: kdb.PlanBody{
									PlanId: Padding36("plan/ch1#1:uploaded"), Active: true,
									Hash:   Padding64("#plan/ch1#1:uploaded"),
									Pseudo: &kdb.PseudoPlanDetail{Name: kdb.Uploaded},
								},
							},
							MountPoint: kdb.MountPoint{
								Id: 9_01_01_010, Path: "/out",
							},
						},
						Downstreams: []kdb.Dependency{
							{
								RunBody: kdb.RunBody{
									Id: Padding36("run/ch3#1:plan/ch3#1"), Status: kdb.Done,
									Exit: &kdb.RunExit{
										Code:    0,
										Message: "succeeded",
									},
									UpdatedAt: START_AT.Add(30*time.Second + 100*time.Millisecond),
									PlanBody: kdb.PlanBody{
										PlanId: Padding36("plan/ch3#1:trainer"), Active: true,
										Hash:  Padding64("#plan/ch3#1:trainer"),
										Image: &kdb.ImageIdentifier{Image: "repo.invalid/trainer", Version: "ch3#1"},
										OnNode: []kdb.OnNode{
											{Mode: kdb.MustOnNode, Key: "accelerator", Value: "gpu"},
											{Mode: kdb.PreferOnNode, Key: "accelerator", Value: "high-power"},
										},
										Resources: map[string]resource.Quantity{
											"gpu":    resource.MustParse("1"),
											"cpu":    resource.MustParse("0.5"),
											"memory": resource.MustParse("1Gi"),
										},
									},
								},
								MountPoint: kdb.MountPoint{
									Id: 9_03_01_100, Path: "/in",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "training-data"},
									}),
								},
							},
						},
						NominatedBy: []kdb.Nomination{
							{
								PlanBody: kdb.PlanBody{
									PlanId: Padding36("plan/ch3#1:trainer"), Active: true,
									Hash:  Padding64("#plan/ch3#1:trainer"),
									Image: &kdb.ImageIdentifier{Image: "repo.invalid/trainer", Version: "ch3#1"},
									OnNode: []kdb.OnNode{
										{Mode: kdb.MustOnNode, Key: "accelerator", Value: "gpu"},
										{Mode: kdb.PreferOnNode, Key: "accelerator", Value: "high-power"},
									},
									Resources: map[string]resource.Quantity{
										"gpu":    resource.MustParse("1"),
										"cpu":    resource.MustParse("0.5"),
										"memory": resource.MustParse("1Gi"),
									},
								},
								MountPoint: kdb.MountPoint{
									Id: 9_03_01_100, Path: "/in",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "training-data"},
									}),
								},
							},
						},
					},
					{
						KnitDataBody: kdb.KnitDataBody{
							KnitId: Padding36("data/ch3#1:run/ch3#1/out/1"), VolumeRef: "#data/ch3#1",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "project", Value: "testing"},
								{Key: "type", Value: "model"},
								{Key: "task", Value: "encode"},
								{Key: "model-version", Value: "1"},
								{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#1:run/ch3#1/out/1")},
								{
									Key:   kdb.KeyKnitTimestamp,
									Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 101*time.Millisecond)).String(),
								},
							}),
						},
						Upsteram: kdb.Dependency{
							RunBody: kdb.RunBody{
								Id: Padding36("run/ch3#1:plan/ch3#1"), Status: kdb.Done,
								Exit: &kdb.RunExit{
									Code:    0,
									Message: "succeeded",
								},
								UpdatedAt: START_AT.Add(30*time.Second + 100*time.Millisecond),
								PlanBody: kdb.PlanBody{
									PlanId: Padding36("plan/ch3#1:trainer"), Active: true,
									Hash:  Padding64("#plan/ch3#1:trainer"),
									Image: &kdb.ImageIdentifier{Image: "repo.invalid/trainer", Version: "ch3#1"},
									OnNode: []kdb.OnNode{
										{Mode: kdb.MustOnNode, Key: "accelerator", Value: "gpu"},
										{Mode: kdb.PreferOnNode, Key: "accelerator", Value: "high-power"},
									},
									Resources: map[string]resource.Quantity{
										"gpu":    resource.MustParse("1"),
										"cpu":    resource.MustParse("0.5"),
										"memory": resource.MustParse("1Gi"),
									},
								},
							},
							MountPoint: kdb.MountPoint{
								Id: 9_03_01_010, Path: "/out/1",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "model"},
									{Key: "task", Value: "encode"},
								}),
							},
						},
					},
					{
						KnitDataBody: kdb.KnitDataBody{
							KnitId: Padding36("data/ch3#2:run/ch3#1/out/2"), VolumeRef: "#data/ch3#2",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "project", Value: "testing"},
								{Key: "type", Value: "model"},
								{Key: "task", Value: "decode"},
								{Key: "model-version", Value: "1"},
								{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#2:run/ch3#1/out/2")},
								{
									Key:   kdb.KeyKnitTimestamp,
									Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 102*time.Millisecond)).String(),
								},
							}),
						},
						Upsteram: kdb.Dependency{
							RunBody: kdb.RunBody{
								Id: Padding36("run/ch3#1:plan/ch3#1"), Status: kdb.Done,
								Exit: &kdb.RunExit{
									Code:    0,
									Message: "succeeded",
								},
								UpdatedAt: START_AT.Add(30*time.Second + 100*time.Millisecond),
								PlanBody: kdb.PlanBody{
									PlanId: Padding36("plan/ch3#1:trainer"), Active: true,
									Hash:  Padding64("#plan/ch3#1:trainer"),
									Image: &kdb.ImageIdentifier{Image: "repo.invalid/trainer", Version: "ch3#1"},
									OnNode: []kdb.OnNode{
										{Mode: kdb.MustOnNode, Key: "accelerator", Value: "gpu"},
										{Mode: kdb.PreferOnNode, Key: "accelerator", Value: "high-power"},
									},
									Resources: map[string]resource.Quantity{
										"gpu":    resource.MustParse("1"),
										"cpu":    resource.MustParse("0.5"),
										"memory": resource.MustParse("1Gi"),
									},
								},
							},
							MountPoint: kdb.MountPoint{
								Id: 9_03_01_020, Path: "/out/2",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "model"},
									{Key: "task", Value: "decode"},
								}),
							},
						},
					},
					{
						KnitDataBody: kdb.KnitDataBody{
							KnitId: Padding36("data/ch3#3:run/ch3#1/out/3"), VolumeRef: "#data/ch3#3",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "project", Value: "testing"},
								{Key: "type", Value: "validation-stats"},
								{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#3:run/ch3#1/out/3")},
								{
									Key:   kdb.KeyKnitTimestamp,
									Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 103*time.Millisecond)).String(),
								},
							}),
						},
						Upsteram: kdb.Dependency{
							RunBody: kdb.RunBody{
								Id: Padding36("run/ch3#1:plan/ch3#1"), Status: kdb.Done,
								Exit: &kdb.RunExit{
									Code:    0,
									Message: "succeeded",
								},
								UpdatedAt: START_AT.Add(30*time.Second + 100*time.Millisecond),
								PlanBody: kdb.PlanBody{
									PlanId: Padding36("plan/ch3#1:trainer"), Active: true,
									Hash:  Padding64("#plan/ch3#1:trainer"),
									Image: &kdb.ImageIdentifier{Image: "repo.invalid/trainer", Version: "ch3#1"},
									OnNode: []kdb.OnNode{
										{Mode: kdb.MustOnNode, Key: "accelerator", Value: "gpu"},
										{Mode: kdb.PreferOnNode, Key: "accelerator", Value: "high-power"},
									},
									Resources: map[string]resource.Quantity{
										"gpu":    resource.MustParse("1"),
										"cpu":    resource.MustParse("0.5"),
										"memory": resource.MustParse("1Gi"),
									},
								},
							},
							MountPoint: kdb.MountPoint{
								Id: 9_03_01_030, Path: "/out/3",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "validation-stats"},
								}),
							},
						},
					},
					{
						KnitDataBody: kdb.KnitDataBody{
							KnitId: Padding36("data/ch3#4:run/ch3#1/log"), VolumeRef: "#data/ch3#4",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "project", Value: "testing"},
								{Key: "type", Value: "log"},
								{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#4:run/ch3#1/log")},
								{
									Key:   kdb.KeyKnitTimestamp,
									Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 104*time.Millisecond)).String(),
								},
							}),
						},
						Upsteram: kdb.Dependency{
							RunBody: kdb.RunBody{
								Id: Padding36("run/ch3#1:plan/ch3#1"), Status: kdb.Done,
								Exit: &kdb.RunExit{
									Code:    0,
									Message: "succeeded",
								},
								UpdatedAt: START_AT.Add(30*time.Second + 100*time.Millisecond),
								PlanBody: kdb.PlanBody{
									PlanId: Padding36("plan/ch3#1:trainer"), Active: true,
									Hash:  Padding64("#plan/ch3#1:trainer"),
									Image: &kdb.ImageIdentifier{Image: "repo.invalid/trainer", Version: "ch3#1"},
									OnNode: []kdb.OnNode{
										{Mode: kdb.MustOnNode, Key: "accelerator", Value: "gpu"},
										{Mode: kdb.PreferOnNode, Key: "accelerator", Value: "high-power"},
									},
									Resources: map[string]resource.Quantity{
										"gpu":    resource.MustParse("1"),
										"cpu":    resource.MustParse("0.5"),
										"memory": resource.MustParse("1Gi"),
									},
								},
							},
							MountPoint: kdb.MountPoint{
								Id: 9_03_01_001, Path: "/log",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "log"},
								}),
							},
						},
					},
				},
			},
		},
		{ // chapter 4: test model and reporting (is runnning)
			"test model and reporting (is runnning)",
			tables.Operation{
				Plan: []tables.Plan{
					{
						PlanId: Padding36("plan/ch4#1:test"),
						Hash:   Padding64("#plan/ch4#1"), Active: true,
					},
					{
						PlanId: Padding36("plan/ch4#2:notify"),
						Hash:   Padding64("#plan/ch4#2"), Active: false,
					},
					{
						PlanId: Padding36("plan/ch4#3:report"),
						Hash:   Padding64("#plan/ch4#3"), Active: true,
					},
				},
				PlanImage: []tables.PlanImage{
					{
						PlanId: Padding36("plan/ch4#1:test"),
						Image:  "repo.invalid/test", Version: "v4#1",
					},
					{
						PlanId: Padding36("plan/ch4#2:notify"),
						Image:  "repo.invalid/norifier", Version: "v4#2",
					},
					{
						PlanId: Padding36("plan/ch4#3:report"),
						Image:  "repo.invalid/reporter", Version: "v4#3",
					},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{
						PlanId:  Padding36("plan/ch4#1:test"),
						InputId: 9_04_01_100,
						Path:    "/in",
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "model"},
							{Key: "extra-key", Value: "extra-value"},
							// tags in mountpoint and assigned data can be unmatch
							// if tags in data are modified after assignation.
						},
					},

					{
						PlanId:  Padding36("plan/ch4#2:notify"),
						InputId: 9_04_02_100,
						Path:    "/trigger",
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
						},
						KnitId: []string{Padding36("data/ch3#4:run/ch3#1/log")},
					},

					{
						PlanId:  Padding36("plan/ch4#3:report"),
						InputId: 9_04_03_100,
						Path:    "/metrics",
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
						},
						Timestamp: []time.Time{
							START_AT.Add(30*time.Second + 103*time.Millisecond),
						},
					},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{
						PlanId:   Padding36("plan/ch4#1:test"),
						OutputId: 9_04_01_010,
						Path:     "/out",
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "test-report"},
						},
					},
					{
						PlanId:   Padding36("plan/ch4#3:report"),
						OutputId: 9_04_03_010,
						Path:     "/out",
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "report"},
							{Key: "format", Value: "pdf"},
						},
					},
				},
				Steps: []tables.Step{
					{
						Run: tables.Run{
							RunId: Padding36("run/ch4#1:plan/ch4#1"), Status: kdb.Waiting,
							PlanId:    Padding36("plan/ch4#1:test"),
							UpdatedAt: START_AT.Add(40*time.Second + 101*time.Millisecond),
						},
						Assign: []tables.Assign{
							{
								KnitId:  Padding36("data/ch3#1:run/ch3#1/out/1"),
								InputId: 9_04_01_100,
								RunId:   Padding36("run/ch4#1:plan/ch4#1"),
								PlanId:  Padding36("plan/ch4#1:test"),
							},
						},
						// no output since run/ch4#1... is waiting run
					},

					{
						Run: tables.Run{
							RunId: Padding36("run/ch4#2:plan/ch4#1"), Status: kdb.Invalidated,
							PlanId:    Padding36("plan/ch4#1:test"),
							UpdatedAt: START_AT.Add(40*time.Second + 102*time.Millisecond),
						},
						Assign: []tables.Assign{
							{
								InputId: 9_04_01_100,
								RunId:   Padding36("run/ch4#2:plan/ch4#1"),
								KnitId:  Padding36("data/ch3#2:run/ch3#1/out/2"),
								PlanId:  Padding36("plan/ch4#1:test"),
							},
						},
						// no output since run/ch4#2... is waiting
					},

					{
						Run: tables.Run{
							RunId: Padding36("run/ch4#3:plan/ch4#2"), Status: kdb.Deactivated,
							PlanId:    Padding36("plan/ch4#2:notify"),
							UpdatedAt: START_AT.Add(40*time.Second + 103*time.Millisecond),
						},
						Assign: []tables.Assign{
							{
								InputId: 9_04_02_100,
								RunId:   Padding36("run/ch4#3:plan/ch4#2"),
								KnitId:  Padding36("data/ch3#4:run/ch3#1/log"),
								PlanId:  Padding36("plan/ch4#2:notify"),
							},
						},
						// no output since run/ch4#3... is deactivated
					},

					{
						Run: tables.Run{
							RunId: Padding36("run/ch4#4:plan/ch4#3"), Status: kdb.Running,
							PlanId:    Padding36("plan/ch4#3:report"),
							UpdatedAt: START_AT.Add(40*time.Second + 104*time.Millisecond),
						},
						Worker: "worker/ch4#1:run/ch4#4",
						Assign: []tables.Assign{
							{
								KnitId:  Padding36("data/ch3#3:run/ch3#1/out/3"),
								InputId: 9_04_03_100,
								RunId:   Padding36("run/ch4#4:plan/ch4#3"),
								PlanId:  Padding36("plan/ch4#3:report"),
							},
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    Padding36("data/ch4#1:run/ch4#4/out"),
								VolumeRef: "#data/ch4#1",
								OutputId:  9_04_03_010,
								RunId:     Padding36("run/ch4#4:plan/ch4#3"),
								PlanId:    Padding36("plan/ch4#3:report"),
							}: {
								UserTag: []kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "report"},
									{Key: "format", Value: "pdf"},
								},
							},
						},
					},
				},
			},
			expectation{
				plan: []kdb.Plan{
					{
						PlanBody: kdb.PlanBody{
							PlanId: Padding36("plan/ch4#1:test"),
							Hash:   Padding64("#plan/ch4#1"), Active: true,
							Image: &kdb.ImageIdentifier{Image: "repo.invalid/test", Version: "v4#1"},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 9_04_01_100, Path: "/in",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "model"},
									{Key: "extra-key", Value: "extra-value"},
								}),
							},
						},
						Outputs: []kdb.MountPoint{
							{
								Id: 9_04_01_010, Path: "/out",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "test-report"},
								}),
							},
						},
					},
					{
						PlanBody: kdb.PlanBody{
							PlanId: Padding36("plan/ch4#2:notify"),
							Hash:   Padding64("#plan/ch4#2"), Active: false,
							Image: &kdb.ImageIdentifier{Image: "repo.invalid/norifier", Version: "v4#2"},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 9_04_02_100, Path: "/trigger",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#4:run/ch3#1/log")},
								}),
							},
						},
					},
					{
						PlanBody: kdb.PlanBody{
							PlanId: Padding36("plan/ch4#3:report"),
							Hash:   Padding64("#plan/ch4#3"), Active: true,
							Image: &kdb.ImageIdentifier{Image: "repo.invalid/reporter", Version: "v4#3"},
						},
						Inputs: []kdb.MountPoint{
							{
								Id: 9_04_03_100, Path: "/metrics",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{
										Key:   kdb.KeyKnitTimestamp,
										Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 103*time.Millisecond)).String(),
									},
								}),
							},
						},
						Outputs: []kdb.MountPoint{
							{
								Id: 9_04_03_010, Path: "/out",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{Key: "type", Value: "report"},
									{Key: "format", Value: "pdf"},
								}),
							},
						},
					},
				},
				run: []kdb.Run{
					{
						RunBody: kdb.RunBody{
							Id: Padding36("run/ch4#1:plan/ch4#1"), Status: kdb.Waiting,
							UpdatedAt: START_AT.Add(40*time.Second + 101*time.Millisecond),
							PlanBody: kdb.PlanBody{
								PlanId: Padding36("plan/ch4#1:test"),
								Hash:   Padding64("#plan/ch4#1"), Active: true,
								Image: &kdb.ImageIdentifier{Image: "repo.invalid/test", Version: "v4#1"},
							},
						},
						Inputs: []kdb.Assignment{
							{
								MountPoint: kdb.MountPoint{
									Id: 9_04_01_100, Path: "/in",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "model"},
										{Key: "extra-key", Value: "extra-value"},
									}),
								},
								KnitDataBody: kdb.KnitDataBody{
									KnitId: Padding36("data/ch3#1:run/ch3#1/out/1"), VolumeRef: "#data/ch3#1",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "model"},
										{Key: "task", Value: "encode"},
										{Key: "model-version", Value: "1"},
										{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#1:run/ch3#1/out/1")},
										{
											Key:   kdb.KeyKnitTimestamp,
											Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 101*time.Millisecond)).String(),
										},
									}),
								},
							},
						},
						Outputs: []kdb.Assignment{
							{
								MountPoint: kdb.MountPoint{
									Id: 9_04_01_010, Path: "/out",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "test-report"},
									}),
								},
								// no data are assigned.
							},
						},
					},

					{
						RunBody: kdb.RunBody{
							Id: Padding36("run/ch4#2:plan/ch4#1"), Status: kdb.Invalidated,
							UpdatedAt: START_AT.Add(40*time.Second + 102*time.Millisecond),
							PlanBody: kdb.PlanBody{
								PlanId: Padding36("plan/ch4#1:test"),
								Hash:   Padding64("#plan/ch4#1"), Active: true,
								Image: &kdb.ImageIdentifier{Image: "repo.invalid/test", Version: "v4#1"},
							},
						},
						Inputs: []kdb.Assignment{
							{
								MountPoint: kdb.MountPoint{
									Id: 9_04_01_100, Path: "/in",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "model"},
										{Key: "extra-key", Value: "extra-value"},
									}),
								},
								KnitDataBody: kdb.KnitDataBody{
									KnitId: Padding36("data/ch3#2:run/ch3#1/out/2"), VolumeRef: "#data/ch3#2",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "model"},
										{Key: "task", Value: "decode"},
										{Key: "model-version", Value: "1"},
										{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#2:run/ch3#1/out/2")},
										{
											Key:   kdb.KeyKnitTimestamp,
											Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 102*time.Millisecond)).String(),
										},
									}),
								},
							},
						},
						Outputs: []kdb.Assignment{
							{
								MountPoint: kdb.MountPoint{
									Id: 9_04_01_010, Path: "/out",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "test-report"},
									}),
								},
							},
						},
					},

					{
						RunBody: kdb.RunBody{
							Id: Padding36("run/ch4#3:plan/ch4#2"), Status: kdb.Deactivated,
							UpdatedAt: START_AT.Add(40*time.Second + 103*time.Millisecond),
							PlanBody: kdb.PlanBody{
								PlanId: Padding36("plan/ch4#2:notify"),
								Hash:   Padding64("#plan/ch4#2"), Active: false,
								Image: &kdb.ImageIdentifier{
									Image: "repo.invalid/norifier", Version: "v4#2",
								},
							},
						},
						Inputs: []kdb.Assignment{
							{
								MountPoint: kdb.MountPoint{
									Id:   9_04_02_100,
									Path: "/trigger",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#4:run/ch3#1/log")},
									}),
								},

								KnitDataBody: kdb.KnitDataBody{
									KnitId: Padding36("data/ch3#4:run/ch3#1/log"), VolumeRef: "#data/ch3#4",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "log"},
										{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#4:run/ch3#1/log")},
										{
											Key:   kdb.KeyKnitTimestamp,
											Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 104*time.Millisecond)).String(),
										},
									}),
								},
							},
							// no output since run/ch4#3... is deactivated
						},
					},

					{
						RunBody: kdb.RunBody{
							Id: Padding36("run/ch4#4:plan/ch4#3"), Status: kdb.Running,
							UpdatedAt: START_AT.Add(40*time.Second + 104*time.Millisecond),
							PlanBody: kdb.PlanBody{
								PlanId: Padding36("plan/ch4#3:report"),
								Hash:   Padding64("#plan/ch4#3"), Active: true,
								Image: &kdb.ImageIdentifier{
									Image: "repo.invalid/reporter", Version: "v4#3",
								},
							},
							WorkerName: "worker/ch4#1:run/ch4#4",
						},
						Inputs: []kdb.Assignment{
							{
								MountPoint: kdb.MountPoint{
									Id: 9_04_03_100, Path: "/metrics",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{
											Key:   kdb.KeyKnitTimestamp,
											Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 103*time.Millisecond)).String(),
										},
									}),
								},
								KnitDataBody: kdb.KnitDataBody{
									KnitId: Padding36("data/ch3#3:run/ch3#1/out/3"), VolumeRef: "#data/ch3#3",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "validation-stats"},
										{Key: kdb.KeyKnitId, Value: Padding36("data/ch3#3:run/ch3#1/out/3")},
										{
											Key:   kdb.KeyKnitTimestamp,
											Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 103*time.Millisecond)).String(),
										},
									}),
								},
							},
						},
						Outputs: []kdb.Assignment{
							{
								MountPoint: kdb.MountPoint{
									Id:   9_04_03_010,
									Path: "/out",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "report"},
										{Key: "format", Value: "pdf"},
									}),
								},
								KnitDataBody: kdb.KnitDataBody{
									KnitId:    Padding36("data/ch4#1:run/ch4#4/out"),
									VolumeRef: "#data/ch4#1",
									Tags: kdb.NewTagSet([]kdb.Tag{
										{Key: "project", Value: "testing"},
										{Key: "type", Value: "report"},
										{Key: "format", Value: "pdf"},
										{Key: kdb.KeyKnitId, Value: Padding36("data/ch4#1:run/ch4#4/out")},
										{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
									}),
								},
							},
						},
					},
				},
				data: []kdb.KnitData{
					{
						KnitDataBody: kdb.KnitDataBody{
							KnitId:    Padding36("data/ch4#1:run/ch4#4/out"),
							VolumeRef: "#data/ch4#1",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "project", Value: "testing"},
								{Key: "type", Value: "report"},
								{Key: "format", Value: "pdf"},
								{Key: kdb.KeyKnitId, Value: Padding36("data/ch4#1:run/ch4#4/out")},
								{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
							}),
						},
						Upsteram: kdb.Dependency{
							MountPoint: kdb.MountPoint{
								Id: 9_04_03_100, Path: "/metrics",
								Tags: kdb.NewTagSet([]kdb.Tag{
									{Key: "project", Value: "testing"},
									{
										Key:   kdb.KeyKnitTimestamp,
										Value: rfctime.RFC3339(START_AT.Add(30*time.Second + 103*time.Millisecond)).String(),
									},
								}),
							},
							RunBody: kdb.RunBody{
								Id: Padding36("run/ch4#4:plan/ch4#3"), Status: kdb.Running,
								UpdatedAt: START_AT.Add(40*time.Second + 104*time.Millisecond),
								PlanBody: kdb.PlanBody{
									PlanId: Padding36("plan/ch4#3:report"),
									Hash:   Padding64("#plan/ch4#3"), Active: true,
									Image: &kdb.ImageIdentifier{
										Image: "repo.invalid/reporter", Version: "v4#3",
									},
								},
							},
						},
					},
				},
			},
		},
	} {
		t.Run(fmt.Sprintf("[#%d] %s", nth+1, chap.title), func(t *testing.T) {
			if err := chap.operation.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}
			{ // theory: should always be held for all chapters.
				t.Run("it returns empty for empty query", func(t *testing.T) {
					testee := kpgdata.New(pgpool)
					actual := try.To(testee.Get(ctx, []string{})).OrFatal(t)

					if len(actual) != 0 {
						t.Errorf("unexpected data is returned: %+v", actual)
					}
				})
				t.Run("it returns empty for query having only missing knitIds", func(t *testing.T) {
					testee := kpgdata.New(pgpool)
					actual := try.To(testee.Get(ctx, []string{"data/missing"})).OrFatal(t)

					if len(actual) != 0 {
						t.Errorf("unexpected data is returned: %+v", actual)
					}
				})
			}

			t.Run("test: KnitData", func(t *testing.T) {
				expected := utils.ToMap(chap.expectation.data, func(d kdb.KnitData) string { return d.KnitId })
				for name, query := range map[string][]string{
					"get existing items":                      utils.KeysOf(expected),
					"get existing items + non-existing items": utils.Concat([]string{"data/missing"}, utils.KeysOf(expected)),
				} {
					t.Run(name, func(t *testing.T) {
						testee := kpgdata.New(pgpool)
						actual := try.To(testee.Get(ctx, query)).OrFatal(t)

						if !cmp.MapEqWith(actual, expected, func(a, x kdb.KnitData) bool { return a.Equal(&x) }) {
							t.Errorf(
								"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
								actual, expected,
							)
						}
					})
				}
			})

			t.Run("test: Run", func(t *testing.T) {
				expected := utils.ToMap(chap.expectation.run, func(d kdb.Run) string { return d.Id })
				for name, query := range map[string][]string{
					"get existing items":                      utils.KeysOf(expected),
					"get existing items + non-existing items": utils.Concat([]string{"data/missing"}, utils.KeysOf(expected)),
				} {
					t.Run(name, func(t *testing.T) {
						testee := kpgrun.New(pgpool) // mocks are not used in this testcase
						actual := try.To(testee.Get(ctx, query)).OrFatal(t)

						if !cmp.MapEqWith(actual, expected, func(a, x kdb.Run) bool { return a.Equal(&x) }) {
							t.Errorf(
								"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
								actual, expected,
							)
						}
					})
				}
			})

			t.Run("test: Plan", func(t *testing.T) {
				expected := utils.ToMap(chap.expectation.plan, func(d kdb.Plan) string { return d.PlanId })
				for name, query := range map[string][]string{
					"get existing items":                      utils.KeysOf(expected),
					"get existing items + non-existing items": utils.Concat([]string{"data/missing"}, utils.KeysOf(expected)),
				} {
					t.Run(name, func(t *testing.T) {
						testee := kpgplan.New(pgpool) // mocks are not used in this testcase
						actual := try.To(testee.Get(ctx, query)).OrFatal(t)

						if !cmp.MapEqWith(actual, expected, func(a *kdb.Plan, x kdb.Plan) bool {
							return a.Equal(&x)
						}) {
							t.Errorf(
								"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
								maps.DerefOf(actual), expected,
							)
						}
					})
				}
			})
		})
	}
}
