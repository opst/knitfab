package tables_test

import (
	"context"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/db/postgres/marshal"
	"github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils"
	ptr "github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/try"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestOperation(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	testee := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: th.Padding36("plan-1"), Active: true, Hash: th.Padding64("#plan-1")},
			{PlanId: th.Padding36("plan-2"), Active: false, Hash: th.Padding64("#plan-2")},
			{PlanId: th.Padding36("plan-3"), Active: true, Hash: th.Padding64("#plan-3")},
			{PlanId: th.Padding36("plan-4"), Active: false, Hash: th.Padding64("#plan-4")},
		},
		PlanResources: []tables.PlanResource{
			{PlanId: th.Padding36("plan-1"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("1"))},
			{PlanId: th.Padding36("plan-2"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("500Mi"))},
		},
		OnNode: []tables.PlanOnNode{
			{PlanId: th.Padding36("plan-1"), Mode: kdb.MayOnNode, Key: "key1", Value: "value1"},
			{PlanId: th.Padding36("plan-2"), Mode: kdb.MustOnNode, Key: "key2", Value: "value2"},
			{PlanId: th.Padding36("plan-2"), Mode: kdb.PreferOnNode, Key: "key2", Value: "value2"},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: th.Padding36("plan-1"), Name: "knit#example-1"},
			{PlanId: th.Padding36("plan-2"), Name: "knit#exmaple-2"},
		},
		PlanImage: []tables.PlanImage{
			{PlanId: th.Padding36("plan-3"), Image: "repo.invalid/image-3", Version: "v0.1"},
			{PlanId: th.Padding36("plan-4"), Image: "repo.invalid/image-4", Version: "v0.2"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 1010, PlanId: th.Padding36("plan-1"), Path: "/out/1"}: {},
			{OutputId: 2010, PlanId: th.Padding36("plan-2"), Path: "/out/1"}: {},

			{OutputId: 3010, PlanId: th.Padding36("plan-3"), Path: "/out/1"}: {
				UserTag: []kdb.Tag{
					{Key: "project", Value: "testing"},
					{Key: "type", Value: "training-data"},
					{Key: "format", Value: "png"},
				},
			},
			{OutputId: 3020, PlanId: th.Padding36("plan-3"), Path: "/out/2"}: {
				UserTag: []kdb.Tag{
					{Key: "project", Value: "testing"},
					{Key: "type", Value: "parameter"},
					{Key: "format", Value: "json"},
				},
			},
			{OutputId: 3030, PlanId: th.Padding36("plan-3"), Path: "/out/3"}: {
				UserTag: []kdb.Tag{}, // no user tags
			},
			{OutputId: 3001, PlanId: th.Padding36("plan-3"), Path: "/log"}: {
				IsLog: true,
				UserTag: []kdb.Tag{
					{Key: "type", Value: "log"},
				},
			},

			{OutputId: 4010, PlanId: th.Padding36("plan-4"), Path: "/out/1"}: {
				UserTag: []kdb.Tag{
					{Key: "project", Value: "testing"},
					{Key: "format", Value: "onnx"},
				},
			},
			{OutputId: 4020, PlanId: th.Padding36("plan-4"), Path: "/out/2"}: {
				UserTag: []kdb.Tag{
					{Key: "project", Value: "testing"},
					{Key: "type", Value: "tensorboard"},
				},
			},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			{InputId: 3100, PlanId: th.Padding36("plan-3"), Path: "/in/1"}: {
				UserTag: []kdb.Tag{
					{Key: "project", Value: "testing"},
					{Key: "type", Value: "raw-data"},
				},
			},
			{InputId: 3200, PlanId: th.Padding36("plan-3"), Path: "/in/2"}: {
				UserTag: []kdb.Tag{
					{Key: "project", Value: "testing"},
					{Key: "type", Value: "config"},
					{Key: "task", Value: "preprocess"},
				},
			},

			{InputId: 4100, PlanId: th.Padding36("plan-4"), Path: "/in/1"}: {
				UserTag: []kdb.Tag{
					{Key: "project", Value: "testing"},
					{Key: "type", Value: "training-data"},
				},
			},
			{InputId: 4200, PlanId: th.Padding36("plan-3"), Path: "/in/2"}: {
				UserTag: []kdb.Tag{
					{Key: "project", Value: "testing"},
					{Key: "type", Value: "config"},
					{Key: "task", Value: "training"},
				},
			},
			{InputId: 4300, PlanId: th.Padding36("plan-4"), Path: "/in/3"}: {
				UserTag: []kdb.Tag{
					{Key: "project", Value: "testing"},
					{Key: "type", Value: "validation-data"},
				},
				Timestamp: []time.Time{
					try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
			},
			{InputId: 4400, PlanId: th.Padding36("plan-4"), Path: "/in/4"}: {
				UserTag: []kdb.Tag{
					{Key: "project", Value: "testing"},
					{Key: "type", Value: "config"},
					{Key: "task", Value: "validation"},
				},
				KnitId: []string{th.Padding36("data-a.run-b.plan-c")},
			},
		},
		Steps: []tables.Step{
			{
				Run: tables.Run{
					RunId: th.Padding36("run-1.plan-1"), PlanId: th.Padding36("plan-1"),
					Status: kdb.Done,
					LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
						"2021-10-11T12:13:24.567+09:00",
					)).OrFatal(t).Time(),

					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2021-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data-1.run-1.plan-1"),
						VolumeRef: "pvc-1-1-1",
						PlanId:    th.Padding36("plan-1"),
						RunId:     th.Padding36("run-1.plan-1"),
						OutputId:  1010,
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "raw-data"},
							{Key: "source", Value: "https://website.invalid/images"},
						},
						Timestamp: th.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2021-10-11T12:13:14.567+09:00",
						)).OrFatal(t).Time()),
					},
				},
				Exit: &tables.RunExit{
					RunId:    th.Padding36("run-1.plan-1"),
					ExitCode: 0,
					Message:  "successed",
				},
			},
			{
				Run: tables.Run{
					RunId: th.Padding36("run-2.plan-1"), PlanId: th.Padding36("plan-1"),
					Status: kdb.Failed,
					LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
						"2021-10-11T12:23:15.567+09:00",
					)).OrFatal(t).Time(),
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2021-10-11T12:13:15.567+09:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data-1.run-2.plan-1"),
						VolumeRef: "pvc-1-2-1",
						PlanId:    th.Padding36("plan-1"),
						RunId:     th.Padding36("run-2.plan-1"),
						OutputId:  1010,
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "raw-data"},
							{Key: "source", Value: "https://website.invalid/images/subset1"},
						},
						Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2021-10-11T12:13:15.667+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},

			{
				Run: tables.Run{
					RunId: th.Padding36("run-1.plan-2"), PlanId: th.Padding36("plan-2"),
					Status: kdb.Deactivated,
					LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
						"2021-10-11T12:24:15.567+09:00",
					)).OrFatal(t).Time(),
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2021-10-11T12:14:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data-1.run-1.plan-2"),
						VolumeRef: "pvc-2-1-1",
						PlanId:    th.Padding36("plan-2"),
						RunId:     th.Padding36("run-1.plan-2"),
						OutputId:  2010,
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "config"},
							{Key: "task", Value: "preprocess"},
						},
						Agent: []tables.DataAgent{
							{
								Name:   "agent-1.data-1.run-1.plan-2",
								Mode:   kdb.DataAgentRead.String(),
								KnitId: th.Padding36("data-1.run-1.plan-2"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2021-10-11T12:13:14.567+09:00",
								)).OrFatal(t).Time(),
							},
							{
								Name:   "agent-2.data-1.run-1.plan-2",
								Mode:   kdb.DataAgentWrite.String(),
								KnitId: th.Padding36("data-1.run-1.plan-2"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2021-10-11T12:13:15.567+09:00",
								)).OrFatal(t).Time(),
							},
						},
					},
				},
			},

			{
				Run: tables.Run{
					RunId: th.Padding36("run-1.plan-3"), PlanId: th.Padding36("plan-3"),
					Status: kdb.Done,
					LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
						"2021-10-11T12:24:15.567+09:00",
					)).OrFatal(t).Time(),
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2021-10-11T13:14:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Assign: []tables.Assign{
					{
						RunId:   th.Padding36("run-1.plan-3"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3100,
						KnitId:  th.Padding36("data-1.run-1.plan-1"),
					},
					{
						RunId:   th.Padding36("run-1.plan-3"),
						PlanId:  th.Padding36("plan-3"),
						InputId: 3200,
						KnitId:  th.Padding36("data-1.run-1.plan-2"),
					},
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId:    th.Padding36("data-1.run-1.plan-3"),
						VolumeRef: "pvc-3-1-1",
						PlanId:    th.Padding36("plan-3"),
						RunId:     th.Padding36("run-1.plan-3"),
						OutputId:  3010,
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "config"},
							{Key: "task", Value: "pretrain"},
						},
						Timestamp: th.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2021-10-11T12:13:14.567+09:00",
						)).OrFatal(t).Time()),
						Agent: []tables.DataAgent{
							{
								Name:   "agent-1.data-1.run-1.plan-3",
								Mode:   kdb.DataAgentRead.String(),
								KnitId: th.Padding36("data-1.run-1.plan-3"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2021-10-11T12:13:14.567+09:00",
								)).OrFatal(t).Time(),
							},
							{
								Name:   "agent-2.data-1.run-1.plan-3",
								Mode:   kdb.DataAgentWrite.String(),
								KnitId: th.Padding36("data-1.run-1.plan-3"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2021-10-11T12:13:15.567+09:00",
								)).OrFatal(t).Time(),
							},
						},
					},
					{
						KnitId:    th.Padding36("data-2.run-1.plan-3"),
						VolumeRef: "pvc-3-1-2",
						PlanId:    th.Padding36("plan-3"),
						RunId:     th.Padding36("run-1.plan-3"),
						OutputId:  3020,
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "config"},
							{Key: "task", Value: "pretrain"},
						},
						Timestamp: th.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2021-10-11T12:13:14.567+09:00",
						)).OrFatal(t).Time()),
					},
					{
						KnitId:    th.Padding36("data-3.run-1.plan-3"),
						VolumeRef: "pvc-3-1-3",
						PlanId:    th.Padding36("plan-3"),
						RunId:     th.Padding36("run-1.plan-3"),
						OutputId:  3030,
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "config"},
							{Key: "task", Value: "pretrain"},
						},
						Timestamp: th.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2021-10-11T12:13:14.567+09:00",
						)).OrFatal(t).Time()),
					},
					{
						KnitId:    th.Padding36("log-1.run-1.plan-3"),
						VolumeRef: "pvc-log1-1-3",
						PlanId:    th.Padding36("plan-3"),
						RunId:     th.Padding36("run-1.plan-3"),
						OutputId:  3001,
					}: {
						UserTag: []kdb.Tag{
							{Key: "project", Value: "testing"},
							{Key: "type", Value: "config"},
							{Key: "task", Value: "pretrain"},
						},
						Timestamp: th.Ref(try.To(rfctime.ParseRFC3339DateTime(
							"2021-10-11T12:13:14.567+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
		},
		Nomination: []tables.Nomination{
			{InputId: 3100, KnitId: th.Padding36("data-1.run-1.plan-1"), Updated: true},
			{InputId: 3200, KnitId: th.Padding36("data-1.run-1.plan-3"), Updated: false},
		},
		Garbage: []tables.Garbage{
			{KnitId: th.Padding36("data-x.run-x.plan-1"), VolumeRef: "pvc-data-x"},
			{KnitId: th.Padding36("data-y.run-x.plan-1"), VolumeRef: "pvc-data-y"},
		},
	}

	ctx := context.Background()
	pool := poolBroaker.GetPool(ctx, t)
	if err := testee.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}

	t.Run("plan", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		actual := try.To(scanner.New[tables.Plan]().QueryAll(
			ctx, conn, `table "plan"`,
		)).OrFatal(t)

		expected := testee.Plan

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("resources", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		actual := try.To(scanner.New[tables.PlanResource]().QueryAll(
			ctx, conn, `table "plan_resource"`,
		)).OrFatal(t)

		expected := testee.PlanResources
		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("plan_on_node", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		actual := try.To(scanner.New[tables.PlanOnNode]().QueryAll(
			ctx, conn, `table "plan_on_node"`,
		)).OrFatal(t)

		expected := testee.OnNode

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("plan_pseudo", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		actual := try.To(scanner.New[tables.PlanPseudo]().QueryAll(
			ctx, conn, `table "plan_pseudo"`,
		)).OrFatal(t)

		expected := testee.PlanPseudo

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("plan_image", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		actual := try.To(scanner.New[tables.PlanImage]().QueryAll(
			ctx, conn, `table "plan_image"`,
		)).OrFatal(t)

		expected := testee.PlanImage

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("output", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()
		actual := try.To(scanner.New[tables.Output]().QueryAll(
			ctx, conn, `table "output"`,
		)).OrFatal(t)

		expected := utils.KeysOf(testee.Outputs)

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("output tags", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		type tagOutput struct {
			OutputId int
			Key      string
			Value    string
		}
		actual := try.To(scanner.New[tagOutput]().QueryAll(
			ctx, conn,
			`
			with
			"a" as (select "output_id", "tag_id" from "tag_output"),
			"b" as (
				select "output_id", "value", "key_id" from "a"
				inner join "tag" on "a"."tag_id" = "id"
			)
			select "output_id", "value", "key" from "b"
			inner join "tag_key" on "key_id" = "id"
			`,
		)).OrFatal(t)

		expected := []tagOutput{}
		for output, attr := range testee.Outputs {
			outputId := output.OutputId
			for _, a := range attr.UserTag {
				expected = append(expected, tagOutput{
					OutputId: outputId, Key: a.Key, Value: a.Value,
				})
			}
		}

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("log", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		type log struct {
			OutputId int
			PlanId   string
		}
		actual := try.To(scanner.New[log]().QueryAll(
			ctx, conn, `table "log"`,
		)).OrFatal(t)

		expected := []log{}
		for output, attr := range testee.Outputs {
			if !attr.IsLog {
				continue
			}
			expected = append(expected, log{
				OutputId: output.OutputId, PlanId: output.PlanId,
			})
		}

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("input", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		actual := try.To(scanner.New[tables.Input]().QueryAll(
			ctx, conn, `table "input"`,
		)).OrFatal(t)

		expected := utils.KeysOf(testee.Inputs)

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("input tags", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		type tagInput struct {
			InputId int
			Key     string
			Value   string
		}
		actual := try.To(scanner.New[tagInput]().QueryAll(
			ctx, conn,
			`
			with
			"a" as (select "input_id", "tag_id" from "tag_input"),
			"b" as (
				select "input_id", "value", "key_id" from "a"
				inner join "tag" on "a"."tag_id" = "id"
			)
			select "input_id", "value", "key" from "b"
			inner join "tag_key" on "key_id" = "id"
			`,
		)).OrFatal(t)

		expected := []tagInput{}
		for input, attr := range testee.Inputs {
			outputId := input.InputId
			for _, a := range attr.UserTag {
				expected = append(expected, tagInput{
					InputId: outputId, Key: a.Key, Value: a.Value,
				})
			}
		}

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("input knit#id", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		type knitIdInput struct {
			InputId int
			KnitId  string
		}
		actual := try.To(scanner.New[knitIdInput]().QueryAll(
			ctx, conn, `table "knitid_input"`,
		)).OrFatal(t)

		expected := []knitIdInput{}
		for input, attr := range testee.Inputs {
			if attr.KnitId == nil {
				continue
			}
			for _, kid := range attr.KnitId {
				expected = append(expected, knitIdInput{
					InputId: input.InputId, KnitId: kid,
				})
			}
		}

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})
	t.Run("input knit#timestamp", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		type knitIdTimestamp struct {
			InputId   int
			Timestamp time.Time
		}
		actual := try.To(scanner.New[knitIdTimestamp]().QueryAll(
			ctx, conn, `table "timestamp_input"`,
		)).OrFatal(t)

		expected := []knitIdTimestamp{}
		for input, attr := range testee.Inputs {
			if attr.Timestamp == nil {
				continue
			}
			for _, timestamp := range attr.Timestamp {
				expected = append(expected, knitIdTimestamp{
					InputId: input.InputId, Timestamp: timestamp,
				})
			}
		}

		if !cmp.SliceContentEqWith(
			actual, expected,
			func(a, b knitIdTimestamp) bool {
				return a.InputId == b.InputId && a.Timestamp.Equal(b.Timestamp)
			},
		) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("run", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		actual := try.To(scanner.New[tables.Run]().QueryAll(
			ctx, conn, `table "run"`,
		)).OrFatal(t)

		expected := utils.Map(testee.Steps, func(s tables.Step) tables.Run {
			return s.Run
		})

		if !cmp.SliceContentEqWith(
			actual, expected,
			func(a, b tables.Run) bool { return a.Equal(&b) },
		) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("assign", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		actual := try.To(scanner.New[tables.Assign]().QueryAll(
			ctx, conn, `table "assign"`,
		)).OrFatal(t)

		expected := utils.Concat(
			utils.Map(
				testee.Steps,
				func(s tables.Step) []tables.Assign { return s.Assign },
			)...,
		)

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("run exit", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		actual := try.To(scanner.New[tables.RunExit]().QueryAll(
			ctx, conn, `table "run_exit"`,
		)).OrFatal(t)

		expected := []tables.RunExit{}
		for _, s := range testee.Steps {
			if s.Exit == nil {
				continue
			}
			expected = append(expected, *s.Exit)
		}

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("data", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		actual := try.To(scanner.New[tables.Data]().QueryAll(
			ctx, conn, `table "data"`,
		)).OrFatal(t)

		expected := utils.Concat(
			utils.Map(
				testee.Steps,
				func(s tables.Step) []tables.Data { return utils.KeysOf(s.Outcomes) },
			)...,
		)

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("data_tag", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		type tagData struct {
			KnitId string
			Key    string
			Value  string
		}
		actual := try.To(scanner.New[tagData]().QueryAll(
			ctx, conn,
			`
			with
			"a" as (select "knit_id", "tag_id" from "tag_data"),
			"b" as (
				select "knit_id", "value", "key_id" from "a"
				inner join "tag" on "a"."tag_id" = "id"
			)
			select "knit_id", "value", "key" from "b"
			inner join "tag_key" on "key_id" = "id"
			`,
		)).OrFatal(t)

		expected := utils.Concat(
			utils.Map(
				testee.Steps,
				func(s tables.Step) []tagData {
					ret := []tagData{}
					for d, tags := range s.Outcomes {
						for _, utag := range tags.UserTag {
							ret = append(ret, tagData{
								KnitId: d.KnitId, Key: utag.Key, Value: utag.Value,
							})
						}
					}
					return ret
				},
			)...,
		)

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("data_timestamp", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		type tagTimestamp struct {
			KnitId    string
			Timestamp time.Time
		}
		actual := try.To(scanner.New[tagTimestamp]().QueryAll(
			ctx, conn, `table "knit_timestamp"`,
		)).OrFatal(t)

		expected := utils.Concat(
			utils.Map(
				testee.Steps,
				func(s tables.Step) []tagTimestamp {
					ret := []tagTimestamp{}
					for d, tags := range s.Outcomes {
						if tags.Timestamp == nil {
							continue
						}
						ret = append(ret, tagTimestamp{
							KnitId: d.KnitId, Timestamp: *tags.Timestamp,
						})
					}
					return ret
				},
			)...,
		)

		if !cmp.SliceContentEqWith(
			actual, expected,
			func(a, b tagTimestamp) bool {
				return a.KnitId == b.KnitId && a.Timestamp.Equal(b.Timestamp)
			},
		) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("data agent", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		actual := try.To(scanner.New[tables.DataAgent]().QueryAll(
			ctx, conn, `table "data_agent"`,
		)).OrFatal(t)

		expected := utils.Concat(utils.Map( // concat * map = flatmap
			testee.Steps,
			func(s tables.Step) []tables.DataAgent {
				return utils.Concat(utils.Map(
					utils.ValuesOf(s.Outcomes),
					func(a tables.DataAttibutes) []tables.DataAgent {
						if a.Agent == nil {
							return []tables.DataAgent{}
						}
						return a.Agent
					},
				)...)
			},
		)...)

		if !cmp.SliceContentEqWith(
			actual, expected,
			func(a, b tables.DataAgent) bool { return a.Equal(&b) },
		) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)

		}

	})

	t.Run("nomination", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		actual := try.To(scanner.New[tables.Nomination]().QueryAll(
			ctx, conn, `table "nomination"`,
		)).OrFatal(t)

		expected := testee.Nomination

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

	t.Run("garbage", func(t *testing.T) {
		conn := try.To(pool.Acquire(ctx)).OrFatal(t)
		defer conn.Release()

		actual := try.To(scanner.New[tables.Garbage]().QueryAll(
			ctx, conn, `table "garbage"`,
		)).OrFatal(t)

		expected := testee.Garbage

		if !cmp.SliceContentEq(actual, expected) {
			t.Errorf("unmatch:\n===actual===\n%+v\n===expected===\n%+v", actual, expected)
		}
	})

}
