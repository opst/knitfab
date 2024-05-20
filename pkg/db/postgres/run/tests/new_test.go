package tests_test

import (
	"context"
	"testing"

	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	kpgnommock "github.com/opst/knitfab/pkg/db/postgres/nominator/mock"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
	"github.com/opst/knitfab/pkg/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	kpgrun "github.com/opst/knitfab/pkg/db/postgres/run"
	"github.com/opst/knitfab/pkg/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils"
	ptr "github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/opst/knitfab/pkg/utils/tuple"
)

func TestRun_New(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	type runComponent struct {
		body   tables.Run
		worker string
		assign []tables.Assign

		// output id -> data & its tag
		data map[int]tuple.Pair[tables.Data, []kdb.Tag]
	}

	equivRunComponent := func(a, b runComponent) bool {
		return a.body.PlanId == b.body.PlanId &&
			a.body.Status == b.body.Status &&
			cmp.SliceContentEqWith(
				a.assign, b.assign,
				func(aa, ba tables.Assign) bool {
					return aa.PlanId == ba.PlanId && aa.InputId == ba.InputId
				},
			) &&
			cmp.MapEqWith(
				a.data, b.data,
				func(da, db tuple.Pair[tables.Data, []kdb.Tag]) bool {
					return da.First.OutputId == db.First.OutputId &&
						da.First.PlanId == db.First.PlanId &&
						cmp.SliceContentEqWith(
							da.Second, db.Second,
							func(ta, tb kdb.Tag) bool { return ta.Equal(&tb) },
						)
				},
			)
	}

	consistentRunComponent := func(rc runComponent) bool {
		planId := rc.body.PlanId
		runId := rc.body.RunId
		for _, a := range rc.assign {
			if a.RunId != runId || a.PlanId != planId {
				return false
			}
		}
		for _, d := range rc.data {
			if d.First.RunId != runId || d.First.PlanId != planId {
				return false
			}
		}
		return true
	}

	type expectation struct {
		newRuns []runComponent

		// - key: plan id
		//
		// - value: knit ids which are to be locked when plan in key is locked.
		planLockData map[string][]string
	}

	type testcase struct {
		given tables.Operation
		then  expectation
	}

	// get run and its run_mountpoints pair.
	//
	// Args
	//
	// - context.Context, conn
	//
	// - []string runIds to be restrected to. if empty, all runs will be retuned.
	getRunEntity := func(ctx context.Context, conn kpool.Queryer) ([]runComponent, error) {
		runs, err := scanner.New[tables.Run]().QueryAll(
			ctx, conn, `select * from "run"`,
		)
		if err != nil {
			return nil, err
		}
		runIds := utils.Map(runs, func(r tables.Run) string { return r.RunId })

		_workers, err := scanner.New[tuple.Pair[string, string]]().QueryAll(
			ctx, conn,
			`select "run_id" as "first", "name" as "second" from "worker" where "run_id" = any($1)`,
			runIds,
		)
		if err != nil {
			return nil, err
		}
		// runId -> worker name
		workres := tuple.ToMap(_workers)

		assigns, err := scanner.New[tables.Assign]().QueryAll(
			ctx, conn,
			`select * from "assign" where "run_id" = any($1)`,
			runIds,
		)
		if err != nil {
			return nil, err
		}
		data, err := scanner.New[tables.Data]().QueryAll(
			ctx, conn,
			`select * from "data" where "run_id" = any($1)`,
			runIds,
		)
		if err != nil {
			return nil, err
		}

		type Tagging struct {
			KnitId string
			Key    string
			Value  string
		}
		_tags, err := scanner.New[Tagging]().QueryAll(
			ctx, conn,
			`
			with
			"t1" as (
				select "knit_id", "tag_id" as "id" from "tag_data" where "knit_id" = any($1)
			),
			"t2" as (
				select "knit_id", "value", "key_id" as "id" from "tag"
				inner join "t1" using("id")
			)
			select "knit_id", "value", "key" from "tag_key"
			inner join "t2" using("id")
			`,
			utils.Map(data, func(d tables.Data) string { return d.KnitId }),
		)
		if err != nil {
			return nil, err
		}
		tags := utils.ToMultiMap(_tags, func(tag Tagging) (string, kdb.Tag) {
			return tag.KnitId, kdb.Tag{Key: tag.Key, Value: tag.Value}
		})

		var actualRuns []runComponent
		{
			_assigns := utils.ToMultiMap(assigns, func(a tables.Assign) (string, tables.Assign) {
				return a.RunId, a
			})
			_data := utils.ToMultiMap(data, func(d tables.Data) (string, tuple.Pair[tables.Data, []kdb.Tag]) {
				return d.RunId, tuple.PairOf(d, tags[d.KnitId])
			})

			_actualRuns := map[string]runComponent{}
			for _, ar := range runs {
				_actualRuns[ar.RunId] = runComponent{
					body:   ar,
					worker: workres[ar.RunId],
					assign: _assigns[ar.RunId],
					data: utils.ToMap(
						_data[ar.RunId],
						func(d tuple.Pair[tables.Data, []kdb.Tag]) int { return d.First.OutputId },
					),
				}
			}
			actualRuns = utils.ValuesOf(_actualRuns)
		}
		return actualRuns, nil
	}

	volumeRefPrefix := "test-pvc-"
	workerNamePrefix := "test-worker-"

	for name, testcase := range map[string]testcase{
		"No nominations are given, no projections are performed (even if nominable)": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: th.Padding36("plan:1/uploaded"), Hash: "#plan:1", Active: true},
					{PlanId: th.Padding36("plan:2/preprocessing"), Hash: "#plan:2", Active: true},
				},
				PlanPseudo: []tables.PlanPseudo{
					{PlanId: th.Padding36("plan:1/uploaded"), Name: "knit#uploaded"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: th.Padding36("plan:2/preprocessing"), Image: "repo.invalid/preprocessing", Version: "v1.0"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/raw-data", InputId: 2100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/configs", InputId: 2200}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "configs"}},
					},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{PlanId: th.Padding36("plan:1/uploaded"), Path: "/out", OutputId: 1010}:      {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/out", OutputId: 2010}: {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/logs", OutputId: 2001}: {
						IsLog: true,
					},
				},
				Steps: []tables.Step{
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-1/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-1/run:1-1/uploaded//out"),
								VolumeRef: "vol#data:1-1",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-1/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
							},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-2/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:13.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-2/run:1-2/uploaded//out"),
								VolumeRef: "vol#data:1-2",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-2/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:13.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "configs"}},
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					// no nominations
				},
			},
			then: expectation{
				planLockData: map[string][]string{},
				newRuns:      []runComponent{}, // empty
			},
		},
		"Not enough data are given, no projections are performed": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: th.Padding36("plan:1/uploaded"), Hash: "#plan:1", Active: true},
					{PlanId: th.Padding36("plan:2/preprocessing"), Hash: "#plan:2", Active: true},
				},
				PlanPseudo: []tables.PlanPseudo{
					{PlanId: th.Padding36("plan:1/uploaded"), Name: "knit#uploaded"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: th.Padding36("plan:2/preprocessing"), Image: "repo.invalid/preprocessing", Version: "v1.0"},
				},
				Inputs: map[tables.Input]tables.InputAttr{

					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/raw-data", InputId: 2100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/configs", InputId: 2200}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "configs"}},
					},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{PlanId: th.Padding36("plan:1/uploaded"), Path: "/out", OutputId: 1010}:      {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/out", OutputId: 2010}: {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/logs", OutputId: 2001}: {
						IsLog: true,
					},
				},
				Steps: []tables.Step{
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-1/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-1/run:1-1/uploaded//out"),
								VolumeRef: "vol#data:1-1",
								RunId:     th.Padding36("run:1-1/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
								OutputId:  1010,
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 2100, Updated: true},
				},
			},
			then: expectation{
				planLockData: map[string][]string{
					th.Padding36("plan:2/preprocessing"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
					},
				},
				newRuns: []runComponent{}, // empty
			},
		},
		"Enough data and nominations are given, projection should be performed": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: th.Padding36("plan:1/uploaded"), Hash: "#plan:1", Active: true},
					{PlanId: th.Padding36("plan:2/preprocessing"), Hash: "#plan:2", Active: true},
				},
				PlanPseudo: []tables.PlanPseudo{
					{PlanId: th.Padding36("plan:1/uploaded"), Name: "knit#uploaded"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: th.Padding36("plan:2/preprocessing"), Image: "repo.invalid/preprocessing", Version: "v1.0"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/raw-data", InputId: 2100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/configs", InputId: 2200}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "configs"}},
					},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{PlanId: th.Padding36("plan:1/uploaded"), Path: "/out", OutputId: 1010}: {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/out", OutputId: 2010}: {
						UserTag: []kdb.Tag{
							{Key: "tag-key-1", Value: "tag-value-1"},
							{Key: "tag-key-2", Value: "tag-value-2"},
						},
					},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/logs", OutputId: 2001}: {
						IsLog: true,
						UserTag: []kdb.Tag{
							{Key: "tag-key-1", Value: "tag-value-1"},
							{Key: "tag-key-3", Value: "tag-value-3"},
						},
					},
				},
				Steps: []tables.Step{
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-1/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-1/run:1-1/uploaded//out"),
								VolumeRef: "vol#data:1-1",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-1/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
							},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-2/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:13.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-2/run:1-2/uploaded//out"),
								VolumeRef: "vol#data:1-2",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-2/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:13.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "configs"}},
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 2100, Updated: true},
					{KnitId: th.Padding36("data:1-2/run:1-2/uploaded//out"), InputId: 2200, Updated: true},
				},
			},
			then: expectation{
				planLockData: map[string][]string{
					th.Padding36("plan:2/preprocessing"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
						th.Padding36("data:1-2/run:1-2/uploaded//out"),
					},
				},
				newRuns: []runComponent{
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:2/preprocessing"),
						},
						assign: []tables.Assign{
							{
								InputId: 2100, KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"),
								PlanId: th.Padding36("plan:2/preprocessing"),
							},
							{
								InputId: 2200, KnitId: th.Padding36("data:1-2/run:1-2/uploaded//out"),
								PlanId: th.Padding36("plan:2/preprocessing"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							2010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2010,
								},
								[]kdb.Tag{
									{Key: "tag-key-1", Value: "tag-value-1"},
									{Key: "tag-key-2", Value: "tag-value-2"},
								},
							),
							2001: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2001,
								},
								[]kdb.Tag{
									{Key: "tag-key-1", Value: "tag-value-1"},
									{Key: "tag-key-3", Value: "tag-value-3"},
								},
							),
						},
					},
				},
			},
		},
		"When triggered plan is deactivated, it should generates run as deactivated": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: th.Padding36("plan:1/uploaded"), Hash: "#plan:1", Active: true},
					{PlanId: th.Padding36("plan:2/preprocessing"), Hash: "#plan:2", Active: false},
				},
				PlanPseudo: []tables.PlanPseudo{
					{PlanId: th.Padding36("plan:1/uploaded"), Name: "knit#uploaded"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: th.Padding36("plan:2/preprocessing"), Image: "repo.invalid/preprocessing", Version: "v1.0"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/raw-data", InputId: 2100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/configs", InputId: 2200}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "configs"}},
					},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{PlanId: th.Padding36("plan:1/uploaded"), Path: "/out", OutputId: 1010}: {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/out", OutputId: 2010}: {
						UserTag: []kdb.Tag{
							{Key: "tag-key-1", Value: "tag-value-1"},
							{Key: "tag-key-2", Value: "tag-value-2"},
						},
					},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/logs", OutputId: 2001}: {
						UserTag: []kdb.Tag{
							{Key: "tag-key-1", Value: "tag-value-1"},
							{Key: "tag-key-3", Value: "tag-value-3"},
						},
						IsLog: true,
					},
				},
				Steps: []tables.Step{
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-1/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-1/run:1-1/uploaded//out"),
								VolumeRef: "vol#data:1-1",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-1/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
							},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-2/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:13.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-2/run:1-2/uploaded//out"),
								VolumeRef: "vol#data:1-2",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-2/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:13.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "configs"}},
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 2100, Updated: true},
					{KnitId: th.Padding36("data:1-2/run:1-2/uploaded//out"), InputId: 2200, Updated: true},
				},
			},
			then: expectation{
				planLockData: map[string][]string{
					th.Padding36("plan:2/preprocessing"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
						th.Padding36("data:1-2/run:1-2/uploaded//out"),
					},
				},
				newRuns: []runComponent{
					{
						body: tables.Run{
							Status: kdb.Deactivated,
							PlanId: th.Padding36("plan:2/preprocessing"),
						},
						assign: []tables.Assign{
							{
								InputId: 2100, KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"),
								PlanId: th.Padding36("plan:2/preprocessing"),
							},
							{
								InputId: 2200, KnitId: th.Padding36("data:1-2/run:1-2/uploaded//out"),
								PlanId: th.Padding36("plan:2/preprocessing"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							2010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2010,
								},
								[]kdb.Tag{
									{Key: "tag-key-1", Value: "tag-value-1"},
									{Key: "tag-key-2", Value: "tag-value-2"},
								},
							),
							2001: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2001,
								},
								[]kdb.Tag{
									{Key: "tag-key-1", Value: "tag-value-1"},
									{Key: "tag-key-3", Value: "tag-value-3"},
								},
							),
						},
					},
				},
			},
		},
		"When single nomination triggers many runs, it should generates them all (single input)": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: th.Padding36("plan:1/uploaded"), Hash: "#plan:1", Active: true},
					{PlanId: th.Padding36("plan:2/preprocessing"), Hash: "#plan:2", Active: true},
					{PlanId: th.Padding36("plan:3/split"), Hash: "#plan:3", Active: true},
				},
				PlanPseudo: []tables.PlanPseudo{
					{PlanId: th.Padding36("plan:1/uploaded"), Name: "knit#uploaded"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: th.Padding36("plan:2/preprocessing"), Image: "repo.invalid/preprocessing", Version: "v1.0"},
					{PlanId: th.Padding36("plan:3/split"), Image: "repo.invalid/split", Version: "v1.0"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/raw-data", InputId: 2100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
					{PlanId: th.Padding36("plan:3/split"), Path: "/in/configs", InputId: 3100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{PlanId: th.Padding36("plan:1/uploaded"), Path: "/out", OutputId: 1010}:      {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/out", OutputId: 2010}: {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/logs", OutputId: 2001}: {
						UserTag: []kdb.Tag{
							{Key: "key-a", Value: "tag-a"},
						},
						IsLog: true,
					},
					{PlanId: th.Padding36("plan:3/split"), Path: "/out/1", OutputId: 3010}: {},
					{PlanId: th.Padding36("plan:3/split"), Path: "/out/2", OutputId: 3020}: {
						UserTag: []kdb.Tag{
							{Key: "key-b", Value: "tag-b"},
						},
					},
				},
				Steps: []tables.Step{
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-1/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-1/run:1-1/uploaded//out"),
								VolumeRef: "vol#data:1-1",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-1/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 2100, Updated: true},
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 3100, Updated: true},
				},
			},
			then: expectation{
				planLockData: map[string][]string{
					th.Padding36("plan:2/preprocessing"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
					},
					th.Padding36("plan:3/split"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
					},
				},
				newRuns: []runComponent{
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:2/preprocessing"),
						},
						assign: []tables.Assign{
							{
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								InputId: 2100,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							2010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2010,
								},
								[]kdb.Tag{}, // no tags.
							),
							2001: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2001,
								},
								[]kdb.Tag{
									{Key: "key-a", Value: "tag-a"},
								},
							),
						},
					},
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:3/split"),
						},
						assign: []tables.Assign{
							{
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								InputId: 3100,
								PlanId:  th.Padding36("plan:3/split"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							3010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:3/split"),
									OutputId: 3010,
								},
								[]kdb.Tag{},
							),
							3020: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:3/split"),
									OutputId: 3020,
								},
								[]kdb.Tag{
									{Key: "key-b", Value: "tag-b"},
								},
							),
						},
					},
				},
			},
		},
		"When single nomination triggers many runs, it should generates them all (multiple input)": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: th.Padding36("plan:1/uploaded"), Hash: "#plan:1", Active: true},
					{PlanId: th.Padding36("plan:2/preprocessing"), Hash: "#plan:2", Active: true},
					{PlanId: th.Padding36("plan:3/split"), Hash: "#plan:3", Active: true},
				},
				PlanPseudo: []tables.PlanPseudo{
					{PlanId: th.Padding36("plan:1/uploaded"), Name: "knit#uploaded"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: th.Padding36("plan:2/preprocessing"), Image: "repo.invalid/preprocessing", Version: "v1.0"},
					{PlanId: th.Padding36("plan:3/split"), Image: "repo.invalid/split", Version: "v1.0"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/raw-data-1", InputId: 2100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/raw-data-2", InputId: 2200}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/params", InputId: 2300}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "params"}},
					},
					{PlanId: th.Padding36("plan:3/split"), Path: "/in/configs", InputId: 3100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{PlanId: th.Padding36("plan:1/uploaded"), Path: "/out", OutputId: 1010}:      {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/out", OutputId: 2010}: {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/logs", OutputId: 2001}: {
						UserTag: []kdb.Tag{
							{Key: "key-a", Value: "tag-a"},
						},
						IsLog: true,
					},
					{PlanId: th.Padding36("plan:3/split"), Path: "/out/1", OutputId: 3010}: {},
					{PlanId: th.Padding36("plan:3/split"), Path: "/out/2", OutputId: 3020}: {
						UserTag: []kdb.Tag{
							{Key: "key-b", Value: "tag-b"},
						},
					},
				},
				Steps: []tables.Step{
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-1/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-1/run:1-1/uploaded//out"),
								VolumeRef: "vol#data:1-1",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-1/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
							},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-2/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-2/run:1-2/uploaded//out"),
								VolumeRef: "vol#data:1-2",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-2/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
							},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-3/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-3/run:1-3/uploaded//out"),
								VolumeRef: "vol#data:1-3",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-3/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "params"}},
							},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-4/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-4/run:1-4/uploaded//out"),
								VolumeRef: "vol#data:1-4",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-4/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "params"}},
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 2100, Updated: true},
					{KnitId: th.Padding36("data:1-2/run:1-2/uploaded//out"), InputId: 2100, Updated: false},
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 2200, Updated: true},
					{KnitId: th.Padding36("data:1-2/run:1-2/uploaded//out"), InputId: 2200, Updated: false},
					{KnitId: th.Padding36("data:1-3/run:1-3/uploaded//out"), InputId: 2300, Updated: false},
					{KnitId: th.Padding36("data:1-4/run:1-4/uploaded//out"), InputId: 2300, Updated: false},

					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 3100, Updated: true},
					{KnitId: th.Padding36("data:1-2/run:1-2/uploaded//out"), InputId: 3100, Updated: false},
				},
			},
			then: expectation{
				planLockData: map[string][]string{
					th.Padding36("plan:2/preprocessing"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
						th.Padding36("data:1-2/run:1-2/uploaded//out"),
						th.Padding36("data:1-3/run:1-3/uploaded//out"),
					},
					th.Padding36("plan:3/split"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
					},
				},
				newRuns: []runComponent{
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:2/preprocessing"),
						},
						assign: []tables.Assign{
							{
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								InputId: 2100,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								InputId: 2200,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-3/run:1-3/uploaded//out"),
								InputId: 2300,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							2010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2010,
								},
								[]kdb.Tag{}, // no tags.
							),
							2001: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2001,
								},
								[]kdb.Tag{
									{Key: "key-a", Value: "tag-a"},
								},
							),
						},
					},
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:2/preprocessing"),
						},
						assign: []tables.Assign{
							{
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								InputId: 2100,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-2/run:1-2/uploaded//out"),
								InputId: 2200,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-3/run:1-3/uploaded//out"),
								InputId: 2300,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							2010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2010,
								},
								[]kdb.Tag{}, // no tags.
							),
							2001: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2001,
								},
								[]kdb.Tag{
									{Key: "key-a", Value: "tag-a"},
								},
							),
						},
					},
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:2/preprocessing"),
						},
						assign: []tables.Assign{
							{
								KnitId:  th.Padding36("data:1-2/run:1-2/uploaded//out"),
								InputId: 2100,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								InputId: 2200,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-3/run:1-3/uploaded//out"),
								InputId: 2300,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							2010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2010,
								},
								[]kdb.Tag{}, // no tags.
							),
							2001: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2001,
								},
								[]kdb.Tag{
									{Key: "key-a", Value: "tag-a"},
								},
							),
						},
					},
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:2/preprocessing"),
						},
						assign: []tables.Assign{
							{
								KnitId:  th.Padding36("data:1-2/run:1-2/uploaded//out"),
								InputId: 2100,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-2/run:1-2/uploaded//out"),
								InputId: 2200,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-3/run:1-3/uploaded//out"),
								InputId: 2300,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							2010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2010,
								},
								[]kdb.Tag{}, // no tags.
							),
							2001: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2001,
								},
								[]kdb.Tag{
									{Key: "key-a", Value: "tag-a"},
								},
							),
						},
					},
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:2/preprocessing"),
						},
						assign: []tables.Assign{
							{
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								InputId: 2100,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								InputId: 2200,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-4/run:1-4/uploaded//out"),
								InputId: 2300,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							2010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2010,
								},
								[]kdb.Tag{}, // no tags.
							),
							2001: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2001,
								},
								[]kdb.Tag{
									{Key: "key-a", Value: "tag-a"},
								},
							),
						},
					},
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:2/preprocessing"),
						},
						assign: []tables.Assign{
							{
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								InputId: 2100,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-2/run:1-2/uploaded//out"),
								InputId: 2200,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-4/run:1-4/uploaded//out"),
								InputId: 2300,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							2010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2010,
								},
								[]kdb.Tag{}, // no tags.
							),
							2001: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2001,
								},
								[]kdb.Tag{
									{Key: "key-a", Value: "tag-a"},
								},
							),
						},
					},
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:2/preprocessing"),
						},
						assign: []tables.Assign{
							{
								KnitId:  th.Padding36("data:1-2/run:1-2/uploaded//out"),
								InputId: 2100,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								InputId: 2200,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-4/run:1-4/uploaded//out"),
								InputId: 2300,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							2010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2010,
								},
								[]kdb.Tag{}, // no tags.
							),
							2001: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2001,
								},
								[]kdb.Tag{
									{Key: "key-a", Value: "tag-a"},
								},
							),
						},
					},
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:2/preprocessing"),
						},
						assign: []tables.Assign{
							{
								KnitId:  th.Padding36("data:1-2/run:1-2/uploaded//out"),
								InputId: 2100,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-2/run:1-2/uploaded//out"),
								InputId: 2200,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								KnitId:  th.Padding36("data:1-4/run:1-4/uploaded//out"),
								InputId: 2300,
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							2010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2010,
								},
								[]kdb.Tag{}, // no tags.
							),
							2001: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:2/preprocessing"),
									OutputId: 2001,
								},
								[]kdb.Tag{
									{Key: "key-a", Value: "tag-a"},
								},
							),
						},
					},
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:3/split"),
						},
						assign: []tables.Assign{
							{
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								InputId: 3100,
								PlanId:  th.Padding36("plan:3/split"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							3010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:3/split"),
									OutputId: 3010,
								},
								[]kdb.Tag{},
							),
							3020: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:3/split"),
									OutputId: 3020,
								},
								[]kdb.Tag{
									{Key: "key-b", Value: "tag-b"},
								},
							),
						},
					},
				},
			},
		},
		"When nomination triggers runs but some of them are known, it generates runs except already known (single input)": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: th.Padding36("plan:1/uploaded"), Hash: "#plan:1", Active: true},
					{PlanId: th.Padding36("plan:2/preprocessing"), Hash: "#plan:2", Active: true},
					{PlanId: th.Padding36("plan:3/split"), Hash: "#plan:3", Active: true},
				},
				PlanPseudo: []tables.PlanPseudo{
					{PlanId: th.Padding36("plan:1/uploaded"), Name: "knit#uploaded"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: th.Padding36("plan:2/preprocessing"), Image: "repo.invalid/preprocessing", Version: "v1.0"},
					{PlanId: th.Padding36("plan:3/split"), Image: "repo.invalid/split", Version: "v1.0"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/raw-data", InputId: 2100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
					{PlanId: th.Padding36("plan:3/split"), Path: "/in/configs", InputId: 3100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{PlanId: th.Padding36("plan:1/uploaded"), Path: "/out", OutputId: 1010}:      {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/out", OutputId: 2010}: {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/logs", OutputId: 2001}: {
						IsLog: true,
					},
					{PlanId: th.Padding36("plan:3/split"), Path: "/out/1", OutputId: 3010}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
				},
				Steps: []tables.Step{
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-1/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-1/run:1-1/uploaded//out"),
								VolumeRef: "vol#data:1-1",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-1/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
							},
						},
					},
					{
						Run: tables.Run{
							RunId:  th.Padding36("run:2-1/preprocessing"),
							Status: kdb.Ready,
							PlanId: th.Padding36("plan:2/preprocessing"),
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-06T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								RunId:   th.Padding36("run:2-1/preprocessing"),
								InputId: 2100,
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:2-1/run:2-1/preprocessing//out"),
								VolumeRef: "vol#data:2-1",
								OutputId:  2010,
								RunId:     th.Padding36("run:2-1/preprocessing"),
								PlanId:    th.Padding36("plan:2/preprocessing"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-06T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 2100, Updated: true},
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 3100, Updated: true},
				},
			},
			then: expectation{
				planLockData: map[string][]string{
					th.Padding36("plan:2/preprocessing"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
					},
					th.Padding36("plan:3/split"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
					},
				},
				newRuns: []runComponent{
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:3/split"),
						},
						assign: []tables.Assign{
							{
								InputId: 3100,
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								PlanId:  th.Padding36("plan:3/split"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							3010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:3/split"),
									OutputId: 3010,
								},
								[]kdb.Tag{{Key: "type", Value: "raw-data"}},
							),
						},
					},
				},
			},
		},
		"When nomination triggers runs but some of them are known, it generates runs except already known (multiple input)": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: th.Padding36("plan:1/uploaded"), Hash: "#plan:1", Active: true},
					{PlanId: th.Padding36("plan:2/preprocessing"), Hash: "#plan:2", Active: true},
					{PlanId: th.Padding36("plan:3/split"), Hash: "#plan:3", Active: true},
				},
				PlanPseudo: []tables.PlanPseudo{
					{PlanId: th.Padding36("plan:1/uploaded"), Name: "knit#uploaded"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: th.Padding36("plan:2/preprocessing"), Image: "repo.invalid/preprocessing", Version: "v1.0"},
					{PlanId: th.Padding36("plan:3/split"), Image: "repo.invalid/split", Version: "v1.0"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/raw-data", InputId: 2100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/parameter", InputId: 2200}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "params"}},
					},
					{PlanId: th.Padding36("plan:3/split"), Path: "/in/configs", InputId: 3100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{PlanId: th.Padding36("plan:1/uploaded"), Path: "/out", OutputId: 1010}:      {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/out", OutputId: 2010}: {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/logs", OutputId: 2001}: {
						IsLog: true,
					},
					{PlanId: th.Padding36("plan:3/split"), Path: "/out/1", OutputId: 3010}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
				},
				Steps: []tables.Step{
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-1/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-1/run:1-1/uploaded//out"),
								VolumeRef: "vol#data:1-1",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-1/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
							},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-2/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-2/run:1-2/uploaded//out"),
								VolumeRef: "vol#data:1-2",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-2/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "params"}},
							},
						},
					},
					{
						Run: tables.Run{
							RunId:  th.Padding36("run:2-1/preprocessing"),
							Status: kdb.Ready,
							PlanId: th.Padding36("plan:2/preprocessing"),
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-06T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								RunId:   th.Padding36("run:2-1/preprocessing"),
								InputId: 2100,
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
							{
								RunId:   th.Padding36("run:2-1/preprocessing"),
								InputId: 2200,
								KnitId:  th.Padding36("data:1-2/run:1-2/uploaded//out"),
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:2-1/run:2-1/preprocessing//out"),
								VolumeRef: "vol#data:2-1",
								OutputId:  2010,
								RunId:     th.Padding36("run:2-1/preprocessing"),
								PlanId:    th.Padding36("plan:2/preprocessing"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-06T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 2100, Updated: true},
					{KnitId: th.Padding36("data:1-2/run:1-2/uploaded//out"), InputId: 2200, Updated: false},
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 3100, Updated: true},
				},
			},
			then: expectation{
				planLockData: map[string][]string{
					th.Padding36("plan:2/preprocessing"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
						th.Padding36("data:1-2/run:1-2/uploaded//out"),
					},
					th.Padding36("plan:3/split"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
					},
				},
				newRuns: []runComponent{
					{
						body: tables.Run{
							Status: kdb.Waiting,
							PlanId: th.Padding36("plan:3/split"),
						},
						assign: []tables.Assign{
							{
								InputId: 3100,
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								PlanId:  th.Padding36("plan:3/split"),
							},
						},
						data: map[int]tuple.Pair[tables.Data, []kdb.Tag]{
							3010: tuple.PairOf(
								tables.Data{
									PlanId:   th.Padding36("plan:3/split"),
									OutputId: 3010,
								},
								[]kdb.Tag{{Key: "type", Value: "raw-data"}},
							),
						},
					},
				},
			},
		},
		"When nomination triggers runs but all of them are known, it generates no runs": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: th.Padding36("plan:1/uploaded"), Hash: "#plan:1", Active: true},
					{PlanId: th.Padding36("plan:2/preprocessing"), Hash: "#plan:2", Active: true},
					{PlanId: th.Padding36("plan:3/split"), Hash: "#plan:3", Active: true},
				},
				PlanPseudo: []tables.PlanPseudo{
					{PlanId: th.Padding36("plan:1/uploaded"), Name: "knit#uploaded"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: th.Padding36("plan:2/preprocessing"), Image: "repo.invalid/preprocessing", Version: "v1.0"},
					{PlanId: th.Padding36("plan:3/split"), Image: "repo.invalid/split", Version: "v1.0"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/raw-data", InputId: 2100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
					{PlanId: th.Padding36("plan:3/split"), Path: "/in/configs", InputId: 3100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{PlanId: th.Padding36("plan:1/uploaded"), Path: "/out", OutputId: 1010}:      {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/out", OutputId: 2010}: {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/logs", OutputId: 2001}: {
						IsLog: true,
					},
					{PlanId: th.Padding36("plan:3/split"), Path: "/out/1", OutputId: 3010}: {},
					{PlanId: th.Padding36("plan:3/split"), Path: "/out/2", OutputId: 3020}: {},
				},
				Steps: []tables.Step{
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-1/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-1/run:1-1/uploaded//out"),
								VolumeRef: "vol#data:1-1",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-1/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
							},
						},
					},
					{
						Run: tables.Run{
							RunId:  th.Padding36("run:2-1/preprocessing"),
							Status: kdb.Ready,
							PlanId: th.Padding36("plan:2/preprocessing"),
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-06T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								InputId: 2100,
								RunId:   th.Padding36("run:2-1/preprocessing"),
								PlanId:  th.Padding36("plan:2/preprocessing"),
							},
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:2-1/run:2-1/preprocessing//out"),
								VolumeRef: "vol#data:2-1",
								OutputId:  2010,
								RunId:     th.Padding36("run:2-1/preprocessing"),
								PlanId:    th.Padding36("plan:2/preprocessing"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-06T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
							},
						},
					},
					{
						Run: tables.Run{
							RunId:  th.Padding36("run:3-1/split"),
							Status: kdb.Ready,
							PlanId: th.Padding36("plan:3/split"),
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-06T10:11:15.345+00:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								RunId:   th.Padding36("run:3-1/split"),
								InputId: 3100,
								KnitId:  th.Padding36("data:1-1/run:1-1/uploaded//out"),
								PlanId:  th.Padding36("plan:3/split"),
							},
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:3-1/run:3-1/split//out/1"),
								VolumeRef: "vol#data:3-1",
								OutputId:  3010,
								RunId:     th.Padding36("run:3-1/split"),
								PlanId:    th.Padding36("plan:3/split"),
							}: {},
							{
								KnitId:    th.Padding36("data:3-2/run:3-1/split//out/2"),
								VolumeRef: "vol#data:3-2",
								OutputId:  3020,
								RunId:     th.Padding36("run:3-1/split"),
								PlanId:    th.Padding36("plan:3/split"),
							}: {},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 2100, Updated: true},
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 3100, Updated: true},
				},
			},
			then: expectation{
				planLockData: map[string][]string{
					th.Padding36("plan:2/preprocessing"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
					},
					th.Padding36("plan:3/split"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
					},
				},
				newRuns: []runComponent{}, // empty
			},
		},
		"When no nominations are updated, it does nothing": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: th.Padding36("plan:1/uploaded"), Hash: "#plan:1", Active: true},
					{PlanId: th.Padding36("plan:2/preprocessing"), Hash: "#plan:2", Active: true},
				},
				PlanPseudo: []tables.PlanPseudo{
					{PlanId: th.Padding36("plan:1/uploaded"), Name: "knit#uploaded"},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: th.Padding36("plan:2/preprocessing"), Image: "repo.invalid/preprocessing", Version: "v1.0"},
				},
				Inputs: map[tables.Input]tables.InputAttr{

					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/raw-data", InputId: 2100}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
					},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/in/configs", InputId: 2200}: {
						UserTag: []kdb.Tag{{Key: "type", Value: "configs"}},
					},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{PlanId: th.Padding36("plan:1/uploaded"), Path: "/out", OutputId: 1010}:      {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/out", OutputId: 2010}: {},
					{PlanId: th.Padding36("plan:2/preprocessing"), Path: "/logs", OutputId: 2001}: {
						IsLog: true,
					},
				},
				Steps: []tables.Step{
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-1/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:12.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-1/run:1-1/uploaded//out"),
								VolumeRef: "vol#data:1-1",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-1/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:12.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "raw-data"}},
							},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run:1-2/uploaded"), Status: kdb.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2021-04-05T10:11:13.345+00:00",
							)).OrFatal(t).Time(),
							PlanId: th.Padding36("plan:1/uploaded"),
						},
						Assign: []tables.Assign{},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:    th.Padding36("data:1-2/run:1-2/uploaded//out"),
								VolumeRef: "vol#data:1-2",
								OutputId:  1010,
								RunId:     th.Padding36("run:1-2/uploaded"),
								PlanId:    th.Padding36("plan:1/uploaded"),
							}: {
								Timestamp: ptr.Ref(try.To(rfctime.ParseRFC3339DateTime(
									"2021-04-05T10:11:13.345+00:00",
								)).OrFatal(t).Time()),
								UserTag: []kdb.Tag{{Key: "type", Value: "configs"}},
							},
						},
					},
				},
				Nomination: []tables.Nomination{
					{KnitId: th.Padding36("data:1-1/run:1-1/uploaded//out"), InputId: 2100, Updated: false},
					{KnitId: th.Padding36("data:1-2/run:1-2/uploaded//out"), InputId: 2200, Updated: false},
				},
			},
			then: expectation{
				planLockData: map[string][]string{
					th.Padding36("plan:2/preprocessing"): {
						th.Padding36("data:1-1/run:1-1/uploaded//out"),
						th.Padding36("data:1-2/run:1-2/uploaded//out"),
					},
				},
				newRuns: []runComponent{}, // empty
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pgpool := poolBroaker.GetPool(ctx, t)

			if err := testcase.given.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			wpool := proxy.Wrap(pgpool)
			wpool.Events().Query.After(func() {
				conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()

				// interest #1 : which plans are locked?

				lockedPlanId := try.To(scanner.New[string]().QueryAll(
					ctx, conn,
					`
					with "unlocked" as (select "plan_id" from "plan" for update skip locked)
					select "plan_id" from "plan" except select "plan_id" from "unlocked"
					order by "plan_id"
					`, // all - unlocked = locked
				)).OrFatal(t)

				plansWithUpdatedNomination := try.To(scanner.New[string]().QueryAll(
					ctx, conn,
					`
					with "t" as (
						select distinct "input_id" from "nomination" where "updated"
					)
					select distinct "plan_id" from "input" inner join "t" using("input_id")
					`,
				)).OrFatal(t)

				if 0 < len(plansWithUpdatedNomination) {
					if len(lockedPlanId) <= 0 {
						t.Error("no plans locked, but there are updated nomination")
					}
				} else {
					if 0 < len(lockedPlanId) {
						t.Error("no nominations are updated, but there are locked plan")
					}
				}

				// interest #2 : which data are locked?

				lockedKnitId := try.To(scanner.New[string]().QueryAll(
					ctx, conn,
					`
					with "unlocked" as (select "knit_id" from "data" for update skip locked)
					select "knit_id" from "data" except select "knit_id" from "unlocked"
					order by "knit_id"
					`,
				)).OrFatal(t)

				if !cmp.SliceSubsetWith(plansWithUpdatedNomination, lockedPlanId, cmp.EqEq[string]) {
					t.Errorf(
						"unexpected plan is locked:\n- locked: %v\n- plan have updated input: %v",
						lockedPlanId, plansWithUpdatedNomination,
					)
				}

				knitIdNominatedForLockedPlan := try.To(scanner.New[string]().QueryAll(
					ctx, conn,
					`
					with "t" as (
						select "input_id" from "input" where "plan_id" = any($1)
					)
					select distinct "knit_id" from "nomination"
					inner join "t" using("input_id")
					`,
					lockedPlanId,
				)).OrFatal(t)

				if !cmp.SliceContentEq(knitIdNominatedForLockedPlan, lockedKnitId) {
					t.Errorf(
						"unexpected lock: data:\nactual   = %v\nexpected = %v",
						knitIdNominatedForLockedPlan, lockedKnitId,
					)
				}
			})

			nom := kpgnommock.New(t)
			nom.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
				// nothing to do.
				return nil
			}
			naming := newMockRunNamingConvention(t)
			naming.impl.VolumeRef = func(s string) (string, error) {
				return volumeRefPrefix + s, nil
			}
			naming.impl.Worker = func(s string) (string, error) {
				return workerNamePrefix + s, nil
			}
			testee := kpgrun.New(
				wpool,
				kpgrun.WithNominator(nom), kpgrun.WithNamingConvention(naming),
			)

			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			predefinedRunIds := map[string]struct{}{}
			for _, runId := range try.To(scanner.New[string]().QueryAll(
				ctx, conn, `select "run_id" from "run"`,
			)).OrFatal(t) {
				predefinedRunIds[runId] = struct{}{}
			}

			runIdsAlreadyExist := map[string]struct{}{}
			for _, s := range testcase.given.Steps {
				runIdsAlreadyExist[s.Run.RunId] = struct{}{}
			}

			for {
				runIds, trigger := try.To(tuple.PairAndError(
					testee.New(ctx),
				)).OrFatal(t).Decompose()

				if trigger == nil {
					// no more new runs can be created.
					if len(runIds) != 0 {
						t.Errorf("runs are created without trigger: %v", runIds)
					}
					break
				}

				actualRuns := try.To(getRunEntity(ctx, conn)).OrFatal(t)
				actualRunIds := map[string]struct{}{}
				for _, r := range actualRuns {
					runId := r.body.RunId
					actualRunIds[runId] = struct{}{}
					if _, ok := predefinedRunIds[runId]; ok {
						continue
					}
					if !consistentRunComponent(r) {
						t.Errorf("internal consistency is broken: %+v", r)
					}
					if expectedWorkerName, _ := naming.impl.Worker(r.body.RunId); r.worker != expectedWorkerName {
						t.Errorf(
							"unmatch worker name:\n- actual: %+v\n- expected: %+v",
							r.worker, expectedWorkerName,
						)
					}
					for _, d := range r.data {
						actualVolumeRef := d.First.VolumeRef
						if expectedVolumeRef, _ := naming.impl.VolumeRef(d.First.KnitId); actualVolumeRef != expectedVolumeRef {
							t.Errorf(
								"unmatch volume ref\n- actual: %+v\n- expected: %+v",
								actualVolumeRef, expectedVolumeRef,
							)
						}
					}
					if _, ok := utils.First(testcase.then.newRuns, func(x runComponent) bool {
						return equivRunComponent(x, r)
					}); !ok {
						t.Errorf("created run is not in expected runs %+v", r)
					}
				}
				for _, runId := range runIds {
					if _, ok := actualRunIds[runId]; !ok {
						t.Errorf("return value has an unknwon run_id: %s", runId)
					}
					if _, ok := runIdsAlreadyExist[runId]; ok {
						t.Errorf("return value has a run_id which has exist already, or non unique: %s", runId)
					}
					runIdsAlreadyExist[runId] = struct{}{}
				}

				if !cmp.SliceContentEq(utils.KeysOf(runIdsAlreadyExist), utils.KeysOf(actualRunIds)) {
					t.Errorf(
						"unmatch runId: found in db v.s. total of returned by testee\n found in db: %v\n total of returned by testee: %v",
						utils.KeysOf(actualRunIds), utils.KeysOf(runIdsAlreadyExist),
					)
				}
			}

			{
				actualRuns := try.To(getRunEntity(ctx, conn)).OrFatal(t)
				actualNewRuns, _ := utils.Group(actualRuns, func(ar runComponent) bool {
					for _, s := range testcase.given.Steps {
						if ar.body.RunId == s.Run.RunId {
							return false
						}
					}
					return true
				})
				if !cmp.SliceSubsetWith(testcase.then.newRuns, actualNewRuns, equivRunComponent) {
					t.Errorf(
						"unmatch: actual runs do not satisfy expected runs\n- actual: %+v\n- expected: %+v",
						actualNewRuns, testcase.then.newRuns,
					)
				}
			}

			{
				// mocks: whatever okay. they should not be called for any method
				testee := kpgrun.New(pgpool)

				// try one more New, test no more changes.
				runIds, trigger := try.To(tuple.PairAndError(
					testee.New(ctx),
				)).OrFatal(t).Decompose()
				if len(runIds) != 0 {
					t.Error("not converged; runIds are generated after once trigger get be nil")
				}
				if trigger != nil {
					t.Error("not converged; trigger is non-nil again after once trigger get be nil")
				}
			}
		})
	}
}
