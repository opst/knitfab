package plan_test

import (
	"context"
	"errors"
	"io"
	"log"
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
	kerr "github.com/opst/knitfab/pkg/domain/errors"
	marshal "github.com/opst/knitfab/pkg/domain/internal/db/postgres"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	th "github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	kpgnommock "github.com/opst/knitfab/pkg/domain/nomination/db/mock"
	kpgplan "github.com/opst/knitfab/pkg/domain/plan/db/postgres"
	"github.com/opst/knitfab/pkg/utils/cmp"
	fn "github.com/opst/knitfab/pkg/utils/function"
	"github.com/opst/knitfab/pkg/utils/logic"
	"github.com/opst/knitfab/pkg/utils/slices"
	"github.com/opst/knitfab/pkg/utils/try"
	"k8s.io/apimachinery/pkg/api/resource"
)

// query all plans in pdb.Plan shape.
//
// this function is wrote to be used in testing.
//
// on testing about new plans, we should check...
//   - properties each plans and mountpoints
//   - relations of tags and mountpoints
//
// but, ids of plan and mountpoint are not predictable since postgres generates them.
//
// we cannot know which tag is related to which mountpoint without mountpoint id,
// but plan has multiple mountpoints and id of them are auto-generated.
//
// to avoid this difficulty,
// - we should achieve a function, getAllPlans, which get all plans from postgres. (this implementation)
// - test it (`func TestGetAllPlans`)
// - test some methods creating Plan in more complex routine.
func allPlanIds(ctx context.Context, conn kpool.Conn) ([]string, error) {
	planIds, err := scanner.New[string]().QueryAll(ctx, conn, `select "plan_id" from "plan"`)
	if err != nil {
		return nil, err
	}

	return planIds, nil
}

func TestPlan_Register(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	type when struct {
		spec *domain.PlanSpec
	}

	// success case
	theoryOk := func(given tables.Operation, when when, then []*domain.Plan) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			pool := poolBroaker.GetPool(ctx, t)
			if err := given.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			wpool := proxy.Wrap(pool)
			wpool.Events().Query.After(func() {
				th.BeginFuncToRollback(ctx, pool, fn.Void[error](func(tx kpool.Tx) {
					if _, err := tx.Exec(ctx, `lock table "plan" in ROW EXCLUSIVE mode nowait;`); err == nil {
						t.Errorf("plan is not locked")
					} else if pgerr := new(pgconn.PgError); !errors.As(err, &pgerr) ||
						pgerr.Code != pgerrcode.LockNotAvailable {
						t.Errorf(
							"unexpected error: expected error code is %s, but %s",
							pgerrcode.LockNotAvailable, err,
						)
					}
				}))
			})

			nomi := kpgnommock.New(t)
			nomi.Impl.NominateMountpoints = func(ctx context.Context, conn kpool.Tx, mountpointIds []int) error {
				return nil
			}

			testee := kpgplan.New(wpool, kpgplan.WithNominator(nomi))

			conn := try.To(pool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			planIdsBeforeRegister := try.To(allPlanIds(ctx, conn)).OrFatal(t)
			actualPlanId := try.To(testee.Register(ctx, when.spec)).OrFatal(t)
			planIdsAfrerRegister := try.To(allPlanIds(ctx, conn)).OrFatal(t)
			expectedPlanIds := append([]string{actualPlanId}, planIdsBeforeRegister...)
			if !cmp.SliceContentEq(planIdsAfrerRegister, expectedPlanIds) {
				t.Errorf(
					"not match: planIds: (actual, expected) = (%v, %v)",
					planIdsAfrerRegister, expectedPlanIds,
				)
			}

			plans := try.To(
				// obtain new instance to bypass proxy.Pool
				//          vvvv
				kpgplan.New(pool, kpgplan.WithNominator(nomi)).
					Get(
						ctx, try.To(allPlanIds(ctx, conn)).OrFatal(t),
					),
			).OrFatal(t)

			registeredPlan, ok := plans[actualPlanId]

			if !ok {
				t.Fatalf("plan is registered, but missing: plan id = %s", actualPlanId)
			} else if !when.spec.EquivPlan(registeredPlan) {
				t.Errorf(
					"Registered Plan:\n===actual===\n%+v\n===its spec===\n%+v",
					registeredPlan, when.spec,
				)
			}

			{

				actual := slices.ValuesOf(plans)
				if !cmp.SliceContentEqWith(actual, then, (*domain.Plan).Equiv) {
					t.Errorf(
						"All Plans:\n===actual===\n%+v\n===expected===\n%+v",
						actual, then,
					)
				}

				if !cmp.SliceContentEqWith(actual, then, func(a, b *domain.Plan) bool {
					return cmp.MapEqWith(a.PlanBody.Resources, b.PlanBody.Resources, resource.Quantity.Equal)
				}) {
					t.Errorf(
						"Resources of Plans are not equal:\n===actual===\n%+v\n===expected===\n%+v",
						actual, then,
					)
				}

				if !cmp.SliceContentEqWith(actual, then, func(a, b *domain.Plan) bool {
					return cmp.SliceContentEq(a.Annotations, b.Annotations)
				}) {
					t.Errorf(
						"Annotations of Plans are not equal:\n===actual===\n%+v\n===expected===\n%+v",
						actual, then,
					)
				}

				if !cmp.SliceContentEqWith(actual, then, func(a, b *domain.Plan) bool {
					return a.ServiceAccount == b.ServiceAccount
				}) {
					t.Errorf(
						"ServiceAccount of Plans are not equal:\n===actual===\n%+v\n===expected===\n%+v",
						actual, then,
					)
				}
			}

			expectedNominatorCalls := [][]int{
				slices.Map(
					registeredPlan.Inputs,
					func(mp domain.MountPoint) int { return mp.Id },
				),
			}

			if !cmp.SliceEqWith(
				nomi.Calls.NominateMountpoints, expectedNominatorCalls, cmp.SliceContentEq[int],
			) {
				t.Errorf(
					"unmatch: nominator calls: (actual, expected) = (%v, %v)",
					nomi.Calls.NominateMountpoints, expectedNominatorCalls,
				)
			}
		}
	}

	t.Run("let no plans given, when add a new plan, it should register that", theoryOk(
		tables.Operation{},
		when{
			spec: domain.BypassValidation(
				th.Padding64("test-hash"), nil,
				domain.PlanParam{
					Image: "repo.invalid/test-image", Version: "v0.1", Active: true,
					Entrypoint: []string{"python", "main.py"},
					Args:       []string{"--input", "/in/1", "--output", "/out/1"},
					Inputs: []domain.MountPointParam{
						{
							Path: "/in/1",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "key1", Value: "value1"},
								{Key: "key1", Value: "value2"},
								{Key: "key2", Value: "value1"},
							}),
						},
						{
							Path: "/in/2",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "key1", Value: "value1"},
								{Key: "key1", Value: "value3"},
								{Key: "key2", Value: "value2"},
							}),
						},
					},
					Outputs: []domain.MountPointParam{
						{
							Path: "/out/1",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "key1", Value: "valueA"},
							}),
						},
					},
					Log: &domain.LogParam{
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "log", Value: "true"},
						}),
					},
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("1Gi"),
					},
					OnNode: []domain.OnNode{
						{Mode: domain.MustOnNode, Key: "accelarator", Value: "gpu"},
						{Mode: domain.MustOnNode, Key: "ram", Value: "large"},
						{Mode: domain.PreferOnNode, Key: "accelarator", Value: "tpu"},
						{Mode: domain.PreferOnNode, Key: "ram", Value: "xlarge"},
						{Mode: domain.MayOnNode, Key: "ram", Value: "x2large"},
					},
					ServiceAccount: "service-account",
					Annotations: []domain.Annotation{
						{Key: "anno1", Value: "val1"},
						{Key: "anno1", Value: "val1.2"},
						{Key: "anno2", Value: "val2"},
						{Key: "anno2", Value: "val2"}, // duplicate items shoud be ignored
					},
				},
			),
		},
		[]*domain.Plan{
			{
				PlanBody: domain.PlanBody{
					Active: true, Hash: th.Padding64("test-hash"),
					Image: &domain.ImageIdentifier{
						Image: "repo.invalid/test-image", Version: "v0.1",
					},
					Entrypoint: []string{"python", "main.py"},
					Args:       []string{"--input", "/in/1", "--output", "/out/1"},
					OnNode: []domain.OnNode{
						{Mode: domain.MustOnNode, Key: "accelarator", Value: "gpu"},
						{Mode: domain.MustOnNode, Key: "ram", Value: "large"},
						{Mode: domain.PreferOnNode, Key: "accelarator", Value: "tpu"},
						{Mode: domain.PreferOnNode, Key: "ram", Value: "xlarge"},
						{Mode: domain.MayOnNode, Key: "ram", Value: "x2large"},
					},
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("1Gi"),
					},
					ServiceAccount: "service-account",
					Annotations: []domain.Annotation{
						{Key: "anno1", Value: "val1"},
						{Key: "anno1", Value: "val1.2"},
						{Key: "anno2", Value: "val2"},
					},
				},
				Inputs: []domain.MountPoint{
					{
						Path: "/in/1",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "key1", Value: "value1"},
							{Key: "key1", Value: "value2"},
							{Key: "key2", Value: "value1"},
						}),
					},
					{
						Path: "/in/2",
						Tags: domain.NewTagSet([]domain.Tag{

							{Key: "key1", Value: "value1"},
							{Key: "key1", Value: "value3"},
							{Key: "key2", Value: "value2"},
						}),
					},
				},
				Outputs: []domain.MountPoint{
					{
						Path: "/out/1",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "key1", Value: "valueA"},
						}),
					},
				},
				Log: &domain.LogPoint{
					Tags: domain.NewTagSet([]domain.Tag{
						{Key: "log", Value: "true"},
					}),
				},
			},
		},
	))

	t.Run("let a plan given, when adding a new plan which has same hash as given but not equiverent, it should register a new plan", theoryOk(
		tables.Operation{
			Plan: []tables.Plan{
				{PlanId: th.Padding36("plan-1"), Active: true, Hash: th.Padding64("example-hash")},
			},
			PlanImage: []tables.PlanImage{
				{PlanId: th.Padding36("plan-1"), Image: "repo.invalid/test-image", Version: "v0.1"},
			},
			Inputs: map[tables.Input]tables.InputAttr{
				{InputId: 100, PlanId: th.Padding36("plan-1"), Path: "/in/1"}: {
					UserTag: []domain.Tag{{Key: "tag-1", Value: "value-1"}},
				},
			},
			Outputs: map[tables.Output]tables.OutputAttr{
				{OutputId: 200, PlanId: th.Padding36("plan-1"), Path: "/out/1"}: {
					UserTag: []domain.Tag{{Key: "tag-2", Value: "value-2"}},
				},
			},
		},
		when{
			spec: domain.BypassValidation(
				th.Padding64("example-hash"), nil,
				domain.PlanParam{
					Active: true,
					Image:  "repo.invalid/test-image-2", Version: "v0.2",
					Entrypoint: []string{"python", "main.py"},
					Args:       []string{"--input", "/in/1", "--output", "/out/1"},
					Inputs: []domain.MountPointParam{
						{
							Path: "/in/data",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "tag-1", Value: "value-1"}, // known tag
								{Key: "tag-1", Value: "value-2"}, // known key, new value
								{Key: "tag-x", Value: "value-3"}, // new tag
							}),
						},
						{
							Path: "/in/params",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "tag-x", Value: "value-3"}, // known tag
								{Key: domain.KeyKnitId, Value: th.Padding36("some-knit-id")},
								{Key: domain.KeyKnitTimestamp, Value: "2022-08-15T12:34:56+00:00"},
							}),
						},
					},
					Outputs: []domain.MountPointParam{
						{
							Path: "/out/model",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "tag-y", Value: "value-4"},
							}),
						},
					},
				},
			),
		},
		[]*domain.Plan{
			{
				PlanBody: domain.PlanBody{
					PlanId: th.Padding36("plan-1"), Active: true, Hash: th.Padding64("example-hash"),
					Image: &domain.ImageIdentifier{Image: "repo.invalid/test-image", Version: "v0.1"},
				},
				Inputs: []domain.MountPoint{
					{
						Id: 100, Path: "/in/1",
						Tags: domain.NewTagSet([]domain.Tag{{Key: "tag-1", Value: "value-1"}}),
					},
				},
				Outputs: []domain.MountPoint{
					{
						Id: 200, Path: "/out/1",
						Tags: domain.NewTagSet([]domain.Tag{{Key: "tag-2", Value: "value-2"}}),
					},
				},
			},
			{
				PlanBody: domain.PlanBody{
					PlanId: th.Padding36("UNKNOWN"), Active: true, Hash: th.Padding64("example-hash"),
					Image:      &domain.ImageIdentifier{Image: "repo.invalid/test-image-2", Version: "v0.2"},
					Entrypoint: []string{"python", "main.py"},
					Args:       []string{"--input", "/in/1", "--output", "/out/1"},
				},
				Inputs: []domain.MountPoint{
					{
						Path: "/in/data",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "tag-1", Value: "value-1"},
							{Key: "tag-1", Value: "value-2"},
							{Key: "tag-x", Value: "value-3"},
						}),
					},
					{
						Path: "/in/params",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "tag-x", Value: "value-3"},
							{Key: domain.KeyKnitId, Value: th.Padding36("some-knit-id")},
							{Key: domain.KeyKnitTimestamp, Value: "2022-08-15T12:34:56+00:00"},
						}),
					},
				},
				Outputs: []domain.MountPoint{
					{
						Path: "/out/model",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "tag-y", Value: "value-4"},
						}),
					},
				},
			},
		},
	))
	t.Run("let a plan given, when it is passed a new plan depending on the given one, it should register the new plan", theoryOk(
		tables.Operation{
			// data flow: plan-1>/log/2 --> /in/2>plan-2>/out/2 --> /in/3>plan-3
			//   (notation: [plan name]>[output path] --> [input path]>[plan name])
			Plan: []tables.Plan{
				{PlanId: th.Padding36("plan-3"), Active: true, Hash: th.Padding64("hash:plan-3")},
			},
			PlanImage: []tables.PlanImage{
				{PlanId: th.Padding36("plan-3"), Image: "repo.invalid/image-3", Version: "0.0.1"},
			},
			Inputs: map[tables.Input]tables.InputAttr{
				// plan-3
				{InputId: 31, PlanId: th.Padding36("plan-3"), Path: "/in/3"}: {
					UserTag: []domain.Tag{
						{Key: "type", Value: "log-analysis"},
						{Key: "format", Value: "csv"},
					},
				},
			},
			Outputs: map[tables.Output]tables.OutputAttr{
				{OutputId: 32, PlanId: th.Padding36("plan-3"), Path: "/out/3"}: {
					UserTag: []domain.Tag{
						{Key: "type", Value: "report"},
						{Key: "format", Value: "markdown"},
					},
				},
				{OutputId: 33, PlanId: th.Padding36("plan-3"), Path: "/log/3"}: {
					IsLog: true,
					UserTag: []domain.Tag{
						{Key: "type", Value: "log"},
					},
				},
			},
		},
		when{
			spec: domain.BypassValidation(
				th.Padding64("hash:plan-x"), nil,
				domain.PlanParam{
					Image: "repo.invalid/image-x", Version: "0.0.1",
					Active:     true,
					Entrypoint: []string{"python", "main.py"},
					Args:       []string{"--input", "/in/1", "--output", "/out/1"},
					Inputs: []domain.MountPointParam{
						{
							Path: "/in/x",
							Tags: domain.NewTagSet([]domain.Tag{
								// depends on mountpoint 32 (/out/3)
								{Key: "type", Value: "report"},
								{Key: "format", Value: "markdown"},
							}),
						},
					},
					Outputs: []domain.MountPointParam{
						{
							Path: "/out/x",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "raw data"},
								// consumed by mountpoint 11 (/in/1)
							}),
						},
						{
							Path: "/out/x2",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "project", Value: "example"},
								{Key: "type", Value: "raw data"},
							}),
						},
					},
				},
			),
		},
		[]*domain.Plan{
			{
				PlanBody: domain.PlanBody{
					PlanId: th.Padding36("plan-3"), Active: true, Hash: th.Padding64("hash:plan-3"),
					Image: &domain.ImageIdentifier{Image: "repo.invalid/image-3", Version: "0.0.1"},
				},
				Inputs: []domain.MountPoint{
					{
						Id: 31, Path: "/in/3",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "type", Value: "log-analysis"},
							{Key: "format", Value: "csv"},
						}),
					},
				},
				Outputs: []domain.MountPoint{
					{
						Id: 32, Path: "/out/3",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "type", Value: "report"},
							{Key: "format", Value: "markdown"},
						}),
					},
				},
				Log: &domain.LogPoint{
					Id: 33,
					Tags: domain.NewTagSet([]domain.Tag{
						{Key: "type", Value: "log"},
					}),
				},
			},
			{
				PlanBody: domain.PlanBody{
					PlanId: th.Padding36("UNKNOWN"), Active: true, Hash: th.Padding64("hash:plan-x"),
					Image:      &domain.ImageIdentifier{Image: "repo.invalid/image-x", Version: "0.0.1"},
					Entrypoint: []string{"python", "main.py"},
					Args:       []string{"--input", "/in/1", "--output", "/out/1"},
				},
				Inputs: []domain.MountPoint{
					{
						Path: "/in/x",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "type", Value: "report"},
							{Key: "format", Value: "markdown"},
						}),
					},
				},
				Outputs: []domain.MountPoint{
					{
						Path: "/out/x",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "project", Value: "test"},
							{Key: "type", Value: "raw data"},
						}),
					},
					{
						Path: "/out/x2",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "project", Value: "example"},
							{Key: "type", Value: "raw data"},
						}),
					},
				},
			},
		},
	))
	t.Run("let a plan given, when a new plan is passed and depended by the given one, it should register the new plan", theoryOk(
		tables.Operation{
			Plan: []tables.Plan{
				{PlanId: th.Padding36("plan-1"), Active: true, Hash: th.Padding64("hash:plan-1")},
			},
			PlanImage: []tables.PlanImage{
				{PlanId: th.Padding36("plan-1"), Image: "repo.invalid/image-1", Version: "0.0.1-alpha"},
			},
			Inputs: map[tables.Input]tables.InputAttr{
				{InputId: 11, PlanId: th.Padding36("plan-1"), Path: "/in/1"}: {
					UserTag: []domain.Tag{
						{Key: "project", Value: "test"},
						{Key: "type", Value: "raw data"},
					},
				},
			},
			Outputs: map[tables.Output]tables.OutputAttr{
				{OutputId: 12, PlanId: th.Padding36("plan-1"), Path: "/out/1"}: {
					UserTag: []domain.Tag{
						{Key: "project", Value: "test"},
						{Key: "type", Value: "training data"},
					},
				},
				{OutputId: 13, PlanId: th.Padding36("plan-1"), Path: "/log/1"}: {
					IsLog: true,
					UserTag: []domain.Tag{
						{Key: "project", Value: "test"},
						{Key: "type", Value: "log"},
						{Key: "subtype", Value: "throughput"},
					},
				},
			},
		},
		when{
			spec: domain.BypassValidation(
				th.Padding64("hash:plan-x"), nil,
				domain.PlanParam{
					Image: "repo.invalid/image-x", Version: "0.0.1",
					Active:     true,
					Entrypoint: []string{"python", "main.py"},
					Args:       []string{"--input", "/in/1", "--output", "/out/1"},
					Inputs: []domain.MountPointParam{
						{
							Path: "/in/x",
							Tags: domain.NewTagSet([]domain.Tag{
								// depends on mountpoint 32 (/out/3)
								{Key: "type", Value: "report"},
								{Key: "format", Value: "markdown"},
							}),
						},
					},
					Outputs: []domain.MountPointParam{
						{
							Path: "/out/x",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "project", Value: "test"},
								{Key: "type", Value: "raw data"},
								// consumed by mountpoint 11 (/in/1)
							}),
						},
						{
							Path: "/out/x2",
							Tags: domain.NewTagSet([]domain.Tag{
								{Key: "project", Value: "example"},
								{Key: "type", Value: "raw data"},
							}),
						},
					},
				},
			),
		},
		[]*domain.Plan{
			{
				PlanBody: domain.PlanBody{
					PlanId: th.Padding36("plan-1"), Active: true, Hash: th.Padding64("hash:plan-1"),
					Image: &domain.ImageIdentifier{Image: "repo.invalid/image-1", Version: "0.0.1-alpha"},
				},
				Inputs: []domain.MountPoint{
					{
						Id: 11, Path: "/in/1",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "project", Value: "test"},
							{Key: "type", Value: "raw data"},
						}),
					},
				},
				Outputs: []domain.MountPoint{
					{
						Id: 12, Path: "/out/1",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "project", Value: "test"},
							{Key: "type", Value: "training data"},
						}),
					},
				},
				Log: &domain.LogPoint{
					Id: 13,
					Tags: domain.NewTagSet([]domain.Tag{
						{Key: "project", Value: "test"},
						{Key: "type", Value: "log"},
						{Key: "subtype", Value: "throughput"},
					}),
				},
			},
			{
				PlanBody: domain.PlanBody{
					PlanId: th.Padding36("UNKNOWN"), Active: true, Hash: th.Padding64("hash:plan-x"),
					Image:      &domain.ImageIdentifier{Image: "repo.invalid/image-x", Version: "0.0.1"},
					Entrypoint: []string{"python", "main.py"},
					Args:       []string{"--input", "/in/1", "--output", "/out/1"},
				},
				Inputs: []domain.MountPoint{
					{
						Path: "/in/x",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "type", Value: "report"},
							{Key: "format", Value: "markdown"},
						}),
					},
				},
				Outputs: []domain.MountPoint{
					{
						Path: "/out/x",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "project", Value: "test"},
							{Key: "type", Value: "raw data"},
						}),
					},
					{
						Path: "/out/x2",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "project", Value: "example"},
							{Key: "type", Value: "raw data"},
						}),
					},
				},
			},
		},
	))

	// error case
	theoryErr := func(given tables.Operation, when *domain.PlanSpec, then error) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()

			pool := poolBroaker.GetPool(ctx, t)
			if err := given.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			wpool := proxy.Wrap(pool)
			wpool.Events().Query.After(func() {
				th.BeginFuncToRollback(ctx, pool, fn.Void[error](func(tx kpool.Tx) {
					if _, err := tx.Exec(ctx, `lock table "plan" in ROW EXCLUSIVE mode nowait;`); err == nil {
						t.Errorf("plan is not locked")
					} else if pgerr := new(pgconn.PgError); !errors.As(err, &pgerr) || pgerr.Code != pgerrcode.LockNotAvailable {
						t.Errorf(
							"unexpected error: expected error code is %s, but %s",
							pgerrcode.LockNotAvailable, err,
						)
					}
				}))
			})

			nomi := kpgnommock.New(t)
			nomi.Impl.NominateMountpoints = func(ctx context.Context, conn kpool.Tx, mountpointIds []int) error {
				return nil
			}

			testee := kpgplan.New(wpool, kpgplan.WithNominator(nomi))

			conn := try.To(pool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			planIdsBeforeRegister := try.To(allPlanIds(ctx, conn)).OrFatal(t)

			_, err := testee.Register(ctx, when)
			if !errors.Is(err, then) {
				t.Errorf(
					"error is not unexpected one:\n- actual: %+v\n- expected: %+v",
					err, then,
				)
			}

			if len(nomi.Calls.NominateMountpoints) != 0 {
				t.Errorf("Nominator.NominateMountpoints is called: %+v", nomi.Calls.NominateMountpoints)
			}

			planIdsAfrerRegister := try.To(allPlanIds(ctx, conn)).OrFatal(t)

			if !cmp.SliceContentEq(planIdsAfrerRegister, planIdsBeforeRegister) {
				t.Errorf(
					"not match: planIds: (after, before) = (%v, %v)",
					planIdsAfrerRegister, planIdsBeforeRegister,
				)
			}

			{
				query := `table "plan"`
				actual := try.To(scanner.New[tables.Plan]().QueryAll(ctx, conn, query)).OrFatal(t)
				expected := given.Plan
				if !cmp.SliceContentEq(actual, expected) {
					t.Errorf(
						"changed unexpectedly: %s, (after, before) = (%v. %v)",
						query, actual, expected,
					)
				}
			}
			{
				query := `table "plan_image"`
				actual := try.To(scanner.New[tables.PlanImage]().QueryAll(ctx, conn, query)).OrFatal(t)
				expected := given.PlanImage
				if !cmp.SliceContentEq(actual, expected) {
					t.Errorf(
						"changed unexpectedly: %s, (after, before) = (%v. %v)",
						query, actual, expected,
					)
				}
			}
			{
				query := `table "input"`
				actual := try.To(scanner.New[tables.Input]().QueryAll(ctx, conn, query)).OrFatal(t)
				expected := given.Inputs
				if !cmp.SliceContentEq(actual, slices.KeysOf(expected)) {
					t.Errorf(
						"changed unexpectedly: %s, (after, before) = (%v. %v)",
						query, actual, slices.KeysOf(expected),
					)
				}
			}
			{
				query := `table "output"`
				actual := try.To(scanner.New[tables.Output]().QueryAll(ctx, conn, query)).OrFatal(t)
				expected := given.Outputs
				if !cmp.SliceContentEq(actual, slices.KeysOf(expected)) {
					t.Errorf(
						"changed unexpectedly: %s, (after, before) = (%v. %v)",
						query, actual, slices.KeysOf(expected),
					)
				}
			}
			{
				query := `table "knitid_input"`
				type record struct {
					InputId int
					KnitId  string
				}
				actual := try.To(
					scanner.New[record]().QueryAll(ctx, conn, query),
				).OrFatal(t)

				expected := []record{}
				for input, attr := range given.Inputs {
					for _, kid := range attr.KnitId {
						expected = append(expected, record{
							InputId: input.InputId, KnitId: kid,
						})
					}
				}

				if !cmp.SliceContentEq(actual, expected) {
					t.Errorf(
						"changed unexpectedly: %s, (after, before) = (%v. %v)",
						query, actual, expected,
					)
				}
			}
			{
				query := `table "timestamp_input"`
				type record struct {
					InputId   int
					Timestamp time.Time
				}
				actual := try.To(
					scanner.New[record]().QueryAll(ctx, conn, query),
				).OrFatal(t)

				expected := []record{}
				for input, attr := range given.Inputs {
					for _, timestamp := range attr.Timestamp {
						expected = append(expected, record{
							InputId: input.InputId, Timestamp: timestamp,
						})
					}
				}

				if !cmp.SliceContentEq(actual, expected) {
					t.Errorf(
						"changed unexpectedly: %s, (after, before) = (%v. %v)",
						query, actual, expected,
					)
				}
			}
			{
				type record struct {
					InputId int
					Key     string
					Value   string
				}
				actual := try.To(
					scanner.New[record]().QueryAll(
						ctx, conn,
						`
						with "step1" as (
							select "input_id", "key_id", "value"
							from "tag_input"
							inner join "tag" on "tag_input"."tag_id" = "tag"."id"
						)
						select "input_id", "key", "value"
						from "step1"
						inner join "tag_key" on "step1"."key_id" = "tag_key"."id"
						`,
					),
				).OrFatal(t)

				expected := []record{}
				for input, attr := range given.Inputs {
					expected = append(expected, slices.Map(
						attr.UserTag, func(tag domain.Tag) record {
							return record{
								InputId: input.InputId, Key: tag.Key, Value: tag.Value,
							}
						},
					)...)
				}
				if !cmp.SliceContentEqWith(
					actual, expected,
					func(a, b record) bool {
						return a.InputId == b.InputId &&
							(&domain.Tag{Key: a.Key, Value: a.Value}).Equal(
								&domain.Tag{Key: b.Key, Value: b.Value},
							)
					},
				) {
					t.Errorf(
						"changed unexpectedly: %s, (after, before) = (%v. %v)",
						"tag_input", actual, expected,
					)
				}
			}
			{
				type record struct {
					OutputId int
					Key      string
					Value    string
				}
				actual := try.To(
					scanner.New[record]().QueryAll(
						ctx, conn,
						`
						with "step1" as (
							select "output_id", "key_id", "value"
							from "tag_output"
							inner join "tag" on "tag_output"."tag_id" = "tag"."id"
						)
						select "output_id", "key", "value"
						from "step1"
						inner join "tag_key" on "step1"."key_id" = "tag_key"."id"
						`,
					),
				).OrFatal(t)

				expected := []record{}
				for output, attr := range given.Outputs {
					expected = append(expected, slices.Map(
						attr.UserTag, func(tag domain.Tag) record {
							return record{
								OutputId: output.OutputId, Key: tag.Key, Value: tag.Value,
							}
						},
					)...)
				}
				if !cmp.SliceContentEqWith(
					actual, expected,
					func(a, b record) bool {
						return a.OutputId == b.OutputId &&
							(&domain.Tag{Key: a.Key, Value: a.Value}).Equal(
								&domain.Tag{Key: b.Key, Value: b.Value},
							)
					},
				) {
					t.Errorf(
						"changed unexpectedly: %s, (after, before) = (%v. %v)",
						"tag_input", actual, expected,
					)
				}
			}
		}
	}
	fakeError := errors.New("fake error")
	t.Run("when it is passed an invalid plan, it causes error", theoryErr(
		tables.Operation{},
		domain.BypassValidation(
			th.Padding64("hash:plan-x"), fakeError,
			domain.PlanParam{
				Image: "repo.invalid/image-x", Version: "0.0.1",
				Active: true,
				Inputs: []domain.MountPointParam{
					{
						Path: "/in/x",
						Tags: domain.NewTagSet([]domain.Tag{
							// depends on mountpoint 32 (/out/3)
							{Key: "type", Value: "report"},
							{Key: "format", Value: "markdown"},
						}),
					},
				},
				Outputs: []domain.MountPointParam{
					{
						Path: "/out/x",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "project", Value: "test"},
							{Key: "type", Value: "raw data"},
							// consumed by mountpoint 11 (/in/1)
						}),
					},
					{
						Path: "/out/x2",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "project", Value: "example"},
							{Key: "type", Value: "raw data"},
						}),
					},
				},
			},
		),
		fakeError,
	))
	t.Run("let a plan given, when it is passed a plan spec which is equivarent to given, it causes ErrEquivPlanExistsAlready", theoryErr(
		tables.Operation{
			Plan: []tables.Plan{
				{PlanId: th.Padding36("given-plan"), Active: true, Hash: th.Padding64("example-hash")},
			},
			PlanImage: []tables.PlanImage{
				{PlanId: th.Padding36("given-plan"), Image: "repo.invalid/image-name", Version: "v0.1"},
			},
			Inputs: map[tables.Input]tables.InputAttr{
				{InputId: 1, PlanId: th.Padding36("given-plan"), Path: "/in/1"}: {
					UserTag: []domain.Tag{{Key: "tag-1", Value: "val-1"}},
				},
			},
			Outputs: map[tables.Output]tables.OutputAttr{
				{OutputId: 2, PlanId: th.Padding36("given-plan"), Path: "/out/1"}: {
					UserTag: []domain.Tag{{Key: "tag-2", Value: "val-2"}},
				},
			},
		},
		domain.BypassValidation(
			th.Padding64("example-hash"), nil,
			domain.PlanParam{
				Image: "repo.invalid/image-name", Version: "v0.1", Active: true,
				Inputs: []domain.MountPointParam{
					{
						Path: "/in/1",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "tag-1", Value: "val-1"},
						}),
					},
				},
				Outputs: []domain.MountPointParam{
					{
						Path: "/out/1",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "tag-2", Value: "val-2"},
						}),
					},
				},
			},
		),
		domain.NewErrEquivPlanExists(th.Padding36("given-plan")),
	))
	t.Run("when it is passed self-dependant plan, it causes ErrCyclicPlan", theoryErr(
		tables.Operation{},
		domain.BypassValidation(
			th.Padding64("hash"), nil,
			domain.PlanParam{
				Image: "repo.invalid/test-image", Version: "v1.0",
				Active: true,
				Inputs: []domain.MountPointParam{
					{
						Path: "/in",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "tag-1", Value: "value-1"},
						}),
					},
				},
				Outputs: []domain.MountPointParam{
					{
						Path: "/out",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "tag-1", Value: "value-1"},
						}),
					},
				},
			},
		),
		domain.ErrCyclicPlan,
	))
	t.Run("let plans one depends another given, when it is passed a new plan which makes cycle, it causes ErrCyclicPlan", theoryErr(
		tables.Operation{
			// data flow: plan-1>/log/2 --> /in/2>plan-2>/out/2 --> /in/3>plan-3
			//   (notation: [plan name]>[output path] --> [input path]>[plan name])
			Plan: []tables.Plan{
				{PlanId: th.Padding36("plan-1"), Active: true, Hash: th.Padding64("hash:plan-1")},
				{PlanId: th.Padding36("plan-2"), Active: false, Hash: th.Padding64("hash:plan-2")},
				{PlanId: th.Padding36("plan-3"), Active: true, Hash: th.Padding64("hash:plan-3")},
			},
			PlanImage: []tables.PlanImage{
				{PlanId: th.Padding36("plan-1"), Image: "repo.invalid/image-1", Version: "0.0.1-alpha"},
				{PlanId: th.Padding36("plan-2"), Image: "repo.invalid/image-2", Version: "0.0.1-beta"},
				{PlanId: th.Padding36("plan-3"), Image: "repo.invalid/image-3", Version: "0.0.1"},
			},
			Inputs: map[tables.Input]tables.InputAttr{
				{InputId: 11, PlanId: th.Padding36("plan-1"), Path: "/in/1"}: {
					UserTag: []domain.Tag{
						{Key: "project", Value: "test"},
						{Key: "type", Value: "raw data"},
					},
				},
				{InputId: 21, PlanId: th.Padding36("plan-2"), Path: "/in/2"}: {
					UserTag: []domain.Tag{
						{Key: "type", Value: "log"},
						{Key: "subtype", Value: "throughput"},
					},
				},
				{InputId: 31, PlanId: th.Padding36("plan-3"), Path: "/in/3"}: {
					UserTag: []domain.Tag{
						{Key: "type", Value: "log-analysis"},
						{Key: "format", Value: "csv"},
					},
				},
			},
			Outputs: map[tables.Output]tables.OutputAttr{
				{OutputId: 12, PlanId: th.Padding36("plan-1"), Path: "/out/1"}: {
					UserTag: []domain.Tag{
						{Key: "project", Value: "test"},
						{Key: "type", Value: "training data"},
					},
				},
				{OutputId: 13, PlanId: th.Padding36("plan-1"), Path: "/log/1"}: {
					IsLog: true,
					UserTag: []domain.Tag{ // --> plan-2::/in/2
						{Key: "project", Value: "test"},
						{Key: "type", Value: "log"},
						{Key: "subtype", Value: "throughput"},
					},
				},
				{OutputId: 22, PlanId: th.Padding36("plan-2"), Path: "/out/2"}: {
					UserTag: []domain.Tag{
						{Key: "type", Value: "log-analysis"},
						{Key: "format", Value: "csv"},
					},
				},
				{OutputId: 23, PlanId: th.Padding36("plan-2"), Path: "/log/2"}: {
					IsLog: true,
					UserTag: []domain.Tag{
						{Key: "type", Value: "log"},
					},
				},
				{OutputId: 32, PlanId: th.Padding36("plan-3"), Path: "/out/3"}: {
					UserTag: []domain.Tag{
						{Key: "type", Value: "report"},
						{Key: "format", Value: "markdown"},
					},
				},
				{OutputId: 33, PlanId: th.Padding36("plan-3"), Path: "/log/3"}: {
					IsLog: true,
					UserTag: []domain.Tag{
						{Key: "type", Value: "log"},
					},
				},
			},
		},
		domain.BypassValidation(
			th.Padding64("hash:plan-x"), nil,
			domain.PlanParam{
				Image: "repo.invalid/image-x", Version: "0.0.1",
				Active: true,
				Inputs: []domain.MountPointParam{
					{
						Path: "/in/x",
						Tags: domain.NewTagSet([]domain.Tag{
							// depends on mountpoint 32 (/out/3)
							{Key: "type", Value: "report"},
							{Key: "format", Value: "markdown"},
						}),
					},
				},
				Outputs: []domain.MountPointParam{
					{
						Path: "/out/x",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "project", Value: "test"},
							{Key: "type", Value: "raw data"},
							// consumed by mountpoint 11 (/in/1)
						}),
					},
					{
						Path: "/out/x2",
						Tags: domain.NewTagSet([]domain.Tag{
							{Key: "project", Value: "example"},
							{Key: "type", Value: "raw data"},
						}),
					},
				},
			},
		),
		domain.ErrCyclicPlan,
	))
}

func TestPlan_Find(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: th.Padding36("plan-99"), Active: true, Hash: th.Padding64("hash-x")},
			{PlanId: th.Padding36("plan-1"), Active: true, Hash: th.Padding64("hash-x")},
			{PlanId: th.Padding36("plan-2"), Active: false, Hash: th.Padding64("hash-x")},
			{PlanId: th.Padding36("plan-3"), Active: true, Hash: th.Padding64("hash-x")},
			{PlanId: th.Padding36("plan-4"), Active: false, Hash: th.Padding64("hash-x")},
			{PlanId: th.Padding36("plan-5"), Active: true, Hash: th.Padding64("hash-x")},
			{PlanId: th.Padding36("plan-6"), Active: false, Hash: th.Padding64("hash-x")},
			{PlanId: th.Padding36("plan-7"), Active: true, Hash: th.Padding64("hash-x")},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: th.Padding36("plan-99"), Name: "pseudo"},
		},
		PlanImage: []tables.PlanImage{
			{PlanId: th.Padding36("plan-1"), Image: "repo.invalid/image-a", Version: "v1.0"},
			{PlanId: th.Padding36("plan-2"), Image: "repo.invalid/image-a", Version: "v1.0"},
			{PlanId: th.Padding36("plan-3"), Image: "repo.invalid/image-a", Version: "v1.1"},
			{PlanId: th.Padding36("plan-4"), Image: "repo.invalid/image-a", Version: "v1.1"},
			{PlanId: th.Padding36("plan-5"), Image: "repo.invalid/image-b", Version: "v1.0"},
			{PlanId: th.Padding36("plan-6"), Image: "repo.invalid/image-b", Version: "v1.1"},
			{PlanId: th.Padding36("plan-7"), Image: "repo.invalid/image-c", Version: "v1.0"},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			{InputId: 1_100, PlanId: th.Padding36("plan-1"), Path: "/in/1"}: {
				UserTag: []domain.Tag{{Key: "key1", Value: "val1"}},
			},

			{InputId: 2_100, PlanId: th.Padding36("plan-2"), Path: "/in/1"}: {
				UserTag: []domain.Tag{{Key: "key1", Value: "val2"}},
			},
			{InputId: 2_200, PlanId: th.Padding36("plan-2"), Path: "/in/2"}: {
				UserTag: []domain.Tag{{Key: "key2", Value: "val1"}},
			},

			{InputId: 3_100, PlanId: th.Padding36("plan-3"), Path: "/in/1"}: {
				UserTag: []domain.Tag{
					{Key: "key1", Value: "val2"},
					{Key: "key2", Value: "val1"},
				},
			},

			{InputId: 4_100, PlanId: th.Padding36("plan-4"), Path: "/in/1"}: {
				UserTag: []domain.Tag{{Key: "key2", Value: "val1"}},
			},

			{InputId: 5_100, PlanId: th.Padding36("plan-5"), Path: "/in/1"}: {
				UserTag: []domain.Tag{{Key: "key2", Value: "val1"}},
				KnitId:  []string{th.Padding36("knit-1")},
				Timestamp: []time.Time{
					try.To(rfctime.ParseRFC3339DateTime("2023-04-05T06:07:08Z")).OrFatal(t).Time(),
				},
			},

			{InputId: 6_100, PlanId: th.Padding36("plan-6"), Path: "/in/1"}: {
				KnitId: []string{th.Padding36("knit-2")},
			},

			{InputId: 7_100, PlanId: th.Padding36("plan-7"), Path: "/in/1"}: {
				Timestamp: []time.Time{
					try.To(rfctime.ParseRFC3339DateTime("2023-05-04T06:07:08Z")).OrFatal(t).Time(),
				},
			},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 99_010, PlanId: th.Padding36("plan-99"), Path: "/out/1"}: {},

			{OutputId: 1_010, PlanId: th.Padding36("plan-1"), Path: "/out/1"}: {
				UserTag: []domain.Tag{{Key: "key2", Value: "val1"}},
			},
			{OutputId: 1_020, PlanId: th.Padding36("plan-1"), Path: "/out/2"}: {
				UserTag: []domain.Tag{{Key: "key2", Value: "val2"}},
				IsLog:   true,
			},

			// plan-2 has no output

			{OutputId: 3_010, PlanId: th.Padding36("plan-3"), Path: "/out/1"}: {
				// no tags
			},
			{OutputId: 3_020, PlanId: th.Padding36("plan-3"), Path: "/out/2"}: {
				IsLog: true,
			},

			// plan-4 has only log output
			{OutputId: 4_010, PlanId: th.Padding36("plan-4"), Path: "/out/1"}: {
				UserTag: []domain.Tag{{Key: "key1", Value: "val1"}},
				IsLog:   true,
			},

			{OutputId: 5_010, PlanId: th.Padding36("plan-5"), Path: "/out/1"}: {
				UserTag: []domain.Tag{
					{Key: "key1", Value: "val1"},
					{Key: "key2", Value: "val1"},
				},
			},

			{OutputId: 6_010, PlanId: th.Padding36("plan-6"), Path: "/out/1"}: {
				UserTag: []domain.Tag{
					{Key: "key3", Value: "val3"},
					{Key: "key2", Value: "val2"},
				},
			},

			// plan-7 has no output
		},
	}

	type When struct {
		active   logic.Ternary
		imageVer domain.ImageIdentifier
		inTag    []domain.Tag
		outTag   []domain.Tag
	}

	type Then struct {
		planIds []string
		wantErr error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pgpool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			nom := kpgnommock.New(t)
			testee := kpgplan.New(pgpool, kpgplan.WithNominator(nom))

			actual, err := testee.Find(ctx, when.active, when.imageVer, when.inTag, when.outTag)
			if !errors.Is(err, then.wantErr) {
				t.Fatal(err)
			}
			if !cmp.SliceEq(actual, then.planIds) {
				t.Errorf("unexpected result: %v", actual)
			}
		}
	}

	t.Run("wheh query is empty, it returns all", theory(
		When{},
		Then{
			planIds: []string{
				th.Padding36("plan-1"), th.Padding36("plan-2"), th.Padding36("plan-3"),
				th.Padding36("plan-4"), th.Padding36("plan-5"), th.Padding36("plan-6"),
				th.Padding36("plan-7"), th.Padding36("plan-99"),
			},
		},
	))

	t.Run("active=true", theory(
		When{active: logic.True},
		Then{
			planIds: []string{
				th.Padding36("plan-1"), th.Padding36("plan-3"), th.Padding36("plan-5"), th.Padding36("plan-7"),
				th.Padding36("plan-99"),
			},
		},
	))

	t.Run("active=false", theory(
		When{active: logic.False},
		Then{
			planIds: []string{
				th.Padding36("plan-2"), th.Padding36("plan-4"), th.Padding36("plan-6"),
			},
		},
	))

	t.Run("imageVer", theory(
		When{imageVer: domain.ImageIdentifier{Image: "repo.invalid/image-a", Version: "v1.0"}},
		Then{
			planIds: []string{
				th.Padding36("plan-1"), th.Padding36("plan-2"),
			},
		},
	))

	t.Run("imageVer without tag", theory(
		When{imageVer: domain.ImageIdentifier{Image: "repo.invalid/image-a", Version: ""}},
		Then{
			planIds: []string{
				th.Padding36("plan-1"), th.Padding36("plan-2"), th.Padding36("plan-3"), th.Padding36("plan-4"),
			},
		},
	))

	t.Run("inTag with a single tag", theory(
		When{inTag: []domain.Tag{{Key: "key2", Value: "val1"}}},
		Then{
			planIds: []string{
				th.Padding36("plan-2"), th.Padding36("plan-3"), th.Padding36("plan-4"), th.Padding36("plan-5"),
			},
		},
	))

	t.Run("inTag with multiple tags", theory(
		When{inTag: []domain.Tag{{Key: "key1", Value: "val2"}, {Key: "key2", Value: "val1"}}},
		Then{
			planIds: []string{
				th.Padding36("plan-3"),
			},
		},
	))

	t.Run("inTag with knit#id tag", theory(
		When{inTag: []domain.Tag{{Key: domain.KeyKnitId, Value: th.Padding36("knit-1")}}},
		Then{
			planIds: []string{
				th.Padding36("plan-5"),
			},
		},
	))

	t.Run("inTag with timestamp tag", theory(
		When{inTag: []domain.Tag{{Key: domain.KeyKnitTimestamp, Value: "2023-05-04T06:07:08Z"}}},
		Then{
			planIds: []string{
				th.Padding36("plan-7"),
			},
		},
	))

	t.Run("outTag with a single tag", theory(
		When{outTag: []domain.Tag{{Key: "key2", Value: "val1"}}},
		Then{
			planIds: []string{
				th.Padding36("plan-1"), th.Padding36("plan-5"),
			},
		},
	))

	t.Run("outTag with multiple tags", theory(
		When{outTag: []domain.Tag{{Key: "key1", Value: "val1"}, {Key: "key2", Value: "val1"}}},
		Then{
			planIds: []string{
				th.Padding36("plan-5"),
			},
		},
	))

	t.Run("active=false, imageVer, inTag, outTag", theory(
		When{
			active:   logic.False,
			imageVer: domain.ImageIdentifier{Image: "repo.invalid/image-a", Version: "v1.1"},
			inTag:    []domain.Tag{{Key: "key2", Value: "val1"}},
			outTag:   []domain.Tag{{Key: "key1", Value: "val1"}},
		},
		Then{
			planIds: []string{
				th.Padding36("plan-4"),
			},
		},
	))

	t.Run("imageVer, inTag with knit#id, outTag", theory(
		When{
			imageVer: domain.ImageIdentifier{Image: "repo.invalid/image-b", Version: "v1.1"},
			inTag:    []domain.Tag{{Key: domain.KeyKnitId, Value: th.Padding36("knit-2")}},
			outTag:   []domain.Tag{{Key: "key2", Value: "val2"}},
		},
		Then{
			planIds: []string{
				th.Padding36("plan-6"),
			},
		},
	))

	t.Run("active=active, imageVer, inTag with timestamp", theory(
		When{
			active:   logic.True,
			imageVer: domain.ImageIdentifier{Image: "repo.invalid/image-c", Version: "v1.0"},
			inTag:    []domain.Tag{{Key: domain.KeyKnitTimestamp, Value: "2023-05-04T06:07:08Z"}},
		},
		Then{
			planIds: []string{
				th.Padding36("plan-7"),
			},
		},
	))
}

func TestPlan_Activate(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	original := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(original)

	type when struct {
		query          string
		activenessToBe bool
	}

	type lock struct {
		planId []string
		runId  []string
		knitId []string
	}

	type then struct {
		plan tables.Plan
		run  map[string]domain.KnitRunStatus
		err  error
	}

	type testcase struct {
		when when
		lock lock
		then then
	}

	type situation struct {
		given    tables.Operation
		testcase map[string]testcase
	}

	for situationName, situation := range map[string]situation{
		"Let a plan which has runs without output data be given: ": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: th.Padding36("plan-1"), Active: true, Hash: th.Padding64("xxx-hash-xxx")},
					{PlanId: th.Padding36("plan-2"), Active: false, Hash: th.Padding64("yyy-hash-yyy")}, // update this
					{PlanId: th.Padding36("pseudo-plan-1"), Active: true, Hash: th.Padding64("knit#upload")},
					{PlanId: th.Padding36("pseudo-plan-2"), Active: true, Hash: th.Padding64("knit#import")},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: th.Padding36("plan-1"), Image: "repo.invalid/image1", Version: "v0.1"},
					{PlanId: th.Padding36("plan-2"), Image: "repo.invalid/image2", Version: "v0.2"},
				},
				PlanPseudo: []tables.PlanPseudo{
					{PlanId: th.Padding36("pseudo-plan-1"), Name: "knit#upload"},
					{PlanId: th.Padding36("pseudo-plan-2"), Name: "knit#import"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					{InputId: 1100, PlanId: th.Padding36("plan-1"), Path: "/in/1"}: {
						KnitId: []string{th.Padding36("another-knit-id")},
						UserTag: []domain.Tag{
							{Key: "tag1", Value: "val1"},
							{Key: "project", Value: "knit"},
						},
					},
					{InputId: 1200, PlanId: th.Padding36("plan-1"), Path: "/in/2"}: {
						UserTag: []domain.Tag{
							{Key: "tag1", Value: "val2"},
							{Key: "project", Value: "knit"},
						},
						KnitId: []string{th.Padding36("future-knit-id")},
						Timestamp: []time.Time{
							try.To(rfctime.ParseRFC3339DateTime(
								"2022-08-15T12:34:56+00:00",
							)).OrFatal(t).Time(),
						},
					},
					{InputId: 2100, PlanId: th.Padding36("plan-2"), Path: "/in/1"}: {
						UserTag: []domain.Tag{
							{Key: "tag1", Value: "val1"},
							{Key: "tag2", Value: "val2"},
							{Key: "project", Value: "knit"},
						},
						Timestamp: []time.Time{
							try.To(rfctime.ParseRFC3339DateTime(
								"2022-08-15T12:34:56+00:00",
							)).OrFatal(t).Time(),
						},
					},
				},
				Outputs: map[tables.Output]tables.OutputAttr{
					{OutputId: 1010, PlanId: th.Padding36("plan-1"), Path: "/out/1"}: {

						UserTag: []domain.Tag{
							{Key: "tag2", Value: "val3"},
							{Key: "project", Value: "knit"},
						},
					},
					{OutputId: 1001, PlanId: th.Padding36("plan-1"), Path: "/out/log"}: {
						IsLog: true,
						UserTag: []domain.Tag{
							{Key: "tag2", Value: "val3"},
							{Key: "project", Value: "knit"},
							{Key: "type", Value: "log"},
						},
					},
					{OutputId: 2010, PlanId: th.Padding36("plan-2"), Path: "/out/1"}: {
						UserTag: []domain.Tag{
							{Key: "tag1", Value: "val1"},
							{Key: "tag2", Value: "val3"},
							{Key: "project", Value: "knit"},
						},
					},
					{OutputId: 1, PlanId: th.Padding36("pseudo-plan-1"), Path: "/out"}: {},
					{OutputId: 2, PlanId: th.Padding36("pseudo-plan-2"), Path: "/out"}: {},
				},

				Steps: []tables.Step{
					{
						Run: tables.Run{
							RunId: th.Padding36("run-1-a"), PlanId: th.Padding36("plan-1"), Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-07-30T01:10:25.111+09:00",
							)).OrFatal(t).Time(),
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run-1-b"), PlanId: th.Padding36("plan-1"), Status: domain.Waiting,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-07-30T01:10:25.222+09:00",
							)).OrFatal(t).Time(),
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run-2-a"), PlanId: th.Padding36("plan-2"), Status: domain.Deactivated,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-07-30T01:10:25.333+09:00",
							)).OrFatal(t).Time(),
						},
					},
				},
			},
			testcase: map[string]testcase{
				"[inactive -> active] it should return metadata of the plan after activated": {
					when{
						query:          th.Padding36("plan-2"),
						activenessToBe: true,
					},
					lock{
						planId: []string{th.Padding36("plan-2")},
						runId:  []string{th.Padding36("run-2-a")},
						knitId: nil,
					},
					then{
						run: map[string]domain.KnitRunStatus{
							th.Padding36("run-1-a"): domain.Done,
							th.Padding36("run-1-b"): domain.Waiting,
							th.Padding36("run-2-a"): domain.Waiting,
						},
						plan: tables.Plan{
							PlanId: th.Padding36("plan-2"), Active: true, Hash: th.Padding64("yyy-hash-yyy"),
						},
					},
				},
				"[active -> active] it should do nothing but return metadata of the plan": {
					when{query: th.Padding36("plan-1"), activenessToBe: true},
					lock{
						planId: []string{th.Padding36("plan-1")},
						runId:  nil, // no lock; should not be changed
						knitId: nil,
					},
					then{
						run: map[string]domain.KnitRunStatus{
							th.Padding36("run-1-a"): domain.Done,
							th.Padding36("run-1-b"): domain.Waiting,
							th.Padding36("run-2-a"): domain.Deactivated,
						},
						plan: tables.Plan{
							PlanId: th.Padding36("plan-1"), Active: true, Hash: th.Padding64("xxx-hash-xxx"),
						},
					},
				},
				"[inactive -> inactive] it should do nothing but return metadata of the plan": {
					when{query: th.Padding36("plan-2"), activenessToBe: false},
					lock{
						planId: []string{th.Padding36("plan-2")},
						runId:  nil, // no lock; should not be changed.
						knitId: nil,
					},
					then{
						run: map[string]domain.KnitRunStatus{
							th.Padding36("run-1-a"): domain.Done,
							th.Padding36("run-1-b"): domain.Waiting,
							th.Padding36("run-2-a"): domain.Deactivated,
						},
						plan: tables.Plan{
							PlanId: th.Padding36("plan-2"), Active: false, Hash: th.Padding64("yyy-hash-yyy"),
						},
					},
				},
				"[active -> inactive] it should return metadata of plan same after deactivated": {
					when{query: th.Padding36("plan-1"), activenessToBe: false},
					lock{
						planId: []string{th.Padding36("plan-1")},
						runId:  []string{th.Padding36("run-1-b")},
						knitId: nil,
					},
					then{
						run: map[string]domain.KnitRunStatus{
							th.Padding36("run-1-a"): domain.Done,
							th.Padding36("run-1-b"): domain.Deactivated,
							th.Padding36("run-2-a"): domain.Deactivated,
						},
						plan: tables.Plan{
							PlanId: th.Padding36("plan-1"), Active: false, Hash: th.Padding64("xxx-hash-xxx"),
						},
					},
				},
				"when we deactivate a non-existing plan, it should cause MissingError": {
					when{query: th.Padding36("plan-300"), activenessToBe: false},
					lock{}, // nothing to be locked
					then{
						run: map[string]domain.KnitRunStatus{
							th.Padding36("run-1-a"): domain.Done,
							th.Padding36("run-1-b"): domain.Waiting,
							th.Padding36("run-2-a"): domain.Deactivated,
						},
						plan: tables.Plan{}, // when error, this field not required for testing
						err:  kerr.ErrMissing,
					},
				},
				"when we activate a non-existing plan, it should cause MissingError": {
					when{query: th.Padding36("plan-300"), activenessToBe: true},
					lock{}, // nothing to be locked
					then{
						run: map[string]domain.KnitRunStatus{
							th.Padding36("run-1-a"): domain.Done,
							th.Padding36("run-1-b"): domain.Waiting,
							th.Padding36("run-2-a"): domain.Deactivated,
						},
						plan: tables.Plan{}, // when error, this field not required for testing
						err:  kerr.ErrMissing,
					},
				},
			},
		},
		"Let a plan which has runs with output be given: ": {
			given: tables.Operation{
				Plan: []tables.Plan{
					{PlanId: th.Padding36("plan-1"), Active: true, Hash: th.Padding64("xxx-hash-xxx")},
					{PlanId: th.Padding36("plan-2"), Active: false, Hash: th.Padding64("yyy-hash-yyy")},
					{PlanId: th.Padding36("pseudo-plan-1"), Active: true, Hash: th.Padding64("knit#upload")},
					{PlanId: th.Padding36("pseudo-plan-2"), Active: true, Hash: th.Padding64("knit#import")},
				},
				PlanImage: []tables.PlanImage{
					{PlanId: th.Padding36("plan-1"), Image: "repo.invalid/image1", Version: "v0.1"},
					{PlanId: th.Padding36("plan-2"), Image: "repo.invalid/image2", Version: "v0.2"},
				},
				PlanPseudo: []tables.PlanPseudo{
					{PlanId: th.Padding36("pseudo-plan-1"), Name: "knit#upload"},
					{PlanId: th.Padding36("pseudo-plan-2"), Name: "knit#import"},
				},
				Inputs: map[tables.Input]tables.InputAttr{
					//        ,----- plan id suffix
					//        |,---- input index
					//        ||,--- output index
					//        |||,-- log?
					//        vvvv
					{InputId: 1100, PlanId: th.Padding36("plan-1"), Path: "/in/1"}: {
						UserTag: []domain.Tag{
							{Key: "tag1", Value: "val1"},
							{Key: "project", Value: "knit"},
						},
						KnitId: []string{th.Padding36("another-knit-id")},
					},
					{InputId: 1200, PlanId: th.Padding36("plan-1"), Path: "/in/2"}: {
						UserTag: []domain.Tag{
							{Key: "tag1", Value: "val2"},
							{Key: "project", Value: "knit"},
						},
						KnitId: []string{th.Padding36("future-knit-id")},
						Timestamp: []time.Time{
							try.To(rfctime.ParseRFC3339DateTime(
								"2022-08-15T12:34:56+00:00",
							)).OrFatal(t).Time(),
						},
					},
					{InputId: 2100, PlanId: th.Padding36("plan-2"), Path: "/in/1"}: {
						UserTag: []domain.Tag{
							{Key: "tag1", Value: "val1"},
							{Key: "tag2", Value: "val2"},
							{Key: "project", Value: "knit"},
						},
						Timestamp: []time.Time{
							try.To(rfctime.ParseRFC3339DateTime(
								"2022-08-15T12:34:56+00:00",
							)).OrFatal(t).Time(),
						},
					},
				},

				Outputs: map[tables.Output]tables.OutputAttr{
					{OutputId: 1010, PlanId: th.Padding36("plan-1"), Path: "/out/1"}: {
						UserTag: []domain.Tag{
							{Key: "tag2", Value: "val3"},
							{Key: "project", Value: "knit"},
						},
					},
					{OutputId: 1001, PlanId: th.Padding36("plan-1"), Path: "/out/log"}: {
						IsLog: true,
						UserTag: []domain.Tag{
							{Key: "tag2", Value: "val3"},
							{Key: "project", Value: "knit"},
							{Key: "type", Value: "log"},
						},
					},
					{OutputId: 2010, PlanId: th.Padding36("plan-2"), Path: "/out/1"}: {
						UserTag: []domain.Tag{
							{Key: "tag1", Value: "val1"},
							{Key: "tag2", Value: "val3"},
							{Key: "project", Value: "knit"},
						},
					},
					{OutputId: 1, PlanId: th.Padding36("pseudo-plan-1"), Path: "/out"}: {},
					{OutputId: 2, PlanId: th.Padding36("pseudo-plan-2"), Path: "/out"}: {},
				},

				Steps: []tables.Step{
					{
						Run: tables.Run{
							RunId: th.Padding36("run-1-pseudo-1"), PlanId: th.Padding36("pseudo-plan-1"), Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-08-15T12:34:56+00:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: th.Padding36("knit-1"), VolumeRef: "vol-1",
								OutputId: 1, RunId: th.Padding36("run-1-pseudo-1"), PlanId: th.Padding36("pseudo-plan-1"),
							}: {},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run-2-pseudo-1"), PlanId: th.Padding36("pseudo-plan-1"), Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-08-15T12:34:56+00:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: th.Padding36("knit-2"), VolumeRef: "vol-2",
								OutputId: 1, RunId: th.Padding36("run-2-pseudo-1"), PlanId: th.Padding36("pseudo-plan-1"),
							}: {},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run-3-pseudo-1"), PlanId: th.Padding36("pseudo-plan-1"), Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-08-15T12:34:56+00:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: th.Padding36("knit-5"), VolumeRef: "vol-5",
								OutputId: 1, RunId: th.Padding36("run-3-pseudo-1"), PlanId: th.Padding36("pseudo-plan-1"),
							}: {},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run-4-pseudo-1"), PlanId: th.Padding36("pseudo-plan-1"), Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-08-15T12:34:56+00:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: th.Padding36("knit-8"), VolumeRef: "vol-8",
								OutputId: 1, RunId: th.Padding36("run-4-pseudo-1"), PlanId: th.Padding36("pseudo-plan-1"),
							}: {},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run-1-a"), PlanId: th.Padding36("plan-1"), Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-07-30T01:10:25.111+09:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								KnitId:  th.Padding36("knit-1"),
								InputId: 1100, RunId: th.Padding36("run-1-a"), PlanId: th.Padding36("plan-1"),
							},
							{
								KnitId:  th.Padding36("knit-2"),
								InputId: 1200, RunId: th.Padding36("run-1-a"), PlanId: th.Padding36("plan-1"),
							},
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: th.Padding36("knit-3"), VolumeRef: "vol-3",
								OutputId: 1010, RunId: th.Padding36("run-1-a"), PlanId: th.Padding36("plan-1"),
							}: {},
							{
								KnitId: th.Padding36("knit-4"), VolumeRef: "vol-4",
								OutputId: 1001, RunId: th.Padding36("run-1-a"), PlanId: th.Padding36("plan-1"),
							}: {},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run-1-b"), PlanId: th.Padding36("plan-1"), Status: domain.Waiting,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-07-30T01:10:25.222+09:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{KnitId: th.Padding36("knit-1"), InputId: 1100, RunId: th.Padding36("run-1-b"), PlanId: th.Padding36("plan-1")},
							{KnitId: th.Padding36("knit-5"), InputId: 1200, RunId: th.Padding36("run-1-b"), PlanId: th.Padding36("plan-1")},
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: th.Padding36("knit-6"), VolumeRef: "vol-6",
								OutputId: 1010, RunId: th.Padding36("run-1-b"), PlanId: th.Padding36("plan-1"),
							}: {},
							{
								KnitId: th.Padding36("knit-7"), VolumeRef: "vol-7",
								OutputId: 1001, RunId: th.Padding36("run-1-b"), PlanId: th.Padding36("plan-1"),
							}: {},
						},
					},
					{
						Run: tables.Run{
							RunId: th.Padding36("run-2-a"), PlanId: th.Padding36("plan-2"), Status: domain.Deactivated,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-07-30T01:10:25.333+09:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{KnitId: th.Padding36("knit-8"), InputId: 2100, RunId: th.Padding36("run-2-a"), PlanId: th.Padding36("plan-2")},
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: th.Padding36("knit-9"), VolumeRef: "vol-9",
								OutputId: 2010, RunId: th.Padding36("run-2-a"), PlanId: th.Padding36("plan-2"),
							}: {},
						},
					},
				},
			},
			testcase: map[string]testcase{
				"when we deactivate a plan which has runs with output data, it should lock the data": {
					when{query: th.Padding36("plan-1"), activenessToBe: false},
					lock{
						planId: []string{th.Padding36("plan-1")},
						runId:  []string{th.Padding36("run-1-b")},
						knitId: []string{th.Padding36("knit-6"), th.Padding36("knit-7")},
					},
					then{
						run: map[string]domain.KnitRunStatus{
							th.Padding36("run-1-pseudo-1"): domain.Done,
							th.Padding36("run-2-pseudo-1"): domain.Done,
							th.Padding36("run-3-pseudo-1"): domain.Done,
							th.Padding36("run-4-pseudo-1"): domain.Done,
							th.Padding36("run-1-a"):        domain.Done,
							th.Padding36("run-1-b"):        domain.Deactivated,
							th.Padding36("run-2-a"):        domain.Deactivated,
						},
						plan: tables.Plan{
							PlanId: th.Padding36("plan-1"), Active: false, Hash: th.Padding64("xxx-hash-xxx"),
						},
					},
				},
			},
		},
	} {
		for name, testcase := range situation.testcase {
			t.Run(situationName+name, func(t *testing.T) {
				when, lock, then := testcase.when, testcase.lock, testcase.then

				ctx := context.Background()
				pgpool := poolBroaker.GetPool(ctx, t)
				wpool := proxy.Wrap(pgpool)

				if err := situation.given.Apply(ctx, pgpool); err != nil {
					t.Fatal(err)
				}

				wpool.Events().Query.After(
					func() {
						th.BeginFuncToRollback(ctx, pgpool, fn.Void[error](func(tx kpool.Tx) {
							locked := try.To(scanner.New[string]().QueryAll(
								ctx, tx,
								`
								with "unlocked" as (select "plan_id" from "plan" order by "plan_id" for update skip locked)
								select "plan_id" from "plan" EXCEPT select "plan_id" from "unlocked"
								`,
							)).OrFatal(t)

							if !cmp.SliceContentEq(locked, lock.planId) {
								t.Errorf(
									"unmatch: locked plan: (actual, expected) = (%v, %v)",
									locked, lock.planId,
								)
							}
						}))
					},
					func() {
						th.BeginFuncToRollback(ctx, pgpool, fn.Void[error](func(tx kpool.Tx) {
							locked := try.To(scanner.New[string]().QueryAll(
								ctx, tx,
								`
								with "unlocked" as (select "run_id" from "run" order by "run_id" for update skip locked)
								select "run_id" from "run" EXCEPT select "run_id" from "unlocked"
								`,
							)).OrFatal(t)

							if !cmp.SliceContentEq(locked, lock.runId) {
								t.Errorf(
									"unmatch: locked run: (actual, expected) = (%v, %v)",
									locked, lock.runId,
								)
							}
						}))
					},
					func() {
						th.BeginFuncToRollback(ctx, pgpool, fn.Void[error](func(tx kpool.Tx) {
							locked := try.To(scanner.New[string]().QueryAll(
								ctx, tx,
								`
								with "unlocked" as (select "knit_id" from "data" order by "knit_id" for update skip locked)
								select "knit_id" from "data" EXCEPT select "knit_id" from "unlocked"
								`,
							)).OrFatal(t)

							if !cmp.SliceContentEq(locked, lock.knitId) {
								t.Errorf(
									"unmatch: locked data: (actual, expected) = (%v, %v)",
									locked, lock.knitId,
								)
							}
						}))
					},
				)

				nom := kpgnommock.New(t)
				testee := kpgplan.New(wpool, kpgplan.WithNominator(nom))
				err := testee.Activate(ctx, when.query, when.activenessToBe)

				if expected := testcase.then.err; expected != nil {
					if !errors.Is(err, expected) {
						t.Errorf("unmatch error: (actual, expected) = (%v, %v)", err, expected)
					}
					return
				} else if err != nil {
					t.Fatalf("failed to retreive plans. error = %v", err)
				}

				conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()
				{
					var actual tables.Plan
					plans := try.To(
						scanner.New[tables.Plan]().QueryAll(
							ctx, conn,
							`select * from "plan" where "plan_id" = $1`,
							when.query,
						),
					).OrFatal(t)

					if len(plans) == 0 {
						t.Fatal("acrivated plan is missing")
					}
					actual = plans[0]

					if then.plan != actual {
						t.Errorf(
							"Plan\n===actual===\n%+v\n===expected===\n%+v",
							actual, then.plan,
						)
					}
				}
				{
					runs := try.To(
						scanner.New[tables.Run]().QueryAll(ctx, conn, `select * from "run"`),
					).OrFatal(t)
					actual := map[string]domain.KnitRunStatus{}
					for _, r := range runs {
						actual[r.RunId] = r.Status
					}
					if !cmp.MapEq(actual, then.run) {
						t.Errorf(
							"run status\n===actual===\n%+v\n===expected===\n%+v",
							actual, then.run,
						)
					}
				}
			})
		}
	}
}

func TestSetResouceLimit(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	original := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(original)

	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: th.Padding36("plan-1"), Active: true, Hash: th.Padding64("xxx-hash-xxx")},
			{PlanId: th.Padding36("plan-2"), Active: false, Hash: th.Padding64("yyy-hash-yyy")},
		},
		PlanImage: []tables.PlanImage{
			{PlanId: th.Padding36("plan-1"), Image: "repo.invalid/image1", Version: "v0.1"},
			{PlanId: th.Padding36("plan-2"), Image: "repo.invalid/image2", Version: "v0.2"},
		},
		PlanResources: []tables.PlanResource{
			{PlanId: th.Padding36("plan-1"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("1"))},
			{PlanId: th.Padding36("plan-1"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("1Gi"))},
		},
	}

	type When struct {
		planId    string
		resources map[string]resource.Quantity
	}

	type Then struct {
		want    []tables.PlanResource
		wantErr error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pgpool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			testee := kpgplan.New(pgpool)
			err := testee.SetResourceLimit(ctx, when.planId, when.resources)
			if !errors.Is(err, then.wantErr) {
				t.Fatal("err:", err)
			}

			actual := try.To(
				scanner.New[tables.PlanResource]().QueryAll(
					ctx, conn, `table "plan_resource"`,
				),
			).OrFatal(t)

			if !cmp.SliceContentEq(actual, then.want) {
				t.Errorf(
					"plan_resource\n===actual===\n%+v\n===expected===\n%+v",
					actual, then.want,
				)
			}
		}
	}

	t.Run("when setting resource limit to a plan, it should be updated", theory(
		When{
			planId: th.Padding36("plan-1"),
			resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("2"),
				"memory": resource.MustParse("2Gi"),
			},
		},
		Then{
			want: []tables.PlanResource{
				{PlanId: th.Padding36("plan-1"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("2"))},
				{PlanId: th.Padding36("plan-1"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("2Gi"))},
			},
		},
	))

	t.Run("when setting resource limit to a plan which has no resource limit, it should be inserted", theory(
		When{
			planId: th.Padding36("plan-2"),
			resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("2"),
				"memory": resource.MustParse("2Gi"),
			},
		},
		Then{
			want: []tables.PlanResource{
				{PlanId: th.Padding36("plan-1"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("1"))},
				{PlanId: th.Padding36("plan-1"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("1Gi"))},
				{PlanId: th.Padding36("plan-2"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("2"))},
				{PlanId: th.Padding36("plan-2"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("2Gi"))},
			},
		},
	))

	t.Run("when setting resource limit to a non existing plan, it returns ErrMissing", theory(
		When{
			planId: th.Padding36("plan-3"),
			resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("2"),
				"memory": resource.MustParse("2Gi"),
			},
		},
		Then{
			want: []tables.PlanResource{
				{PlanId: th.Padding36("plan-1"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("1"))},
				{PlanId: th.Padding36("plan-1"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("1Gi"))},
			},
			wantErr: kerr.ErrMissing,
		},
	))
}

func TestUnsetResourceLimit(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	original := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(original)

	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: th.Padding36("plan-1"), Active: true, Hash: th.Padding64("xxx-hash-xxx")},
			{PlanId: th.Padding36("plan-2"), Active: false, Hash: th.Padding64("yyy-hash-yyy")},
		},
		PlanImage: []tables.PlanImage{
			{PlanId: th.Padding36("plan-1"), Image: "repo.invalid/image1", Version: "v0.1"},
			{PlanId: th.Padding36("plan-2"), Image: "repo.invalid/image2", Version: "v0.2"},
		},
		PlanResources: []tables.PlanResource{
			{PlanId: th.Padding36("plan-1"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("1"))},
			{PlanId: th.Padding36("plan-1"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("1Gi"))},
			{PlanId: th.Padding36("plan-2"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("2"))},
			{PlanId: th.Padding36("plan-2"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("2Gi"))},
		},
	}

	type When struct {
		planId string
		types  []string
	}

	type Then struct {
		want    []tables.PlanResource
		wantErr error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pgpool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			testee := kpgplan.New(pgpool)
			err := testee.UnsetResourceLimit(ctx, when.planId, when.types)
			if !errors.Is(err, then.wantErr) {
				t.Fatal("error: ", err)
			}

			actual := try.To(
				scanner.New[tables.PlanResource]().QueryAll(
					ctx, conn, `table "plan_resource"`,
				),
			).OrFatal(t)

			if !cmp.SliceContentEq(actual, then.want) {
				t.Errorf(
					"plan_resource\n===actual===\n%+v\n===expected===\n%+v",
					actual, then.want,
				)
			}
		}
	}

	t.Run("when unsetting a resource limit to a plan, it should be removed", theory(
		When{
			planId: th.Padding36("plan-1"),
			types:  []string{"cpu"},
		},
		Then{
			want: []tables.PlanResource{
				{PlanId: th.Padding36("plan-1"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("1Gi"))},
				{PlanId: th.Padding36("plan-2"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("2"))},
				{PlanId: th.Padding36("plan-2"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("2Gi"))},
			},
		},
	))

	t.Run("when unsetting resources limit to a plan, it should be removed", theory(
		When{
			planId: th.Padding36("plan-1"),
			types:  []string{"cpu", "memory"},
		},
		Then{
			want: []tables.PlanResource{
				{PlanId: th.Padding36("plan-2"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("2"))},
				{PlanId: th.Padding36("plan-2"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("2Gi"))},
			},
		},
	))

	t.Run("when unsetting resource limit to a plan which has no resource limit, it should do nothing", theory(
		When{
			planId: th.Padding36("plan-2"),
			types:  []string{"gpu"},
		},
		Then{
			want: []tables.PlanResource{
				{PlanId: th.Padding36("plan-1"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("1"))},
				{PlanId: th.Padding36("plan-1"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("1Gi"))},
				{PlanId: th.Padding36("plan-2"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("2"))},
				{PlanId: th.Padding36("plan-2"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("2Gi"))},
			},
		},
	))

	t.Run("when unsetting resources limit to a non existing plan, it returns ErrMissing", theory(
		When{
			planId: th.Padding36("plan-3"),
			types:  []string{"cpu", "memory"},
		},
		Then{
			want: []tables.PlanResource{
				{PlanId: th.Padding36("plan-1"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("1"))},
				{PlanId: th.Padding36("plan-1"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("1Gi"))},
				{PlanId: th.Padding36("plan-2"), Type: "cpu", Value: marshal.ResourceQuantity(resource.MustParse("2"))},
				{PlanId: th.Padding36("plan-2"), Type: "memory", Value: marshal.ResourceQuantity(resource.MustParse("2Gi"))},
			},
			wantErr: kerr.ErrMissing,
		},
	))
}

func TestUpdateAnnotations(t *testing.T) {
	given := tables.Operation{
		Plan: []tables.Plan{
			{
				PlanId: th.Padding36("plan-1"),
				Active: true,
				Hash:   th.Padding64("xxx-hash-xxx"),
			},
			{
				PlanId: th.Padding36("plan-2"),
				Active: true,
				Hash:   th.Padding64("yyy-hash-yyy"),
			},
			{
				PlanId: th.Padding36("plan-3"),
				Active: true,
				Hash:   th.Padding64("zzz-hash-zzz"),
			},
		},
		PlanAnnotations: []tables.Annotation{
			{
				PlanId: th.Padding36("plan-1"),
				Key:    "key1",
				Value:  "val1",
			},
			{
				PlanId: th.Padding36("plan-1"),
				Key:    "key2",
				Value:  "val2",
			},
			{
				PlanId: th.Padding36("plan-1"),
				Key:    "key2",
				Value:  "val2b",
			},
			{
				PlanId: th.Padding36("plan-3"),
				Key:    "key1",
				Value:  "val1",
			},
		},
	}

	type When struct {
		planId string
		delta  domain.AnnotationDelta
	}

	type Then struct {
		want      []tables.Annotation
		wantError error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			poolBroaker := testenv.NewPoolBroaker(ctx, t)
			pgpool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			testee := kpgplan.New(pgpool)

			err := testee.UpdateAnnotations(ctx, when.planId, when.delta)
			if err != nil {
				if then.wantError == nil {
					t.Fatal(err)
				} else if !errors.Is(err, then.wantError) {
					t.Errorf("unexpected error: %v", err)
				}
			}

			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			actual := try.To(
				scanner.New[tables.Annotation]().QueryAll(
					ctx, conn, `table "plan_annotation"`,
				),
			).OrFatal(t)

			if !cmp.SliceContentEq(actual, then.want) {
				t.Errorf(
					"plan_annotation\n===actual===\n%+v\n===expected===\n%+v",
					actual, then.want,
				)
			}
		}
	}

	t.Run("when adding annotations to a plan, it should be updated", theory(
		When{
			planId: th.Padding36("plan-1"),
			delta: domain.AnnotationDelta{
				Add: []domain.Annotation{
					{Key: "key3", Value: "val3"},
					{Key: "key4", Value: "val4"},
				},
			},
		},
		Then{
			want: []tables.Annotation{
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key1",
					Value:  "val1",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key2",
					Value:  "val2",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key2",
					Value:  "val2b",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key3",
					Value:  "val3",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key4",
					Value:  "val4",
				},
				{
					PlanId: th.Padding36("plan-3"),
					Key:    "key1",
					Value:  "val1",
				},
			},
		},
	))

	t.Run("when removing annotations from a plan, it should be updated", theory(
		When{
			planId: th.Padding36("plan-1"),
			delta: domain.AnnotationDelta{
				Remove: []domain.Annotation{
					{Key: "key1", Value: "val1"},
				},
			},
		},
		Then{
			want: []tables.Annotation{
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key2",
					Value:  "val2",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key2",
					Value:  "val2b",
				},
				{
					PlanId: th.Padding36("plan-3"),
					Key:    "key1",
					Value:  "val1",
				},
			},
		},
	))

	t.Run("when removing annotations by key from a plan, it should be updated", theory(
		When{
			planId: th.Padding36("plan-1"),
			delta: domain.AnnotationDelta{
				RemoveKey: []string{"key2"},
			},
		},
		Then{
			want: []tables.Annotation{
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key1",
					Value:  "val1",
				},
				{
					PlanId: th.Padding36("plan-3"),
					Key:    "key1",
					Value:  "val1",
				},
			},
		},
	))

	t.Run("when removing annotations , it should be updated", theory(
		When{
			planId: th.Padding36("plan-1"),
			delta: domain.AnnotationDelta{
				Remove: []domain.Annotation{
					{Key: "key1", Value: "val1"},
				},
				RemoveKey: []string{"key2"},
			},
		},
		Then{
			want: []tables.Annotation{
				{
					PlanId: th.Padding36("plan-3"),
					Key:    "key1",
					Value:  "val1",
				},
			},
		},
	))

	t.Run("when adding annotation to a non existing plan, it returns ErrMissing (add only)", theory(
		When{
			planId: th.Padding36("plan-9"),
			delta: domain.AnnotationDelta{
				Add: []domain.Annotation{
					{Key: "key3", Value: "val3"},
				},
			},
		},
		Then{
			want: []tables.Annotation{
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key1",
					Value:  "val1",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key2",
					Value:  "val2",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key2",
					Value:  "val2b",
				},
				{
					PlanId: th.Padding36("plan-3"),
					Key:    "key1",
					Value:  "val1",
				},
			},
			wantError: kerr.ErrMissing,
		},
	))

	t.Run("when adding annotation to a non existing plan, it returns ErrMissing (remove only)", theory(
		When{
			planId: th.Padding36("plan-9"),
			delta: domain.AnnotationDelta{
				Remove: []domain.Annotation{
					{Key: "key1", Value: "val1"},
				},
			},
		},
		Then{
			want: []tables.Annotation{
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key1",
					Value:  "val1",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key2",
					Value:  "val2",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key2",
					Value:  "val2b",
				},
				{
					PlanId: th.Padding36("plan-3"),
					Key:    "key1",
					Value:  "val1",
				},
			},
			wantError: kerr.ErrMissing,
		},
	))

	t.Run("when adding annotation as same as existing one, it should do nothing", theory(
		When{
			planId: th.Padding36("plan-1"),
			delta: domain.AnnotationDelta{
				Add: []domain.Annotation{
					{Key: "key1", Value: "val1"},
				},
			},
		},
		Then{
			want: []tables.Annotation{
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key1",
					Value:  "val1",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key2",
					Value:  "val2",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key2",
					Value:  "val2b",
				},
				{
					PlanId: th.Padding36("plan-3"),
					Key:    "key1",
					Value:  "val1",
				},
			},
		},
	))

	t.Run("when removing annotation not existing, it should do nothing", theory(
		When{
			planId: th.Padding36("plan-1"),
			delta: domain.AnnotationDelta{
				Remove: []domain.Annotation{
					{Key: "key3", Value: "val3"},
				},
			},
		},
		Then{
			want: []tables.Annotation{
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key1",
					Value:  "val1",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key2",
					Value:  "val2",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key2",
					Value:  "val2b",
				},
				{
					PlanId: th.Padding36("plan-3"),
					Key:    "key1",
					Value:  "val1",
				},
			},
		},
	))

	t.Run("when adding and removing annotations to a plan, remove should be done first", theory(
		When{
			planId: th.Padding36("plan-1"),
			delta: domain.AnnotationDelta{
				Add: []domain.Annotation{
					{Key: "key1", Value: "val1"},
					{Key: "key3", Value: "val3"},
				},
				Remove: []domain.Annotation{
					{Key: "key1", Value: "val1"},
					{Key: "key2", Value: "val2"},
				},
			},
		},
		Then{
			want: []tables.Annotation{
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key1",
					Value:  "val1",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key2",
					Value:  "val2b",
				},
				{
					PlanId: th.Padding36("plan-1"),
					Key:    "key3",
					Value:  "val3",
				},
				{
					PlanId: th.Padding36("plan-3"),
					Key:    "key1",
					Value:  "val1",
				},
			},
		},
	))
}

func TestSetServiceAccount(t *testing.T) {
	given := tables.Operation{
		Plan: []tables.Plan{
			{
				PlanId: th.Padding36("plan-1"),
				Active: true,
				Hash:   th.Padding64("xxx-hash-xxx"),
			},
			{
				PlanId: th.Padding36("plan-2"),
				Active: true,
				Hash:   th.Padding64("yyy-hash-yyy"),
			},
			{
				PlanId: th.Padding36("plan-3"),
				Active: true,
				Hash:   th.Padding64("zzz-hash-zzz"),
			},
		},
		PlanServiceAccount: []tables.ServiceAccount{
			{
				PlanId:         th.Padding36("plan-1"),
				ServiceAccount: "sa1",
			},
			{
				PlanId:         th.Padding36("plan-3"),
				ServiceAccount: "sa2",
			},
		},
	}

	type When struct {
		planId string
		sa     string
	}

	type Then struct {
		want    []tables.ServiceAccount
		wantErr error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			poolBroaker := testenv.NewPoolBroaker(ctx, t)
			pgpool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			testee := kpgplan.New(pgpool)

			err := testee.SetServiceAccount(ctx, when.planId, when.sa)
			if err != nil {
				if then.wantErr == nil {
					t.Fatal(err)
				} else if !errors.Is(err, then.wantErr) {
					t.Errorf("unexpected error: %v", err)
				}
			}

			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			actual := try.To(
				scanner.New[tables.ServiceAccount]().QueryAll(
					ctx, conn, `table "plan_service_account"`,
				),
			).OrFatal(t)

			if !cmp.SliceContentEq(actual, then.want) {
				t.Errorf(
					"plan_service_account\n===actual===\n%+v\n===expected===\n%+v",
					actual, then.want,
				)
			}
		}
	}

	t.Run("when setting a service account to a plan which have service account, it should be updated", theory(
		When{
			planId: th.Padding36("plan-1"),
			sa:     "sa3",
		},
		Then{
			want: []tables.ServiceAccount{
				{
					PlanId:         th.Padding36("plan-1"),
					ServiceAccount: "sa3",
				},
				{
					PlanId:         th.Padding36("plan-3"),
					ServiceAccount: "sa2",
				},
			},
		},
	))

	t.Run("when setting a service account to a plan which have no service account, it should be inserted", theory(
		When{
			planId: th.Padding36("plan-2"),
			sa:     "sa3",
		},
		Then{
			want: []tables.ServiceAccount{
				{
					PlanId:         th.Padding36("plan-1"),
					ServiceAccount: "sa1",
				},
				{
					PlanId:         th.Padding36("plan-3"),
					ServiceAccount: "sa2",
				},
				{
					PlanId:         th.Padding36("plan-2"),
					ServiceAccount: "sa3",
				},
			},
		},
	))

	t.Run("when setting a service account to a non existing plan, it returns ErrMissing", theory(
		When{
			planId: th.Padding36("plan-9"),
			sa:     "sa3",
		},
		Then{
			want: []tables.ServiceAccount{
				{
					PlanId:         th.Padding36("plan-1"),
					ServiceAccount: "sa1",
				},
				{
					PlanId:         th.Padding36("plan-3"),
					ServiceAccount: "sa2",
				},
			},
			wantErr: kerr.ErrMissing,
		},
	))
}

func TestUnsetServiceAccount(t *testing.T) {
	given := tables.Operation{
		Plan: []tables.Plan{
			{
				PlanId: th.Padding36("plan-1"),
				Active: true,
				Hash:   th.Padding64("xxx-hash-xxx"),
			},
			{
				PlanId: th.Padding36("plan-2"),
				Active: true,
				Hash:   th.Padding64("yyy-hash-yyy"),
			},
			{
				PlanId: th.Padding36("plan-3"),
				Active: true,
				Hash:   th.Padding64("zzz-hash-zzz"),
			},
		},
		PlanServiceAccount: []tables.ServiceAccount{
			{
				PlanId:         th.Padding36("plan-1"),
				ServiceAccount: "sa1",
			},
			{
				PlanId:         th.Padding36("plan-3"),
				ServiceAccount: "sa2",
			},
		},
	}

	type When struct {
		planId string
	}

	type Then struct {
		want    []tables.ServiceAccount
		wantErr error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			poolBroaker := testenv.NewPoolBroaker(ctx, t)
			pgpool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			testee := kpgplan.New(pgpool)

			err := testee.UnsetServiceAccount(ctx, when.planId)
			if err != nil {
				if then.wantErr == nil {
					t.Fatal(err)
				} else if !errors.Is(err, then.wantErr) {
					t.Errorf("unexpected error: %v", err)
				}
			}

			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			actual := try.To(
				scanner.New[tables.ServiceAccount]().QueryAll(
					ctx, conn, `table "plan_service_account"`,
				),
			).OrFatal(t)

			if !cmp.SliceContentEq(actual, then.want) {
				t.Errorf("plan_service_account\n===actual===\n%+v\n===expected===\n%+v", actual, then.want)
			}
		}
	}

	t.Run("when unsetting a service account to a plan which have service account, it should be removed", theory(
		When{
			planId: th.Padding36("plan-1"),
		},
		Then{
			want: []tables.ServiceAccount{
				{
					PlanId:         th.Padding36("plan-3"),
					ServiceAccount: "sa2",
				},
			},
		},
	))

	t.Run("when unsetting a service account to a plan which have no service account, it should do nothing", theory(
		When{
			planId: th.Padding36("plan-2"),
		},
		Then{
			want: []tables.ServiceAccount{
				{
					PlanId:         th.Padding36("plan-1"),
					ServiceAccount: "sa1",
				},
				{
					PlanId:         th.Padding36("plan-3"),
					ServiceAccount: "sa2",
				},
			},
		},
	))

	t.Run("when unsetting a service account to a non existing plan, it returns ErrMissing", theory(
		When{
			planId: th.Padding36("plan-9"),
		},
		Then{
			want: []tables.ServiceAccount{
				{
					PlanId:         th.Padding36("plan-1"),
					ServiceAccount: "sa1",
				},
				{
					PlanId:         th.Padding36("plan-3"),
					ServiceAccount: "sa2",
				},
			},
			wantErr: kerr.ErrMissing,
		},
	))
}
