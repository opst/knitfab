package purge_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/opst/knitfab-api-types/v2/misc/rfctime"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/proxy"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/conn/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/data/db/postgres"
	domerr "github.com/opst/knitfab/pkg/domain/errors"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/tables"
	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
	mocknom "github.com/opst/knitfab/pkg/domain/nomination/db/mock"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestPurge(t *testing.T) {
	ctx := context.Background()
	poolBroaker := testenv.NewPoolBroaker(ctx, t)
	pgpool := poolBroaker.GetPool(ctx, t)

	given := tables.Operation{
		Plan: []tables.Plan{
			{
				PlanId: testhelpers.Padding36("plan1-pseudo"),
				Active: true,
				Hash:   "#plan1-pseudo",
			},
			{
				PlanId: testhelpers.Padding36("plan2-training"),
				Active: true,
				Hash:   "#plan2-training",
			},
		},
		PlanPseudo: []tables.PlanPseudo{
			{
				PlanId: testhelpers.Padding36("plan1-pseudo"),
				Name:   "pseudo",
			},
		},
		PlanImage: []tables.PlanImage{
			{
				PlanId:  testhelpers.Padding36("plan2-training"),
				Image:   "repo.invalid/plan2-training",
				Version: "v1.0.0",
			},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{
				PlanId:   testhelpers.Padding36("plan1-pseudo"),
				OutputId: 1_010,
				Path:     "/out",
			}: {},
		},
		Inputs: map[tables.Input]tables.InputAttr{
			{
				PlanId:  testhelpers.Padding36("plan2-training"),
				InputId: 2_100,
				Path:    "/in",
			}: {},
		},
	}

	type When struct {
		fixture          tables.Operation
		knitIdToBePurged string
		nominatorError   error
	}

	type Then struct {
		wantErr   error
		garbage   []tables.Garbage
		volumeRef []tables.VolumeRef
	}

	theory := func(when When, then Then) func(*testing.T) {
		testenv.ClearTables(ctx, pgpool, t)
		return func(t *testing.T) {

			func() {
				tx := try.To(pgpool.Begin(ctx)).OrFatal(t)
				defer tx.Rollback(ctx)
				if err := given.ApplyWithConn(ctx, tx); err != nil {
					t.Fatal(err)
				}
				if err := when.fixture.ApplyWithConn(ctx, tx); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(ctx); err != nil {
					t.Fatal(err)
				}
			}()

			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			wpool := proxy.Wrap(pgpool)
			wpool.Events().Query.After(func() {

				{
					notLocked := try.To(
						scanner.New[string]().QueryAll(
							ctx, conn,
							`select "knit_id" from "data" where "knit_id" = $1 for update skip locked`,
							when.knitIdToBePurged,
						),
					).OrFatal(t)
					if len(notLocked) != 0 {
						t.Errorf("unexpectedly not locked: %v", notLocked)
					}
				}

				{
					notLocked := try.To(
						scanner.New[tables.VolumeRef]().QueryAll(
							ctx, conn,
							`select * from "volume_ref" where "knit_id" = $1 for update skip locked`,
							when.knitIdToBePurged,
						),
					).OrFatal(t)
					if len(notLocked) != 0 {
						t.Errorf("unexpectedly not locked: %v", notLocked)
					}
				}
			})

			mockNominator := mocknom.New(t)
			mockNominator.Impl.DropData = func(ctx context.Context, tx pool.Tx, knitIds []string) error {
				if want := []string{when.knitIdToBePurged}; !cmp.SliceContentEq(knitIds, want) {
					t.Errorf("unexpected knitIds: want %v, got %v", want, knitIds)
				}
				return when.nominatorError
			}
			testee := postgres.New(wpool, postgres.WithNominator(mockNominator))

			gotErr := testee.Purge(ctx, when.knitIdToBePurged)
			if then.wantErr != nil {
				if gotErr == nil {
					t.Errorf("unexpected error: want %v, got %v", then.wantErr, gotErr)
				}
				return
			}
			if gotErr != nil {
				t.Fatal(gotErr)
			}

			{
				gotGarbage := try.To(
					scanner.New[tables.Garbage]().QueryAll(ctx, conn, "table garbage"),
				).OrFatal(t)

				if !cmp.SliceContentEq(gotGarbage, then.garbage) {
					t.Errorf(
						"unexpected garbage:\n===actual===\n%+v\n===expected===\n%+v",
						gotGarbage, then.garbage,
					)
				}
			}

			{
				gotVolumeRef := try.To(
					scanner.New[tables.VolumeRef]().QueryAll(ctx, conn, "table volume_ref"),
				).OrFatal(t)

				if !cmp.SliceContentEq(gotVolumeRef, then.volumeRef) {
					t.Errorf(
						"unexpected volume_ref:\n===actual===\n%+v\n===expected===\n%+v",
						gotVolumeRef, then.volumeRef,
					)
				}
			}
		}
	}

	// Section 1: success cases

	t.Run("independent Data (upstream: Done) should be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run1-done"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run1-done/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run1-done"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run1-done/out",
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run1-done/out"),
		},
		Then{
			garbage: []tables.Garbage{
				{
					KnitId:    testhelpers.Padding36("run1-done/out"),
					VolumeRef: "#run1-done/out",
				},
			},
			volumeRef: []tables.VolumeRef{},
		},
	))

	t.Run("independent Data (upstream: Failed) should be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run1-failed"),
							Status: domain.Failed,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run1-failed/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run1-failed"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run1-failed/out",
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run1-failed/out"),
		},
		Then{
			garbage: []tables.Garbage{
				{
					KnitId:    testhelpers.Padding36("run1-failed/out"),
					VolumeRef: "#run1-failed/out",
				},
			},
			volumeRef: []tables.VolumeRef{},
		},
	))

	t.Run("independent Data (upstream: Invalidated) should be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run1-invalidated"),
							Status: domain.Invalidated,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run1-invalidated/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run1-invalidated"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run1-invalidated/out",
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run1-invalidated/out"),
		},
		Then{
			garbage: []tables.Garbage{
				{
					KnitId:    testhelpers.Padding36("run1-invalidated/out"),
					VolumeRef: "#run1-invalidated/out",
				},
			},
			volumeRef: []tables.VolumeRef{},
		},
	))

	t.Run("Data without VolumeRef should be ignored silently'", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run1-no-volume-ref"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run1-no-volume-ref/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run1-no-volume-ref"),
								OutputId: 1_010,
							}: {},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run1-no-volume-ref/out"),
		},
		Then{
			garbage:   []tables.Garbage{},
			volumeRef: []tables.VolumeRef{},
		},
	))

	t.Run("Data which has downstream (Done) can be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run1-done"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run1-done/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run1-done"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run1-done/out",
							},
						},
					},
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan2-training"),
							RunId:  testhelpers.Padding36("downstream"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								KnitId:  testhelpers.Padding36("run1-done/out"),
								RunId:   testhelpers.Padding36("downstream"),
								PlanId:  testhelpers.Padding36("plan2-training"),
								InputId: 2_100,
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run1-done/out"),
		},
		Then{
			garbage: []tables.Garbage{
				{
					KnitId:    testhelpers.Padding36("run1-done/out"),
					VolumeRef: "#run1-done/out",
				},
			},
			volumeRef: []tables.VolumeRef{},
		},
	))

	t.Run("Data which has downstream (Failed) can be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run1-failed"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run1-failed/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run1-failed"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run1-failed/out",
							},
						},
					},
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan2-training"),
							RunId:  testhelpers.Padding36("downstream"),
							Status: domain.Failed,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								KnitId:  testhelpers.Padding36("run1-failed/out"),
								RunId:   testhelpers.Padding36("downstream"),
								PlanId:  testhelpers.Padding36("plan2-training"),
								InputId: 2_100,
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run1-failed/out"),
		},
		Then{
			garbage: []tables.Garbage{
				{
					KnitId:    testhelpers.Padding36("run1-failed/out"),
					VolumeRef: "#run1-failed/out",
				},
			},
			volumeRef: []tables.VolumeRef{},
		},
	))

	t.Run("Data which has downstream (Invalidated) can be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run1-invalidated"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run1-invalidated/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run1-invalidated"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run1-invalidated/out",
							},
						},
					},
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan2-training"),
							RunId:  testhelpers.Padding36("downstream"),
							Status: domain.Invalidated,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								KnitId:  testhelpers.Padding36("run1-invalidated/out"),
								RunId:   testhelpers.Padding36("downstream"),
								PlanId:  testhelpers.Padding36("plan2-training"),
								InputId: 2_100,
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run1-invalidated/out"),
		},
		Then{
			garbage: []tables.Garbage{
				{
					KnitId:    testhelpers.Padding36("run1-invalidated/out"),
					VolumeRef: "#run1-invalidated/out",
				},
			},
			volumeRef: []tables.VolumeRef{},
		},
	))

	// Section 2: failure cases (Data in use)

	t.Run("Data with DataAgent should not be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run2-done"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run2-done/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run2-done"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run2-done/out",
								Agent: []tables.DataAgent{
									{
										Name:                  "agent1",
										Mode:                  domain.DataAgentRead.String(),
										KnitId:                testhelpers.Padding36("run2-done/out"),
										LifecycleSuspendUntil: time.Now().Add(10 * time.Hour),
									},
								},
							},
						},
					},
				},
			},
		},
		Then{
			wantErr: domain.ErrDataInUse,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("run2-done/out"),
					VolumeRef: "#run2-done/out",
				},
			},
		},
	))

	t.Run("Data with on-going Run (waiting) should not be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run2-waiting"),
							Status: domain.Waiting,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run2-waiting/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run2-waiting"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run2-waiting/out",
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run2-waiting/out"),
		},
		Then{
			wantErr: domain.ErrDataInUse,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("run2-waiting/out"),
					VolumeRef: "#run2-waiting/out",
				},
			},
		},
	))

	t.Run("Data with on-going Run (ready) should not be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run2-ready"),
							Status: domain.Ready,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run2-ready/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run2-ready"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run2-ready/out",
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run2-ready/out"),
		},
		Then{
			wantErr: domain.ErrDataInUse,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("run2-ready/out"),
					VolumeRef: "#run2-ready/out",
				},
			},
		},
	))

	t.Run("Data with on-going Run (starting) should not be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run2-starting"),
							Status: domain.Starting,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run2-starting/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run2-starting"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run2-starting/out",
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run2-starting/out"),
		},
		Then{
			wantErr: domain.ErrDataInUse,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("run2-starting/out"),
					VolumeRef: "#run2-starting/out",
				},
			},
		},
	))

	t.Run("Data with on-going Run (running) should not be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run2-running"),
							Status: domain.Running,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run2-running/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run2-running"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run2-running/out",
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run2-running/out"),
		},
		Then{
			wantErr: domain.ErrDataInUse,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("run2-running/out"),
					VolumeRef: "#run2-running/out",
				},
			},
		},
	))

	t.Run("Data with on-going Run (aborting) should not be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run2-aborting"),
							Status: domain.Aborting,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run2-aborting/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run2-aborting"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run2-aborting/out",
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run2-aborting/out"),
		},
		Then{
			wantErr: domain.ErrDataInUse,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("run2-aborting/out"),
					VolumeRef: "#run2-aborting/out",
				},
			},
		},
	))

	t.Run("Data with on-going Run (completing) should not be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run2-completing"),
							Status: domain.Completing,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run2-completing"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run2-completing"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run2-completing",
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run2-completing"),
		},
		Then{
			wantErr: domain.ErrDataInUse,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("run2-completing"),
					VolumeRef: "#run2-completing",
				},
			},
		},
	))

	t.Run("Data which has on-going downstream (Waiting) can not be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("upstream"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("upstream/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("upstream"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#upstream/out",
							},
						},
					},
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan2-training"),
							RunId:  testhelpers.Padding36("downstream"),
							Status: domain.Waiting,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								KnitId:  testhelpers.Padding36("upstream/out"),
								RunId:   testhelpers.Padding36("downstream"),
								PlanId:  testhelpers.Padding36("plan2-training"),
								InputId: 2_100,
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("upstream/out"),
		},
		Then{
			wantErr: domain.ErrDataInUse,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("upstream/out"),
					VolumeRef: "#upstream/out",
				},
			},
		},
	))

	t.Run("Data which has on-going downstream (Ready) can not be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("upstream"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("upstream/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("upstream"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#upstream/out",
							},
						},
					},
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan2-training"),
							RunId:  testhelpers.Padding36("downstream"),
							Status: domain.Ready,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								KnitId:  testhelpers.Padding36("upstream/out"),
								RunId:   testhelpers.Padding36("downstream"),
								PlanId:  testhelpers.Padding36("plan2-training"),
								InputId: 2_100,
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("upstream/out"),
		},
		Then{
			wantErr: domain.ErrDataInUse,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("upstream/out"),
					VolumeRef: "#upstream/out",
				},
			},
		},
	))

	t.Run("Data which has on-going downstream (Starting) can not be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("upstream"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("upstream/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("upstream"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#upstream/out",
							},
						},
					},
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan2-training"),
							RunId:  testhelpers.Padding36("downstream"),
							Status: domain.Starting,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								KnitId:  testhelpers.Padding36("upstream/out"),
								RunId:   testhelpers.Padding36("downstream"),
								PlanId:  testhelpers.Padding36("plan2-training"),
								InputId: 2_100,
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("upstream/out"),
		},
		Then{
			wantErr: domain.ErrDataInUse,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("upstream/out"),
					VolumeRef: "#upstream/out",
				},
			},
		},
	))

	t.Run("Data which has on-going downstream (Running) can not be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("upstream"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("upstream/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("upstream"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#upstream/out",
							},
						},
					},
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan2-training"),
							RunId:  testhelpers.Padding36("downstream"),
							Status: domain.Running,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								KnitId:  testhelpers.Padding36("upstream/out"),
								RunId:   testhelpers.Padding36("downstream"),
								PlanId:  testhelpers.Padding36("plan2-training"),
								InputId: 2_100,
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("upstream/out"),
		},
		Then{
			wantErr: domain.ErrDataInUse,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("upstream/out"),
					VolumeRef: "#upstream/out",
				},
			},
		},
	))

	t.Run("Data which has on-going downstream (Aborting) can not be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("upstream"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("upstream/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("upstream"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#upstream/out",
							},
						},
					},
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan2-training"),
							RunId:  testhelpers.Padding36("downstream"),
							Status: domain.Aborting,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								KnitId:  testhelpers.Padding36("upstream/out"),
								RunId:   testhelpers.Padding36("downstream"),
								PlanId:  testhelpers.Padding36("plan2-training"),
								InputId: 2_100,
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("upstream/out"),
		},
		Then{
			wantErr: domain.ErrDataInUse,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("upstream/out"),
					VolumeRef: "#upstream/out",
				},
			},
		},
	))

	t.Run("Data which has on-going downstream (Completing) can not be purged", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("upstream"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("upstream/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("upstream"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#upstream/out",
							},
						},
					},
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan2-training"),
							RunId:  testhelpers.Padding36("downstream"),
							Status: domain.Completing,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Assign: []tables.Assign{
							{
								KnitId:  testhelpers.Padding36("upstream/out"),
								RunId:   testhelpers.Padding36("downstream"),
								PlanId:  testhelpers.Padding36("plan2-training"),
								InputId: 2_100,
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("upstream/out"),
		},
		Then{
			wantErr: domain.ErrDataInUse,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("upstream/out"),
					VolumeRef: "#upstream/out",
				},
			},
		},
	))

	// Section 3: failure cases (Other Errors)

	t.Run("Data which does not exist should return ErrMissing", theory(
		When{
			fixture:          tables.Operation{},
			knitIdToBePurged: testhelpers.Padding36("non-existent"),
		},
		Then{
			wantErr:   domerr.ErrMissing,
			garbage:   []tables.Garbage{},
			volumeRef: []tables.VolumeRef{},
		},
	))

	wantErr := errors.New("unknown error")
	t.Run("nominator.DropData returns error, Purge returns that error", theory(
		When{
			fixture: tables.Operation{
				Steps: []tables.Step{
					{
						Run: tables.Run{
							PlanId: testhelpers.Padding36("plan1-pseudo"),
							RunId:  testhelpers.Padding36("run3-done"),
							Status: domain.Done,
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14.567+09:00",
							)).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId:   testhelpers.Padding36("run3-done/out"),
								PlanId:   testhelpers.Padding36("plan1-pseudo"),
								RunId:    testhelpers.Padding36("run3-done"),
								OutputId: 1_010,
							}: {
								VolumeRef: "#run3-done/out",
							},
						},
					},
				},
			},
			knitIdToBePurged: testhelpers.Padding36("run3-done/out"),
			nominatorError:   wantErr,
		},
		Then{
			wantErr: wantErr,
			garbage: []tables.Garbage{},
			volumeRef: []tables.VolumeRef{
				{
					KnitId:    testhelpers.Padding36("run3-done/out"),
					VolumeRef: "#run3-done/out",
				},
			},
		},
	))
}
