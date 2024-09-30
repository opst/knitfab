package initialize_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/opst/knitfab-api-types/misc/rfctime"
	apiruns "github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab/cmd/loops/hook"
	"github.com/opst/knitfab/cmd/loops/tasks/initialize"
	bindruns "github.com/opst/knitfab/pkg/api-types-binding/runs"
	kdb "github.com/opst/knitfab/pkg/db"
	kdbmock "github.com/opst/knitfab/pkg/db/mocks"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/opst/knitfab/pkg/workloads/data"
	k8smock "github.com/opst/knitfab/pkg/workloads/k8s/mock"
	"github.com/opst/knitfab/pkg/workloads/k8s/testenv"
	kubecore "k8s.io/api/core/v1"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPVCinitializer(t *testing.T) {
	t.Run("it creates PVCs for run's output", func(t *testing.T) {
		ctx, cancel := context.Background(), func() {}
		if deadline, ok := t.Deadline(); ok {
			ctx, cancel = context.WithDeadline(ctx, deadline.Add(-time.Second))
		}
		defer cancel()

		cluster, clientset := testenv.NewCluster(t)
		template := data.VolumeTemplate{
			Namespece:    testenv.Namespace(),
			StorageClass: testenv.STORAGE_CLASS_NAME,
			Capacity:     resource.MustParse("1Ki"),
		}

		testee := initialize.PVCInitializer(cluster, template)

		run := kdb.Run{
			// RunBody: do not care
			Inputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    "initialize-input-1",
						VolumeRef: "ref-initialize-input-1",
					},
				},
				{
					MountPoint: kdb.MountPoint{},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    "initialize-input-2",
						VolumeRef: "ref-initialize-input-2",
					},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    "initialize-output-1",
						VolumeRef: "ref-initialize-output-1",
					},
				},
				{
					MountPoint: kdb.MountPoint{},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    "initialize-output-2",
						VolumeRef: "ref-initialize-output-2",
					},
				},
			},
			Log: &kdb.Log{
				KnitDataBody: kdb.KnitDataBody{
					KnitId:    "initialize-log",
					VolumeRef: "ref-initialize-log",
				},
			},
		}
		defer func() {
			volumeref := utils.Map(
				run.Inputs,
				func(a kdb.Assignment) string { return a.KnitDataBody.VolumeRef },
			)
			volumeref = append(
				volumeref,
				utils.Map(
					run.Outputs,
					func(a kdb.Assignment) string { return a.KnitDataBody.VolumeRef },
				)...,
			)
			volumeref = append(volumeref, run.Log.KnitDataBody.VolumeRef)
			for _, v := range volumeref {
				clientset.CoreV1().
					PersistentVolumeClaims(testenv.Namespace()).
					Delete(ctx, v, *v1.NewDeleteOptions(0))
			}
		}()

		if err := testee(ctx, run); err != nil {
			t.Fatal(err)
		}

		for _, in := range run.Inputs {
			_, err := clientset.CoreV1().
				PersistentVolumeClaims(testenv.Namespace()).
				Get(ctx, in.KnitDataBody.VolumeRef, v1.GetOptions{})
			if !kubeerr.IsNotFound(err) {
				t.Errorf("unexpected error (input: %s): %s", in.KnitDataBody.VolumeRef, err)
			}
		}

		for _, out := range run.Outputs {
			_, err := clientset.CoreV1().
				PersistentVolumeClaims(testenv.Namespace()).
				Get(ctx, out.KnitDataBody.VolumeRef, v1.GetOptions{})
			if err != nil {
				t.Errorf("unexpected error (output: %s): %s", out.KnitDataBody.VolumeRef, err)
			}
		}

		{
			_, err := clientset.CoreV1().
				PersistentVolumeClaims(testenv.Namespace()).
				Get(ctx, run.Log.KnitDataBody.VolumeRef, v1.GetOptions{})
			if err != nil {
				t.Errorf("unexpected error (log: %s): %s", run.Log.KnitDataBody.VolumeRef, err)
			}
		}
	})

	t.Run("it successes when pvcs exist already", func(t *testing.T) {
		ctx, cancel := context.Background(), func() {}
		if deadline, ok := t.Deadline(); ok {
			ctx, cancel = context.WithDeadline(ctx, deadline.Add(-time.Second))
		}
		defer cancel()

		cluster, clientset := testenv.NewCluster(t)
		template := data.VolumeTemplate{
			Namespece:    testenv.Namespace(),
			StorageClass: testenv.STORAGE_CLASS_NAME,
			Capacity:     resource.MustParse("1Ki"),
		}

		testee := initialize.PVCInitializer(cluster, template)

		run := kdb.Run{
			// RunBody: do not care
			Inputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    "initialize-input-1",
						VolumeRef: "ref-initialize-input-1",
					},
				},
				{
					MountPoint: kdb.MountPoint{},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    "initialize-input-2",
						VolumeRef: "ref-initialize-input-2",
					},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    "initialize-output-1",
						VolumeRef: "ref-initialize-output-1",
					},
				},
				{
					MountPoint: kdb.MountPoint{},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    "initialize-output-2",
						VolumeRef: "ref-initialize-output-2",
					},
				},
			},
			Log: &kdb.Log{
				KnitDataBody: kdb.KnitDataBody{
					KnitId:    "initialize-log",
					VolumeRef: "ref-initialize-log",
				},
			},
		}
		defer func() {
			volumeRefs := utils.Map(
				run.Inputs,
				func(a kdb.Assignment) string { return a.KnitDataBody.VolumeRef },
			)
			volumeRefs = append(
				volumeRefs,
				utils.Map(
					run.Outputs,
					func(a kdb.Assignment) string { return a.KnitDataBody.VolumeRef },
				)...,
			)
			volumeRefs = append(volumeRefs, run.Log.KnitDataBody.VolumeRef)
			for _, v := range volumeRefs {
				clientset.CoreV1().
					PersistentVolumeClaims(testenv.Namespace()).
					Delete(ctx, v, *v1.NewDeleteOptions(0))
			}
		}()

		for _, out := range run.Outputs {
			pvc := try.To(data.Of(out.KnitDataBody)).OrFatal(t).Build(template)
			_, err := clientset.CoreV1().
				PersistentVolumeClaims(testenv.Namespace()).
				Create(ctx, pvc, v1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
		}

		if err := testee(ctx, run); err != nil {
			t.Fatal(err)
		}

		for _, in := range run.Inputs {
			_, err := clientset.CoreV1().
				PersistentVolumeClaims(testenv.Namespace()).
				Get(ctx, in.KnitDataBody.VolumeRef, v1.GetOptions{})
			if !kubeerr.IsNotFound(err) {
				t.Errorf("unexpected error (input: %s): %s", in.KnitDataBody.VolumeRef, err)
			}
		}

		for _, out := range run.Outputs {
			_, err := clientset.CoreV1().
				PersistentVolumeClaims(testenv.Namespace()).
				Get(ctx, out.KnitDataBody.VolumeRef, v1.GetOptions{})
			if err != nil {
				t.Errorf("unexpected error (output: %s): %s", out.KnitDataBody.VolumeRef, err)
			}
		}

		_, err := clientset.CoreV1().
			PersistentVolumeClaims(testenv.Namespace()).
			Get(ctx, run.Log.KnitDataBody.VolumeRef, v1.GetOptions{})
		if err != nil {
			t.Errorf("unexpected error (log: %s): %s", run.Log.KnitDataBody.VolumeRef, err)
		}
	})

	t.Run("it escarate error from creating PVC", func(t *testing.T) {
		ctx, cancel := context.Background(), func() {}
		if deadline, ok := t.Deadline(); ok {
			ctx, cancel = context.WithDeadline(ctx, deadline.Add(-time.Second))
		}
		defer cancel()

		cluster, client := k8smock.NewCluster()
		expectedError := errors.New("fake error")
		client.Impl.CreatePVC = func(ctx context.Context, namespace string, pvc *kubecore.PersistentVolumeClaim) (*kubecore.PersistentVolumeClaim, error) {
			return nil, expectedError
		}
		template := data.VolumeTemplate{
			Namespece:    testenv.Namespace(),
			StorageClass: testenv.STORAGE_CLASS_NAME,
			Capacity:     resource.MustParse("1Ki"),
		}

		testee := initialize.PVCInitializer(cluster, template)

		run := kdb.Run{
			// RunBody: do not care
			Inputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    "initialize-input-1",
						VolumeRef: "ref-initialize-input-1",
					},
				},
				{
					MountPoint: kdb.MountPoint{},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    "initialize-input-2",
						VolumeRef: "ref-initialize-input-2",
					},
				},
			},
			Outputs: []kdb.Assignment{
				{
					MountPoint: kdb.MountPoint{},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    "initialize-output-1",
						VolumeRef: "ref-initialize-output-1",
					},
				},
				{
					MountPoint: kdb.MountPoint{},
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    "initialize-output-2",
						VolumeRef: "ref-initialize-output-2",
					},
				},
			},
			Log: &kdb.Log{
				KnitDataBody: kdb.KnitDataBody{
					KnitId:    "initialize-log",
					VolumeRef: "ref-initialize-log",
				},
			},
		}

		if err := testee(ctx, run); !errors.Is(err, expectedError) {
			t.Errorf("unexpected error: %s", err)
		}
	})
}

func TestTask_Outside_of_PickAndSetStatus(t *testing.T) {

	type When struct {
		Cursor            kdb.RunCursor
		NextCursor        kdb.RunCursor
		StatusChanged     bool
		Err               error
		IRunGetReturnsNil bool
		UpdatedRun        kdb.Run
	}

	type Then struct {
		Cursor   kdb.RunCursor
		Continue bool
		Err      error
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			run := kdbmock.NewRunInterface()
			run.Impl.PickAndSetStatus = func(
				ctx context.Context, value kdb.RunCursor,
				f func(kdb.Run) (kdb.KnitRunStatus, error),
			) (kdb.RunCursor, bool, error) {
				return when.NextCursor, when.StatusChanged, when.Err
			}

			run.Impl.Get = func(ctx context.Context, ids []string) (map[string]kdb.Run, error) {
				if when.IRunGetReturnsNil {
					return nil, errors.New("irun.Get: should be ignored")
				}
				return map[string]kdb.Run{when.NextCursor.Head: when.UpdatedRun}, nil
			}

			hookAfterHasBeenCalled := false
			testee := initialize.Task(run, nil, hook.Func[apiruns.Detail]{
				AfterFn: func(d apiruns.Detail) error {
					hookAfterHasBeenCalled = true
					want := bindruns.ComposeDetail(when.UpdatedRun)
					if !d.Equal(want) {
						t.Errorf(
							"unexpected detail:\n===actual==\n%+v\n===expected===\n%+v",
							d, want,
						)
					}
					return errors.New("hook after: should be ignored")
				},
			})

			value, ok, err := testee(ctx, when.Cursor)

			if !errors.Is(err, then.Err) {
				t.Errorf("unexpected error: %+v", err)
			}
			if ok != then.Continue {
				t.Errorf("unexpected Continue: %v", ok)
			}
			if !value.Equal(then.Cursor) {
				t.Errorf(
					"unexpected value:\n===actual==\n%+v\n===expected===\n%+v",
					value, then.Cursor,
				)
			}
			if when.StatusChanged != hookAfterHasBeenCalled {
				t.Errorf("unexpected hook.After has been called: %v", hookAfterHasBeenCalled)
			}
		}
	}

	t.Run("it continues when PickAndSetStatus returns a new cursor", theory(
		When{
			Cursor: kdb.RunCursor{
				Head:   "previous-run",
				Status: []kdb.KnitRunStatus{kdb.Waiting},
			},

			NextCursor: kdb.RunCursor{
				Head:   "next-run",
				Status: []kdb.KnitRunStatus{kdb.Waiting},
			},
			StatusChanged: true,
			Err:           nil,

			UpdatedRun: kdb.Run{
				RunBody: kdb.RunBody{
					Id:         "next-run",
					Status:     kdb.Ready,
					WorkerName: "worker-name",
					UpdatedAt: try.To(
						rfctime.ParseRFC3339DateTime("2021-10-11T12:13:14+09:00"),
					).OrFatal(t).Time(),
					PlanBody: kdb.PlanBody{
						PlanId: "plan-id",
						Image: &kdb.ImageIdentifier{
							Image:   "example.repo.invalid/image",
							Version: "v1.0.0",
						},
					},
				},
				Inputs: []kdb.Assignment{
					{
						MountPoint: kdb.MountPoint{
							Id:   100_100,
							Path: "/in/1",
							Tags: kdb.NewTagSet([]kdb.Tag{{Key: "type", Value: "csv"}}),
						},
						KnitDataBody: kdb.KnitDataBody{
							KnitId:    "next-run-input-1",
							VolumeRef: "ref-next-run-input-1",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "type", Value: "csv"},
								{Key: "input", Value: "1"},
							}),
						},
					},
				},
				Outputs: []kdb.Assignment{
					{
						MountPoint: kdb.MountPoint{
							Id:   100_010,
							Path: "/out/1",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "type", Value: "model"},
								{Key: "output", Value: "1"},
							}),
						},
					},
				},
				Log: &kdb.Log{
					Id: 100_001,
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "type", Value: "jsonl"},
					}),
					KnitDataBody: kdb.KnitDataBody{
						KnitId:    "next-run-log",
						VolumeRef: "ref-next-run-log",
						Tags: kdb.NewTagSet([]kdb.Tag{
							{Key: "type", Value: "jsonl"},
							{Key: "log", Value: "1"},
						}),
					},
				},
			},
		},
		Then{
			Cursor: kdb.RunCursor{
				Head: "next-run", Status: []kdb.KnitRunStatus{kdb.Waiting},
			},
			Continue: true,
			Err:      nil,
		},
	))

	t.Run("it stops when PickAndSetStatus does not move cursor", theory(
		When{
			Cursor: kdb.RunCursor{
				Head:   "previous-run",
				Status: []kdb.KnitRunStatus{kdb.Waiting},
			},

			NextCursor: kdb.RunCursor{
				Head:   "previous-run",
				Status: []kdb.KnitRunStatus{kdb.Waiting},
			},
			StatusChanged: false,
			Err:           nil,
		},
		Then{
			Cursor: kdb.RunCursor{
				Head: "previous-run", Status: []kdb.KnitRunStatus{kdb.Waiting},
			},
			Continue: false,
			Err:      nil,
		},
	))

	t.Run("it ignores context.Canceled", theory(
		When{
			Cursor: kdb.RunCursor{
				Head:   "previous-run",
				Status: []kdb.KnitRunStatus{kdb.Waiting},
			},

			NextCursor: kdb.RunCursor{
				Head:   "next-run",
				Status: []kdb.KnitRunStatus{kdb.Waiting},
			},
			StatusChanged: false,
			Err:           context.Canceled,
		},
		Then{
			Cursor: kdb.RunCursor{
				Head: "next-run", Status: []kdb.KnitRunStatus{kdb.Waiting},
			},
			Continue: true,
		},
	))

	t.Run("it ignores context.DeadlineExceeded", theory(
		When{
			Cursor: kdb.RunCursor{
				Head:   "previous-run",
				Status: []kdb.KnitRunStatus{kdb.Waiting},
			},

			NextCursor: kdb.RunCursor{
				Head:   "next-run",
				Status: []kdb.KnitRunStatus{kdb.Waiting},
			},
			StatusChanged: false,
			Err:           context.DeadlineExceeded,
		},
		Then{
			Cursor: kdb.RunCursor{
				Head: "next-run", Status: []kdb.KnitRunStatus{kdb.Waiting},
			},
			Continue: true,
			Err:      nil,
		},
	))
}

func TestTask_Inside_of_PickAndSetStatus(t *testing.T) {
	ctx := context.Background()

	pickedRun := kdb.Run{
		RunBody: kdb.RunBody{
			Id:         "picked-run",
			Status:     kdb.Waiting,
			WorkerName: "worker-name",
			UpdatedAt: try.To(
				rfctime.ParseRFC3339DateTime("2021-10-11T12:13:14+09:00"),
			).OrFatal(t).Time(),
			PlanBody: kdb.PlanBody{
				PlanId: "plan-id",
				Image: &kdb.ImageIdentifier{
					Image:   "example.repo.invalid/image",
					Version: "v1.0.0",
				},
			},
		},
		Inputs: []kdb.Assignment{
			{
				MountPoint: kdb.MountPoint{
					Id:   100_100,
					Path: "/in/1",
					Tags: kdb.NewTagSet([]kdb.Tag{{Key: "type", Value: "csv"}}),
				},
				KnitDataBody: kdb.KnitDataBody{
					KnitId:    "picked-run-input-1",
					VolumeRef: "ref-picked-run-input-1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "type", Value: "csv"},
						{Key: "input", Value: "1"},
					}),
				},
			},
		},
		Outputs: []kdb.Assignment{
			{
				MountPoint: kdb.MountPoint{
					Id:   100_010,
					Path: "/out/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "type", Value: "model"},
						{Key: "output", Value: "1"},
					}),
				},
			},
		},
		Log: &kdb.Log{
			Id: 100_001,
			Tags: kdb.NewTagSet([]kdb.Tag{
				{Key: "type", Value: "jsonl"},
				{Key: "log", Value: "1"},
			}),
		},
	}
	seed := kdb.RunCursor{
		Head:   "previous-run",
		Status: []kdb.KnitRunStatus{kdb.Waiting},
	}

	type When struct {
		BeforeErr error
		InitErr   error
	}

	type Then struct {
		NewStatus kdb.KnitRunStatus
		Err       error
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			run := kdbmock.NewRunInterface()
			run.Impl.PickAndSetStatus = func(
				ctx context.Context, value kdb.RunCursor,
				f func(kdb.Run) (kdb.KnitRunStatus, error),
			) (kdb.RunCursor, bool, error) {
				gotStatus, err := f(pickedRun)

				if when.BeforeErr != nil {
					if !errors.Is(err, when.BeforeErr) {
						t.Errorf("unexpected error: %+v", err)
					}
				} else {
					if !errors.Is(err, when.InitErr) {
						t.Errorf("unexpected error: %+v", err)
					}
				}

				if gotStatus != then.NewStatus {
					t.Errorf("unexpected new status: %s (expected: %s)", gotStatus, then.NewStatus)
				}

				return seed, true, err
			}
			run.Impl.Get = func(ctx context.Context, ids []string) (map[string]kdb.Run, error) {
				return map[string]kdb.Run{pickedRun.Id: pickedRun}, nil
			}

			initHasBeenCalled := false
			pvcInitializer := func(ctx context.Context, r kdb.Run) error {
				initHasBeenCalled = true
				if !r.Equal(&pickedRun) {
					t.Errorf(
						"unexpected run is passed to PVC Initializer:\n===actual==\n%+v\n===expected===\n%+v",
						r, pickedRun,
					)
				}
				return when.InitErr
			}

			beforeFnHasBeenCalled := false
			testee := initialize.Task(run, pvcInitializer, hook.Func[apiruns.Detail]{
				BeforeFn: func(d apiruns.Detail) error {
					beforeFnHasBeenCalled = true
					if want := bindruns.ComposeDetail(pickedRun); !d.Equal(want) {
						t.Errorf(
							"unexpected detail:\n===actual==\n%+v\n===expected===\n%+v",
							d, want,
						)
					}

					return when.BeforeErr
				},
			})

			testee(ctx, seed)

			if !beforeFnHasBeenCalled {
				t.Error("BeforeFn has not been called")
			}

			if when.BeforeErr == nil {
				if !initHasBeenCalled {
					t.Error("PVCInitializer has not been called")
				}
			}
		}
	}

	beforeErr := errors.New("fake error (before)")
	initErr := errors.New("fake error (init)")

	t.Run("it continues when BeforeFn and PVCInitializer successes", theory(
		When{
			BeforeErr: nil,
			InitErr:   nil,
		},
		Then{
			NewStatus: kdb.Ready,
			Err:       nil,
		},
	))

	t.Run("it stops when BeforeFn returns an error", theory(
		When{
			BeforeErr: beforeErr,
			InitErr:   nil,
		},
		Then{
			NewStatus: pickedRun.Status,
			Err:       beforeErr,
		},
	))

	t.Run("it stops when PVCInitializer returns an error", theory(
		When{
			BeforeErr: nil,
			InitErr:   initErr,
		},
		Then{
			NewStatus: pickedRun.Status,
			Err:       initErr,
		},
	))
}
