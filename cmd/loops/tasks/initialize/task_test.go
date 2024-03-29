package initialize_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/opst/knitfab/cmd/loops/tasks/initialize"
	kdb "github.com/opst/knitfab/pkg/db"
	kdbmock "github.com/opst/knitfab/pkg/db/mocks"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/rfctime"
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

func TestTask(t *testing.T) {
	for _, pvcInitBehaviour := range []struct {
		err error
	}{
		{err: nil}, {err: errors.New("fake error (pvcinit)")},
	} {
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
		for _, pickAndSetStatusBehaviour := range []struct {
			cursor kdb.RunCursor
			err    error
		}{
			{
				cursor: kdb.RunCursor{
					Head:   pickedRun.Id,
					Status: seed.Status,
				},
			},
			{
				cursor: kdb.RunCursor{
					Head:   pickedRun.Id,
					Status: seed.Status,
				},
				err: errors.New("fake error (pickandsetstatus)"),
			},
			{
				cursor: seed,
			},
			{
				cursor: seed,
				err:    errors.New("fake error (pickandsetstatus)"),
			},
		} {
			t.Run(fmt.Sprintf(
				"[error from PVCInitializer is %v] x [PickAndSetStatus cursor=%+v, error=%v]",
				pvcInitBehaviour.err, pickAndSetStatusBehaviour.cursor, pickAndSetStatusBehaviour.err,
			), func(t *testing.T) {

				newStatus := kdb.Invalidated
				var errInPVCInit error
				run := kdbmock.NewRunInterface()
				run.Impl.PickAndSetStatus = func(
					ctx context.Context, value kdb.RunCursor,
					f func(kdb.Run) (kdb.KnitRunStatus, error),
				) (kdb.RunCursor, error) {
					newStatus, errInPVCInit = f(pickedRun)

					return pickAndSetStatusBehaviour.cursor, pickAndSetStatusBehaviour.err
				}

				pvcInitializerHasBeenCalledWith := kdb.Run{}
				pvcInitializer := func(ctx context.Context, r kdb.Run) error {
					pvcInitializerHasBeenCalledWith = r
					return pvcInitBehaviour.err
				}

				testee := initialize.Task(run, pvcInitializer)
				value, ok, err := testee(ctx, seed)

				t.Run("interaction with PVCInitializer", func(t *testing.T) {
					if !pvcInitializerHasBeenCalledWith.Equal(&pickedRun) {
						t.Errorf(
							"unexpected run is passed to PVC Initializer:\n===actual==\n%+v\n===expected===\n%+v",
							pvcInitializerHasBeenCalledWith, pickedRun,
						)
					}

					if !errors.Is(errInPVCInit, pvcInitBehaviour.err) {
						t.Errorf(
							"unexpected error: %+v (expected: %+v)",
							errInPVCInit, pvcInitBehaviour.err,
						)
					}
				})

				t.Run("interaction with PickAndSetStatus", func(t *testing.T) {
					if !errors.Is(err, pickAndSetStatusBehaviour.err) {
						t.Errorf("unexpected error: %+v", err)
					}

					picked := !seed.Equal(pickAndSetStatusBehaviour.cursor)

					if ok != picked {
						t.Errorf("unexpected ok: %v", ok)
					}

					{
						expected := kdb.Ready
						if pvcInitBehaviour.err != nil {
							expected = pickedRun.Status
						}
						if newStatus != expected {
							t.Errorf("unexpected new status: %s (expected: %s)", newStatus, expected)
						}
					}

					if picked {
						if !value.Equal(pickAndSetStatusBehaviour.cursor) {
							t.Errorf(
								"unexpected value:\n===actual==\n%+v\n===expected===\n%+v",
								value,
								pickAndSetStatusBehaviour,
							)
						}
					}
				})
			})
		}
	}

	t.Run("it ignores errors from context", func(t *testing.T) {
		for name, testcase := range map[string]struct{ err error }{
			"cancel":            {err: context.Canceled},
			"deadline exceeded": {err: context.DeadlineExceeded},
		} {
			t.Run(name, func(t *testing.T) {
				run := kdbmock.NewRunInterface()

				expectedCursor := kdb.RunCursor{
					Head:   "run-id",
					Status: []kdb.KnitRunStatus{kdb.Waiting},
				}
				run.Impl.PickAndSetStatus = func(
					ctx context.Context, value kdb.RunCursor,
					f func(kdb.Run) (kdb.KnitRunStatus, error),
				) (kdb.RunCursor, error) {
					return expectedCursor, testcase.err
				}

				testee := initialize.Task(
					run,
					func(ctx context.Context, r kdb.Run) error { return nil },
				)
				seed := kdb.RunCursor{
					Head:   "previous-run",
					Status: []kdb.KnitRunStatus{kdb.Waiting},
				}
				value, ok, err := testee(context.Background(), seed)

				if err != nil {
					t.Errorf("unexpected error: %+v", err)
				}
				if !ok {
					t.Errorf("unexpected ok: %v", ok)
				}
				{
					if !value.Equal(expectedCursor) {
						t.Errorf(
							"unexpected value:\n===actual==\n%+v\n===expected===\n%+v",
							value, expectedCursor,
						)
					}
				}
			})
		}
	})

}
