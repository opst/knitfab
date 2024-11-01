package run_test

import (
	"context"
	"errors"
	"testing"
	"time"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	"github.com/opst/knitfab/pkg/conn/k8s/testenv"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/data/k8s/data"
	clustermock "github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster/mock"
	k8srun "github.com/opst/knitfab/pkg/domain/run/k8s/run"
	"github.com/opst/knitfab/pkg/utils/slices"
	"github.com/opst/knitfab/pkg/utils/try"
	kubecore "k8s.io/api/core/v1"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRun_Initlialize(t *testing.T) {
	t.Run("it creates PVCs for run's output", func(t *testing.T) {
		ctx, cancel := context.Background(), func() {}
		if deadline, ok := t.Deadline(); ok {
			ctx, cancel = context.WithDeadline(ctx, deadline.Add(-time.Second))
		}
		defer cancel()

		cluster, clientset := testenv.NewCluster(t)
		conf := bconf.TrySeal(
			&bconf.KnitClusterConfigMarshall{
				Namespace: testenv.Namespace(),
				Domain:    "cluster.local",
				Database:  "postgres://do-not-care",
				DataAgent: &bconf.DataAgentConfigMarshall{
					Image: "repo.invalid/dataagt:latest",
					Volume: &bconf.VolumeConfigMarshall{
						StorageClassName: testenv.STORAGE_CLASS_NAME,
						InitialCapacity:  "1Ki",
					},
					Port: 8080,
				},
				Worker: &bconf.WorkerConfigMarshall{
					Priority: "worker-priority",
					Init: &bconf.InitContainerConfigMarshall{
						Image: testenv.Images().Empty,
					},
					Nurse: &bconf.NurseContainerConfigMarshall{
						Image:                testenv.Images().Nurse,
						ServiceAccountSecret: "fake-serviceAccount",
					},
				},
				Keychains: &bconf.KeychainsConfigMarshall{
					SignKeyForImportToken: &bconf.HS256KeyChainMarshall{
						Name: "signe-for-import-token",
					},
				},
			},
		)
		testee := k8srun.New(cluster, conf)

		run := domain.Run{
			// RunBody: do not care
			Inputs: []domain.Assignment{
				{
					MountPoint: domain.MountPoint{},
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "initialize-input-1",
						VolumeRef: "ref-initialize-input-1",
					},
				},
				{
					MountPoint: domain.MountPoint{},
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "initialize-input-2",
						VolumeRef: "ref-initialize-input-2",
					},
				},
			},
			Outputs: []domain.Assignment{
				{
					MountPoint: domain.MountPoint{},
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "initialize-output-1",
						VolumeRef: "ref-initialize-output-1",
					},
				},
				{
					MountPoint: domain.MountPoint{},
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "initialize-output-2",
						VolumeRef: "ref-initialize-output-2",
					},
				},
			},
			Log: &domain.Log{
				KnitDataBody: domain.KnitDataBody{
					KnitId:    "initialize-log",
					VolumeRef: "ref-initialize-log",
				},
			},
		}
		defer func() {
			volumeref := slices.Map(
				run.Inputs,
				func(a domain.Assignment) string { return a.KnitDataBody.VolumeRef },
			)
			volumeref = append(
				volumeref,
				slices.Map(
					run.Outputs,
					func(a domain.Assignment) string { return a.KnitDataBody.VolumeRef },
				)...,
			)
			volumeref = append(volumeref, run.Log.KnitDataBody.VolumeRef)
			for _, v := range volumeref {
				clientset.CoreV1().
					PersistentVolumeClaims(testenv.Namespace()).
					Delete(ctx, v, *v1.NewDeleteOptions(0))
			}
		}()

		if err := testee.Initialize(ctx, run); err != nil {
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

		conf := bconf.TrySeal(
			&bconf.KnitClusterConfigMarshall{
				Namespace: testenv.Namespace(),
				Domain:    "cluster.local",
				Database:  "postgres://do-not-care",
				DataAgent: &bconf.DataAgentConfigMarshall{
					Image: "repo.invalid/dataagt:latest",
					Volume: &bconf.VolumeConfigMarshall{
						StorageClassName: testenv.STORAGE_CLASS_NAME,
						InitialCapacity:  "1Ki",
					},
					Port: 8080,
				},
				Worker: &bconf.WorkerConfigMarshall{
					Priority: "worker-priority",
					Init: &bconf.InitContainerConfigMarshall{
						Image: testenv.Images().Empty,
					},
					Nurse: &bconf.NurseContainerConfigMarshall{
						Image:                testenv.Images().Nurse,
						ServiceAccountSecret: "fake-serviceAccount",
					},
				},
				Keychains: &bconf.KeychainsConfigMarshall{
					SignKeyForImportToken: &bconf.HS256KeyChainMarshall{
						Name: "signe-for-import-token",
					},
				},
			},
		)
		testee := k8srun.New(cluster, conf)

		run := domain.Run{
			// RunBody: do not care
			Inputs: []domain.Assignment{
				{
					MountPoint: domain.MountPoint{},
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "initialize-input-1",
						VolumeRef: "ref-initialize-input-1",
					},
				},
				{
					MountPoint: domain.MountPoint{},
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "initialize-input-2",
						VolumeRef: "ref-initialize-input-2",
					},
				},
			},
			Outputs: []domain.Assignment{
				{
					MountPoint: domain.MountPoint{},
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "initialize-output-1",
						VolumeRef: "ref-initialize-output-1",
					},
				},
				{
					MountPoint: domain.MountPoint{},
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "initialize-output-2",
						VolumeRef: "ref-initialize-output-2",
					},
				},
			},
			Log: &domain.Log{
				KnitDataBody: domain.KnitDataBody{
					KnitId:    "initialize-log",
					VolumeRef: "ref-initialize-log",
				},
			},
		}
		defer func() {
			volumeRefs := slices.Map(
				run.Inputs,
				func(a domain.Assignment) string { return a.KnitDataBody.VolumeRef },
			)
			volumeRefs = append(
				volumeRefs,
				slices.Map(
					run.Outputs,
					func(a domain.Assignment) string { return a.KnitDataBody.VolumeRef },
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
			pvc := try.To(data.Of(out.KnitDataBody)).OrFatal(t).Build(conf)
			_, err := clientset.CoreV1().
				PersistentVolumeClaims(testenv.Namespace()).
				Create(ctx, pvc, v1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
		}

		if err := testee.Initialize(ctx, run); err != nil {
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

		cluster, client := clustermock.NewCluster()
		expectedError := errors.New("fake error")
		client.Impl.CreatePVC = func(ctx context.Context, namespace string, pvc *kubecore.PersistentVolumeClaim) (*kubecore.PersistentVolumeClaim, error) {
			return nil, expectedError
		}

		conf := bconf.TrySeal(
			&bconf.KnitClusterConfigMarshall{
				Namespace: testenv.Namespace(),
				Domain:    "cluster.local",
				Database:  "postgres://do-not-care",
				DataAgent: &bconf.DataAgentConfigMarshall{
					Image: "repo.invalid/dataagt:latest",
					Volume: &bconf.VolumeConfigMarshall{
						StorageClassName: testenv.STORAGE_CLASS_NAME,
						InitialCapacity:  "1Ki",
					},
					Port: 8080,
				},
				Worker: &bconf.WorkerConfigMarshall{
					Priority: "worker-priority",
					Init: &bconf.InitContainerConfigMarshall{
						Image: testenv.Images().Empty,
					},
					Nurse: &bconf.NurseContainerConfigMarshall{
						Image:                testenv.Images().Nurse,
						ServiceAccountSecret: "fake-serviceAccount",
					},
				},
				Keychains: &bconf.KeychainsConfigMarshall{
					SignKeyForImportToken: &bconf.HS256KeyChainMarshall{
						Name: "signe-for-import-token",
					},
				},
			},
		)
		testee := k8srun.New(cluster, conf)

		run := domain.Run{
			// RunBody: do not care
			Inputs: []domain.Assignment{
				{
					MountPoint: domain.MountPoint{},
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "initialize-input-1",
						VolumeRef: "ref-initialize-input-1",
					},
				},
				{
					MountPoint: domain.MountPoint{},
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "initialize-input-2",
						VolumeRef: "ref-initialize-input-2",
					},
				},
			},
			Outputs: []domain.Assignment{
				{
					MountPoint: domain.MountPoint{},
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "initialize-output-1",
						VolumeRef: "ref-initialize-output-1",
					},
				},
				{
					MountPoint: domain.MountPoint{},
					KnitDataBody: domain.KnitDataBody{
						KnitId:    "initialize-output-2",
						VolumeRef: "ref-initialize-output-2",
					},
				},
			},
			Log: &domain.Log{
				KnitDataBody: domain.KnitDataBody{
					KnitId:    "initialize-log",
					VolumeRef: "ref-initialize-log",
				},
			},
		}

		if err := testee.Initialize(ctx, run); !errors.Is(err, expectedError) {
			t.Errorf("unexpected error: %s", err)
		}
	})
}
