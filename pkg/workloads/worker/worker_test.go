package worker_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
	bconf "github.com/opst/knitfab/pkg/configs/backend"
	kdb "github.com/opst/knitfab/pkg/db"
	ptr "github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/try"
	k8smock "github.com/opst/knitfab/pkg/workloads/k8s/mock"
	"github.com/opst/knitfab/pkg/workloads/k8s/testenv"
	"github.com/opst/knitfab/pkg/workloads/metasource"
	"github.com/opst/knitfab/pkg/workloads/worker"
	kubebatch "k8s.io/api/batch/v1"
	kubecore "k8s.io/api/core/v1"
	v1 "k8s.io/api/scheduling/v1"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
	kubeapimeta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type FakeExecutable struct {
	worker.RunIdentifier

	job func(name string) *kubebatch.Job

	configs []*bconf.KnitClusterConfig
}

var _ metasource.ResourceBuilder[*bconf.KnitClusterConfig, *kubebatch.Job] = &FakeExecutable{}

func (fr *FakeExecutable) Build(conf *bconf.KnitClusterConfig) *kubebatch.Job {
	fr.configs = append(fr.configs, conf)
	return fr.job(fr.Instance())
}

func TestWorkerRunning(t *testing.T) {
	ctx := context.Background()
	cluster, clientset := testenv.NewCluster(t)
	labels := map[string]string{
		"knit.test/test":     "true",
		"knit.test/testcase": k8smock.LabelValue(t),
	}
	run := kdb.RunBody{
		Id:         "fake-run-id",
		WorkerName: "worker-runid-fake-run-id-running",
		PlanBody: kdb.PlanBody{
			Image: &kdb.ImageIdentifier{
				Image:   "busybox",
				Version: "1.35",
			},
		},
	}
	executable := &FakeExecutable{
		RunIdentifier: worker.RunIdentifier{RunBody: run},
		job: func(name string) *kubebatch.Job {
			return &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name:   name,
					Labels: labels,
				},
				Spec: kubebatch.JobSpec{
					Parallelism:  ptr.Ref[int32](1),
					BackoffLimit: ptr.Ref[int32](0),
					Template: kubecore.PodTemplateSpec{
						Spec: kubecore.PodSpec{
							RestartPolicy:                 kubecore.RestartPolicyNever,
							TerminationGracePeriodSeconds: ptr.Ref[int64](0),
							Containers: []kubecore.Container{
								{
									Name:    "main",
									Image:   "busybox:1.35",
									Command: []string{"sleep", "infinity"},
								},
							},
						},
					},
				},
			}
		},
	}

	conf := bconf.TrySeal(
		&bconf.KnitClusterConfigMarshall{
			Namespace: "test-namespace",
			Domain:    "cluster.local",
			Database:  "postgres://do-not-care",
			DataAgent: &bconf.DataAgentConfigMarshall{
				Image: "repo.invalid/dataagt:latest",
				Volume: &bconf.VolumeConfigMarshall{
					StorageClassName: "test-sc",
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
					Image:          testenv.Images().Nurse,
					ServiceAccount: "fake-serviceAccount",
				},
			},
		},
	)

	{
		pc := try.To(
			clientset.SchedulingV1().PriorityClasses().Create(
				ctx, &v1.PriorityClass{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name: "worker-priority",
					},
					Description:      "priority for tseting",
					Value:            100_000_000,
					PreemptionPolicy: ptr.Ref(kubecore.PreemptNever),
					GlobalDefault:    false,
				},
				kubeapimeta.CreateOptions{},
			),
		).OrFatal(t)
		t.Cleanup(func() {
			clientset.SchedulingV1().PriorityClasses().Delete(
				context.Background(), pc.Name,
				*kubeapimeta.NewDeleteOptions(0),
			)
		})
	}

	{ // assert precondition
		_, err := clientset.BatchV1().
			Jobs(cluster.Namespace()).
			Get(ctx, executable.Instance(), kubeapimeta.GetOptions{})

		if err == nil || !kubeerr.IsNotFound(err) {
			t.Fatal("job to be created should not have existed already", err)
		}

		_, err = worker.Find(ctx, cluster, run)
		if err == nil || !kubeerr.IsNotFound(err) {
			t.Fatal(err)
		}
	}

	t.Run("when it spawns worker,", func(t *testing.T) {

		testee := try.To(worker.Spawn(ctx, cluster, conf, executable)).OrFatal(t)

		t.Run("it is passed a JobEnvironment on building", func(t *testing.T) {
			expected := []*bconf.KnitClusterConfig{conf}
			if !cmp.SliceEq(executable.configs, expected) {
				t.Errorf(
					"job environment: unmatch: (actual, expected) = (%+v, %+v)",
					executable.configs, expected,
				)
			}
		})

		t.Run("the k8s job for the worker is found", func(t *testing.T) {
			_, err := clientset.BatchV1().
				Jobs(cluster.Namespace()).
				Get(ctx, executable.Instance(), kubeapimeta.GetOptions{})
			if err != nil {
				t.Fatal("the job should be found, but not", err)
			}
		})

		t.Run("the worker represents the k8s job", func(t *testing.T) {
			if testee.RunId() != executable.Id() {
				t.Errorf(
					"unmatch: runId (actual, expected) = (%s, %s)",
					testee.RunId(), executable.Id(),
				)
			}

			if testee.JobStatus() == worker.Failed {
				t.Fatal("unexpected worker status: failed")
			}
		})

		t.Run("the worker is found and get running", func(t *testing.T) {
			before := time.Now()
			for {
				found := try.To(worker.Find(ctx, cluster, run)).OrFatal(t)

				_, _, ok := found.ExitCode()
				if ok {
					t.Error("unexpectedly exit code is found")
				}

				if found.JobStatus() == worker.Running {
					return // ok!
				}
				if (10 * time.Second) < time.Since(before) {
					t.Error("the run's status is not running: ", found.JobStatus())
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
		})

		t.Run("and close it, the job is not found", func(t *testing.T) {
			if err := testee.Close(); err != nil {
				t.Fatal("unexpected error:", err)
			}

			before := time.Now()
			for { // it should be not-found eventualy.
				_, err := clientset.BatchV1().
					Jobs(cluster.Namespace()).
					Get(ctx, executable.Instance(), kubeapimeta.GetOptions{})

				if err != nil && kubeerr.IsNotFound(err) {
					return // ok!
				}

				if (1 * time.Second) < time.Since(before) {
					t.Fatal("closed job should not be found, but found")
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
		})
	})
}

func TestWorkerStoppedInSuccess(t *testing.T) {
	ctx := context.Background()
	cluster, clientset := testenv.NewCluster(t)
	labels := map[string]string{
		"knit.test/test":     "true",
		"knit.test/testcase": k8smock.LabelValue(t),
	}
	run := kdb.RunBody{
		Id:         "fake-run-id",
		WorkerName: "worker-runid-fake-run-id-success",
		PlanBody: kdb.PlanBody{
			Image: &kdb.ImageIdentifier{
				Image:   "busybox",
				Version: "1.35",
			},
		},
	}

	conf := bconf.TrySeal[*bconf.KnitClusterConfig](
		&bconf.KnitClusterConfigMarshall{
			Namespace: "test-namespace",
			Domain:    "cluster.local",
			Database:  "postgres://do-not-care",
			DataAgent: &bconf.DataAgentConfigMarshall{
				Image: "repo.invalid/dataagt:latest",
				Volume: &bconf.VolumeConfigMarshall{
					StorageClassName: "test-sc",
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
					Image:          testenv.Images().Nurse,
					ServiceAccount: "fake-serviceAccount",
				},
			},
		},
	)

	{
		pc := try.To(
			clientset.SchedulingV1().PriorityClasses().Create(
				ctx, &v1.PriorityClass{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name: "worker-priority",
					},
					Description:      "priority for tseting",
					Value:            100_000_000,
					PreemptionPolicy: ptr.Ref(kubecore.PreemptNever),
					GlobalDefault:    false,
				},
				kubeapimeta.CreateOptions{},
			),
		).OrFatal(t)
		t.Cleanup(func() {
			clientset.SchedulingV1().PriorityClasses().Delete(
				context.Background(), pc.Name,
				*kubeapimeta.NewDeleteOptions(0),
			)
		})
	}

	executable := &FakeExecutable{
		RunIdentifier: worker.RunIdentifier{RunBody: run},
		job: func(name string) *kubebatch.Job {
			return &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name:   name,
					Labels: labels,
				},
				Spec: kubebatch.JobSpec{
					Parallelism:  ptr.Ref[int32](1),
					BackoffLimit: ptr.Ref[int32](0),
					Template: kubecore.PodTemplateSpec{
						Spec: kubecore.PodSpec{
							RestartPolicy:                 kubecore.RestartPolicyNever,
							TerminationGracePeriodSeconds: ptr.Ref[int64](0),
							Containers: []kubecore.Container{
								{
									Name:    "main",
									Image:   "busybox:1.35",
									Command: []string{"sh", "-c", "echo line 1; echo line 2; echo line 3; exit 0"},
								},
							},
						},
					},
				},
			}
		},
	}

	{ // assert precondition
		_, err := clientset.BatchV1().
			Jobs(cluster.Namespace()).
			Get(ctx, executable.Instance(), kubeapimeta.GetOptions{})

		if err == nil || !kubeerr.IsNotFound(err) {
			t.Fatal("job to be created should not have existed already", err)
		}

		_, err = worker.Find(ctx, cluster, run)
		if err == nil || !kubeerr.IsNotFound(err) {
			t.Fatal(err)
		}
	}

	workerNewlySpawned := try.To(worker.Spawn(ctx, cluster, conf, executable)).OrFatal(t)
	defer workerNewlySpawned.Close()

	t.Run("when it spawns worker,", func(t *testing.T) {
		t.Run("it should has no exitcode", func(t *testing.T) {
			_, _, ok := workerNewlySpawned.ExitCode()
			if ok {
				t.Error("unexpectedly exit code is found")
			}
		})
	})

	t.Run("after worker get be done, it should has exitcode 0", func(t *testing.T) {
		var testee worker.Worker
		for {
			testee = try.To(worker.Find(ctx, cluster, executable.RunBody)).OrFatal(t)
			code, reason, ok := testee.ExitCode()
			if !ok {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			if reason == "" {
				t.Error("unexpected reason: (it is empty)")
			}
			if code == 0 {
				break
			}
			t.Errorf("unexpected exit code: %d", code)
		}

		logContent := new(strings.Builder)
		logStream := try.To(testee.Log(ctx)).OrFatal(t)
		defer logStream.Close()

		if _, err := io.Copy(logContent, logStream); err != nil {
			t.Fatal(err)
		}

		if !strings.Contains(logContent.String(), "line 1\nline 2\nline 3\n") {
			t.Error("unexpected log content:", logContent.String())
		}
	})
}

func TestWorkerStoppedInFailure(t *testing.T) {
	ctx := context.Background()
	cluster, clientset := testenv.NewCluster(t)
	labels := map[string]string{
		"knit.test/test":     "true",
		"knit.test/testcase": k8smock.LabelValue(t),
	}
	run := kdb.RunBody{
		Id:         "fake-run-id",
		WorkerName: "worker-runid-fake-run-id-failure",
		PlanBody: kdb.PlanBody{
			Image: &kdb.ImageIdentifier{
				Image:   "busybox",
				Version: "1.35",
			},
		},
	}

	conf := bconf.TrySeal[*bconf.KnitClusterConfig](
		&bconf.KnitClusterConfigMarshall{
			Namespace: "test-namespace",
			Domain:    "cluster.local",
			Database:  "postgres://do-not-care",
			DataAgent: &bconf.DataAgentConfigMarshall{
				Image: "repo.invalid/dataagt:latest",
				Volume: &bconf.VolumeConfigMarshall{
					StorageClassName: "test-sc",
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
					Image:          testenv.Images().Nurse,
					ServiceAccount: "fake-serviceAccount",
				},
			},
		},
	)

	{
		pc := try.To(
			clientset.SchedulingV1().PriorityClasses().Create(
				ctx, &v1.PriorityClass{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name: "worker-priority",
					},
					Description:      "priority for tseting",
					Value:            100_000_000,
					PreemptionPolicy: ptr.Ref(kubecore.PreemptNever),
					GlobalDefault:    false,
				},
				kubeapimeta.CreateOptions{},
			),
		).OrFatal(t)
		t.Cleanup(func() {
			clientset.SchedulingV1().PriorityClasses().Delete(
				context.Background(), pc.Name,
				*kubeapimeta.NewDeleteOptions(0),
			)
		})
	}

	executable := &FakeExecutable{
		RunIdentifier: worker.RunIdentifier{RunBody: run},
		job: func(name string) *kubebatch.Job {
			return &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name:   name,
					Labels: labels,
				},
				Spec: kubebatch.JobSpec{
					Parallelism:  ptr.Ref[int32](1),
					BackoffLimit: ptr.Ref[int32](0),
					Template: kubecore.PodTemplateSpec{
						Spec: kubecore.PodSpec{
							RestartPolicy:                 kubecore.RestartPolicyNever,
							TerminationGracePeriodSeconds: ptr.Ref[int64](0),
							Containers: []kubecore.Container{
								{
									Name:    "main",
									Image:   "busybox:1.35",
									Command: []string{"sh", "-c", "exit 42"},
								},
							},
						},
					},
				},
			}
		},
	}

	{ // assert precondition
		_, err := clientset.BatchV1().
			Jobs(cluster.Namespace()).
			Get(ctx, executable.Instance(), kubeapimeta.GetOptions{})

		if err == nil || !kubeerr.IsNotFound(err) {
			t.Fatal("job to be created should not have existed already", err)
		}

		_, err = worker.Find(ctx, cluster, run)
		if err == nil || !kubeerr.IsNotFound(err) {
			t.Fatal(err)
		}
	}

	workerNewlySpawned := try.To(worker.Spawn(ctx, cluster, conf, executable)).OrFatal(t)
	defer workerNewlySpawned.Close()

	t.Run("when it spawns worker,", func(t *testing.T) {
		t.Run("it should has no exitcode", func(t *testing.T) {
			_, _, ok := workerNewlySpawned.ExitCode()
			if ok {
				t.Error("unexpectedly exit code is found")
			}
		})
	})

	t.Run("after worker get be done, it should has exitcode 42", func(t *testing.T) {
		for {
			testee := try.To(worker.Find(ctx, cluster, executable.RunBody)).OrFatal(t)
			code, reason, ok := testee.ExitCode()
			if !ok {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			if reason == "" {
				t.Error("unexpected reason (it is empty)")
			}

			if code == 42 {
				return
			}
			t.Errorf("unexpected exit code: %d", code)
		}
	})
}
