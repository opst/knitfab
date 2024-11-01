package dataagt_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	testutilctx "github.com/opst/knitfab/internal/testutils/context"
	bconf "github.com/opst/knitfab/pkg/configs/backend"
	"github.com/opst/knitfab/pkg/conn/k8s/testenv"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/data/k8s/dataagt"
	k8serrors "github.com/opst/knitfab/pkg/domain/errors/k8serrors"
	k8smock "github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster/mock"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/metasource"
	utils "github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/try"
	kubecore "k8s.io/api/core/v1"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
	kubeapiresource "k8s.io/apimachinery/pkg/api/resource"
	kubeapimeta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const lenUUID = 36

type Iota_ struct{ value int }

func Iota() *Iota_ {
	i := Iota_{value: 0}
	return &i
}

func (i *Iota_) Format(s fmt.State, r rune) {
	fmt.Fprintf(s, "%d", int(i.value))
	i.value += 1
}

var _iota = Iota()

func TestSpawn(t *testing.T) {

	{
		theory := func(mode domain.DataAgentMode) func(t *testing.T) {
			return func(t *testing.T) {
				t.Run("When Dataagt is spawned without VolumeRef, it should cause error", func(t *testing.T) {
					ctx, cancel := testutilctx.WithTest(context.Background(), t)
					defer cancel()

					cluster, _ := k8smock.NewCluster()
					configs := (&bconf.KnitClusterConfigMarshall{
						Namespace: cluster.Namespace(),
						Database:  "postgres://do-not-care",
						DataAgent: &bconf.DataAgentConfigMarshall{
							Image: testenv.Images().Dataagt,
							Port:  8080,
							Volume: &bconf.VolumeConfigMarshall{
								StorageClassName: testenv.STORAGE_CLASS_NAME,
								InitialCapacity:  "1Ki",
							},
						},
						Worker: &bconf.WorkerConfigMarshall{
							Priority: "fake-priority",
							Init: &bconf.InitContainerConfigMarshall{
								Image: "repo.invalid/init-image:latest",
							},
							Nurse: &bconf.NurseContainerConfigMarshall{
								Image:                "repo.invalid/nurse-image:latest",
								ServiceAccountSecret: "fake-sa",
							},
						},
						Keychains: &bconf.KeychainsConfigMarshall{
							SignKeyForImportToken: &bconf.HS256KeyChainMarshall{
								Name: "signe-for-import-token",
							},
						},
					}).TrySeal()

					targetData := domain.KnitDataBody{
						KnitId: k8smock.LabelValue(t, lenUUID),
						// VolumeRef: zero value = "",
					}

					dbDataAgent := domain.DataAgent{
						Name:         fmt.Sprintf("test-dataagt-%s-%s", mode, targetData.KnitId),
						Mode:         mode,
						KnitDataBody: targetData,
					}

					_, err := dataagt.Spawn(ctx, configs, cluster, dbDataAgent, time.Now().Add(1*time.Hour))
					if err == nil {
						t.Error("expected error is not retuerned")
					}
				})
			}
		}

		t.Run("[Read]", theory(domain.DataAgentRead))
		t.Run("[Write]", theory(domain.DataAgentWrite))
	}

	{
		theory := func(mode domain.DataAgentMode) func(t *testing.T) {
			return func(t *testing.T) {
				t.Run("When Dataagt exceeds deadline, it returns workloads.ErrDeadlineExceeded", func(t *testing.T) {
					ctx, cancel := testutilctx.WithTest(context.Background(), t)
					defer cancel()

					cluster, clientset := testenv.NewCluster(t)
					namespace := cluster.Namespace()
					pvcname := "knit-test-deadline-test-k8s-get-pvc"
					if mode == domain.DataAgentRead {
						try.To(clientset.CoreV1().PersistentVolumeClaims(namespace).Create(
							ctx,
							&kubecore.PersistentVolumeClaim{
								ObjectMeta: kubeapimeta.ObjectMeta{
									Name: pvcname,
								},
								Spec: kubecore.PersistentVolumeClaimSpec{
									StorageClassName: pointer.Ref("knit-test-workloads-k8s-sc"),
									AccessModes:      []kubecore.PersistentVolumeAccessMode{kubecore.ReadOnlyMany},
									Resources: kubecore.VolumeResourceRequirements{
										Requests: kubecore.ResourceList{
											kubecore.ResourceStorage: kubeapiresource.MustParse("10Mi"),
										},
									},
								},
							},
							kubeapimeta.CreateOptions{},
						)).OrFatal(t)
					}
					defer func() {
						clientset.CoreV1().
							PersistentVolumeClaims(namespace).
							Delete(
								ctx, pvcname,
								kubeapimeta.DeleteOptions{
									GracePeriodSeconds: pointer.Ref[int64](0),
									PropagationPolicy:  pointer.Ref(kubeapimeta.DeletePropagationForeground),
								},
							)
						for {
							_, err := clientset.CoreV1().
								PersistentVolumeClaims(namespace).
								Get(ctx, pvcname, kubeapimeta.GetOptions{})
							if kubeerr.IsNotFound(err) {
								return
							}
							time.Sleep(50 * time.Millisecond)
						}
					}()
					configs := (&bconf.KnitClusterConfigMarshall{
						Namespace: namespace,
						Database:  "postgres://do-not-care",
						DataAgent: &bconf.DataAgentConfigMarshall{
							Image: testenv.Images().Dataagt,
							Port:  8080,
							Volume: &bconf.VolumeConfigMarshall{
								StorageClassName: testenv.STORAGE_CLASS_NAME,
								InitialCapacity:  "1Ki",
							},
						},
						Worker: &bconf.WorkerConfigMarshall{
							Priority: "fake-priority",
							Init: &bconf.InitContainerConfigMarshall{
								Image: "repo.invalid/init-image:latest",
							},
							Nurse: &bconf.NurseContainerConfigMarshall{
								Image:                "repo.invalid/nurse-image:latest",
								ServiceAccountSecret: "fake-sa",
							},
						},
						Keychains: &bconf.KeychainsConfigMarshall{
							SignKeyForImportToken: &bconf.HS256KeyChainMarshall{
								Name: "signe-for-import-token",
							},
						},
					}).TrySeal()

					targetData := domain.KnitDataBody{
						KnitId:    k8smock.LabelValue(t, lenUUID),
						VolumeRef: pvcname,
					}

					dbDataAgent := domain.DataAgent{
						Name:         fmt.Sprintf("test-dataagt-%s-%s", mode, targetData.KnitId),
						Mode:         mode,
						KnitDataBody: targetData,
					}

					defer func() {
						clientset.CoreV1().
							Pods(namespace).
							Delete(
								ctx, dbDataAgent.Name, kubeapimeta.DeleteOptions{
									GracePeriodSeconds: pointer.Ref[int64](0),
									PropagationPolicy:  pointer.Ref(kubeapimeta.DeletePropagationForeground),
								},
							)
					}()

					_, err := dataagt.Spawn(ctx, configs, cluster, dbDataAgent, time.Now().Add(-1*time.Second))
					if !errors.Is(err, k8serrors.ErrDeadlineExceeded) {
						t.Errorf("expected error is not returned. %+v", err)
					}
				})
			}
		}

		t.Run("[Read]", theory(domain.DataAgentRead))
		t.Run("[Write]", theory(domain.DataAgentWrite))
	}

	{
		theory := func(mode domain.DataAgentMode) func(t *testing.T) {
			return func(t *testing.T) {
				t.Run("When Read Mode Dataagt is spawned, it should set properties as configured:", func(t *testing.T) {
					ctx, cancel := testutilctx.WithTest(context.Background(), t)
					defer cancel()

					cluster, clientset := testenv.NewCluster(t)
					namespace := cluster.Namespace()
					configs := (&bconf.KnitClusterConfigMarshall{
						Namespace: namespace,
						Database:  "postgres://do-not-care",
						DataAgent: &bconf.DataAgentConfigMarshall{
							Image: testenv.Images().Dataagt,
							Port:  8080,
							Volume: &bconf.VolumeConfigMarshall{
								StorageClassName: testenv.STORAGE_CLASS_NAME,
								InitialCapacity:  "1Ki",
							},
						},
						Worker: &bconf.WorkerConfigMarshall{
							Priority: "fake-priority",
							Init: &bconf.InitContainerConfigMarshall{
								Image: "repo.invalid/init-image:latest",
							},
							Nurse: &bconf.NurseContainerConfigMarshall{
								Image:                "repo.invalid/nurse-image:latest",
								ServiceAccountSecret: "fake-sa",
							},
						},
						Keychains: &bconf.KeychainsConfigMarshall{
							SignKeyForImportToken: &bconf.HS256KeyChainMarshall{
								Name: "signe-for-import-token",
							},
						},
					}).TrySeal()

					pvcname := "knit-test-spawning-k8s-get-pvc"
					if mode == domain.DataAgentRead {
						try.To(clientset.CoreV1().PersistentVolumeClaims(namespace).Create(
							ctx,
							&kubecore.PersistentVolumeClaim{
								ObjectMeta: kubeapimeta.ObjectMeta{
									Name: pvcname,
								},
								Spec: kubecore.PersistentVolumeClaimSpec{
									StorageClassName: pointer.Ref("knit-test-workloads-k8s-sc"),
									AccessModes:      []kubecore.PersistentVolumeAccessMode{kubecore.ReadOnlyMany},
									Resources: kubecore.VolumeResourceRequirements{
										Requests: kubecore.ResourceList{
											kubecore.ResourceStorage: configs.DataAgent().Volume().InitialCapacity(),
										},
									},
								},
							},
							kubeapimeta.CreateOptions{},
						)).OrFatal(t)
					}
					defer func() {
						clientset.CoreV1().
							PersistentVolumeClaims(namespace).
							Delete(
								ctx, pvcname,
								kubeapimeta.DeleteOptions{
									GracePeriodSeconds: pointer.Ref[int64](0),
									PropagationPolicy:  pointer.Ref(kubeapimeta.DeletePropagationForeground),
								},
							)
						for {
							_, err := clientset.CoreV1().
								PersistentVolumeClaims(namespace).
								Get(ctx, pvcname, kubeapimeta.GetOptions{})
							if kubeerr.IsNotFound(err) {
								return
							}
							time.Sleep(50 * time.Millisecond)
						}
					}()

					targetData := domain.KnitDataBody{
						KnitId:    k8smock.LabelValue(t, lenUUID),
						VolumeRef: pvcname,
					}

					dbDataAgent := domain.DataAgent{
						Name:         fmt.Sprintf("test-dataagt-%s-%s", mode, targetData.KnitId),
						Mode:         mode,
						KnitDataBody: targetData,
					}
					sAgent := try.To(dataagt.Of(dbDataAgent)).OrFatal(t)
					testee, err := dataagt.Spawn(ctx, configs, cluster, dbDataAgent, time.Now().Add(1*time.Hour))

					if err != nil {
						t.Fatalf("cannot create dataagt.: %v", err)
					}

					// ------ assert testee props. -------
					t.Run("returned Dataagt object", func(t *testing.T) {
						if testee.APIPort() != configs.DataAgent().Port() {
							t.Errorf(
								"APIPort unmatch (actual, expected) = (%d, %d)",
								testee.APIPort(), configs.DataAgent().Port(),
							)
						}

						if testee.Name() != dbDataAgent.Name {
							t.Errorf(
								"dataagt name should be its service name. (acutal, expected) = (%s, %s)",
								testee.Name(), dbDataAgent.Name,
							)
						}

						if testee.Mode() != mode {
							t.Errorf("Dataagt should be %s mode, but %s", mode, testee.Mode())
						}
					})

					t.Run("k8s PVC", func(t *testing.T) {
						pvc := try.To(
							clientset.CoreV1().
								PersistentVolumeClaims(cluster.Namespace()).
								Get(ctx, targetData.VolumeRef, kubeapimeta.GetOptions{}),
						).OrFatal(t)

						pvcCapacity := configs.DataAgent().Volume().InitialCapacity()
						if !pvc.Spec.Resources.Requests.Storage().Equal(pvcCapacity) {
							t.Errorf(
								"PVC initial capacity unmatch. (actual, expected) = (%v. %v)",
								*pvc.Spec.Resources.Requests.Storage(),
								pvcCapacity,
							)
						}
					})

					t.Run("k8s Pod", func(t *testing.T) {
						pod, err := clientset.CoreV1().
							Pods(cluster.Namespace()).
							Get(ctx, dbDataAgent.Name, kubeapimeta.GetOptions{})
						if err != nil {
							t.Fatal("failed to get pod")
						}

						container, ok := utils.First(
							pod.Spec.Containers,
							func(c kubecore.Container) bool {
								return c.Image == configs.DataAgent().Image()
							},
						)
						if !ok {
							t.Fatalf("Deployment has no container as config.: %v", pod.Spec.Containers)
						}

						if !cmp.SliceContains(container.Args, []string{"--mode", string(mode)}) {
							t.Errorf("dataagt container is not %s mode. %v", mode, container.Args)
						}
						if !cmp.SliceContains(container.Args, []string{"--port", fmt.Sprintf("%d", configs.DataAgent().Port())}) {
							t.Errorf("dataagt container is not expose port as configure. %v", container.Args)
						}
						if _, ok := utils.First(container.Ports, func(p kubecore.ContainerPort) bool {
							return p.ContainerPort == configs.DataAgent().Port()
						}); !ok {
							t.Errorf("dataagt container is not expose port as configure. %v", container.Ports)
						}

						if container.Image != configs.DataAgent().Image() {
							t.Errorf(
								"dataagt starts wrong image (actual, expected) = (%s, %s)",
								container.Image, configs.DataAgent().Image(),
							)
						}
						volumes := container.VolumeMounts
						if len(volumes) != 1 {
							t.Errorf("dataagt starts container with too many volumes. %d > 1", len(volumes))
						}

						switch mode {
						case domain.DataAgentRead:
							if !volumes[0].ReadOnly {
								t.Error("read mode dataagt mounts writeable volume.")
							}
						case domain.DataAgentWrite:
							if volumes[0].ReadOnly {
								t.Error("read mode dataagt mounts readonly volume.")
							}
						}

						if !cmp.MapGeq(pod.ObjectMeta.Labels, metasource.ToLabels(sAgent)) {
							t.Errorf(
								"depl labels: unmatch (actual, expected) = (%#v, %#v)",
								pod.Labels, metasource.ToLabels(sAgent),
							)
						}
						if !cmp.MapGeq(pod.Labels, metasource.ToLabels(sAgent)) {
							t.Errorf(
								"pod labels: unmatch (actual, expected) = (%#v, %#v)",
								pod.Labels, metasource.ToLabels(sAgent),
							)
						}
					})

					// --- close test ---
					t.Run("closeablitity", func(t *testing.T) {
						if err := testee.Close(); err != nil {
							t.Errorf("close caused error. %#v", err)
						}

						if _, err := clientset.CoreV1().
							PersistentVolumeClaims(cluster.Namespace()).
							Get(ctx, targetData.VolumeRef, kubeapimeta.GetOptions{}); err != nil {
							t.Errorf("PVC should not be removed if Dataagt is closed. %#v", err)
						}

						if _, err := clientset.CoreV1().
							Pods(cluster.Namespace()).
							Get(ctx, dbDataAgent.Name, kubeapimeta.GetOptions{}); !kubeerr.IsNotFound(err) {
							t.Errorf("pod is found after closed. error = %#v", err)
						}
					})
				})
			}
		}
		t.Run("[Read]", theory(domain.DataAgentRead))
		t.Run("[Write]", theory(domain.DataAgentWrite))
	}

	t.Run("Dataagt in Read mode for same Knit Data can be spawned many", func(t *testing.T) {
		ctx := context.Background()
		namespace := testenv.Namespace()
		if namespace == "" {
			t.Fatal("no namespace given.")
		}

		cluster, clientset := testenv.NewCluster(t)
		pvcname := "knit-test-multireader-k8s-get-pvc"
		{
			try.To(clientset.CoreV1().PersistentVolumeClaims(namespace).Create(
				ctx,
				&kubecore.PersistentVolumeClaim{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name: pvcname,
					},
					Spec: kubecore.PersistentVolumeClaimSpec{
						StorageClassName: pointer.Ref("knit-test-workloads-k8s-sc"),
						AccessModes:      []kubecore.PersistentVolumeAccessMode{kubecore.ReadOnlyMany},
						Resources: kubecore.VolumeResourceRequirements{
							Requests: kubecore.ResourceList{
								kubecore.ResourceStorage: kubeapiresource.MustParse("10Mi"),
							},
						},
					},
				},
				kubeapimeta.CreateOptions{},
			)).OrFatal(t)
			defer func() {
				clientset.CoreV1().
					PersistentVolumeClaims(namespace).
					Delete(
						ctx, pvcname,
						kubeapimeta.DeleteOptions{
							GracePeriodSeconds: pointer.Ref[int64](0),
							PropagationPolicy:  pointer.Ref(kubeapimeta.DeletePropagationForeground),
						},
					)
				for {
					_, err := clientset.CoreV1().
						PersistentVolumeClaims(namespace).
						Get(ctx, pvcname, kubeapimeta.GetOptions{})
					if kubeerr.IsNotFound(err) {
						return
					}
					time.Sleep(50 * time.Millisecond)
				}
			}()
		}

		configs := (&bconf.KnitClusterConfigMarshall{
			Namespace: namespace,
			Database:  "postgres://do-not-care",
			DataAgent: &bconf.DataAgentConfigMarshall{
				Image: testenv.Images().Dataagt,
				Port:  8080,
				Volume: &bconf.VolumeConfigMarshall{
					StorageClassName: testenv.STORAGE_CLASS_NAME,
					InitialCapacity:  "1Ki",
				},
			},
			Worker: &bconf.WorkerConfigMarshall{
				Priority: "fake-priority",
				Init: &bconf.InitContainerConfigMarshall{
					Image: "repo.invalid/init-image:latest",
				},
				Nurse: &bconf.NurseContainerConfigMarshall{
					Image:                "repo.invalid/nurse-image:latest",
					ServiceAccountSecret: "fake-sa",
				},
			},
			Keychains: &bconf.KeychainsConfigMarshall{
				SignKeyForImportToken: &bconf.HS256KeyChainMarshall{
					Name: "signe-for-import-token",
				},
			},
		}).TrySeal()

		suffixies := []string{"a", "b"}
		for nth := range suffixies {
			suffix := suffixies[nth]

			targetData := domain.KnitDataBody{
				KnitId:    k8smock.LabelValue(t, lenUUID),
				VolumeRef: pvcname,
			}
			dbDataAgent := domain.DataAgent{
				Name:         fmt.Sprintf("test-dataagt-read-%s-%s", targetData.KnitId, suffix),
				Mode:         domain.DataAgentRead,
				KnitDataBody: targetData,
			}

			testee, err := dataagt.Spawn(ctx, configs, cluster, dbDataAgent, time.Now().Add(1*time.Hour))
			if err != nil {
				t.Fatalf("cannot create dataagt at #%d.: %v", nth, err)
			}
			defer testee.Close()
		}
	})

	t.Run("Dataagt in Write mode for same Knit Data can not be spawned many", func(t *testing.T) {
		ctx, cancel := testutilctx.WithTest(context.Background(), t)
		defer cancel()
		cluster, _ := testenv.NewCluster(t)
		configs := (&bconf.KnitClusterConfigMarshall{
			Namespace: cluster.Namespace(),
			Database:  "postgres://do-not-care",
			DataAgent: &bconf.DataAgentConfigMarshall{
				Image: testenv.Images().Dataagt,
				Port:  8080,
				Volume: &bconf.VolumeConfigMarshall{
					StorageClassName: testenv.STORAGE_CLASS_NAME,
					InitialCapacity:  "1Ki",
				},
			},
			Worker: &bconf.WorkerConfigMarshall{
				Priority: "fake-priority",
				Init: &bconf.InitContainerConfigMarshall{
					Image: "repo.invalid/init-image:latest",
				},
				Nurse: &bconf.NurseContainerConfigMarshall{
					Image:                "repo.invalid/nurse-image:latest",
					ServiceAccountSecret: "fake-sa",
				},
			},
			Keychains: &bconf.KeychainsConfigMarshall{
				SignKeyForImportToken: &bconf.HS256KeyChainMarshall{
					Name: "signe-for-import-token",
				},
			},
		}).TrySeal()

		suffixies := []string{"a", "b"}
		input := domain.KnitDataBody{
			KnitId:    k8smock.LabelValue(t, lenUUID),
			VolumeRef: fmt.Sprintf("volume-ref-%d", _iota),
		}
		for nth := range suffixies {
			suffix := suffixies[nth]

			dbDataAgent := domain.DataAgent{
				Name: fmt.Sprintf(
					"test-dataagt-write-%s-%d-%s", input.KnitId, nth, suffix,
				),
				Mode:         domain.DataAgentWrite,
				KnitDataBody: input,
			}

			testee, err := dataagt.Spawn(ctx, configs, cluster, dbDataAgent, time.Now().Add(1*time.Hour))

			if nth == 0 {
				// first time. : it SHOULD NOT cause error.
				if err != nil {
					t.Fatalf("cannot create dataagt at #%d.: %v", nth, err)
				} else {
					defer testee.Close()
				}
			} else {
				// second time (or more): it SHOULD cause error.
				if err == nil {
					t.Errorf("dataagt has been created unexpectedly at #%d", nth)
					defer testee.Close()
				}
			}
		}
	})

	t.Run("multiple dataagt can exist together if writemode instance is upto 1.", func(t *testing.T) {
		ctx, cancel := testutilctx.WithTest(context.Background(), t)
		defer cancel()

		cluster, _ := testenv.NewCluster(t)
		configs := (&bconf.KnitClusterConfigMarshall{
			Namespace: cluster.Namespace(),
			Database:  "postgres://do-not-care",
			DataAgent: &bconf.DataAgentConfigMarshall{
				Image: testenv.Images().Dataagt,
				Port:  8080,
				Volume: &bconf.VolumeConfigMarshall{
					StorageClassName: testenv.STORAGE_CLASS_NAME,
					InitialCapacity:  "1Ki",
				},
			},
			Worker: &bconf.WorkerConfigMarshall{
				Priority: "fake-priority",
				Init: &bconf.InitContainerConfigMarshall{
					Image: "repo.invalid/init-image:latest",
				},
				Nurse: &bconf.NurseContainerConfigMarshall{
					Image:                "repo.invalid/nurse-image:latest",
					ServiceAccountSecret: "fake-sa",
				},
			},
			Keychains: &bconf.KeychainsConfigMarshall{
				SignKeyForImportToken: &bconf.HS256KeyChainMarshall{
					Name: "signe-for-import-token",
				},
			},
		}).TrySeal()

		input := domain.KnitDataBody{
			KnitId:    k8smock.LabelValue(t, lenUUID),
			VolumeRef: fmt.Sprintf("volume-ref-%d", _iota),
		}

		{ // Write mode agent x1
			suffix := "write-0"
			dbDataAgent := domain.DataAgent{
				Name: fmt.Sprintf(
					"test-dataagt-write-%s-%s", input.KnitId, suffix,
				),
				Mode:         domain.DataAgentWrite,
				KnitDataBody: input,
			}
			testee := try.To(
				dataagt.Spawn(ctx, configs, cluster, dbDataAgent, time.Now().Add(1*time.Hour)),
			).OrFatal(t)
			defer testee.Close()
		}

		{ // Read mode agents x2
			suffixies := []string{"read-1", "read-2"}
			for nth := range suffixies {
				suffix := suffixies[nth]
				dbDataAgent := domain.DataAgent{
					Name: fmt.Sprintf(
						"test-dataagt-%s-%s", suffix, input.KnitId,
					),
					Mode:         domain.DataAgentRead,
					KnitDataBody: input,
				}

				testee := try.To(
					dataagt.Spawn(ctx, configs, cluster, dbDataAgent, time.Now().Add(1*time.Hour)),
				).OrFatal(t)
				defer testee.Close()
			}
		}
	})
}

func TestFind(t *testing.T) {
	{
		theory := func(mode domain.DataAgentMode) func(t *testing.T) {
			return func(t *testing.T) {
				t.Run("When Dataagt is exists, it should be found", func(t *testing.T) {
					ctx, cancel := testutilctx.WithTest(context.Background(), t)
					defer cancel()

					cluster, clientset := testenv.NewCluster(t)
					namespace := cluster.Namespace()
					configs := (&bconf.KnitClusterConfigMarshall{
						Namespace: namespace,
						Database:  "postgres://do-not-care",
						DataAgent: &bconf.DataAgentConfigMarshall{
							Image: testenv.Images().Dataagt,
							Port:  8080,
							Volume: &bconf.VolumeConfigMarshall{
								StorageClassName: testenv.STORAGE_CLASS_NAME,
								InitialCapacity:  "1Ki",
							},
						},
						Worker: &bconf.WorkerConfigMarshall{
							Priority: "fake-priority",
							Init: &bconf.InitContainerConfigMarshall{
								Image: "repo.invalid/init-image:latest",
							},
							Nurse: &bconf.NurseContainerConfigMarshall{
								Image:                "repo.invalid/nurse-image:latest",
								ServiceAccountSecret: "fake-sa",
							},
						},
						Keychains: &bconf.KeychainsConfigMarshall{
							SignKeyForImportToken: &bconf.HS256KeyChainMarshall{
								Name: "signe-for-import-token",
							},
						},
					}).TrySeal()

					pvcname := "knit-test-spawning-k8s-get-pvc"
					if mode == domain.DataAgentRead {
						try.To(clientset.CoreV1().PersistentVolumeClaims(namespace).Create(
							ctx,
							&kubecore.PersistentVolumeClaim{
								ObjectMeta: kubeapimeta.ObjectMeta{
									Name: pvcname,
								},
								Spec: kubecore.PersistentVolumeClaimSpec{
									StorageClassName: pointer.Ref("knit-test-workloads-k8s-sc"),
									AccessModes:      []kubecore.PersistentVolumeAccessMode{kubecore.ReadOnlyMany},
									Resources: kubecore.VolumeResourceRequirements{
										Requests: kubecore.ResourceList{
											kubecore.ResourceStorage: configs.DataAgent().Volume().InitialCapacity(),
										},
									},
								},
							},
							kubeapimeta.CreateOptions{},
						)).OrFatal(t)
					}
					defer func() {
						clientset.CoreV1().
							PersistentVolumeClaims(namespace).
							Delete(
								ctx, pvcname,
								kubeapimeta.DeleteOptions{
									GracePeriodSeconds: pointer.Ref[int64](0),
									PropagationPolicy:  pointer.Ref(kubeapimeta.DeletePropagationForeground),
								},
							)
						for {
							_, err := clientset.CoreV1().
								PersistentVolumeClaims(namespace).
								Get(ctx, pvcname, kubeapimeta.GetOptions{})
							if kubeerr.IsNotFound(err) {
								return
							}
							time.Sleep(50 * time.Millisecond)
						}
					}()

					targetData := domain.KnitDataBody{
						KnitId:    k8smock.LabelValue(t, lenUUID),
						VolumeRef: pvcname,
					}

					dbDataAgent := domain.DataAgent{
						Name:         fmt.Sprintf("test-dataagt-%s-%s", mode, targetData.KnitId),
						Mode:         mode,
						KnitDataBody: targetData,
					}
					try.To(dataagt.Spawn(ctx, configs, cluster, dbDataAgent, time.Now().Add(1*time.Hour))).OrFatal(t)

					testee := try.To(dataagt.Find(ctx, cluster, dbDataAgent)).OrFatal(t)

					// ------ assert testee props. -------
					if testee.APIPort() != configs.DataAgent().Port() {
						t.Errorf(
							"APIPort unmatch (actual, expected) = (%d, %d)",
							testee.APIPort(), configs.DataAgent().Port(),
						)
					}

					if testee.Name() != dbDataAgent.Name {
						t.Errorf(
							"dataagt name should be its service name. (acutal, expected) = (%s, %s)",
							testee.Name(), dbDataAgent.Name,
						)
					}

					if testee.Mode() != mode {
						t.Errorf("Dataagt should be %s mode, but %s", mode, testee.Mode())
					}

					if testee.VolumeRef() != targetData.VolumeRef {
						t.Errorf("Dataagt should have volume ref. %s", testee.VolumeRef())
					}

					// --- close test ---
					t.Run("closeablitity", func(t *testing.T) {
						if err := testee.Close(); err != nil {
							t.Errorf("close caused error. %#v", err)
						}

						if _, err := clientset.CoreV1().
							PersistentVolumeClaims(cluster.Namespace()).
							Get(ctx, targetData.VolumeRef, kubeapimeta.GetOptions{}); err != nil {
							t.Errorf("PVC should not be removed if Dataagt is closed. %#v", err)
						}

						if _, err := clientset.CoreV1().
							Pods(cluster.Namespace()).
							Get(ctx, dbDataAgent.Name, kubeapimeta.GetOptions{}); !kubeerr.IsNotFound(err) {
							t.Errorf("pod is found after closed. error = %#v", err)
						}
					})
				})
			}
		}
		t.Run("[Read]", theory(domain.DataAgentRead))
		t.Run("[Write]", theory(domain.DataAgentWrite))
	}

	t.Run("When Dataagt is not exists, it should return error", func(t *testing.T) {
		ctx, cancel := testutilctx.WithTest(context.Background(), t)
		defer cancel()

		cluster, _ := testenv.NewCluster(t)

		pvcname := "knit-test-spawning-k8s-get-pvc"

		targetData := domain.KnitDataBody{
			KnitId:    k8smock.LabelValue(t, lenUUID),
			VolumeRef: pvcname,
		}

		dbDataAgent := domain.DataAgent{
			Name:         fmt.Sprintf("test-dataagt-read-%s", targetData.KnitId),
			Mode:         domain.DataAgentRead,
			KnitDataBody: targetData,
		}

		_, err := dataagt.Find(ctx, cluster, dbDataAgent)
		if !kubeerr.IsNotFound(err) {
			t.Errorf("expected error is not returned. %#v", err)
		}
	})
}
