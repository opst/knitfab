package worker_test

import (
	"reflect"
	"testing"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/run/k8s/worker"
	"github.com/opst/knitfab/pkg/utils/cmp"
	ptr "github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/try"
	kubebatch "k8s.io/api/batch/v1"
	kubecore "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestRunExecutable(t *testing.T) {
	dsIn1 := domain.KnitDataBody{KnitId: "input-1", VolumeRef: "ref-input-1"}
	dsIn2 := domain.KnitDataBody{KnitId: "input-2", VolumeRef: "ref-input-2"}
	dsOut3 := domain.KnitDataBody{KnitId: "output-3", VolumeRef: "ref-output-3"}
	dsOut4 := domain.KnitDataBody{KnitId: "output-4", VolumeRef: "ref-output-4"}
	dsLog5 := domain.KnitDataBody{KnitId: "log-5", VolumeRef: "ref-log-5"}

	config := bconf.TrySeal(&bconf.KnitClusterConfigMarshall{
		Namespace: "knit-test",
		Database:  "postgrees://do-no-care",
		DataAgent: &bconf.DataAgentConfigMarshall{
			Image: "repo.invalid/dataagt:latest",
			Volume: &bconf.VolumeConfigMarshall{
				StorageClassName: "do-no-care",
				InitialCapacity:  "1Ki",
			},
			Port: 8080,
		},
		Worker: &bconf.WorkerConfigMarshall{
			Priority: "knit-worker-priority",
			Init: &bconf.InitContainerConfigMarshall{
				Image: "repo.invalid/init:latest",
			},
			Nurse: &bconf.NurseContainerConfigMarshall{
				ServiceAccountSecret: "test-sa-secret",
				Image:                "repo.invalid/nurse:latest",
			},
		},
		Keychains: &bconf.KeychainsConfigMarshall{
			SignKeyForImportToken: &bconf.HS256KeyChainMarshall{
				Name: "signe-for-import-token",
			},
		},
	})

	type When struct {
		run    domain.Run
		envvar map[string]string
	}

	theoryOk := func(when When, then kubebatch.JobSpec) func(*testing.T) {
		return func(t *testing.T) {

			ex := try.To(worker.New(&when.run, when.envvar)).OrFatal(t)

			testee := ex.Build(config)

			if ex.Instance() != testee.ObjectMeta.Name {
				t.Errorf(
					"source.Instance != ObjectMeta.Name: (actual, expected) = (%s, %s)",
					testee.ObjectMeta.Name, ex.Instance(),
				)
			}

			{
				actual := *testee.Spec.Parallelism
				expected := *then.Parallelism

				if actual != expected {
					t.Errorf(
						"Parallelism: (actual, expected) = (%d, %d)",
						actual, expected,
					)
				}
			}

			{
				actual := *testee.Spec.BackoffLimit
				expected := *then.BackoffLimit
				if actual != expected {
					t.Errorf(
						"BackoffLimit: (actual, expected) = (%d, %d)",
						actual, expected,
					)
				}
			}

			{
				actual := testee.Spec.Template.Spec.RestartPolicy
				expected := then.Template.Spec.RestartPolicy
				if actual != expected {
					t.Errorf(
						"RestartPolicy: (actual, expected) = (%s, %s)",
						actual, expected,
					)
				}
			}

			{
				actual := testee.Spec.Template.Spec.ServiceAccountName
				expected := then.Template.Spec.ServiceAccountName
				if actual != expected {
					t.Errorf(
						"ServiceAccountName: (actual, expected) = (%s, %s)",
						actual, expected,
					)
				}
			}

			{
				actual := *testee.Spec.Template.Spec.AutomountServiceAccountToken
				expected := *then.Template.Spec.AutomountServiceAccountToken

				if actual != expected {
					t.Errorf(
						"AutomountServiceAccountToken: (actual, expected) = (%t, %t)",
						actual, expected,
					)
				}
			}

			{
				actual := *testee.Spec.Template.Spec.EnableServiceLinks
				expected := *then.Template.Spec.EnableServiceLinks
				if actual != expected {
					t.Errorf(
						"EnableServiceLinks: (actual, expected) = (%t, %t)",
						actual, expected,
					)
				}
			}

			{
				actual := testee.Spec.Template.Spec.PriorityClassName
				expected := "knit-worker-priority" // comes from config
				if actual != expected {
					t.Errorf(
						"PriorityCalssName: (actual, expected) = (%s, %s)",
						actual, expected,
					)
				}
			}

			{
				actual := testee.Spec.Template.Spec.InitContainers
				expected := then.Template.Spec.InitContainers

				if !cmp.SliceContentEqWith(actual, expected, func(a, b kubecore.Container) bool {
					return a.Name == b.Name &&
						a.Image == b.Image &&
						cmp.SliceEq(a.Args, b.Args) &&
						cmp.SliceContentEqWith(a.Env, b.Env, func(a, b kubecore.EnvVar) bool {
							return reflect.DeepEqual(a, b)
						}) &&
						cmp.SliceContentEqWith(a.VolumeMounts, b.VolumeMounts, func(a, b kubecore.VolumeMount) bool {
							return reflect.DeepEqual(a, b)
						})
				}) {
					t.Errorf(
						"InitContainers:\n=== actual ===\n%+v\n=== expected ===\n%+v",
						actual, expected,
					)
				}
			}

			{
				actual := testee.Spec.Template.Spec.Containers
				expected := then.Template.Spec.Containers
				if !cmp.SliceContentEqWith(actual, expected, func(a, b kubecore.Container) bool {
					return a.Name == b.Name &&
						a.Image == b.Image &&
						cmp.SliceEq(a.Command, b.Command) &&
						cmp.SliceEq(a.Args, b.Args) &&
						cmp.SliceContentEqWith(a.Env, b.Env, func(a, b kubecore.EnvVar) bool {
							return reflect.DeepEqual(a, b)
						}) &&
						cmp.SliceContentEqWith(a.VolumeMounts, b.VolumeMounts, func(a, b kubecore.VolumeMount) bool {
							return reflect.DeepEqual(a, b)
						}) &&
						cmp.MapEqWith(a.Resources.Limits, b.Resources.Limits, resource.Quantity.Equal) &&
						cmp.SliceContentEqWith(a.Env, b.Env, func(a, b kubecore.EnvVar) bool {
							return a.Name == b.Name && a.Value == b.Value
						})
				}) {
					t.Errorf(
						"InitContainers:\n=== actual ===\n%+v\n=== expected ===\n%+v",
						actual, expected,
					)
				}
			}

			{
				actual := testee.Spec.Template.Spec.Volumes
				expected := then.Template.Spec.Volumes
				if !cmp.SliceContentEqWith(actual, expected, func(a, b kubecore.Volume) bool {
					return a.Name == b.Name &&
						reflect.DeepEqual(a.VolumeSource, b.VolumeSource)
				}) {
					t.Errorf(
						"Volumes:\n=== actual ===\n%+v\n=== expected ===\n%+v",
						actual, expected,
					)
				}
			}

			{
				actual := testee.Spec.Template.Spec.Tolerations
				expected := then.Template.Spec.Tolerations
				if !cmp.SliceContentEqWith(
					actual, expected,
					func(a, b kubecore.Toleration) bool { return reflect.DeepEqual(a, b) },
				) {
					t.Errorf(
						"Tolerations:\n=== actual ===\n%+v\n=== expected ===\n%+v",
						actual, expected,
					)
				}
			}

			{
				compairNodeSelectorTerm := func(a, b kubecore.NodeSelectorTerm) bool {
					return cmp.SliceContentEqWith(
						a.MatchExpressions, b.MatchExpressions,
						func(a, b kubecore.NodeSelectorRequirement) bool {
							return a.Key == b.Key &&
								a.Operator == b.Operator &&
								cmp.SliceContentEq(a.Values, b.Values)
						},
					) &&
						cmp.SliceContentEqWith(
							a.MatchFields, b.MatchFields,
							func(a, b kubecore.NodeSelectorRequirement) bool {
								return a.Key == b.Key &&
									a.Operator == b.Operator &&
									cmp.SliceContentEq(a.Values, b.Values)
							},
						)
				}
				if !cmp.PEqualWith(
					testee.Spec.Template.Spec.Affinity,
					then.Template.Spec.Affinity,
					func(actual, expected kubecore.Affinity) bool {
						return cmp.PEqualWith(
							actual.NodeAffinity,
							expected.NodeAffinity,
							func(actual, expected kubecore.NodeAffinity) bool {
								return cmp.PEqualWith(
									actual.RequiredDuringSchedulingIgnoredDuringExecution,
									expected.RequiredDuringSchedulingIgnoredDuringExecution,
									func(actual, expected kubecore.NodeSelector) bool {
										return cmp.SliceContentEqWith(
											actual.NodeSelectorTerms,
											expected.NodeSelectorTerms,
											compairNodeSelectorTerm,
										)
									},
								) &&
									cmp.SliceContentEqWith(
										actual.PreferredDuringSchedulingIgnoredDuringExecution,
										expected.PreferredDuringSchedulingIgnoredDuringExecution,
										func(a, b kubecore.PreferredSchedulingTerm) bool {
											return a.Weight == b.Weight &&
												compairNodeSelectorTerm(a.Preference, b.Preference)
										},
									)
							},
						)
					},
				) {
					t.Errorf(
						"Affinity:\n=== actual ===\n%+v\n=== expected ===\n%+v",
						testee.Spec.Template.Spec.Affinity,
						then.Template.Spec.Affinity,
					)
				}
			}
		}
	}

	t.Run("when it builds a k8s job spec, it creates job specification", theoryOk(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image: "repo.invalid/image-name", Version: "1.0",
						},
						Entrypoint: []string{"python", "main.py"},
						Args:       []string{"arg1", "arg2"},
						Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("1"),
							"memory": resource.MustParse("1Gi"),
							"gpu":    resource.MustParse("1"),
						},
						OnNode: []domain.OnNode{
							{Mode: domain.MayOnNode, Key: "key1", Value: "value1"},
							{Mode: domain.MayOnNode, Key: "key1", Value: "value2"},
							{Mode: domain.PreferOnNode, Key: "key1", Value: "value2"},
							{Mode: domain.PreferOnNode, Key: "key2", Value: "value2"},
							{Mode: domain.MustOnNode, Key: "key3", Value: "value3"},
							{Mode: domain.MustOnNode, Key: "key1", Value: "value3"},
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint:   domain.MountPoint{Id: 1, Path: "/in/1"},
					},
					{
						KnitDataBody: dsIn2,
						MountPoint:   domain.MountPoint{Id: 2, Path: "/in/2"},
					},
				},
				Outputs: []domain.Assignment{
					{
						KnitDataBody: dsOut3,
						MountPoint:   domain.MountPoint{Id: 3, Path: "/out/3"},
					},
					{
						KnitDataBody: dsOut4,
						MountPoint:   domain.MountPoint{Id: 4, Path: "/out/4"},
					},
				},
				Log: &domain.Log{Id: 5, KnitDataBody: dsLog5},
			},
			envvar: map[string]string{
				"foo": "bar",
				"baz": "qux",
			},
		},
		kubebatch.JobSpec{
			Parallelism:  ptr.Ref[int32](1),
			BackoffLimit: ptr.Ref[int32](0),
			Template: kubecore.PodTemplateSpec{
				Spec: kubecore.PodSpec{
					ServiceAccountName:           "",
					AutomountServiceAccountToken: ptr.Ref(false),
					EnableServiceLinks:           ptr.Ref(false),
					RestartPolicy:                kubecore.RestartPolicyNever,
					InitContainers: []kubecore.Container{
						{
							Name:  "init-main",
							Image: config.Worker().Init().Image(),
							Args:  []string{"/out/3", "/out/4"},
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsOut3.KnitId, MountPath: "/out/3",
									ReadOnly: true,
								},
								{
									Name: dsOut4.KnitId, MountPath: "/out/4",
									ReadOnly: true,
								},
							},
							Resources: kubecore.ResourceRequirements{
								Limits: kubecore.ResourceList{
									"cpu":    resource.MustParse("50m"),
									"memory": resource.MustParse("100Mi"),
								},
							},
						},
						{
							Name:  "init-log",
							Image: config.Worker().Init().Image(),
							Args:  []string{"/log"},
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsLog5.KnitId, MountPath: "/log",
									ReadOnly: true,
								},
							},
							Resources: kubecore.ResourceRequirements{
								Limits: kubecore.ResourceList{
									"cpu":    resource.MustParse("50m"),
									"memory": resource.MustParse("100Mi"),
								},
							},
						},
					},
					Containers: []kubecore.Container{
						{
							Name:    "main",
							Image:   "repo.invalid/image-name:1.0",
							Command: []string{"python", "main.py"},
							Args:    []string{"arg1", "arg2"},
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsIn1.KnitId, MountPath: "/in/1",
									ReadOnly: true,
								},
								{
									Name: dsIn2.KnitId, MountPath: "/in/2",
									ReadOnly: true,
								},
								{
									Name: dsOut3.KnitId, MountPath: "/out/3",
								},
								{
									Name: dsOut4.KnitId, MountPath: "/out/4",
								},
							},
							Env: []kubecore.EnvVar{
								{Name: "foo", Value: "bar"},
								{Name: "baz", Value: "qux"},
							},
							Resources: kubecore.ResourceRequirements{
								Limits: kubecore.ResourceList{
									"cpu":    resource.MustParse("1"),
									"memory": resource.MustParse("1Gi"),
									"gpu":    resource.MustParse("1"),
								},
							},
						},
						{
							Name:  "nurse",
							Image: config.Worker().Nurse().Image(),
							Args:  []string{"main", "/log/log"},
							Resources: kubecore.ResourceRequirements{
								Limits: kubecore.ResourceList{
									"cpu":    resource.MustParse("50m"),
									"memory": resource.MustParse("100Mi"),
								},
							},
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsLog5.KnitId, MountPath: "/log",
								},
								{
									Name:      "serviceaccount",
									MountPath: "/var/run/secrets/kubernetes.io/serviceaccount",
									ReadOnly:  true,
								},
							},
							Env: []kubecore.EnvVar{
								{
									Name: "POD_NAME",
									ValueFrom: &kubecore.EnvVarSource{
										FieldRef: &kubecore.ObjectFieldSelector{FieldPath: "metadata.name"},
									},
								},
								{
									Name: "NAMESPACE",
									ValueFrom: &kubecore.EnvVarSource{
										FieldRef: &kubecore.ObjectFieldSelector{FieldPath: "metadata.namespace"},
									},
								},
							},
						},
					},
					Volumes: []kubecore.Volume{
						{
							Name: "serviceaccount",
							VolumeSource: kubecore.VolumeSource{
								Secret: &kubecore.SecretVolumeSource{
									SecretName: "test-sa-secret",
								},
							},
						},
						{
							Name: dsIn1.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsIn1.VolumeRef,
								},
							},
						},
						{
							Name: dsIn2.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsIn2.VolumeRef,
								},
							},
						},
						{
							Name: dsOut3.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsOut3.VolumeRef,
								},
							},
						},
						{
							Name: dsOut4.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsOut4.VolumeRef,
								},
							},
						},
						{
							Name: dsLog5.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsLog5.VolumeRef,
								},
							},
						},
					},
					Tolerations: []kubecore.Toleration{
						{
							Key:      "key1",
							Operator: kubecore.TolerationOpEqual,
							Value:    "value1",
							Effect:   kubecore.TaintEffectNoSchedule,
						},
						{
							Key:      "key1",
							Operator: kubecore.TolerationOpEqual,
							Value:    "value2",
							Effect:   kubecore.TaintEffectNoSchedule,
						},
						{
							Key:      "key1",
							Operator: kubecore.TolerationOpEqual,
							Value:    "value2",
							Effect:   kubecore.TaintEffectPreferNoSchedule,
						},
						{
							Key:      "key1",
							Operator: kubecore.TolerationOpEqual,
							Value:    "value3",
							Effect:   kubecore.TaintEffectNoSchedule,
						},
						{
							Key:      "key1",
							Operator: kubecore.TolerationOpEqual,
							Value:    "value3",
							Effect:   kubecore.TaintEffectPreferNoSchedule,
						},
						{
							Key:      "key2",
							Operator: kubecore.TolerationOpEqual,
							Value:    "value2",
							Effect:   kubecore.TaintEffectNoSchedule,
						},
						{
							Key:      "key2",
							Operator: kubecore.TolerationOpEqual,
							Value:    "value2",
							Effect:   kubecore.TaintEffectPreferNoSchedule,
						},
						{
							Key:      "key3",
							Operator: kubecore.TolerationOpEqual,
							Value:    "value3",
							Effect:   kubecore.TaintEffectNoSchedule,
						},
						{
							Key:      "key3",
							Operator: kubecore.TolerationOpEqual,
							Value:    "value3",
							Effect:   kubecore.TaintEffectPreferNoSchedule,
						},
					},
					Affinity: &kubecore.Affinity{
						NodeAffinity: &kubecore.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &kubecore.NodeSelector{
								NodeSelectorTerms: []kubecore.NodeSelectorTerm{
									{
										MatchExpressions: []kubecore.NodeSelectorRequirement{
											{
												Key: "key1", Values: []string{"value3"},
												Operator: kubecore.NodeSelectorOpIn,
											},
											{
												Key: "key3", Values: []string{"value3"},
												Operator: kubecore.NodeSelectorOpIn,
											},
										},
									},
								},
							},
							PreferredDuringSchedulingIgnoredDuringExecution: []kubecore.PreferredSchedulingTerm{
								{
									Weight: 1,
									Preference: kubecore.NodeSelectorTerm{
										MatchExpressions: []kubecore.NodeSelectorRequirement{
											{
												Key: "key1", Values: []string{"value2"},
												Operator: kubecore.NodeSelectorOpIn,
											},
										},
									},
								},
								{
									Weight: 1,
									Preference: kubecore.NodeSelectorTerm{
										MatchExpressions: []kubecore.NodeSelectorRequirement{
											{
												Key: "key2", Values: []string{"value2"},
												Operator: kubecore.NodeSelectorOpIn,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	))

	t.Run("when it builds with service account, it reflects to job spec", theoryOk(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image: "repo.invalid/image-name", Version: "1.0",
						},
						ServiceAccount: "service-account-name",
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint:   domain.MountPoint{Id: 1, Path: "/in/1"},
					},
				},
				Outputs: []domain.Assignment{
					{
						KnitDataBody: dsOut3,
						MountPoint:   domain.MountPoint{Id: 3, Path: "/out/3"},
					},
				},
				Log: &domain.Log{Id: 5, KnitDataBody: dsLog5},
			},
		},
		kubebatch.JobSpec{
			Parallelism:  ptr.Ref[int32](1),
			BackoffLimit: ptr.Ref[int32](0),
			Template: kubecore.PodTemplateSpec{
				Spec: kubecore.PodSpec{
					ServiceAccountName:           "service-account-name",
					AutomountServiceAccountToken: ptr.Ref(true),
					EnableServiceLinks:           ptr.Ref(false),
					RestartPolicy:                kubecore.RestartPolicyNever,
					InitContainers: []kubecore.Container{
						{
							Name:  "init-main",
							Image: config.Worker().Init().Image(),
							Args:  []string{"/out/3"},
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsOut3.KnitId, MountPath: "/out/3",
									ReadOnly: true,
								},
							},
							Resources: kubecore.ResourceRequirements{
								Limits: kubecore.ResourceList{
									"cpu":    resource.MustParse("50m"),
									"memory": resource.MustParse("100Mi"),
								},
							},
						},
						{
							Name:  "init-log",
							Image: config.Worker().Init().Image(),
							Args:  []string{"/log"},
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsLog5.KnitId, MountPath: "/log",
									ReadOnly: true,
								},
							},
							Resources: kubecore.ResourceRequirements{
								Limits: kubecore.ResourceList{
									"cpu":    resource.MustParse("50m"),
									"memory": resource.MustParse("100Mi"),
								},
							},
						},
					},
					Containers: []kubecore.Container{
						{
							Name:  "main",
							Image: "repo.invalid/image-name:1.0",
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsIn1.KnitId, MountPath: "/in/1",
									ReadOnly: true,
								},
								{
									Name: dsOut3.KnitId, MountPath: "/out/3",
									ReadOnly: false,
								},
							},
						},
						{
							Name:  "nurse",
							Image: config.Worker().Nurse().Image(),
							Args:  []string{"main", "/log/log"},
							Resources: kubecore.ResourceRequirements{
								Limits: kubecore.ResourceList{
									"cpu":    resource.MustParse("50m"),
									"memory": resource.MustParse("100Mi"),
								},
							},
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsLog5.KnitId, MountPath: "/log",
								},
								{
									Name:      "serviceaccount",
									MountPath: "/var/run/secrets/kubernetes.io/serviceaccount",
									ReadOnly:  true,
								},
							},
							Env: []kubecore.EnvVar{
								{
									Name: "POD_NAME",
									ValueFrom: &kubecore.EnvVarSource{
										FieldRef: &kubecore.ObjectFieldSelector{FieldPath: "metadata.name"},
									},
								},
								{
									Name: "NAMESPACE",
									ValueFrom: &kubecore.EnvVarSource{
										FieldRef: &kubecore.ObjectFieldSelector{FieldPath: "metadata.namespace"},
									},
								},
							},
						},
					},
					Volumes: []kubecore.Volume{
						{
							Name: "serviceaccount",
							VolumeSource: kubecore.VolumeSource{
								Secret: &kubecore.SecretVolumeSource{
									SecretName: "test-sa-secret",
								},
							},
						},
						{
							Name: dsIn1.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsIn1.VolumeRef,
								},
							},
						},
						{
							Name: dsOut3.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsOut3.VolumeRef,
								},
							},
						},
						{
							Name: dsLog5.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsLog5.VolumeRef,
								},
							},
						},
					},
				},
			},
		},
	))

	t.Run("when it builds a k8s job spec with output and no log, it creates job specification", theoryOk(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image: "repo.invalid/image-name", Version: "1.0",
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint:   domain.MountPoint{Id: 1, Path: "/in/1"},
					},
					{
						KnitDataBody: dsIn2,
						MountPoint:   domain.MountPoint{Id: 2, Path: "/in/2"},
					},
				},
				Outputs: []domain.Assignment{
					{
						KnitDataBody: dsOut3,
						MountPoint:   domain.MountPoint{Id: 3, Path: "/out/3"},
					},
					{
						KnitDataBody: dsOut4,
						MountPoint:   domain.MountPoint{Id: 4, Path: "/out/4"},
					},
				},
			},
		},
		kubebatch.JobSpec{
			Parallelism:  ptr.Ref[int32](1),
			BackoffLimit: ptr.Ref[int32](0),
			Template: kubecore.PodTemplateSpec{
				Spec: kubecore.PodSpec{
					ServiceAccountName:           "",
					AutomountServiceAccountToken: ptr.Ref(false),
					EnableServiceLinks:           ptr.Ref(false),
					RestartPolicy:                kubecore.RestartPolicyNever,
					InitContainers: []kubecore.Container{
						{
							Name:  "init-main",
							Image: config.Worker().Init().Image(),
							Args:  []string{"/out/3", "/out/4"},
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsOut3.KnitId, MountPath: "/out/3",
									ReadOnly: true,
								},
								{
									Name: dsOut4.KnitId, MountPath: "/out/4",
									ReadOnly: true,
								},
							},
							Resources: kubecore.ResourceRequirements{
								Limits: kubecore.ResourceList{
									"cpu":    resource.MustParse("50m"),
									"memory": resource.MustParse("100Mi"),
								},
							},
						},
					},
					Containers: []kubecore.Container{
						{
							Name:  "main",
							Image: "repo.invalid/image-name:1.0",
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsIn1.KnitId, MountPath: "/in/1",
									ReadOnly: true,
								},
								{
									Name: dsIn2.KnitId, MountPath: "/in/2",
									ReadOnly: true,
								},
								{
									Name: dsOut3.KnitId, MountPath: "/out/3",
								},
								{
									Name: dsOut4.KnitId, MountPath: "/out/4",
								},
							},
						},
					},
					Volumes: []kubecore.Volume{
						{
							Name: dsIn1.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsIn1.VolumeRef,
								},
							},
						},
						{
							Name: dsIn2.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsIn2.VolumeRef,
								},
							},
						},
						{
							Name: dsOut3.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsOut3.VolumeRef,
								},
							},
						},
						{
							Name: dsOut4.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsOut4.VolumeRef,
								},
							},
						},
					},
				},
			},
		},
	))
	t.Run("when it builds a k8s job spec with log but no output, it creates job specification", theoryOk(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image: "repo.invalid/image-name", Version: "1.0",
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint:   domain.MountPoint{Id: 1, Path: "/in/1"},
					},
					{
						KnitDataBody: dsIn2,
						MountPoint:   domain.MountPoint{Id: 2, Path: "/in/2"},
					},
				},
				Outputs: []domain.Assignment{}, // empty
				Log:     &domain.Log{Id: 5, KnitDataBody: dsLog5},
			},
		},
		kubebatch.JobSpec{
			Parallelism:  ptr.Ref[int32](1),
			BackoffLimit: ptr.Ref[int32](0),
			Template: kubecore.PodTemplateSpec{
				Spec: kubecore.PodSpec{
					ServiceAccountName:           "",
					AutomountServiceAccountToken: ptr.Ref(false),
					EnableServiceLinks:           ptr.Ref(false),
					RestartPolicy:                kubecore.RestartPolicyNever,
					InitContainers: []kubecore.Container{
						{
							Name:  "init-log",
							Image: config.Worker().Init().Image(),
							Args:  []string{"/log"},
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsLog5.KnitId, MountPath: "/log",
									ReadOnly: true,
								},
							},
							Resources: kubecore.ResourceRequirements{
								Limits: kubecore.ResourceList{
									"cpu":    resource.MustParse("50m"),
									"memory": resource.MustParse("100Mi"),
								},
							},
						},
					},
					Containers: []kubecore.Container{
						{
							Name:  "main",
							Image: "repo.invalid/image-name:1.0",
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsIn1.KnitId, MountPath: "/in/1",
									ReadOnly: true,
								},
								{
									Name: dsIn2.KnitId, MountPath: "/in/2",
									ReadOnly: true,
								},
							},
						},
						{
							Name:  "nurse",
							Image: config.Worker().Nurse().Image(),
							Args:  []string{"main", "/log/log"},
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsLog5.KnitId, MountPath: "/log",
								},
								{
									Name:      "serviceaccount",
									MountPath: "/var/run/secrets/kubernetes.io/serviceaccount",
									ReadOnly:  true,
								},
							},
							Resources: kubecore.ResourceRequirements{
								Limits: kubecore.ResourceList{
									"cpu":    resource.MustParse("50m"),
									"memory": resource.MustParse("100Mi"),
								},
							},
							Env: []kubecore.EnvVar{
								{
									Name: "POD_NAME",
									ValueFrom: &kubecore.EnvVarSource{
										FieldRef: &kubecore.ObjectFieldSelector{FieldPath: "metadata.name"},
									},
								},
								{
									Name: "NAMESPACE",
									ValueFrom: &kubecore.EnvVarSource{
										FieldRef: &kubecore.ObjectFieldSelector{FieldPath: "metadata.namespace"},
									},
								},
							},
						},
					},
					Volumes: []kubecore.Volume{
						{
							Name: "serviceaccount",
							VolumeSource: kubecore.VolumeSource{
								Secret: &kubecore.SecretVolumeSource{
									SecretName: "test-sa-secret",
								},
							},
						},
						{
							Name: dsIn1.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsIn1.VolumeRef,
								},
							},
						},
						{
							Name: dsIn2.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsIn2.VolumeRef,
								},
							},
						},
						{
							Name: dsLog5.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsLog5.VolumeRef,
								},
							},
						},
					},
				},
			},
		},
	))
	t.Run("when it builds a k8s job spec without output and log, it creates job specification", theoryOk(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image: "repo.invalid/image-name", Version: "1.0",
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint:   domain.MountPoint{Id: 1, Path: "/in/1"},
					},
					{
						KnitDataBody: dsIn2,
						MountPoint:   domain.MountPoint{Id: 2, Path: "/in/2"},
					},
				},
			},
		},
		kubebatch.JobSpec{
			Parallelism:  ptr.Ref[int32](1),
			BackoffLimit: ptr.Ref[int32](0),
			Template: kubecore.PodTemplateSpec{
				Spec: kubecore.PodSpec{
					ServiceAccountName:           "",
					AutomountServiceAccountToken: ptr.Ref(false),
					EnableServiceLinks:           ptr.Ref(false),
					RestartPolicy:                kubecore.RestartPolicyNever,
					Containers: []kubecore.Container{
						{
							Name:  "main",
							Image: "repo.invalid/image-name:1.0",
							VolumeMounts: []kubecore.VolumeMount{
								{
									Name: dsIn1.KnitId, MountPath: "/in/1",
									ReadOnly: true,
								},
								{
									Name: dsIn2.KnitId, MountPath: "/in/2",
									ReadOnly: true,
								},
							},
						},
					},
					Volumes: []kubecore.Volume{
						{
							Name: dsIn1.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsIn1.VolumeRef,
								},
							},
						},
						{
							Name: dsIn2.KnitId,
							VolumeSource: kubecore.VolumeSource{
								PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
									ClaimName: dsIn2.VolumeRef,
								},
							},
						},
					},
				},
			},
		},
	))

	theoryErr := func(when When) func(*testing.T) {
		return func(t *testing.T) {
			if testee, err := worker.New(&when.run, when.envvar); err == nil {
				t.Error("error is not caused, unexpectedly: ", testee)
			}
		}
	}

	t.Run("when kdb.Run has an input without data, it will cause error", theoryErr(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image: "repo.invalid/image-name", Version: "1.0",
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint:   domain.MountPoint{Id: 1, Path: "/in/1"},
					},
					{
						// KnitDataBody: (missing) // all inputs should have data
						MountPoint: domain.MountPoint{Id: 2, Path: "/in/2"},
					},
					{
						KnitDataBody: dsOut3,
						MountPoint:   domain.MountPoint{Id: 3, Path: "/out/3"},
					},
				},
				Outputs: []domain.Assignment{
					{
						KnitDataBody: dsOut4,
						MountPoint:   domain.MountPoint{Id: 4, Path: "/out/4"},
					},
				},
				Log: &domain.Log{Id: 5, KnitDataBody: dsLog5},
			},
		},
	))
	t.Run("when kdb.Run has an input without mouhtpoint path, it will cause error", theoryErr(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image: "repo.invalid/image-name", Version: "1.0",
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint: domain.MountPoint{
							Id:   1,
							Path: "", // no path
						},
					},
					{
						KnitDataBody: dsIn2,
						MountPoint:   domain.MountPoint{Id: 2, Path: "/in/2"},
					},
				},
				Outputs: []domain.Assignment{
					{
						KnitDataBody: dsOut3,
						MountPoint:   domain.MountPoint{Id: 3, Path: "/out/3"},
					},
					{
						KnitDataBody: dsOut4,
						MountPoint:   domain.MountPoint{Id: 4, Path: "/out/4"},
					},
				},
				Log: &domain.Log{Id: 5, KnitDataBody: dsLog5},
			},
		},
	))
	t.Run("when kdb.Run has an output without data, it will cause error", theoryErr(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image: "repo.invalid/image-name", Version: "1.0",
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint:   domain.MountPoint{Id: 1, Path: "/in/1"},
					},
					{
						KnitDataBody: dsIn2,
						MountPoint:   domain.MountPoint{Id: 2, Path: "/in/2"},
					},
				},
				Outputs: []domain.Assignment{
					{
						KnitDataBody: dsOut3,
						MountPoint:   domain.MountPoint{Id: 3, Path: "/out/3"},
					},
					{
						// KnitDataBody: (missing) // all outputs should have data
						MountPoint: domain.MountPoint{Id: 4, Path: "/out/4"},
					},
				},
				Log: &domain.Log{Id: 5, KnitDataBody: dsLog5},
			},
		},
	))
	t.Run("when kdb.Run has an output without mountpoint path, it will cause error", theoryErr(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image: "repo.invalid/image-name", Version: "1.0",
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint:   domain.MountPoint{Id: 1, Path: "/in/1"},
					},
					{
						KnitDataBody: dsIn2,
						MountPoint:   domain.MountPoint{Id: 2, Path: "/in/2"},
					},
				},
				Outputs: []domain.Assignment{
					{
						KnitDataBody: dsOut3,
						MountPoint:   domain.MountPoint{Id: 3, Path: "/out/3"},
					},
					{
						KnitDataBody: dsOut4,
						MountPoint: domain.MountPoint{
							Id:   4,
							Path: "", // no path!
						},
					},
				},
				Log: &domain.Log{Id: 5, KnitDataBody: dsLog5},
			},
		},
	))
	t.Run("when kdb.Run has a log without data, it will cause error", theoryErr(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image: "repo.invalid/image-name", Version: "1.0",
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint:   domain.MountPoint{Id: 1, Path: "/in/1"},
					},
					{
						KnitDataBody: dsIn2,
						MountPoint:   domain.MountPoint{Id: 2, Path: "/in/2"},
					},
				},
				Outputs: []domain.Assignment{
					{
						KnitDataBody: dsOut3,
						MountPoint:   domain.MountPoint{Id: 3, Path: "/out/3"},
					},
					{
						KnitDataBody: dsOut4,
						MountPoint:   domain.MountPoint{Id: 4, Path: "/out/4"},
					},
				},
				Log: &domain.Log{
					Id: 5,
					// KnitDataBody: (missing),
				},
			},
		},
	))
	t.Run("when kdb.Run has image without name, it will cause error", theoryErr(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image:   "", // no name
							Version: "1.0",
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint:   domain.MountPoint{Id: 1, Path: "/in/1"},
					},
					{
						KnitDataBody: dsIn2,
						MountPoint:   domain.MountPoint{Id: 2, Path: "/in/2"},
					},
				},
				Outputs: []domain.Assignment{
					{
						KnitDataBody: dsOut3,
						MountPoint:   domain.MountPoint{Id: 3, Path: "/out/3"},
					},
					{
						KnitDataBody: dsOut4,
						MountPoint:   domain.MountPoint{Id: 4, Path: "/out/4"},
					},
				},
				Log: &domain.Log{Id: 5, KnitDataBody: dsLog5},
			},
		},
	))
	t.Run("when kdb.Run has image without version, it will cause error", theoryErr(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image:   "repo.invalid/image-name",
							Version: "", // no version
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint:   domain.MountPoint{Id: 1, Path: "/in/1"},
					},
					{
						KnitDataBody: dsIn2,
						MountPoint:   domain.MountPoint{Id: 2, Path: "/in/2"},
					},
				},
				Outputs: []domain.Assignment{
					{
						KnitDataBody: dsOut3,
						MountPoint:   domain.MountPoint{Id: 3, Path: "/out/3"},
					},
					{
						KnitDataBody: dsOut4,
						MountPoint:   domain.MountPoint{Id: 4, Path: "/out/4"},
					},
				},
				Log: &domain.Log{Id: 5, KnitDataBody: dsLog5},
			},
		},
	))
	t.Run("when kdb.Run has same knit ids in different output mountpoints, it will cause error", theoryErr(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image: "repo.invalid/image-name", Version: "1.0",
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint:   domain.MountPoint{Id: 1, Path: "/in/1"},
					},
					{
						KnitDataBody: dsIn2,
						MountPoint:   domain.MountPoint{Id: 2, Path: "/in/2"},
					},
				},
				Outputs: []domain.Assignment{
					{
						KnitDataBody: dsOut3,
						MountPoint:   domain.MountPoint{Id: 3, Path: "/out/3"},
					},
					{
						KnitDataBody: dsOut3, // same as Outputs[0].Data
						MountPoint:   domain.MountPoint{Id: 4, Path: "/out/4"},
					},
				},
				Log: &domain.Log{Id: 5, KnitDataBody: dsLog5},
			},
		},
	))
	t.Run("when kdb.Run has same knit ids in output and log mountpoints, it will cause error", theoryErr(
		When{
			run: domain.Run{
				RunBody: domain.RunBody{
					Id: "test-run-id",
					PlanBody: domain.PlanBody{
						PlanId: "test-plan-id",
						Image: &domain.ImageIdentifier{
							Image: "repo.invalid/image-name", Version: "1.0",
						},
					},
				},
				Inputs: []domain.Assignment{
					{
						KnitDataBody: dsIn1,
						MountPoint:   domain.MountPoint{Id: 1, Path: "/in/1"},
					},
					{
						KnitDataBody: dsIn2,
						MountPoint:   domain.MountPoint{Id: 2, Path: "/in/2"},
					},
				},
				Outputs: []domain.Assignment{
					{
						KnitDataBody: dsOut3,
						MountPoint:   domain.MountPoint{Id: 3, Path: "/out/3"},
					},
					{
						KnitDataBody: dsLog5, // same as Log
						MountPoint:   domain.MountPoint{Id: 4, Path: "/out/4"},
					},
				},
				Log: &domain.Log{Id: 5, KnitDataBody: dsLog5},
			},
		},
	))
}
