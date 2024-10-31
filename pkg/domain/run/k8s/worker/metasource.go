package worker

import (
	"fmt"
	"path/filepath"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/data/k8s/data"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/metasource"
	"github.com/opst/knitfab/pkg/utils"
	ptr "github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/tuple"
	kubebatch "k8s.io/api/batch/v1"
	kubecore "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	kubeapimeta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// / fixme? : to be replaced with config object, perhaps.
type JobEnvironment struct {
	// Namespace      string
	ServiceAccount string
	InitImage      string
	NurseImage     string
}

type RunIdentifier struct{ domain.RunBody }

// The name of application/resource.
//
// If there are many resources running a same app, they may have same `Name()`.
//
// For `ObjectMeta.Name`, USE `Instance()`, NOT THIS.
//
// This is set as a value of k8s label "app.kubernetes.io/name".
//
// see: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
func (ri RunIdentifier) Name() string {
	return ri.Component()
}

// This is set as a value of k8s label "app.kubernetes.io/instance"
// AND ALSO `ObjectMeta.Name` .
//
// This will identify an instance from others sharing Name() and Component().
//
// see: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
func (ri RunIdentifier) Instance() string {
	return ri.RunBody.WorkerName
}

// Where is this positioned in system archetecture.
//
// example: database, cache, reverse-proxy, ...
//
// This is set as a value of k8s label "app.kubernetes.io/component".
//
// see: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
func (ri RunIdentifier) Component() string {
	return "worker"
}

// Identifier of entity in knitfab object model.
func (ri RunIdentifier) Id() string {
	return ri.RunBody.Id
}

func (ri RunIdentifier) Extras() map[string]string {
	return map[string]string{
		"run": ri.Id(),
	}
}

// type of "Id()"
//
// example: knit_id, run_id, ...
func (ri RunIdentifier) IdType() string {
	return "runid"
}

func (ri *RunIdentifier) ObjectMeta(namespace string) kubeapimeta.ObjectMeta {
	return metasource.ToObjectMeta(ri, namespace)
}

type Mount struct {
	Data       data.Builder
	MountPoint domain.MountPoint
}

type Executable struct {
	RunIdentifier

	EnvVars map[string]string

	PlanId         string
	Image          domain.ImageIdentifier
	Inputs         []domain.Assignment
	Outputs        []domain.Assignment
	Log            *domain.Assignment
	ServiceAccount string
}

type counter[T comparable] map[T]uint

func (c counter[T]) Max() (T, uint) {
	if len(c) == 0 {
		return *new(T), 0
	}

	max := uint(0)
	var mkey T
	for k, n := range c {
		if max < n {
			max = n
			mkey = k
		}
	}

	return mkey, max
}

func Count[R any, T comparable](seq []R, dim func(R) T) counter[T] {
	ctr := counter[T]{}
	for _, x := range seq {
		k := dim(x)
		ctr[k] += 1
	}

	return ctr
}

func New(ex *domain.Run, envvars map[string]string) (*Executable, error) {

	if !ex.Image.Fulfilled() {
		return nil, fmt.Errorf(
			"malformed [planId:%s runId:%s] : no image or no version",
			ex.PlanId, ex.Id,
		)
	}

	for _, in := range ex.Inputs {
		if in.Path == "" {
			return nil, fmt.Errorf(
				"malformed [planId:%s runId:%s input %d]: no path",
				ex.PlanId, ex.Id, in.Id,
			)
		}
		if in.KnitDataBody.KnitId == "" {
			return nil, fmt.Errorf(
				"malformed [planId:%s runId:%s input %d]: no knit id",
				ex.PlanId, ex.Id, in.Id,
			)
		}
		if in.KnitDataBody.VolumeRef == "" {
			return nil, fmt.Errorf(
				"malformed [planId:%s runId:%s input:%d]: data %s has no volume ref",
				ex.PlanId, ex.Id, in.Id, in.KnitDataBody.KnitId,
			)
		}
	}

	counter := map[string]int{}
	for _, out := range ex.Outputs {
		if out.Path == "" {
			return nil, fmt.Errorf(
				"malformed [planId:%s runId:%s output:%d]: no mount path",
				ex.PlanId, ex.Id, out.Id,
			)
		}
		if out.KnitDataBody.KnitId == "" {
			return nil, fmt.Errorf(
				"malformed [planId:%s runId:%s output:%d]: no knit id",
				ex.PlanId, ex.Id, out.Id,
			)
		}
		if out.KnitDataBody.VolumeRef == "" {
			return nil, fmt.Errorf(
				"malformed [planId:%s runId:%s output:%d] : data %s has no volume ref",
				ex.PlanId, ex.Id, out.Id, out.KnitDataBody.KnitId,
			)
		}
		counter[out.KnitDataBody.KnitId] += 1
	}

	var log *domain.Assignment
	if l := ex.Log; l != nil {
		if l.KnitDataBody.KnitId == "" {
			return nil, fmt.Errorf(
				"malformed [planId:%s runId:%s log]: no knit id",
				ex.PlanId, ex.Id,
			)
		}
		if l.KnitDataBody.VolumeRef == "" {
			return nil, fmt.Errorf(
				"malformed [planId:%s runId:%s log]: data %s has no volume ref",
				ex.PlanId, ex.Id, l.KnitDataBody.KnitId,
			)
		}
		counter[l.KnitDataBody.KnitId] += 1

		log = &domain.Assignment{
			MountPoint:   domain.MountPoint{Id: l.Id, Path: "/log", Tags: l.Tags},
			KnitDataBody: l.KnitDataBody,
		}
	}

	for knitId, num := range counter {
		if 1 < num {
			return nil, fmt.Errorf(
				"malformed run [planId:%s runId:%s] knit id is conflicted!: knit id = %s",
				ex.PlanId, ex.Id, knitId,
			)
		}
	}

	return &Executable{
		RunIdentifier:  RunIdentifier{RunBody: ex.RunBody},
		EnvVars:        envvars,
		PlanId:         ex.PlanId,
		Image:          *ex.Image,
		Inputs:         ex.Inputs,
		Outputs:        ex.Outputs,
		Log:            log,
		ServiceAccount: ex.ServiceAccount,
	}, nil
}

func (ex *Executable) Extras() map[string]string {
	extras := map[string]string{}
	for k, v := range ex.RunIdentifier.Extras() {
		extras[k] = v
	}

	extras["plan"] = ex.PlanId
	return extras
}

func (ex *Executable) ObjectMeta(namespace string) kubeapimeta.ObjectMeta {
	return metasource.ToObjectMeta(ex, namespace)
}

var _ metasource.ResourceBuilder[*bconf.KnitClusterConfig, *kubebatch.Job] = &Executable{}

// convert Executable into kubernetes Job spec.
//
// # params:
//
// - je *WorkerConfig: supplemental component for the job.
//
// # return:
//
// - *kubernetes.Job
//
//   - error : it will be caused when ex cannot be converted to Job
//     because of its inconsistency or missing properties.
//
// Always one of return values are nil, and another one is not nil.
func (r *Executable) Build(conf *bconf.KnitClusterConfig) *kubebatch.Job {

	je := conf.Worker()

	inputs, inputsMount := tuple.UnzipPair(utils.Map(r.Inputs, toVolumeMount))
	outputs, outputsMount := tuple.UnzipPair(utils.Map(r.Outputs, toVolumeMount))
	logs, logsMount := tuple.UnzipPair(utils.Map(
		utils.Default(
			utils.IfNotNil(r.Log, func(log *domain.Assignment) *[]domain.Assignment {
				return &[]domain.Assignment{*log}
			}),
			[]domain.Assignment{},
		),
		toVolumeMount,
	))

	// setup minimal components
	volumes := utils.Concat(inputs, outputs, logs)

	init := []kubecore.Container{}

	resLimits := kubecore.ResourceList{}
	for typ, val := range r.PlanBody.Resources {
		switch typ {
		case "cpu":
			resLimits[kubecore.ResourceCPU] = val
		case "memory":
			resLimits[kubecore.ResourceMemory] = val
		default:
			resLimits[kubecore.ResourceName(typ)] = val
		}

	}

	env := []kubecore.EnvVar{}
	if r.EnvVars != nil {
		for k, v := range r.EnvVars {
			env = append(env, kubecore.EnvVar{Name: k, Value: v})
		}
	}

	var command []string = nil
	var args []string = nil

	if 0 < len(r.Entrypoint) {
		command = r.Entrypoint
	}
	if 0 < len(r.Args) {
		args = r.Args
	}

	containers := []kubecore.Container{
		{
			Name:         "main",
			Image:        fmt.Sprintf("%s:%s", r.Image.Image, r.Image.Version),
			Command:      command,
			Args:         args,
			VolumeMounts: utils.Concat(readonly(inputsMount), writable(outputsMount)),
			Resources: kubecore.ResourceRequirements{
				Limits: resLimits,
			},
			Env: env,
		},
	}

	// output-related requirements
	if 0 < len(outputs) {
		init = append(init, kubecore.Container{
			Name:         "init-main",
			Image:        je.Init().Image(),
			VolumeMounts: readonly(outputsMount),
			Args: utils.Map(outputsMount, func(m kubecore.VolumeMount) string {
				return m.MountPath
			}),
			Resources: kubecore.ResourceRequirements{
				Limits: kubecore.ResourceList{
					"cpu":    resource.MustParse("50m"),
					"memory": resource.MustParse("100Mi"),
				},
			},
		})
	}

	// log-related requirements
	if 0 < len(logs) {
		volumes = utils.Concat(
			[]kubecore.Volume{
				{
					Name: "serviceaccount",
					VolumeSource: kubecore.VolumeSource{
						Secret: &kubecore.SecretVolumeSource{
							SecretName: je.Nurse().ServiceAccountSecret(),
						},
					},
				},
			},
			volumes,
		)

		// to split filesystem between outputs and log, create a container.
		init = append(
			init,
			kubecore.Container{
				Name:         "init-log",
				Image:        je.Init().Image(),
				VolumeMounts: readonly(logsMount),
				Args: utils.Map(logsMount, func(m kubecore.VolumeMount) string {
					return m.MountPath
				}),
				Resources: kubecore.ResourceRequirements{
					Limits: kubecore.ResourceList{
						"cpu":    resource.MustParse("50m"),
						"memory": resource.MustParse("100Mi"),
					},
				},
			},
		)

		containers = append(
			containers,
			kubecore.Container{
				Name:  "nurse",
				Image: je.Nurse().Image(),
				Args: utils.Concat(
					[]string{"main"},
					utils.Map(logsMount, func(v kubecore.VolumeMount) string {
						return filepath.Join(v.MountPath, "log")
					}),
				),
				VolumeMounts: utils.Concat(
					writable(logsMount),
					[]kubecore.VolumeMount{
						{
							// mount service account explicitly. this is opted out
							Name:      "serviceaccount",
							MountPath: "/var/run/secrets/kubernetes.io/serviceaccount",
							ReadOnly:  true,
						},
					},
				),
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
				Resources: kubecore.ResourceRequirements{
					Limits: kubecore.ResourceList{
						"cpu":    resource.MustParse("50m"),
						"memory": resource.MustParse("100Mi"),
					},
				},
			},
		)
	}

	type simpleTorelation struct {
		Key      string
		Operator kubecore.TolerationOperator
		Effect   kubecore.TaintEffect
		Value    string
		// no TorelationSeconds
	}

	tolerationSet := map[simpleTorelation]struct{}{}

	aggregateAffinity := struct {
		//             key  --> set of values
		prefered map[string]map[string]struct{}
		required map[string]map[string]struct{}
	}{
		prefered: map[string]map[string]struct{}{},
		required: map[string]map[string]struct{}{},
	}
	for _, on := range r.OnNode {
		switch on.Mode {
		case domain.MayOnNode:
			tolerationSet[simpleTorelation{
				Key:      on.Key,
				Value:    on.Value,
				Operator: kubecore.TolerationOpEqual,
				Effect:   kubecore.TaintEffectNoSchedule,
			}] = struct{}{}
		case domain.PreferOnNode:
			tolerationSet[simpleTorelation{
				Key:      on.Key,
				Value:    on.Value,
				Operator: kubecore.TolerationOpEqual,
				Effect:   kubecore.TaintEffectNoSchedule,
			}] = struct{}{}
			tolerationSet[simpleTorelation{
				Key:      on.Key,
				Value:    on.Value,
				Operator: kubecore.TolerationOpEqual,
				Effect:   kubecore.TaintEffectPreferNoSchedule,
			}] = struct{}{}

			m, ok := aggregateAffinity.prefered[on.Key]
			if !ok {
				m = map[string]struct{}{}
				aggregateAffinity.prefered[on.Key] = m
			}
			m[on.Value] = struct{}{}
			aggregateAffinity.prefered[on.Key] = m
		case domain.MustOnNode:
			tolerationSet[simpleTorelation{
				Key:      on.Key,
				Value:    on.Value,
				Operator: kubecore.TolerationOpEqual,
				Effect:   kubecore.TaintEffectNoSchedule,
			}] = struct{}{}
			tolerationSet[simpleTorelation{
				Key:      on.Key,
				Value:    on.Value,
				Operator: kubecore.TolerationOpEqual,
				Effect:   kubecore.TaintEffectPreferNoSchedule,
			}] = struct{}{}

			m, ok := aggregateAffinity.required[on.Key]
			if !ok {
				m = map[string]struct{}{}
			}
			m[on.Value] = struct{}{}
			aggregateAffinity.required[on.Key] = m
		}
	}

	var affinity *kubecore.Affinity
	if 0 < len(aggregateAffinity.prefered)+len(aggregateAffinity.required) {
		nodeAffinity := &kubecore.NodeAffinity{}
		affinity = &kubecore.Affinity{NodeAffinity: nodeAffinity}

		for key, values := range aggregateAffinity.prefered {
			for value := range values {
				nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
					nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
					kubecore.PreferredSchedulingTerm{
						Weight: 1,
						Preference: kubecore.NodeSelectorTerm{
							MatchExpressions: []kubecore.NodeSelectorRequirement{
								{
									Key:      key,
									Operator: kubecore.NodeSelectorOpIn,
									Values:   []string{value},
								},
							},
						},
					},
				)
			}
		}
		if 0 < len(aggregateAffinity.required) {
			nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &kubecore.NodeSelector{}
			nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = []kubecore.NodeSelectorTerm{
				{
					MatchExpressions: utils.Map(
						utils.KeysOf(aggregateAffinity.required),
						func(key string) kubecore.NodeSelectorRequirement {
							values := aggregateAffinity.required[key]
							return kubecore.NodeSelectorRequirement{
								Key:      key,
								Operator: kubecore.NodeSelectorOpIn,
								Values:   utils.KeysOf(values),
							}
						},
					),
				},
			}
		}
	}

	var tolerations []kubecore.Toleration
	if len(tolerationSet) != 0 {
		tolerations = utils.Map(
			utils.KeysOf(tolerationSet),
			func(t simpleTorelation) kubecore.Toleration {
				return kubecore.Toleration{
					Key:      t.Key,
					Operator: t.Operator,
					Effect:   t.Effect,
					Value:    t.Value,
				}
			},
		)
	}

	automount := false
	if r.ServiceAccount != "" {
		automount = true
	}

	// compose!
	return &kubebatch.Job{
		ObjectMeta: r.ObjectMeta(conf.Namespace()),
		Spec: kubebatch.JobSpec{
			Parallelism:  ptr.Ref[int32](1),
			BackoffLimit: ptr.Ref[int32](0),
			Template: kubecore.PodTemplateSpec{
				Spec: kubecore.PodSpec{
					RestartPolicy:                kubecore.RestartPolicyNever,
					ServiceAccountName:           r.ServiceAccount,
					AutomountServiceAccountToken: &automount,
					EnableServiceLinks:           ptr.Ref(false), // do not expose Service endpoints for user content image.
					InitContainers:               rectify(init),
					Containers:                   containers,
					Volumes:                      volumes,
					Tolerations:                  tolerations,
					Affinity:                     affinity,
					PriorityClassName:            je.Priority(),
				},
			},
		},
	}
}

func rectify[T any](sli []T) []T {
	if len(sli) == 0 {
		return nil
	}
	return sli
}

func toVolumeMount(a domain.Assignment) tuple.Pair[kubecore.Volume, kubecore.VolumeMount] {
	v := kubecore.Volume{
		Name: a.KnitDataBody.KnitId,
		VolumeSource: kubecore.VolumeSource{
			PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
				ClaimName: a.KnitDataBody.VolumeRef,
			},
		},
	}

	vm := kubecore.VolumeMount{
		Name:      a.KnitDataBody.KnitId,
		MountPath: a.MountPoint.Path,
	}

	return tuple.PairOf(v, vm)
}

func readonly(vms []kubecore.VolumeMount) []kubecore.VolumeMount {
	return utils.Map(vms, func(vm kubecore.VolumeMount) kubecore.VolumeMount {
		new := vm // copy!
		new.ReadOnly = true
		return new
	})
}

func writable(vms []kubecore.VolumeMount) []kubecore.VolumeMount {
	return utils.Map(vms, func(vm kubecore.VolumeMount) kubecore.VolumeMount {
		new := vm // copy!
		new.ReadOnly = false
		return new
	})
}
