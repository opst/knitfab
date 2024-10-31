package dataagt

import (
	"fmt"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	types "github.com/opst/knitfab/pkg/domain"
	data "github.com/opst/knitfab/pkg/domain/data/k8s/data"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/metasource"
	kubecore "k8s.io/api/core/v1"
	kubeapimeta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// k8s resource specs to spawn new DataAgent.
type DataAgentSpec struct {
	Mode types.DataAgentMode

	// knitId which this DataAgent points to
	KnitId string

	// Owner of DataAgt pod.
	//
	// Should not nil.
	Pod *kubecore.Pod

	// PVC storing KnitData.
	//
	// This can be nil when this DataAgent is Read mode
	// since the PVC is to be found.
	PVC *kubecore.PersistentVolumeClaim
}

type Builder struct {
	d data.Builder

	instance string

	mode types.DataAgentMode
}

func Of(agent types.DataAgent) (Builder, error) {
	dataBuilder, err := data.Of(agent.KnitDataBody)
	if err != nil {
		return Builder{}, err
	}
	return Builder{d: dataBuilder, mode: agent.Mode, instance: agent.Name}, nil
}

var _ metasource.Extraer = Builder{}
var _ metasource.ResourceBuilder[*bconf.KnitClusterConfig, *kubecore.Pod] = Builder{}

func (ds Builder) Component() string {
	return "dataagt"
}

func (ds Builder) IdType() string {
	return ds.d.IdType()
}

func (ds Builder) Name() string {
	return ds.Component()
}

func (ds Builder) Instance() string {
	return ds.instance
}

func (ds Builder) Id() string {
	return ds.d.Id()
}

func (ds Builder) Extras() map[string]string {
	return map[string]string{"mode": string(ds.mode)}
}

func (ds Builder) Mode() types.DataAgentMode {
	return ds.mode
}

func (ds Builder) ObjectMeta(namespace string) kubeapimeta.ObjectMeta {
	return metasource.ToObjectMeta(ds, namespace)
}

func (ds Builder) Build(c *bconf.KnitClusterConfig) *kubecore.Pod {

	namespace := c.Namespace()
	dagt := c.DataAgent()
	port := dagt.Port()

	objmeta := ds.ObjectMeta(namespace)
	False := false

	return &kubecore.Pod{
		ObjectMeta: objmeta,
		Spec: kubecore.PodSpec{
			RestartPolicy:                kubecore.RestartPolicyNever,
			AutomountServiceAccountToken: &False,
			Containers: []kubecore.Container{
				{
					Image: c.DataAgent().Image(),
					Args: []string{
						"--mode", string(ds.Mode()),
						"--path", "/data",
						"--port", fmt.Sprintf("%d", port),
						"--deadline", "180", // = 3 minutes
					},
					Name: "dataagt",
					VolumeMounts: []kubecore.VolumeMount{
						{
							Name:      "the-volume",
							MountPath: "/data",
							ReadOnly:  ds.Mode() == types.DataAgentRead,
						},
					},
					Ports: []kubecore.ContainerPort{
						{
							Name:          dataagtPortName,
							ContainerPort: port,
						},
					},
				},
			},
			Volumes: []kubecore.Volume{
				{
					Name: "the-volume",
					VolumeSource: kubecore.VolumeSource{
						PersistentVolumeClaim: &kubecore.PersistentVolumeClaimVolumeSource{
							ClaimName: ds.d.Instance(),
						},
					},
				},
			},
		},
	}
}
