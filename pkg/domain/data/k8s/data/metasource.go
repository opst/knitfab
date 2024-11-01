package data

import (
	"errors"
	"strings"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/metasource"
	"github.com/opst/knitfab/pkg/utils/slices"
	kubecore "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	kubeapimeta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type VolumeTemplate struct {
	Namespece    string
	StorageClass string
	Capacity     resource.Quantity
}

// k8s resource subject based Data
//
// this subject has
//
// - Id() : knit id
// - Instance() : PVC name
type Builder interface {
	metasource.ResourceBuilder[*bconf.KnitClusterConfig, *kubecore.PersistentVolumeClaim]
}

type WrappingDataSource[T any] interface {
	Builder

	Unwrap() T
}

func Of(d domain.KnitDataBody) (Builder, error) {
	if knitId := d.KnitId; knitId != strings.ToLower(knitId) {
		return nil, errors.New("knitId should be consisted with lower alphanumeric chars, '-', '_' and '.'")
	}
	return data(d), nil
}

func OfOutputs(r domain.Run) ([]Builder, error) {
	var datas []Builder
	outputs := slices.Map(
		r.Outputs,
		func(o domain.Assignment) domain.KnitDataBody { return o.KnitDataBody },
	)
	if r.Log != nil {
		outputs = append(outputs, r.Log.KnitDataBody)
	}
	for _, dataBody := range outputs {
		data, err := Of(dataBody)
		if err != nil {
			return nil, err
		}
		datas = append(datas, data)
	}
	return datas, nil
}

func buildDataMetaSource(vt VolumeTemplate, s Builder) *kubecore.PersistentVolumeClaim {
	return &kubecore.PersistentVolumeClaim{
		ObjectMeta: metasource.ToObjectMeta(s, vt.Namespece),
		Spec: kubecore.PersistentVolumeClaimSpec{
			AccessModes:      []kubecore.PersistentVolumeAccessMode{kubecore.ReadWriteMany},
			StorageClassName: &vt.StorageClass,
			Resources: kubecore.VolumeResourceRequirements{
				Requests: kubecore.ResourceList{
					kubecore.ResourceStorage: vt.Capacity,
				},
			},
		},
	}
}

// Subject which describing data under initilization
//
// Use this when your subject is data whose VolumeRef is to be determined.
//
// Otherwise, you have complete KnitData, use Data as subject.
type data domain.KnitDataBody // based knit_id
var _ Builder = data{}

func (ds data) Unwrap() domain.KnitDataBody {
	return domain.KnitDataBody(ds)
}

func (ds data) Component() string {
	return "data"
}

func (ds data) IdType() string {
	return "knitid"
}

func (ds data) Name() string {
	return ds.Component()
}

// points PVC name
func (ds data) Instance() string {
	// this method determins naming convention of PVC.
	return ds.VolumeRef
}

func (ds data) Id() string {
	return ds.KnitId
}

func (ds data) ObjectMeta(namespace string) kubeapimeta.ObjectMeta {
	return metasource.ToObjectMeta(ds, namespace)
}

func (ds data) Build(conf *bconf.KnitClusterConfig) *kubecore.PersistentVolumeClaim {
	vt := VolumeTemplate{
		Namespece:    conf.Namespace(),
		StorageClass: conf.DataAgent().Volume().StorageClassName(),
		Capacity:     conf.DataAgent().Volume().InitialCapacity(),
	}
	return buildDataMetaSource(vt, ds)
}
