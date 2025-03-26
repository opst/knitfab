package data

import (
	"fmt"
	"strings"

	bconf "github.com/opst/knitfab/v2/pkg/configs/backend"
	"github.com/opst/knitfab/v2/pkg/domain"
	"github.com/opst/knitfab/v2/pkg/domain/knitfab/k8s/metasource"
	"github.com/opst/knitfab/v2/pkg/utils/slices"
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
		return nil, fmt.Errorf(
			"knitId %s: should be consisted with lower alphanumeric chars, '-', '_' and '.'",
			knitId,
		)
	}
	if d.VolumeRef == nil {
		return nil, fmt.Errorf("%w: knit#id = %s", domain.ErrDataIsPurged, d.KnitId)
	}
	return Data{
		KnitId:    d.KnitId,
		VolumeRef: *d.VolumeRef,
	}, nil
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

// Subject which describing Data under initilization
//
// Use this when your subject is Data whose VolumeRef is to be determined.
//
// Otherwise, you have complete KnitData, use Data as subject.
type Data struct {
	KnitId    string
	VolumeRef string
}

var _ Builder = Data{}

func (ds Data) Unwrap() Data {
	return Data(ds)
}

func (ds Data) Component() string {
	return "data"
}

func (ds Data) IdType() string {
	return "knitid"
}

func (ds Data) Name() string {
	return ds.Component()
}

// points PVC name
func (ds Data) Instance() string {
	// this method determins naming convention of PVC.
	return ds.VolumeRef
}

func (ds Data) Id() string {
	return ds.KnitId
}

func (ds Data) ObjectMeta(namespace string) kubeapimeta.ObjectMeta {
	return metasource.ToObjectMeta(ds, namespace)
}

func (ds Data) Build(conf *bconf.KnitClusterConfig) *kubecore.PersistentVolumeClaim {
	vt := VolumeTemplate{
		Namespece:    conf.Namespace(),
		StorageClass: conf.DataAgent().Volume().StorageClassName(),
		Capacity:     conf.DataAgent().Volume().InitialCapacity(),
	}
	return buildDataMetaSource(vt, ds)
}
