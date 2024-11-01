package k8s

import (
	bconf "github.com/opst/knitfab/pkg/configs/backend"
	data "github.com/opst/knitfab/pkg/domain/data/k8s"
	garbage "github.com/opst/knitfab/pkg/domain/garbage/k8s"
	keychain "github.com/opst/knitfab/pkg/domain/keychain/k8s"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
	run "github.com/opst/knitfab/pkg/domain/run/k8s"
)

type KubernetesInterfaces interface {
	DataAgant() data.Interface
	Worker() run.Interface
	KeyChain() keychain.Interface
	Garbage() garbage.Interface
}

type impl struct {
	dataAgent data.Interface
	keychain  keychain.Interface
	worker    run.Interface
	garbage   garbage.Interface
}

func New(
	cluster cluster.Cluster,
	config *bconf.KnitClusterConfig,
) KubernetesInterfaces {
	return &impl{
		dataAgent: data.New(config, cluster),
		keychain:  keychain.New(cluster),
		worker:    run.New(config, cluster),
		garbage:   garbage.New(cluster),
	}
}

func (i *impl) DataAgant() data.Interface {
	return i.dataAgent
}

func (i *impl) Worker() run.Interface {
	return i.worker
}

func (i *impl) KeyChain() keychain.Interface {
	return i.keychain
}

func (i *impl) Garbage() garbage.Interface {
	return i.garbage
}
