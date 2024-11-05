package knitfab

import (
	"context"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	connk8s "github.com/opst/knitfab/pkg/conn/k8s"
	"github.com/opst/knitfab/pkg/domain/data"
	"github.com/opst/knitfab/pkg/domain/garbage"
	"github.com/opst/knitfab/pkg/domain/keychain"
	"github.com/opst/knitfab/pkg/domain/knitfab/db/postgres"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
	"github.com/opst/knitfab/pkg/domain/plan"
	"github.com/opst/knitfab/pkg/domain/run"
	"github.com/opst/knitfab/pkg/domain/schema"
	"k8s.io/client-go/kubernetes"
)

type Knitfab interface {
	Config() *bconf.KnitClusterConfig

	Data() data.Interface
	Run() run.Interface
	Plan() plan.Interface

	Garbage() garbage.Interface
	Schema() schema.Interface
	Keychain() keychain.Interface
}

type knitfab struct {
	config *bconf.KnitClusterConfig
	cluser cluster.Cluster

	data data.Interface
	run  run.Interface
	plan plan.Interface

	garbage  garbage.Interface
	schema   schema.Interface
	keychain keychain.Interface
}

func Default(
	ctx context.Context,
	config *bconf.KnitClusterConfig,
	options ...Option,
) (Knitfab, error) {
	clientset := connk8s.ConnectToK8s()
	return New(ctx, config, clientset, options...)
}

func New(
	ctx context.Context,
	config *bconf.KnitClusterConfig,
	clientset *kubernetes.Clientset,
	options ...Option,
) (Knitfab, error) {
	opt := &_options{}
	for _, o := range options {
		o(opt)
	}

	pg, err := postgres.New(ctx, config.Database(), opt.pg...)
	if err != nil {
		return nil, err
	}

	k8sclient := cluster.WrapK8sClient(clientset)
	cluster := cluster.AttachCluster(k8sclient, config.Namespace(), config.Domain())

	k8sifs := k8s.New(cluster, config)

	return &knitfab{
		config: config,
		cluser: cluster,

		data: data.New(pg.Data(), k8sifs.DataAgant()),
		run:  run.New(pg.Run(), k8sifs.Worker()),
		plan: plan.New(pg.Plan()),

		garbage:  garbage.New(pg.Garbage(), k8sifs.Garbage()),
		schema:   schema.New(pg.Schema()),
		keychain: keychain.New(pg.Keychain(), k8sifs.KeyChain()),
	}, nil
}

type Option func(*_options)

type _options struct {
	pg []postgres.Option
}

func WithSchemaRepository(repository string) Option {
	return func(o *_options) {
		o.pg = append(o.pg, postgres.WithSchemaRepository(repository))
	}
}

func (k *knitfab) Config() *bconf.KnitClusterConfig {
	return k.config
}

// deplicated.
//
// Move dependencies from this to the specific domain interfaces.
func (k *knitfab) Cluster() cluster.Cluster {
	return k.cluser
}

func (k *knitfab) Data() data.Interface {
	return k.data
}

func (k *knitfab) Run() run.Interface {
	return k.run
}

func (k *knitfab) Plan() plan.Interface {
	return k.plan
}

func (k *knitfab) Garbage() garbage.Interface {
	return k.garbage
}

func (k *knitfab) Schema() schema.Interface {
	return k.schema
}

func (k *knitfab) Keychain() keychain.Interface {
	return k.keychain
}
