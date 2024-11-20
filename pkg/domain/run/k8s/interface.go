package k8s

import (
	"context"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
	"github.com/opst/knitfab/pkg/domain/run/k8s/run"
	"github.com/opst/knitfab/pkg/domain/run/k8s/worker"
)

type Interface interface {
	Initialize(ctx context.Context, r domain.Run) error
	SpawnWorker(ctx context.Context, r domain.Run, envvars map[string]string) (worker.Worker, error)
	FindWorker(ctx context.Context, r domain.RunBody) (worker.Worker, error)
}

type impl struct {
	cluster cluster.Cluster
	conf    *bconf.KnitClusterConfig
	irun    run.Interface
}

func New(conf *bconf.KnitClusterConfig, cluster cluster.Cluster) Interface {
	return &impl{
		cluster: cluster,
		conf:    conf,
		irun:    run.New(cluster, conf),
	}
}

func (i *impl) Initialize(ctx context.Context, r domain.Run) error {
	return i.irun.Initialize(ctx, r)
}

func (i *impl) SpawnWorker(ctx context.Context, r domain.Run, envvars map[string]string) (worker.Worker, error) {
	ex, err := worker.New(&r, envvars)
	if err != nil {
		return nil, err
	}
	return worker.Spawn(
		ctx, i.cluster, i.conf, ex,
	)
}

func (i *impl) FindWorker(ctx context.Context, rb domain.RunBody) (worker.Worker, error) {
	return worker.Find(ctx, i.cluster, rb)
}
