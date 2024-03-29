package knit

import (
	"context"
	"time"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	kdb "github.com/opst/knitfab/pkg/db"
	dataagt "github.com/opst/knitfab/pkg/workloads/dataagt"
	k8s "github.com/opst/knitfab/pkg/workloads/k8s"
	kw "github.com/opst/knitfab/pkg/workloads/worker"
	"k8s.io/client-go/kubernetes"
)

type KnitMiddlewares interface {
	BaseCluster() k8s.Cluster
	Database() kdb.KnitDatabase
	Config() *bconf.KnitClusterConfig
}

type KnitCluster interface {
	KnitMiddlewares
	Namespace() string
	SpawnDataAgent(ctx context.Context, d kdb.DataAgent, pendingDeadline time.Time) (dataagt.Dataagt, error)

	SpawnWorker(ctx context.Context, r kdb.Run) error
	GetWorker(ctx context.Context, r kdb.Run) (kw.Worker, error)
}

type knitCluster struct { // implements KnitCluster
	config   *bconf.KnitClusterConfig
	cluster  k8s.Cluster
	database kdb.KnitDatabase
}

// &knitCluster implements KnitMiddlewares
var _ KnitMiddlewares = &knitCluster{}

// &knitCluster implements KnitCluster
var _ KnitCluster = &knitCluster{}

type DatabaseConnector func(conf *bconf.KnitClusterConfig) kdb.KnitDatabase

func AttachKnitCluster(
	clientset *kubernetes.Clientset,
	config *bconf.KnitClusterConfig,
	knitdb kdb.KnitDatabase,
) KnitCluster {
	return &knitCluster{
		config: config,
		cluster: k8s.AttachCluster(
			k8s.WrapK8sClient(clientset), config.Namespace(), config.Domain(),
		),
		database: knitdb,
	}
}

func (k *knitCluster) Namespace() string {
	return k.config.Namespace()
}

func (k *knitCluster) Config() *bconf.KnitClusterConfig {
	return k.config
}

func (k *knitCluster) BaseCluster() k8s.Cluster {
	return k.cluster
}

func (k *knitCluster) Database() kdb.KnitDatabase {
	return k.database
}

func (k *knitCluster) SpawnDataAgent(ctx context.Context, d kdb.DataAgent, pendingDeadline time.Time) (dataagt.Dataagt, error) {
	return dataagt.Spawn(ctx, k.Config(), k.BaseCluster(), d, pendingDeadline)
}

func (k *knitCluster) SpawnWorker(ctx context.Context, r kdb.Run) error {
	ex, err := kw.New(&r)
	if err != nil {
		return err
	}
	if _, err := kw.Spawn(
		ctx, k.BaseCluster(), k.Config(), ex,
	); err != nil {
		return err
	}
	return nil
}

// search for the worker
func (k *knitCluster) GetWorker(ctx context.Context, r kdb.Run) (kw.Worker, error) {
	w, err := kw.Find(ctx, k.BaseCluster(), r.RunBody)
	if err != nil {
		return w, err
	}
	return w, nil
}
