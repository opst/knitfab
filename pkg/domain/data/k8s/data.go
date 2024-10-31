package k8s

import (
	"context"
	"time"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/data/k8s/dataagt"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
)

type Interface interface {
	SpawnDatAgent(ctx context.Context, d domain.DataAgent, pendingDeadline time.Time) (dataagt.Dataagt, error)
}

type impl struct {
	config *bconf.KnitClusterConfig
	c      cluster.Cluster
}

func New(config *bconf.KnitClusterConfig, c cluster.Cluster) Interface {
	return &impl{
		config: config,
		c:      c,
	}
}

func (i *impl) SpawnDatAgent(ctx context.Context, d domain.DataAgent, pendingDeadline time.Time) (dataagt.Dataagt, error) {
	return dataagt.Spawn(ctx, i.config, i.c, d, pendingDeadline)
}
