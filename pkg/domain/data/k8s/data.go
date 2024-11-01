package k8s

import (
	"context"
	"time"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/data/k8s/data"
	"github.com/opst/knitfab/pkg/domain/data/k8s/dataagt"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
)

type Interface interface {
	SpawnDataAgent(ctx context.Context, d domain.DataAgent, pendingDeadline time.Time) (dataagt.DataAgent, error)
	FindDataAgent(ctx context.Context, da domain.DataAgent) (dataagt.DataAgent, error)
	CheckDataIsBound(ctx context.Context, da domain.KnitDataBody) (bool, error)
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

func (i *impl) SpawnDataAgent(ctx context.Context, d domain.DataAgent, pendingDeadline time.Time) (dataagt.DataAgent, error) {
	return dataagt.Spawn(ctx, i.config, i.c, d, pendingDeadline)
}

func (i *impl) FindDataAgent(ctx context.Context, da domain.DataAgent) (dataagt.DataAgent, error) {
	return dataagt.Find(ctx, i.c, da)
}

func (i *impl) CheckDataIsBound(ctx context.Context, da domain.KnitDataBody) (bool, error) {
	return data.CheckDataIsBound(ctx, i.c, da)
}
