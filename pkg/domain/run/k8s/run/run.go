package run

import (
	"context"
	"time"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/data/k8s/data"
	k8serrors "github.com/opst/knitfab/pkg/domain/errors/k8serrors"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
	"github.com/opst/knitfab/pkg/utils/retry"
)

type Interface interface {
	Initialize(ctx context.Context, r domain.Run) error
}

type impl struct {
	cluster cluster.Cluster
	conf    *bconf.KnitClusterConfig
}

func New(
	cluster cluster.Cluster,
	conf *bconf.KnitClusterConfig,
) Interface {
	return &impl{
		cluster: cluster,
		conf:    conf,
	}
}

func (i *impl) Initialize(ctx context.Context, r domain.Run) error {
	proms := []retry.Promise[cluster.PVC]{}
	builders, err := data.OfOutputs(r)
	if err != nil {
		return err
	}

	for _, b := range builders {
		pvc := i.cluster.NewPVC(
			ctx,
			retry.StaticBackoff(200*time.Millisecond),
			b.Build(i.conf),
		)
		proms = append(proms, pvc)
	}

	for nth := range proms {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p := <-proms[nth]:
			if err := p.Err; !k8serrors.AsConflict(err) {
				return err
			}
		}
	}

	return nil
}
