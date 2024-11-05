package k8s

import (
	"context"
	"time"

	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
	"github.com/opst/knitfab/pkg/utils/retry"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
)

type Interface interface {
	DestroyGarbage(ctx context.Context, g domain.Garbage) error
}

type impl struct {
	c cluster.Cluster
}

func New(c cluster.Cluster) Interface {
	return &impl{c: c}
}

func (i *impl) DestroyGarbage(ctx context.Context, g domain.Garbage) error {
	ret := <-i.c.DeletePVC(ctx, retry.StaticBackoff(50*time.Millisecond), g.VolumeRef)
	if err := ret.Err; err != nil {
		if kubeerr.IsNotFound(err) { // it is okay if the PVC is already deleted
			return nil
		}
		return err
	}
	return nil
}
