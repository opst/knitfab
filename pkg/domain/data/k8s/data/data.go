package data

import (
	"context"
	"errors"
	"time"

	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
	"github.com/opst/knitfab/pkg/utils/retry"
)

func CheckDataIsBound(
	ctx context.Context, kcluster cluster.Cluster, body domain.KnitDataBody,
) (bool, error) {
	_ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	result := <-kcluster.GetPVC(
		_ctx, retry.StaticBackoff(1*time.Second), body.VolumeRef,
		cluster.PVCIsBound,
	)
	if err := result.Err; err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
