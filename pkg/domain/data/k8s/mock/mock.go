package mock

import (
	"context"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/data/k8s/dataagt"
)

type MockK8sDataInterface struct {
	t    *testing.T
	Impl struct {
		SpawnDataAgent func(
			ctx context.Context, d domain.DataAgent, pendingDeadline time.Time,
		) (dataagt.DataAgent, error)
		FindDataAgent func(
			ctx context.Context, da domain.DataAgent,
		) (dataagt.DataAgent, error)
		ChechDataisBound func(
			ctx context.Context, da domain.KnitDataBody,
		) (bool, error)
	}
}

func New(t *testing.T) *MockK8sDataInterface {
	return &MockK8sDataInterface{t: t}
}

func (m *MockK8sDataInterface) SpawnDataAgent(
	ctx context.Context, d domain.DataAgent, pendingDeadline time.Time,
) (dataagt.DataAgent, error) {
	if m.Impl.SpawnDataAgent == nil {
		m.t.Fatal("SpawnDatAgent not implemented")
	}
	return m.Impl.SpawnDataAgent(ctx, d, pendingDeadline)
}

func (m *MockK8sDataInterface) FindDataAgent(
	ctx context.Context, da domain.DataAgent,
) (dataagt.DataAgent, error) {
	if m.Impl.FindDataAgent == nil {
		m.t.Fatal("FindDataAgent not implemented")
	}
	return m.Impl.FindDataAgent(ctx, da)
}

func (m *MockK8sDataInterface) CheckDataIsBound(
	ctx context.Context, da domain.KnitDataBody,
) (bool, error) {
	if m.Impl.ChechDataisBound == nil {
		m.t.Fatal("CheckDataIsBound not implemented")
	}
	return m.Impl.ChechDataisBound(ctx, da)
}
