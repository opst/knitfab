package mock

import (
	"context"
	"testing"

	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/run/k8s"
	"github.com/opst/knitfab/pkg/domain/run/k8s/worker"
)

type MockRunInterface struct {
	t    *testing.T
	Impl struct {
		Initialize  func(ctx context.Context, r domain.Run) error
		FindWorker  func(ctx context.Context, r domain.RunBody) (worker.Worker, error)
		SpawnWorker func(ctx context.Context, r domain.Run, envvars map[string]string) (worker.Worker, error)
	}
}

var _ k8s.Interface = &MockRunInterface{}

func New(t *testing.T) *MockRunInterface {
	return &MockRunInterface{
		t: t,
	}
}

func (m *MockRunInterface) Initialize(ctx context.Context, r domain.Run) error {
	if m.Impl.Initialize == nil {
		m.t.Fatal("Initialize is not implemented")
	}
	return m.Impl.Initialize(ctx, r)
}

func (m *MockRunInterface) FindWorker(ctx context.Context, r domain.RunBody) (worker.Worker, error) {
	if m.Impl.FindWorker == nil {
		m.t.Fatal("FindWorker is not implemented")
	}
	return m.Impl.FindWorker(ctx, r)
}

func (m *MockRunInterface) SpawnWorker(ctx context.Context, r domain.Run, envvars map[string]string) (worker.Worker, error) {
	if m.Impl.SpawnWorker == nil {
		m.t.Fatal("SpawnWorker is not implemented")
	}
	return m.Impl.SpawnWorker(ctx, r, envvars)
}
