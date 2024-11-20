package mock

import (
	"context"
	"testing"

	"github.com/opst/knitfab/pkg/domain"
)

type MockInterface struct {
	t    *testing.T
	Impl struct {
		DestroyGarbage func(ctx context.Context, g domain.Garbage) error
	}
}

func New(t *testing.T) *MockInterface {
	return &MockInterface{t: t}
}

func (m *MockInterface) DestroyGarbage(ctx context.Context, g domain.Garbage) error {
	if m.Impl.DestroyGarbage == nil {
		m.t.Fatal("DestroyGarbage is not implemented")
	}

	return m.Impl.DestroyGarbage(ctx, g)
}
