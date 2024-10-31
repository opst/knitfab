// this package provide "mock" implementation of database for testing.
package mocks

import (
	"context"
	"errors"

	"github.com/opst/knitfab/pkg/domain"
)

type MockGarbageInterface struct {
	Impl struct {
		Pop func(context.Context, func(domain.Garbage) error) (bool, error)
	}
}

func NewMockGarbageInterface() *MockGarbageInterface {
	return &MockGarbageInterface{}
}
func (m *MockGarbageInterface) Pop(ctx context.Context, callback func(domain.Garbage) error) (bool, error) {
	if m.Impl.Pop == nil {
		return false, errors.New("[MOCK] not implemented")
	}
	return m.Impl.Pop(ctx, callback)
}
