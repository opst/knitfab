package mocks

import (
	"context"
	"errors"
)

type MockKeychainInterface struct {
	Impl struct {
		Lock func(context.Context, string, func(context.Context) error) error
	}
}

func NewMockKeychainInterface() *MockKeychainInterface {
	return &MockKeychainInterface{}
}

func (m *MockKeychainInterface) Lock(ctx context.Context, name string, criticalSection func(context.Context) error) error {
	if m.Impl.Lock == nil {
		return errors.New("[MOCK] not implemented")
	}
	return m.Impl.Lock(ctx, name, criticalSection)
}
