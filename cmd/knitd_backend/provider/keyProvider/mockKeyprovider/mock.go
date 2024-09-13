package mockkeyprovider

import (
	"context"
	"testing"

	keyprovider "github.com/opst/knitfab/cmd/knitd_backend/provider/keyProvider"
	"github.com/opst/knitfab/pkg/workloads/keychain"
	"github.com/opst/knitfab/pkg/workloads/keychain/key"
)

type MockKeyProvider struct {
	t    *testing.T
	Impl struct {
		Provide     func(ctx context.Context, req ...keychain.KeyRequirement) (string, key.Key, error)
		GetKeychain func(ctx context.Context) (keychain.Keychain, error)
	}
}

var _ keyprovider.KeyProvider = &MockKeyProvider{}

func New(t *testing.T) *MockKeyProvider {
	return &MockKeyProvider{t: t}
}

func (m *MockKeyProvider) Provide(ctx context.Context, req ...keychain.KeyRequirement) (string, key.Key, error) {
	if m.Impl.Provide == nil {
		m.t.Fatal("Provide is not implemented")
	}
	return m.Impl.Provide(ctx, req...)
}

func (m *MockKeyProvider) GetKeychain(ctx context.Context) (keychain.Keychain, error) {
	if m.Impl.GetKeychain == nil {
		m.t.Fatal("GetKeychain is not implemented")
	}
	return m.Impl.GetKeychain(ctx)
}
