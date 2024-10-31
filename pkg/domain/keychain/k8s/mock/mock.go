package mockkeychain

import (
	"context"
	"testing"

	keychain "github.com/opst/knitfab/pkg/domain/keychain/k8s"
	"github.com/opst/knitfab/pkg/domain/keychain/k8s/key"
)

type MockKeychain struct {
	t    *testing.T
	Impl struct {
		Name   func() string
		GetKey func(options ...keychain.KeyRequirement) (string, key.Key, bool)
		Add    func(kid string, k key.Key)
		Set    func(kid string, k key.Key)
		Delete func(kid string)
		Update func(ctx context.Context) error
	}
}

var _ keychain.Keychain = (*MockKeychain)(nil)

func New(t *testing.T) *MockKeychain {
	return &MockKeychain{t: t}
}

func (m *MockKeychain) Name() string {
	if m.Impl.Name == nil {
		m.t.Fatal("Name not implemented")
	}
	return m.Impl.Name()
}

func (m *MockKeychain) GetKey(options ...keychain.KeyRequirement) (string, key.Key, bool) {
	if m.Impl.GetKey == nil {
		m.t.Fatal("Get not implemented")
	}
	return m.Impl.GetKey(options...)
}

func (m *MockKeychain) Add(kid string, k key.Key) {
	if m.Impl.Add == nil {
		m.t.Fatal("Add not implemented")
	}
	m.Impl.Add(kid, k)
}

func (m *MockKeychain) Set(kid string, k key.Key) {
	if m.Impl.Set == nil {
		m.t.Fatal("Set not implemented")
	}
	m.Impl.Set(kid, k)
}

func (m *MockKeychain) Delete(kid string) {
	if m.Impl.Delete == nil {
		m.t.Fatal("Delete not implemented")
	}
	m.Impl.Delete(kid)
}

func (m *MockKeychain) Update(ctx context.Context) error {
	if m.Impl.Update == nil {
		m.t.Fatal("Update not implemented")
	}
	return m.Impl.Update(ctx)
}
