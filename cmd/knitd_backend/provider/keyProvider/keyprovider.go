package keyprovider

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/workloads/keychain"
	"github.com/opst/knitfab/pkg/workloads/keychain/key"
)

var ErrBadNewKey = errors.New("new key is bad. It does not satisfy the requirements")

type KeyProvider interface {
	// Provide returns a key from the keychain.
	// If no key satifies options in the keychain, it issues a new key.
	Provide(ctx context.Context, option ...keychain.KeyRequirement) (string, key.Key, error)

	// GetKeychain returns the refleshed keychain in the KeyLocker.
	GetKeychain(ctx context.Context) (keychain.Keychain, error)
}

var DefaultKeyPolicy = key.HS256(3*time.Hour, 2048/8)

type Option func(*keyProvider)

func WithPolicy(policy key.KeyPolicy) Option {
	return func(kl *keyProvider) {
		kl.policy = policy
	}
}

func New(
	keychainName string,
	dbKeychain kdb.KeychainInterface,
	getKeychain func(context.Context, string) (keychain.Keychain, error),
	options ...Option,
) KeyProvider {
	base := &keyProvider{
		keychainName: keychainName,
		policy:       DefaultKeyPolicy,
		getKeychain:  getKeychain,
		dbKeychain:   dbKeychain,
	}
	for _, option := range options {
		option(base)
	}
	return base
}

type keyProvider struct {
	policy       key.KeyPolicy
	keychainName string
	getKeychain  func(context.Context, string) (keychain.Keychain, error)
	dbKeychain   kdb.KeychainInterface
}

func (kp *keyProvider) Provide(ctx context.Context, req ...keychain.KeyRequirement) (string, key.Key, error) {
	kc, err := kp.GetKeychain(ctx)
	if err != nil {
		return "", nil, err
	}

	kid, key, ok := kc.GetKey(req...)
	if !ok {
		if err := kp.dbKeychain.Lock(ctx, kc.Name(), func(ctx context.Context) error {
			_kid := uuid.NewString()
			_key, err := kp.policy.Issue()
			if err != nil {
				return err
			}
			for _, r := range req {
				if !r(_kid, _key) {
					return ErrBadNewKey
				}
			}
			kc.Set(_kid, _key)
			kid, key = _kid, _key
			return kc.Update(ctx)
		}); err != nil {
			return "", nil, err
		}
	}

	return kid, key, nil
}

func (kp *keyProvider) GetKeychain(ctx context.Context) (keychain.Keychain, error) {
	return kp.getKeychain(ctx, kp.keychainName)
}
