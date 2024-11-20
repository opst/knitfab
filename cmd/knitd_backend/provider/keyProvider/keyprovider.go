package keyprovider

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	kdb "github.com/opst/knitfab/pkg/domain/keychain/db"
	keychain "github.com/opst/knitfab/pkg/domain/keychain/k8s"
	"github.com/opst/knitfab/pkg/domain/keychain/k8s/key"
)

var ErrBadNewKey = errors.New("new key is bad. It does not satisfy the requirements")

// KeyProvider provides a flesh key from the base keychain and rotates the keys if needed.
//
// (It is a wrapper around the keychain.Keychain.)
type KeyProvider interface {
	// Provide returns a key from the keychain.
	//
	// Expired keys will be purged.
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

// New returns a new KeyProvider.
//
// # Args
//
// - keychainName: Name of the keychain
//
// - dbKeychain: Keychain in the database. With this, KeyProvider synchronizes Key rotatiton.
//
// - getKeychain: Function to get the keychain by the keychainName.
//
// - options: Options to configure the KeyProvider.
//
// # Returns
//
// - KeyProvider
func New(
	dbKeychain kdb.KeychainInterface,
	getKeychain func(context.Context) (keychain.Keychain, error),
	options ...Option,
) KeyProvider {
	base := &keyProvider{
		policy:      DefaultKeyPolicy,
		getKeychain: getKeychain,
		dbKeychain:  dbKeychain,
	}
	for _, option := range options {
		option(base)
	}
	return base
}

type keyProvider struct {
	policy      key.KeyPolicy
	getKeychain func(context.Context) (keychain.Keychain, error)
	dbKeychain  kdb.KeychainInterface
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
	return kp.getKeychain(ctx)
}
