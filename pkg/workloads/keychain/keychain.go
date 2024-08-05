package keychain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/opst/knitfab/pkg/workloads/k8s"
	"github.com/opst/knitfab/pkg/workloads/keychain/internal"
	"github.com/opst/knitfab/pkg/workloads/keychain/key"
	kubeapierr "k8s.io/apimachinery/pkg/api/errors"
	applyconfigurationsCorev1 "k8s.io/client-go/applyconfigurations/core/v1"

	jwt "github.com/golang-jwt/jwt/v5"
)

var ErrNoKeyFound error = errors.New("no key found")
var ErrInvalidToken error = errors.New("invalid token")

const keychainItem = "keychain"

// NewJWS signs for claim and returns a JWS (JSON Web Signature) token string
//
// # Args
//
// - kid: Key ID
//
// - k: Key to sign
//
// - claims: Claims to be signed
//
// # Returns
//
// - string: JWT token string
//
// - error: from [jwt.Token.SignedString]
func NewJWS[C jwt.Claims](kid string, k key.Key, claims C) (string, error) {
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tok.Header["kid"] = kid
	return tok.SignedString(k.ToSign())
}

// VerifyJWS verifies a JWS (JSON Web Signature) token and returns the claims
//
// # Args
//
// - keychain: Keychain to find the key to verify the token
//
// - token: JWT token string
//
// # Returns
//
// - C: Claims.The type C should be a pointer to a struct that implements [jwt.Claims] and [json.Unmarshaler].
//
// - error: can be [ErrNoKeyFound] when available key is not found in the Keychain,
// [ErrInvalidClaimType] when the claims type is not C (in type args),
// or any errors from [jwt.ParseWithClaims]
func VerifyJWS[C jwt.Claims](keychain Keychain, token string) (C, error) {
	now := time.Now()

	_c := *new(C)

	{
		rc := reflect.ValueOf(_c)
		if rc.Kind() != reflect.Ptr {
			return *new(C), errors.New("claims type must be a pointer")
		}

		val := reflect.New(rc.Type().Elem()).Interface()
		cp := val.(C)
		_c = cp
	}

	tok, err := jwt.ParseWithClaims(token, _c, func(t *jwt.Token) (interface{}, error) {
		q := []KeyRequirement{
			WithExpAfter(now),
			WithAlg(t.Method.Alg()),
		}
		if kid, ok := t.Header["kid"].(string); ok {
			q = append(q, WithKeyId(kid))
		}
		_, k, ok := keychain.GetKey(q...)
		if !ok {
			return nil, ErrNoKeyFound
		}
		return k.ToVerify(), nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrTokenMalformed) {
			return *new(C), errors.Join(ErrInvalidToken, err)
		}
		if errors.Is(err, jwt.ErrSignatureInvalid) {
			return *new(C), errors.Join(ErrInvalidToken, err)
		}
		if errors.Is(err, jwt.ErrTokenExpired) {
			return *new(C), errors.Join(ErrInvalidToken, err)
		}
		return *new(C), err
	}
	if c, ok := tok.Claims.(C); ok {
		return c, nil
	} else {
		return *new(C), fmt.Errorf("%w: unexpected claims type: %T", ErrInvalidToken, tok.Claims)
	}
}

type KeyRequirement func(kid string, k key.Key) bool

// WithAlg returns a KeychainGetOption that filters the key by the algorithm.
func WithAlg(alg string) KeyRequirement {
	return func(_ string, k key.Key) bool {
		return k.Alg() == alg
	}
}

// WithExpAfter returns a KeychainGetOption that filters the key by the expiration time.
//
// It returns true if the key's expiration time is after the given time.
func WithExpAfter(t time.Time) KeyRequirement {
	return func(_ string, k key.Key) bool {
		return k.Exp().After(t)
	}
}

// WithKeyId returns a KeychainGetOption that filters the key by the Key ID.
func WithKeyId(kid string) KeyRequirement {
	return func(_kid string, _ key.Key) bool {
		return _kid == kid
	}
}

type Keychain interface {
	// Name of the keychain
	Name() string

	// GetKey a key from the keychain
	//
	// # Args
	//
	// - req: Requirements of the key. If multiple keys satisfy requirements, random one is returned.
	//
	// # Returns
	//
	// - string: Key ID of the key found. If not found, it returns an empty string
	//
	// - Key: The key found. If not found, it returns an empty key
	//
	// - bool: True if the key is found
	GetKey(req ...KeyRequirement) (string, key.Key, bool)

	// Set a key in the keychain. If the key for Key ID exists, it is overwritten.
	//
	// # Args
	//
	// - kid: Key ID
	//
	// - key: Key to set
	Set(kid string, key key.Key)

	// Delete a key from the keychain
	Delete(kid string)

	// Update the keychain.
	//
	// This method store only unexpired keys and remove expired ones.
	//
	// # Args
	//
	// - ctx: Context
	//
	// # Returns
	//
	// - error: from [k8s.Cluster.UpsertSecret]
	Update(ctx context.Context) error
}

type keychain struct {
	name    string
	keys    map[string]key.Key
	cluster k8s.Cluster
}

func Get(ctx context.Context, cluster k8s.Cluster, keychainName string) (Keychain, error) {
	kc := &keychain{
		name:    keychainName,
		keys:    make(map[string]key.Key),
		cluster: cluster,
	}
	secret, err := cluster.GetSecret(ctx, kc.name)

	if kubeapierr.IsNotFound(err) {
		return kc, nil
	} else if err != nil {
		return nil, err
	}

	mkeys := make(map[string]internal.MarshalKey)
	kcraw, ok := secret.Data()[keychainItem]
	if ok {
		if err := json.Unmarshal(kcraw, &mkeys); err != nil {
			return nil, err
		}
		for kid, m := range mkeys {
			k, err := key.Unmarshal(m)
			if err != nil {
				return nil, err
			}
			kc.keys[kid] = k
		}
	}
	return kc, nil
}

func (kc *keychain) Name() string {
	return kc.name
}

func (kc *keychain) GetKey(req ...KeyRequirement) (string, key.Key, bool) {
KEY:
	for kid, key := range kc.keys {
		for _, r := range req {
			ok := r(kid, key)
			if !ok {
				continue KEY
			}
		}
		return kid, key, true
	}

	return "", nil, false
}

func (kc *keychain) Set(kid string, key key.Key) {
	kc.keys[kid] = key
}

func (kc *keychain) Delete(kid string) {
	delete(kc.keys, kid)
}

func (kc *keychain) Update(ctx context.Context) error {
	now := time.Now()

	keys := make(map[string]internal.MarshalKey)
	for kid, key := range kc.keys {
		if key.Exp().After(now) {
			keys[kid] = key.Marshal()
		}
	}

	keysMarshalled, err := json.Marshal(keys)
	if err != nil {
		return err
	}

	secretData := map[string][]byte{
		keychainItem: keysMarshalled,
	}

	s := applyconfigurationsCorev1.Secret(kc.name, kc.cluster.Namespace()).
		WithData(secretData)

	if s, err := kc.cluster.UpsertSecret(ctx, s); err != nil {
		return err
	} else {
		marshalKeys := make(map[string]internal.MarshalKey)
		kcraw, ok := s.Data()[keychainItem]
		if ok {
			if err := json.Unmarshal(kcraw, &marshalKeys); err != nil {
				return err
			}
		}
		kc.keys = make(map[string]key.Key)

		for kid, m := range marshalKeys {
			k, err := key.Unmarshal(m)
			if err != nil {
				return err
			}
			kc.keys[kid] = k
		}
	}
	return nil
}
