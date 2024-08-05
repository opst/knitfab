package key

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/opst/knitfab/pkg/workloads/keychain/internal"
)

type Key interface {
	// Name of the algorithm
	Alg() string

	// Expiration time of the key
	Exp() time.Time

	// Key to sign messages.
	//
	// Almost always it is Private key
	ToSign() any

	// Key to verify messages.
	//
	// Almost always it is Public key.
	ToVerify() any

	// Equal returns true if the key is equal to the other key
	Equal(k Key) bool

	// String returns the key in string format
	String() string

	// marshal returns the key in marshallKey format
	Marshal() internal.MarshalKey

	// unmarshal unmarshals the key from marshalKey format
	unmarshal(internal.MarshalKey) error
}

func Unmarshal(m internal.MarshalKey) (Key, error) {
	switch m.Alg {
	case jwt.SigningMethodHS256.Name:
		k := &hs256Key{}
		if err := k.unmarshal(m); err != nil {
			return nil, err
		}
		return k, nil
	default:
		return nil, fmt.Errorf("unsupported algorithm: %s", m.Alg)
	}
}

type KeyPolicy interface {
	// Issue a new key
	Issue() (Key, error)
}
