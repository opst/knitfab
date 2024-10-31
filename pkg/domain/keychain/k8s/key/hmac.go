package key

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab/pkg/domain/keychain/k8s/internal"
	"github.com/opst/knitfab/pkg/utils/base64marshall"
)

type hs256policy struct {
	ttl    time.Duration
	keyLen uint
}

func (f hs256policy) Issue() (Key, error) {

	k := make([]byte, f.keyLen)
	if _, err := rand.Read(k); err != nil {
		return nil, err
	}

	return &hs256Key{
		exp:      time.Now().Add(f.ttl).Truncate(time.Second),
		toSign:   k,
		toVerify: k,
	}, nil
}

// HS256 returns a KeyPolicy for HMAC-SHA256 algorithm.
//
// # Args
//
// - ttl: Time to live of new keys
//
// - klen: Length of the key in *bytes*, not bits.
func HS256(ttl time.Duration, klen uint) KeyPolicy {
	return hs256policy{
		ttl:    ttl,
		keyLen: klen,
	}
}

type hs256Key struct {
	// Time to live of this key
	exp time.Time

	// Key to sign messages.
	//
	// Almost always it is Private key
	toSign []byte

	// Key to verify messages.
	//
	// Almosy always it is Public key.
	toVerify []byte
}

func (*hs256Key) Alg() string {
	return jwt.SigningMethodHS256.Name
}

func (hk *hs256Key) Exp() time.Time {
	return hk.exp
}

func (hk *hs256Key) ToSign() any {
	return hk.toSign
}

func (hk *hs256Key) ToVerify() any {
	return hk.toVerify
}

func (hk *hs256Key) Equal(k Key) bool {
	other, ok := k.(*hs256Key)
	if !ok {
		return false
	}

	return hk.exp.Equal(other.exp) &&
		bytes.Equal(hk.toSign, other.toSign) &&
		bytes.Equal(hk.toVerify, other.toVerify)
}

func (hk hs256Key) Marshal() internal.MarshalKey {
	return internal.MarshalKey{
		Alg:      hk.Alg(),
		Exp:      rfctime.RFC3339(hk.exp),
		ToSign:   base64marshall.Bytes(hk.toSign),
		ToVerify: base64marshall.Bytes(hk.toVerify),
	}
}

func (hk *hs256Key) unmarshal(mk internal.MarshalKey) error {
	if mk.Alg != hk.Alg() {
		return fmt.Errorf("invalid algorithm: %s", mk.Alg)
	}
	hk.exp = mk.Exp.Time()
	hk.toSign = mk.ToSign.Bytes()
	hk.toVerify = mk.ToVerify.Bytes()
	return nil
}

func (hk hs256Key) String() string {
	return fmt.Sprintf(
		"Key{Alg: %s, Exp: %s, ToSign: (%d bytes), ToVerify: (%d bytes)}",
		hk.Alg(), rfctime.RFC3339(hk.exp),
		len(hk.toSign), len(hk.toVerify),
	)
}
