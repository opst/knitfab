package internal

import (
	"bytes"
	"fmt"

	"github.com/opst/knitfab/pkg/utils/base64marshall"
	"github.com/opst/knitfab/pkg/utils/rfctime"
)

type MarshalKey struct {
	// Algorithm of this key
	Alg string `json:"alg"`

	// Time to live of this key
	Exp rfctime.RFC3339 `json:"exp"`

	// Key to sign messages.
	//
	// Almost always it is Private key
	ToSign base64marshall.Bytes `json:"toSign"`

	// Key to verify messages.
	//
	// Almosy always it is Public key.
	ToVerify base64marshall.Bytes `json:"toVerify"`
}

func (k MarshalKey) String() string {
	return fmt.Sprintf(
		"Key{Alg: %s, Exp: %s, ToSign: (%d bytes), ToVerify: (%d bytes)}",
		k.Alg, k.Exp,
		len(k.ToSign.Bytes()), len(k.ToVerify.Bytes()),
	)
}

func (k MarshalKey) Equal(other MarshalKey) bool {
	return k.Alg == other.Alg &&
		k.Exp.Equal(&other.Exp) &&
		bytes.Equal(k.ToSign.Bytes(), other.ToSign.Bytes()) &&
		bytes.Equal(k.ToVerify.Bytes(), other.ToVerify.Bytes())
}
