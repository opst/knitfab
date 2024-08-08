package base64marshall

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
)

// Bytes is a byte slice that will be encoded as base64 when marshaled to JSON.
type Bytes []byte

// New creates a new Bytes from a raw byte slice.
func New(b []byte) Bytes {
	return Bytes(b)
}

// Bytes returns the byte slice underlying the Bytes.
func (b Bytes) Bytes() []byte {
	return []byte(b)
}

// String returns the base64 encoded string of the Bytes.
func (b Bytes) String() string {
	return base64.StdEncoding.EncodeToString(b)
}

// MarshalJSON implements the json.Marshaler interface.
func (b Bytes) MarshalJSON() ([]byte, error) {
	return []byte(`"` + b.String() + `"`), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (b *Bytes) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		*b = nil
		return nil
	}

	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	dlen := base64.StdEncoding.DecodedLen(len(s))
	decoded := make([]byte, dlen)
	if n, err := base64.StdEncoding.Decode(decoded, []byte(s)); err != nil {
		return err
	} else {
		*b = New(decoded[:n])
	}
	return nil
}
