package key_test

import (
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/opst/knitfab/pkg/workloads/keychain/key"
)

func TestHS256(t *testing.T) {

	ttl := 24 * time.Hour
	testee := key.HS256(ttl, 2048/8)
	before := time.Now().Truncate(time.Second)
	k := try.To(testee.Issue()).OrFatal(t)
	after := time.Now().Truncate(time.Second)

	t.Run("Alg", func(t *testing.T) {
		if got := k.Alg(); got != "HS256" {
			t.Errorf("Expected alg to be %q, but got %q", "HS256", got)
		}
	})

	t.Run("Exp", func(t *testing.T) {
		if got := k.Exp(); got.Before(before.Add(ttl)) || got.After(after.Add(ttl)) {
			t.Errorf(
				"Expected expiration time is between %s to %s, but got %s",
				rfctime.RFC3339(before), rfctime.RFC3339(after), rfctime.RFC3339(got),
			)
		}
	})

	t.Run("Sign and Verify (success)", func(t *testing.T) {
		claims := jwt.RegisteredClaims{
			Issuer:    "knitfab",
			Subject:   "test",
			Audience:  jwt.ClaimStrings{"any audience"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
			NotBefore: jwt.NewNumericDate(time.Now().Add(-1 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ID:        "test#1",
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		signedString := try.To(token.SignedString(k.ToSign())).OrFatal(t)

		parsed := try.To(jwt.ParseWithClaims(
			signedString, new(jwt.RegisteredClaims),
			func(token *jwt.Token) (interface{}, error) { return k.ToVerify(), nil },
		)).OrFatal(t)

		if parsedClaims, ok := parsed.Claims.(*jwt.RegisteredClaims); !ok {
			t.Fatalf("Unexpected claims type: %T", parsed.Claims)
		} else {
			if parsedClaims.Issuer != claims.Issuer {
				t.Errorf("Expected issuer to be %q, but got %q", claims.Issuer, parsedClaims.Issuer)
			}
			if parsedClaims.Subject != claims.Subject {
				t.Errorf("Expected subject to be %q, but got %q", claims.Subject, parsedClaims.Subject)
			}
			if !cmp.SliceContentEq(parsedClaims.Audience, claims.Audience) {
				t.Errorf("Expected audience to be %v, but got %v", claims.Audience, parsedClaims.Audience)
			}
			if !parsedClaims.ExpiresAt.Time.Equal(claims.ExpiresAt.Time) {
				t.Errorf("Expected expiration time to be %s, but got %s", claims.ExpiresAt.Time, parsedClaims.ExpiresAt.Time)
			}
			if !parsedClaims.NotBefore.Time.Equal(claims.NotBefore.Time) {
				t.Errorf("Expected not-before time to be %s, but got %s", claims.NotBefore.Time, parsedClaims.NotBefore.Time)
			}
			if !parsedClaims.IssuedAt.Time.Equal(claims.IssuedAt.Time) {
				t.Errorf("Expected issued-at time to be %s, but got %s", claims.IssuedAt.Time, parsedClaims.IssuedAt.Time)
			}
			if parsedClaims.ID != claims.ID {
				t.Errorf("Expected JWT ID to be %q, but got %q", claims.ID, parsedClaims.ID)
			}
		}
	})

	t.Run("Sign and Verify (failure by exp)", func(t *testing.T) {
		claims := jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-1 * time.Hour)),
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		signedString := try.To(token.SignedString(k.ToSign())).OrFatal(t)

		_, err := jwt.ParseWithClaims(
			signedString, new(jwt.RegisteredClaims),
			func(token *jwt.Token) (interface{}, error) { return k.ToVerify(), nil },
		)
		if !errors.Is(err, jwt.ErrTokenExpired) {
			t.Error("Expected error, but got nil")
		}
	})

	t.Run("Sign and Verify (failure by wrong key)", func(t *testing.T) {
		trueKey := try.To(testee.Issue()).OrFatal(t)
		wrongKey := try.To(testee.Issue()).OrFatal(t)

		claims := jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		signedString := try.To(token.SignedString(trueKey.ToSign())).OrFatal(t)

		_, err := jwt.ParseWithClaims(
			signedString, new(jwt.RegisteredClaims),
			func(token *jwt.Token) (interface{}, error) { return wrongKey.ToVerify(), nil },
		)
		if !errors.Is(err, jwt.ErrSignatureInvalid) {
			t.Error("Expected error, but got nil")
		}
	})
}
