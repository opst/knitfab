package keychain_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	testutilctx "github.com/opst/knitfab/internal/testutils/context"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/opst/knitfab/pkg/workloads/k8s/testenv"
	"github.com/opst/knitfab/pkg/workloads/keychain"
	"github.com/opst/knitfab/pkg/workloads/keychain/key"
	v1 "k8s.io/api/core/v1"
	kubeapimeta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestKeychain(t *testing.T) {

	ctx, cancel := testutilctx.WithTest(context.Background(), t)
	defer cancel()

	cluster, clientset := testenv.NewCluster(t)
	keychainName := "keychain1"

	{
		t.Cleanup(func() {
			clientset.CoreV1().
				Secrets(cluster.Namespace()).
				Delete(
					context.Background(),
					keychainName,
					kubeapimeta.DeleteOptions{},
				)
		})
		if _, err := clientset.CoreV1().
			Secrets(cluster.Namespace()).
			Create(
				ctx,
				&v1.Secret{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name: keychainName,
						Labels: map[string]string{
							"knitfab/test":  "true",
							"example/label": "example",
						},
						Annotations: map[string]string{
							"knitfab/test":       "true",
							"example/annotation": "example",
						},
					},
					Type:      v1.SecretTypeOpaque,
					Immutable: pointer.Ref(false),
					Data:      map[string][]byte{}, // start as empty secret
				},
				kubeapimeta.CreateOptions{},
			); err != nil {
			t.Fatal(err)
		}
	}

	// test 1. : When Geting a empty keychain, it should return empty one.
	kc1 := try.To(keychain.Get(ctx, cluster, keychainName)).OrFatal(t)
	{
		if got := kc1.Name(); got != keychainName {
			t.Errorf("Expected keychain name to be %q, but got %q", keychainName, got)
		}

		if _, _, found := kc1.GetKey(); found {
			t.Errorf("Expected keychain to be empty, but found a key")
		}
	}

	// test 2. : When setting a key in the keychain, it should be found.
	kidKey1 := "key1"
	key1 := try.To(key.HS256(24*time.Hour, 2048/8).Issue()).OrFatal(t)
	{
		kc1.Set(kidKey1, key1)

		if kid, key, found := kc1.GetKey(); !found {
			t.Errorf("Expected key to be found, but not found")
		} else {
			if kid != kidKey1 {
				t.Errorf("Expected key ID to be %q, but got %q", kidKey1, kid)
			}
			if !key.Equal(key1) {
				t.Errorf("Expected key to be %s, but got %s", key1, key)
			}
		}

		// query by key ID
		if kid, key, found := kc1.GetKey(keychain.WithKeyId(kidKey1)); !found {
			t.Errorf("Expected key to be found, but not found")
		} else {
			if kid != kidKey1 {
				t.Errorf("Expected key ID to be %q, but got %q", kidKey1, kid)
			}
			if !key.Equal(key1) {
				t.Errorf("Expected key to be %s, but got %s", key1, key)
			}
		}

		if kid, key, found := kc1.GetKey(keychain.WithKeyId("???unknown???")); found {
			t.Errorf("Expected key to be not found, but found: %q with alg %s", kid, key.Alg())
		}

		// query by algorithm
		if kid, key, found := kc1.GetKey(keychain.WithAlg("HS256")); !found {
			t.Errorf("Expected key to be found, but not found")
		} else {
			if kid != kidKey1 {
				t.Errorf("Expected key ID to be %q, but got %q", kidKey1, kid)
			}
			if !key.Equal(key1) {
				t.Errorf("Expected key to be %s, but got %s", key1, key)
			}
		}

		if kid, key, found := kc1.GetKey(keychain.WithAlg("???unknown???")); found {
			t.Errorf("Expected key to be not found, but found: %q with alg %s", kid, key.Alg())
		}

		// query by expiration time
		if kid, key, found := kc1.GetKey(keychain.WithExpAfter(key1.Exp().Add(-1 * time.Second))); !found {
			t.Errorf("Expected key to be found, but not found")
		} else {
			if kid != kidKey1 {
				t.Errorf("Expected key ID to be %q, but got %q", kidKey1, kid)
			}
			if !key.Equal(key1) {
				t.Errorf("Expected key to be %s, but got %s", key1, key)
			}
		}

		if kid, key, found := kc1.GetKey(keychain.WithExpAfter(key1.Exp())); found {
			t.Errorf("Expected key to be not found, but found: %q with alg %s", kid, key.Alg())
		}
	}

	kidKey2 := "key2"
	key2 := try.To(key.HS256(24*time.Hour+30*time.Minute, 2048/8).Issue()).OrFatal(t)
	{
		kc1.Set(kidKey2, key2)

		// query by kid
		if kid, key, found := kc1.GetKey(keychain.WithKeyId(kidKey2)); !found {
			t.Errorf("Expected key to be found, but not found")
		} else {
			if kid != kidKey2 {
				t.Errorf("Expected key ID to be %q, but got %q", kidKey2, kid)
			}
			if !key.Equal(key2) {
				t.Errorf("Expected key to be %s, but got %s", key2, key)
			}
		}
		if kid, key, found := kc1.GetKey(keychain.WithKeyId(kidKey1)); !found {
			t.Errorf("Expected key to be found, but not found")
		} else {
			if kid != kidKey1 {
				t.Errorf("Expected key ID to be %q, but got %q", kidKey1, kid)
			}
			if !key.Equal(key1) {
				t.Errorf("Expected key to be %s, but got %s", key1, key)
			}
		}

		// query by exp
		if kid, key, found := kc1.GetKey(keychain.WithExpAfter(key2.Exp().Add(-1 * time.Second))); !found {
			t.Errorf("Expected key to be found, but not found")
		} else {
			if kid != kidKey2 {
				t.Errorf("Expected key ID to be %q, but got %q", kidKey2, kid)
			}
			if !key.Equal(key2) {
				t.Errorf("Expected key to be %s, but got %s", key2, key)
			}
		}

		if kid, key, found := kc1.GetKey(keychain.WithExpAfter(key2.Exp())); found {
			t.Errorf("Expected key to be not found, but found: %s (key = %s)", kid, key)
		}
	}

	// test 3. : Unless the keychain is Updated, the key should be found in k8s.
	{
		sec := try.To(cluster.GetSecret(ctx, keychainName)).OrFatal(t)
		if !cmp.MapEqWith(sec.Data(), map[string][]byte{}, bytes.Equal) {
			t.Errorf("Expected secret to be empty, but got %v", sec.Data())
		}
	}

	// test 4. : Update should success.
	{
		if err := kc1.Update(ctx); err != nil {
			t.Fatalf("Failed to update keychain: %v", err)
		}
	}

	// test 5. : After the keychain is Updated, the saved keychain can be retreived.
	{
		kc := try.To(keychain.Get(ctx, cluster, keychainName)).OrFatal(t)

		// query by kid
		if kid, key, found := kc.GetKey(keychain.WithKeyId(kidKey1)); !found {
			t.Errorf("Expected key to be found, but not found")
		} else {
			if kid != kidKey1 {
				t.Errorf("Expected key ID to be %q, but got %q", kidKey1, kid)
			}
			if !key.Equal(key1) {
				t.Errorf("Expected key to be %s, but got %s", key1, key)
			}
		}
		if kid, key, found := kc.GetKey(keychain.WithKeyId(kidKey2)); !found {
			t.Errorf("Expected key to be found, but not found")
		} else {
			if kid != kidKey2 {
				t.Errorf("Expected key ID to be %q, but got %q", kidKey2, kid)
			}
			if !key.Equal(key2) {
				t.Errorf("Expected key to be %s, but got %s", key2, key)
			}
		}
	}

	// test 6. : Secrets' metadata is not changed by the keychain.
	{
		sec := try.To(
			clientset.CoreV1().
				Secrets(cluster.Namespace()).
				Get(ctx, keychainName, kubeapimeta.GetOptions{}),
		).OrFatal(t)

		// labels
		if got := sec.Labels; !cmp.MapEq(got, map[string]string{
			"knitfab/test":  "true",
			"example/label": "example",
		}) {
			t.Errorf("Unexpected labels: %v", got)
		}

		// annotations
		if got := sec.Annotations; !cmp.MapEq(got, map[string]string{
			"knitfab/test":       "true",
			"example/annotation": "example",
		}) {
			t.Errorf("Unexpected annotations: %v", got)
		}

		// Type
		if got := sec.Type; got != v1.SecretTypeOpaque {
			t.Errorf("Expected secret type to be %q, but got %q", v1.SecretTypeOpaque, got)
		}

		// Immutable
		if got := sec.Immutable; got != nil && *got {
			t.Errorf("Expected secret to be mutable, but got immutable")
		}
	}

	// test 7. : When the key is deleted, it should not be found.
	{
		kc1.Delete(kidKey1)
		if err := kc1.Update(ctx); err != nil {
			t.Fatal(err)
		}

		kc := try.To(keychain.Get(ctx, cluster, keychainName)).OrFatal(t)

		// query by kid
		if kid, key, found := kc.GetKey(keychain.WithKeyId(kidKey1)); found {
			t.Errorf("Expected key to be not found, but found: %q with (key = %s)", kid, key)
		}
		if kid, key, found := kc.GetKey(); !found {
			t.Errorf("Expected key to be found, but not found")
		} else {
			if kid != kidKey2 {
				t.Errorf("Expected key ID to be %q, but got %q", kidKey2, kid)
			}
			if !key.Equal(key2) {
				t.Errorf("Expected key to be %s, but got %s", key2, key)
			}
		}
	}

	// test 8. : When a key is expired, it should not be found.
	{
		key3id := "key3"
		key3 := try.To(key.HS256(1*time.Second, 2048/8).Issue()).OrFatal(t)
		kc1.Set(key3id, key3)
		if err := kc1.Update(ctx); err != nil {
			t.Fatal(err)
		}

		kcBefore := try.To(keychain.Get(ctx, cluster, keychainName)).OrFatal(t)
		if kid, key, found := kcBefore.GetKey(keychain.WithKeyId(key3id)); !found {
			t.Errorf("Expected key to be found, but not found")
		} else {
			if kid != key3id {
				t.Errorf("Expected key ID to be %q, but got %q", key3id, kid)
			}
			if !key.Equal(key3) {
				t.Errorf("Expected key to be %s, but got %s", key3, key)
			}
		}

		time.Sleep(time.Until(key3.Exp().Add(1 * time.Second)))
		key4id := "key4"
		key4 := try.To(key.HS256(5*time.Second, 2048/8).Issue()).OrFatal(t)
		kcBefore.Set(key4id, key4)
		kcBefore.Update(ctx)

		kcAfter := try.To(keychain.Get(ctx, cluster, keychainName)).OrFatal(t)
		if kid, key, found := kcAfter.GetKey(keychain.WithKeyId(key3id)); found {
			t.Errorf("Expected key to be not found, but found: %q (key = %s)", kid, key)
		}
		if kid, key, found := kcAfter.GetKey(keychain.WithKeyId(key4id)); !found {
			t.Errorf("Expected key to be found, but not found")
		} else {
			if kid != key4id {
				t.Errorf("Expected key ID to be %q, but got %q", key4id, kid)
			}
			if !key.Equal(key4) {
				t.Errorf("Expected key to be %s, but got %s", key4, key)
			}
		}
	}
}

func TestSignAndVerifyJWT(t *testing.T) {

	t.Run("success", func(t *testing.T) {
		ctx, cancel := testutilctx.WithTest(context.Background(), t)
		defer cancel()

		cluster, clientset := testenv.NewCluster(t)

		keychainName := "keychain1"
		{
			t.Cleanup(func() {
				clientset.CoreV1().
					Secrets(cluster.Namespace()).
					Delete(
						context.Background(),
						keychainName,
						kubeapimeta.DeleteOptions{},
					)
			})
			if _, err := clientset.CoreV1().
				Secrets(cluster.Namespace()).
				Create(
					ctx,
					&v1.Secret{
						ObjectMeta: kubeapimeta.ObjectMeta{
							Name: keychainName,
							Labels: map[string]string{
								"knitfab/test":  "true",
								"example/label": "example",
							},
							Annotations: map[string]string{
								"knitfab/test":       "true",
								"example/annotation": "example",
							},
						},
						Type:      v1.SecretTypeOpaque,
						Immutable: pointer.Ref(false),
						Data:      map[string][]byte{}, // start as empty secret
					},
					kubeapimeta.CreateOptions{},
				); err != nil {
				t.Fatal(err)
			}
		}

		kc := try.To(keychain.Get(ctx, cluster, keychainName)).OrFatal(t)

		kid := "key1"
		testee := try.To(key.HS256(24*time.Hour, 2048/8).Issue()).OrFatal(t)
		kc.Set(kid, testee)

		claims := jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
		}
		signedString := try.To(keychain.NewJWS(kid, testee, claims)).OrFatal(t)

		c := try.To(keychain.VerifyJWS[*jwt.RegisteredClaims](kc, signedString)).OrFatal(t)

		if !c.ExpiresAt.Time.Equal(claims.ExpiresAt.Time) {
			t.Errorf("Expected expiration time to be %s, but got %s", claims.ExpiresAt.Time, c.ExpiresAt.Time)
		}
	})

	t.Run("failure by exp of token", func(t *testing.T) {
		ctx, cancel := testutilctx.WithTest(context.Background(), t)
		defer cancel()

		cluster, clientset := testenv.NewCluster(t)

		keychainName := "keychain1"
		{
			t.Cleanup(func() {
				clientset.CoreV1().
					Secrets(cluster.Namespace()).
					Delete(
						context.Background(),
						keychainName,
						kubeapimeta.DeleteOptions{},
					)
			})
			if _, err := clientset.CoreV1().
				Secrets(cluster.Namespace()).
				Create(
					ctx,
					&v1.Secret{
						ObjectMeta: kubeapimeta.ObjectMeta{
							Name: keychainName,
							Labels: map[string]string{
								"knitfab/test":  "true",
								"example/label": "example",
							},
							Annotations: map[string]string{
								"knitfab/test":       "true",
								"example/annotation": "example",
							},
						},
						Type:      v1.SecretTypeOpaque,
						Immutable: pointer.Ref(false),
						Data:      map[string][]byte{}, // start as empty secret
					},
					kubeapimeta.CreateOptions{},
				); err != nil {
				t.Fatal(err)
			}
		}

		kc := try.To(keychain.Get(ctx, cluster, keychainName)).OrFatal(t)

		kid := "key1"
		testee := try.To(key.HS256(24*time.Hour, 2048/8).Issue()).OrFatal(t)
		kc.Set(kid, testee)

		claims := jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-1 * time.Hour)),
		}
		signedString := try.To(keychain.NewJWS(kid, testee, claims)).OrFatal(t)

		_, err := keychain.VerifyJWS[*jwt.RegisteredClaims](kc, signedString)
		if !errors.Is(err, keychain.ErrInvalidToken) {
			t.Errorf("Expected error %v, but got %v", jwt.ErrTokenExpired, err)
		}
		if !errors.Is(err, jwt.ErrTokenExpired) {
			t.Errorf("Expected error %v, but got %v", jwt.ErrTokenExpired, err)
		}
	})

	t.Run("failure by wrong key", func(t *testing.T) {
		ctx, cancel := testutilctx.WithTest(context.Background(), t)
		defer cancel()

		cluster, clientset := testenv.NewCluster(t)

		keychainName := "keychain1"
		{
			if _, err := clientset.CoreV1().
				Secrets(cluster.Namespace()).
				Create(
					ctx,
					&v1.Secret{
						ObjectMeta: kubeapimeta.ObjectMeta{
							Name: keychainName,
							Labels: map[string]string{
								"knitfab/test":  "true",
								"example/label": "example",
							},
							Annotations: map[string]string{
								"knitfab/test":       "true",
								"example/annotation": "example",
							},
						},
						Type:      v1.SecretTypeOpaque,
						Immutable: pointer.Ref(false),
						Data:      map[string][]byte{}, // start as empty secret
					},
					kubeapimeta.CreateOptions{},
				); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				clientset.CoreV1().
					Secrets(cluster.Namespace()).
					Delete(
						context.Background(),
						keychainName,
						kubeapimeta.DeleteOptions{},
					)
			})
		}

		kc := try.To(keychain.Get(ctx, cluster, keychainName)).OrFatal(t)
		keyPolicy := key.HS256(24*time.Hour, 2048/8)

		kid := "key1"
		testee := try.To(keyPolicy.Issue()).OrFatal(t)
		kc.Set(kid, testee)

		claims := jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
		}
		signedString := try.To(keychain.NewJWS(kid, testee, claims)).OrFatal(t)

		wrongKey := try.To(keyPolicy.Issue()).OrFatal(t)
		kc.Set(kid, wrongKey)

		_, err := keychain.VerifyJWS[*jwt.RegisteredClaims](kc, signedString)
		if !errors.Is(err, keychain.ErrInvalidToken) {
			t.Errorf("Expected error %v, but got %v", keychain.ErrInvalidToken, err)
		}
		if !errors.Is(err, jwt.ErrSignatureInvalid) {
			t.Errorf("Expected error %v, but got %v", jwt.ErrSignatureInvalid, err)
		}
	})

	t.Run("failure by malformed key", func(t *testing.T) {
		ctx, cancel := testutilctx.WithTest(context.Background(), t)
		defer cancel()

		cluster, clientset := testenv.NewCluster(t)

		keychainName := "keychain1"
		{
			t.Cleanup(func() {
				clientset.CoreV1().
					Secrets(cluster.Namespace()).
					Delete(
						context.Background(),
						keychainName,
						kubeapimeta.DeleteOptions{},
					)
			})
			if _, err := clientset.CoreV1().
				Secrets(cluster.Namespace()).
				Create(
					ctx,
					&v1.Secret{
						ObjectMeta: kubeapimeta.ObjectMeta{
							Name: keychainName,
							Labels: map[string]string{
								"knitfab/test":  "true",
								"example/label": "example",
							},
							Annotations: map[string]string{
								"knitfab/test":       "true",
								"example/annotation": "example",
							},
						},
						Type:      v1.SecretTypeOpaque,
						Immutable: pointer.Ref(false),
						Data:      map[string][]byte{}, // start as empty secret
					},
					kubeapimeta.CreateOptions{},
				); err != nil {
				t.Fatal(err)
			}
		}

		kc := try.To(keychain.Get(ctx, cluster, keychainName)).OrFatal(t)

		kid := "key1"
		testee := try.To(key.HS256(24*time.Hour, 2048/8).Issue()).OrFatal(t)
		kc.Set(kid, testee)

		signedString := "malformed"

		_, err := keychain.VerifyJWS[*jwt.RegisteredClaims](kc, signedString)
		if !errors.Is(err, keychain.ErrInvalidToken) {
			t.Errorf("Expected error %v, but got %v", keychain.ErrInvalidToken, err)
		}
		if !errors.Is(err, jwt.ErrTokenMalformed) {
			t.Errorf("Expected error %v, but got %v", jwt.ErrTokenMalformed, err)
		}
	})

	t.Run("failure by uncompatible claim format", func(t *testing.T) {
		type ClaimsA struct {
			jwt.RegisteredClaims
			Field []string `json:"field"`
		}

		type ClaimsB struct {
			jwt.RegisteredClaims
			Field string `json:"field"`
		}

		ctx, cancel := testutilctx.WithTest(context.Background(), t)
		defer cancel()

		cluster, clientset := testenv.NewCluster(t)

		keychainName := "keychain1"
		{
			t.Cleanup(func() {
				clientset.CoreV1().
					Secrets(cluster.Namespace()).
					Delete(
						context.Background(),
						keychainName,
						kubeapimeta.DeleteOptions{},
					)
			})
			if _, err := clientset.CoreV1().
				Secrets(cluster.Namespace()).
				Create(
					ctx,
					&v1.Secret{
						ObjectMeta: kubeapimeta.ObjectMeta{
							Name: keychainName,
							Labels: map[string]string{
								"knitfab/test":  "true",
								"example/label": "example",
							},
							Annotations: map[string]string{
								"knitfab/test":       "true",
								"example/annotation": "example",
							},
						},
						Type:      v1.SecretTypeOpaque,
						Immutable: pointer.Ref(false),
						Data:      map[string][]byte{}, // start as empty secret
					},
					kubeapimeta.CreateOptions{},
				); err != nil {
				t.Fatal(err)
			}
		}

		kc := try.To(keychain.Get(ctx, cluster, keychainName)).OrFatal(t)

		kid := "key1"
		testee := try.To(key.HS256(24*time.Hour, 2048/8).Issue()).OrFatal(t)
		kc.Set(kid, testee)

		claims := ClaimsA{
			RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
			},
			Field: []string{"a", "b", "c"},
		}

		signedString := try.To(keychain.NewJWS(kid, testee, claims)).OrFatal(t)

		_, err := keychain.VerifyJWS[*ClaimsB](kc, signedString)
		if !errors.Is(err, keychain.ErrInvalidToken) {
			t.Errorf("Expected error %v, but got %v", keychain.ErrInvalidToken, err)
		}
		if !errors.Is(err, jwt.ErrTokenMalformed) {
			t.Errorf("Expected error %v, but got %v", jwt.ErrTokenMalformed, err)
		}
	})

	t.Run("failure by missing key", func(t *testing.T) {
		ctx, cancel := testutilctx.WithTest(context.Background(), t)
		defer cancel()

		cluster, clientset := testenv.NewCluster(t)

		keychainName := "keychain1"
		{
			t.Cleanup(func() {
				clientset.CoreV1().
					Secrets(cluster.Namespace()).
					Delete(
						context.Background(),
						keychainName,
						kubeapimeta.DeleteOptions{},
					)
			})
			if _, err := clientset.CoreV1().
				Secrets(cluster.Namespace()).
				Create(
					ctx,
					&v1.Secret{
						ObjectMeta: kubeapimeta.ObjectMeta{
							Name: keychainName,
							Labels: map[string]string{
								"knitfab/test":  "true",
								"example/label": "example",
							},
							Annotations: map[string]string{
								"knitfab/test":       "true",
								"example/annotation": "example",
							},
						},
						Type:      v1.SecretTypeOpaque,
						Immutable: pointer.Ref(false),
						Data:      map[string][]byte{}, // start as empty secret
					},
					kubeapimeta.CreateOptions{},
				); err != nil {
				t.Fatal(err)
			}
		}
		kc := try.To(keychain.Get(ctx, cluster, keychainName)).OrFatal(t)

		kid := "key1"
		testee := try.To(key.HS256(24*time.Hour, 2048/8).Issue()).OrFatal(t)
		kc.Set(kid, testee)

		claims := jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
		}
		signedString := try.To(keychain.NewJWS("wrong-kid", testee, claims)).OrFatal(t)

		_, err := keychain.VerifyJWS[*jwt.RegisteredClaims](kc, signedString)
		if !errors.Is(err, keychain.ErrNoKeyFound) {
			t.Errorf("Expected error %v, but got %v", keychain.ErrNoKeyFound, err)
		}
	})
}
