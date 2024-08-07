package keyprovider_test

import (
	"context"
	"errors"
	"testing"
	"time"

	keyprovider "github.com/opst/knitfab/cmd/knitd_backend/provider/keyProvider"
	"github.com/opst/knitfab/pkg/db/mocks"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/opst/knitfab/pkg/workloads/keychain"
	"github.com/opst/knitfab/pkg/workloads/keychain/key"
	mockkeychain "github.com/opst/knitfab/pkg/workloads/keychain/mockKeychain"
)

func TestKeyLocker(t *testing.T) {
	t.Run("when it GetKey for empty Keychain, it issues new one", func(t *testing.T) {
		keychainName := "keychainName"

		inLock := false
		mdbkc := mocks.NewMockKeychainInterface()
		mdbkc.Impl.Lock = func(ctx context.Context, name string, f func(context.Context) error) error {
			inLock = true
			defer func() {
				inLock = false
			}()

			if name != keychainName {
				t.Errorf("expected keychain name 'keychainName', got %s", name)
			}
			return f(ctx)
		}

		k := try.To(key.HS256(3*time.Hour, 2048/8).Issue()).OrFatal(t)
		keyPolicy := key.Fixed(k)

		mkc := mockkeychain.New(t)
		mkc.Impl.Name = func() string {
			return keychainName
		}

		keychainGetKeyHasBeenCalled := false
		mkc.Impl.GetKey = func(option ...keychain.KeyRequirement) (string, key.Key, bool) {
			keychainGetKeyHasBeenCalled = true
			return "", nil, false // empty keychain
		}
		keychainSetHasBeenCalled := false
		mkc.Impl.Set = func(kid string, _k key.Key) {
			keychainSetHasBeenCalled = true
			if !k.Equal(_k) {
				t.Errorf("expected key %s, got %s", k, _k)
			}
		}
		keychainUpdateHasBeenCalled := false
		mkc.Impl.Update = func(context.Context) error {
			if !inLock {
				t.Errorf("expected in lock for update")
			}
			keychainUpdateHasBeenCalled = true
			return nil
		}

		keychainNameForGetKeyChain := "keychainNameForGetKeyChain"

		getKeyChainHasBeenCalled := false
		getKeyChain := func(ctx context.Context, name string) (keychain.Keychain, error) {
			getKeyChainHasBeenCalled = true

			if name != keychainNameForGetKeyChain {
				t.Errorf("expected keychain name 'test', got %s", name)
			}
			return mkc, nil
		}

		options := []keyprovider.Option{
			keyprovider.WithPolicy(keyPolicy),
		}

		testee := keyprovider.New(
			keychainNameForGetKeyChain, mdbkc, getKeyChain,
			options...,
		)
		_, got, err := testee.Provide(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !k.Equal(got) {
			t.Errorf("expected key %s, got %s", k, got)
		}
		if !getKeyChainHasBeenCalled {
			t.Errorf("expected getKeyChain to be called")
		}
		if !keychainGetKeyHasBeenCalled {
			t.Errorf("expected Get to be called")
		}
		if !keychainSetHasBeenCalled {
			t.Errorf("expected Set to be called")
		}
		if !keychainUpdateHasBeenCalled {
			t.Errorf("expected Update to be called")
		}
	})

	t.Run("when it GetKey for non-empty Keychain, it returns the key", func(t *testing.T) {
		keychainName := "keychainName"

		inLock := false
		mdbkc := mocks.NewMockKeychainInterface()
		mdbkc.Impl.Lock = func(ctx context.Context, name string, f func(context.Context) error) error {
			inLock = true
			defer func() {
				inLock = false
			}()

			if name != keychainName {
				t.Errorf("expected keychain name 'keychainName', got %s", name)
			}
			t.Error("should not be called")
			return f(ctx)
		}

		k := try.To(key.HS256(3*time.Hour, 2048/8).Issue()).OrFatal(t)
		keyPolicy := key.Fixed(k)

		mkc := mockkeychain.New(t)
		mkc.Impl.Name = func() string {
			return keychainName
		}

		keychainGetKeyHasBeenCalled := false
		mkc.Impl.GetKey = func(option ...keychain.KeyRequirement) (string, key.Key, bool) {
			if inLock {
				t.Errorf("expected not in lock")
			}
			if keychainGetKeyHasBeenCalled {
				t.Errorf("expected Get to be called once")
			}
			keychainGetKeyHasBeenCalled = true
			return "kid", k, true
		}
		keychainSetHasBeenCalled := false
		mkc.Impl.Set = func(kid string, _k key.Key) {
			keychainSetHasBeenCalled = true
		}
		keychainUpdateHasBeenCalled := false
		mkc.Impl.Update = func(context.Context) error {
			keychainUpdateHasBeenCalled = true
			return nil
		}

		keychainNameForGetKeyChain := "keychainNameForGetKeyChain"

		getKeyChainHasBeenCalled := false
		getKeyChain := func(ctx context.Context, name string) (keychain.Keychain, error) {
			getKeyChainHasBeenCalled = true
			if name != keychainNameForGetKeyChain {
				t.Errorf("expected keychain name %s, got %s", keychainNameForGetKeyChain, name)
			}
			return mkc, nil
		}

		options := []keyprovider.Option{
			keyprovider.WithPolicy(keyPolicy),
		}

		testee := keyprovider.New(
			keychainNameForGetKeyChain, mdbkc, getKeyChain,
			options...,
		)

		kid, got, err := testee.Provide(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if kid != "kid" {
			t.Errorf("expected kid 'kid', got %s", kid)
		}
		if !k.Equal(got) {
			t.Errorf("expected key %s, got %s", k, got)
		}
		if !getKeyChainHasBeenCalled {
			t.Errorf("expected getKeyChain to be called")
		}
		if !keychainGetKeyHasBeenCalled {
			t.Errorf("expected Get to be called")
		}
		if keychainSetHasBeenCalled {
			t.Errorf("expected Set to not be called")
		}
		if keychainUpdateHasBeenCalled {
			t.Errorf("expected Update to not be called")
		}
	})

	t.Run("When it causes an error on issueing a Key, it returns the error", func(t *testing.T) {
		keychainName := "keychainName"

		mdbkc := mocks.NewMockKeychainInterface()
		mdbkc.Impl.Lock = func(ctx context.Context, name string, f func(context.Context) error) error {
			if name != keychainName {
				t.Errorf("expected keychain name 'keychainName', got %s", name)
			}
			return f(ctx)
		}

		errorOnIssueKey := errors.New("error on issue key")
		keyPolicy := key.Failing(errorOnIssueKey)

		mkc := mockkeychain.New(t)
		mkc.Impl.Name = func() string {
			return keychainName
		}

		mkc.Impl.GetKey = func(option ...keychain.KeyRequirement) (string, key.Key, bool) {
			return "", nil, false // empty keychain
		}

		keychainNameForGetKeyChain := "keychainNameForGetKeyChain"

		getKeyChainHasBeenCalled := false
		getKeyChain := func(ctx context.Context, name string) (keychain.Keychain, error) {
			getKeyChainHasBeenCalled = true

			if name != keychainNameForGetKeyChain {
				t.Errorf("expected keychain name 'test', got %s", name)
			}
			return mkc, nil
		}

		options := []keyprovider.Option{
			keyprovider.WithPolicy(keyPolicy),
		}

		testee := keyprovider.New(
			keychainNameForGetKeyChain, mdbkc, getKeyChain,
			options...,
		)
		_, _, err := testee.Provide(context.Background())
		if !errors.Is(err, errorOnIssueKey) {
			t.Fatalf("unexpected error: %v", err)
		}
		if !getKeyChainHasBeenCalled {
			t.Errorf("expected getKeyChain to be called")
		}
	})

	t.Run("When it causes an error on getting a Keychain, it returns the error", func(t *testing.T) {
		keychainName := "keychainName"

		mdbkc := mocks.NewMockKeychainInterface()
		mdbkc.Impl.Lock = func(ctx context.Context, name string, f func(context.Context) error) error {
			if name != keychainName {
				t.Errorf("expected keychain name 'keychainName', got %s", name)
			}
			return f(ctx)
		}

		k := try.To(key.HS256(3*time.Hour, 2048/8).Issue()).OrFatal(t)
		keyPolicy := key.Fixed(k)

		keychainNameForGetKeyChain := "keychainNameForGetKeyChain"

		errorOnGetKeyChain := errors.New("error on get keychain")
		getKeyChain := func(ctx context.Context, name string) (keychain.Keychain, error) {
			if name != keychainNameForGetKeyChain {
				t.Errorf("expected keychain name 'test', got %s", name)
			}
			return nil, errorOnGetKeyChain
		}

		options := []keyprovider.Option{
			keyprovider.WithPolicy(keyPolicy),
		}

		testee := keyprovider.New(
			keychainNameForGetKeyChain, mdbkc, getKeyChain,
			options...,
		)
		_, _, err := testee.Provide(context.Background())
		if !errors.Is(err, errorOnGetKeyChain) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("When it causes an error on Updating Keychain, it returns the error", func(t *testing.T) {
		keychainName := "keychainName"

		inLock := false
		mdbkc := mocks.NewMockKeychainInterface()
		mdbkc.Impl.Lock = func(ctx context.Context, name string, f func(context.Context) error) error {
			inLock = true
			defer func() {
				inLock = false
			}()

			if name != keychainName {
				t.Errorf("expected keychain name 'keychainName', got %s", name)
			}
			return f(ctx)
		}

		k := try.To(key.HS256(3*time.Hour, 2048/8).Issue()).OrFatal(t)
		keyPolicy := key.Fixed(k)

		mkc := mockkeychain.New(t)
		mkc.Impl.Name = func() string {
			return keychainName
		}

		keychainGetKeyHasBeenCalled := false
		mkc.Impl.GetKey = func(option ...keychain.KeyRequirement) (string, key.Key, bool) {
			keychainGetKeyHasBeenCalled = true
			return "", nil, false // empty keychain
		}
		keychainSetHasBeenCalled := false
		mkc.Impl.Set = func(kid string, _k key.Key) {
			keychainSetHasBeenCalled = true
			if !k.Equal(_k) {
				t.Errorf("expected key %s, got %s", k, _k)
			}
		}
		errorOnUpdate := errors.New("error on update")
		mkc.Impl.Update = func(context.Context) error {
			if !inLock {
				t.Errorf("expected in lock for update")
			}
			return errorOnUpdate
		}

		keychainNameForGetKeyChain := "keychainNameForGetKeyChain"

		getKeyChain := func(ctx context.Context, name string) (keychain.Keychain, error) {
			if name != keychainNameForGetKeyChain {
				t.Errorf("expected keychain name 'test', got %s", name)
			}
			return mkc, nil
		}

		options := []keyprovider.Option{
			keyprovider.WithPolicy(keyPolicy),
		}

		testee := keyprovider.New(
			keychainNameForGetKeyChain, mdbkc, getKeyChain,
			options...,
		)
		_, _, err := testee.Provide(context.Background())
		if !errors.Is(err, errorOnUpdate) {
			t.Fatalf("unexpected error: %v", err)
		}
		if !keychainGetKeyHasBeenCalled {
			t.Errorf("expected Get to be called")
		}
		if !keychainSetHasBeenCalled {
			t.Errorf("expected Set to be called")
		}
		if !keychainSetHasBeenCalled {
			t.Errorf("expected Set to be called")
		}
	})

	t.Run("When the new key does not satisfy requirements, it cause an error", func(t *testing.T) {
		keychainName := "keychainName"

		mdbkc := mocks.NewMockKeychainInterface()
		mdbkc.Impl.Lock = func(ctx context.Context, name string, f func(context.Context) error) error {
			if name != keychainName {
				t.Errorf("expected keychain name 'keychainName', got %s", name)
			}
			return f(ctx)
		}

		mkc := mockkeychain.New(t)
		mkc.Impl.Name = func() string {
			return keychainName
		}

		keychainGetKeyHasBeenCalled := false
		mkc.Impl.GetKey = func(option ...keychain.KeyRequirement) (string, key.Key, bool) {
			keychainGetKeyHasBeenCalled = true
			return "", nil, false // empty keychain
		}

		keychainNameForGetKeyChain := "keychainNameForGetKeyChain"

		getKeyChain := func(ctx context.Context, name string) (keychain.Keychain, error) {
			if name != keychainNameForGetKeyChain {
				t.Errorf("expected keychain name 'test', got %s", name)
			}
			return mkc, nil
		}

		k := try.To(key.HS256(3*time.Hour, 2048/8).Issue()).OrFatal(t)
		keyPolicy := key.Fixed(k)
		options := []keyprovider.Option{
			keyprovider.WithPolicy(keyPolicy),
		}

		testee := keyprovider.New(
			keychainNameForGetKeyChain, mdbkc, getKeyChain,
			options...,
		)
		_, _, err := testee.Provide(context.Background(), func(kid string, k key.Key) bool {
			return false // never satisfy
		})
		if !errors.Is(err, keyprovider.ErrBadNewKey) {
			t.Errorf("unexpected error: %v", err)
		}
		if !keychainGetKeyHasBeenCalled {
			t.Errorf("expected Get to be called")
		}
	})
}
