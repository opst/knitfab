package os_test

import (
	"os"
	"testing"

	kos "github.com/opst/knitfab/pkg/utils/os"
)

func TestGetEnvOr(t *testing.T) {
	t.Run("it returns value of envvar, if existing", func(t *testing.T) {
		key, value := "KNIT_TEST_ENVVAR", "test value"
		t.Setenv(key, value)

		actual := kos.GetEnvOr(key, "default")

		if actual != value {
			t.Errorf("wrong value returned: (actual, expected) = (%s, %s)", actual, value)
		}
	})

	t.Run("it returns fallbacl value, if not existing", func(t *testing.T) {
		key := "KNIT_TEST_ENVVAR"

		if original, ok := os.LookupEnv(key); ok {
			os.Unsetenv(key)
			t.Cleanup(func() { os.Setenv(key, original) })
		}

		fallback := "fallback value"
		actual := kos.GetEnvOr(key, fallback)

		if actual != fallback {
			t.Errorf("wrong value returned: (actual, expected) = (%s, %s)", actual, fallback)
		}
	})
}
