package function_test

import (
	"testing"

	"github.com/opst/knitfab/pkg/utils/function"
)

func TestVoid(t *testing.T) {
	t.Run("when it wraps function as returning struct value, it returns zerovalue of that", func(t *testing.T) {
		type T struct {
			V int
		}

		arg := "hello world"

		testee := function.Void[T](func(s string) {
			if s != arg {
				t.Error("it does not pass through argument")
			}
		})

		zero := *new(T)

		actual := testee(arg)
		if actual != zero {
			t.Error("return value is not zerovalue")
		}
	})

	t.Run("when it wraps function as returning pointer of struct, it returns nil", func(t *testing.T) {
		type T struct {
			V int
		}

		arg := "hello world"

		testee := function.Void[*T](func(s string) {
			if s != arg {
				t.Error("it does not pass through argument")
			}
		})

		actual := testee(arg)
		if actual != nil {
			t.Error("return value is not zerovalue")
		}
	})

	t.Run("when it wraps function as returning primitive, it returns zerovalue of that", func(t *testing.T) {
		arg := "hello world"

		testee := function.Void[int](func(s string) {
			if s != arg {
				t.Error("it does not pass through argument")
			}
		})

		actual := testee(arg)
		if actual != 0 {
			t.Error("return value is not zerovalue")
		}
	})

	t.Run("when it wraps function as returning interface, it returns nil", func(t *testing.T) {
		arg := "hello world"

		testee := function.Void[error](func(s string) {
			if s != arg {
				t.Error("it does not pass through argument")
			}
		})

		actual := testee(arg)
		if actual != nil {
			t.Error("return value is not zerovalue")
		}
	})
}
