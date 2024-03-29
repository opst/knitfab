package try_test

import (
	"errors"
	"testing"

	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
)

type fataler struct {
	fatal [][]any
}

func (f *fataler) Fatal(args ...any) {
	f.fatal = append(f.fatal, args)
}

type helperfataler struct {
	fataler

	helper uint
}

func (hf *helperfataler) Helper() {
	hf.helper += 1
}

func TestTry(t *testing.T) {
	t.Run("when it does not have error,", func(t *testing.T) {
		expected := 42
		testee := try.To(expected, nil)

		t.Run("OrFatal with Fataler returns the value", func(t *testing.T) {
			fataler := &fataler{}
			actual := testee.OrFatal(fataler)

			if actual != expected {
				t.Errorf("unexpected result: (actual, expected) = (%d, %d)", actual, expected)
			}
		})

		t.Run("OrDefault returns non-default value", func(t *testing.T) {
			ret := testee.OrDefault(expected + 1)
			if ret != expected {
				t.Errorf("unmatch: (actual, expected) = (%d, %d)", ret, expected)
			}
		})

		t.Run("OrFatal do not call Fatal", func(t *testing.T) {
			fataler := &fataler{}
			testee.OrFatal(fataler)
			if len(fataler.fatal) != 0 {
				t.Errorf("Fatal is called, unexpectedly: %v", fataler.fatal)
			}
		})

		t.Run("OrFatal does not call Helper for HalperFataler", func(t *testing.T) {
			fataler := &helperfataler{}
			testee.OrFatal(fataler)
			if fataler.helper != 0 {
				t.Errorf("Helper is called, unexpectedly")
			}
		})
	})

	t.Run("when it has error,", func(t *testing.T) {
		err := errors.New("error")
		testee := try.To(42, err)

		t.Run("OrDefault returns default value", func(t *testing.T) {
			expected := 99
			actual := testee.OrDefault(expected)

			if actual != expected {
				t.Errorf("unmatch: (actual, expected) = (%d, %d)", actual, expected)
			}
		})

		t.Run("OrFatal with Fataler returns zero value", func(t *testing.T) {
			fataler := &fataler{}
			actual := testee.OrFatal(fataler)

			if actual != 0 {
				t.Errorf("unexpected result: (actual, expected) = (%d, %d)", actual, 0)
			}
		})

		t.Run("OrFatal calls Fatal", func(t *testing.T) {
			fataler := &fataler{}
			testee.OrFatal(fataler)
			if len(fataler.fatal) != 1 {
				t.Errorf("unexpected Fatal call: %v", fataler.fatal)
			}
			if !cmp.SliceEqWith(fataler.fatal[0], []error{err}, func(a any, e error) bool {
				ar, ok := a.(error)
				return ok && errors.Is(ar, e)
			}) {
				t.Error("Fatal is called with unexpected args:", fataler.fatal[0])
			}
		})

		t.Run("OrFatal call Helper for HalperFataler", func(t *testing.T) {
			fataler := &helperfataler{}
			testee.OrFatal(fataler)
			if fataler.helper == 0 {
				t.Errorf("Helper does not call")
			}
		})

	})

}
