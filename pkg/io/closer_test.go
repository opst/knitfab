package io_test

import (
	"bytes"
	"io"
	"testing"

	kio "github.com/opst/knitfab/pkg/io"
)

type spy struct {
	called int
}

func (s *spy) call() {
	s.called += 1
}

func newSpy() *spy {
	return &spy{}
}

func TestWithCloseHook(t *testing.T) {
	t.Run("it proxies base stream", func(t *testing.T) {
		content := []byte("quick brown fox jumps over lazy dog")
		base := bytes.NewBuffer(content)
		s := newSpy()
		testee := kio.WithCloseHook(base, s.call)

		actual, err := io.ReadAll(testee)
		if err != nil {
			t.Fatalf("unexpected error: %s (%#v)", err.Error(), err)
		}

		if !bytes.Equal(actual, content) {
			t.Errorf(
				"does not proxy: (actual content, expected content) = (%s, %s)",
				string(actual), string(content),
			)
		}
	})

	t.Run("it calls hook only for the first `Close`", func(t *testing.T) {
		content := []byte("quick brown fox jumps over lazy dog")
		base := bytes.NewBuffer(content)
		s := newSpy()
		testee := kio.WithCloseHook(base, s.call)

		beforeClose := s.called
		if beforeClose != 0 {
			t.Errorf("hook is called before first close: %d times called", beforeClose)
		}

		if err := testee.Close(); err != nil {
			t.Errorf("Close returns unexpected error: %s (%+v)", err.Error(), err)
		}
		afterFirstClose := s.called

		if (afterFirstClose - beforeClose) < 1 {
			t.Errorf("hook is not called after `Close`")
		} else if 1 < (afterFirstClose - beforeClose) {
			t.Errorf("hook is called too much: %d times called", afterFirstClose-beforeClose)
		}

		if err := testee.Close(); err != nil {
			t.Errorf("Close returns unexpected error: %s (%+v)", err.Error(), err)
		}
		afterSecondClose := s.called

		if 0 < (afterSecondClose - afterFirstClose) {
			t.Errorf("hook is called for not-first close: %d times called", afterSecondClose-afterFirstClose)
		}
	})

	t.Run("when it's base stream is io.ReadCloser, close is propagated to the base", func(t *testing.T) {
		base := &mockReadCloser{}
		nop := func() {}
		testee := kio.WithCloseHook(base, nop)
		err := testee.Close()
		if err != nil {
			t.Fatalf("unexpected error: %s (%#v)", err.Error(), err)
		}

		if !base.closeCalled {
			t.Error("ReadCloser.Close of base is not called")
		}
	})

	t.Run("when nil is passed as hook for io.Reader, it can be closed without panic", func(t *testing.T) {
		base := bytes.NewBuffer(nil)
		testee := kio.WithCloseHook(base, nil)
		err := testee.Close()
		if err != nil {
			t.Fatalf("unexpected error: %s (%#v)", err.Error(), err)
		}
		// expect no panic caused by nil reference
	})

	t.Run("when nil is passed as hook for io.ReadCloser, it returns the given.", func(t *testing.T) {
		base := io.NopCloser(bytes.NewBuffer(nil))
		testee := kio.WithCloseHook(base, nil)
		if base != testee {
			t.Fatalf("it returns different object")
		}
	})

}

type mockReadCloser struct {
	closeCalled bool
}

var _ io.ReadCloser = &mockReadCloser{}

func (m *mockReadCloser) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (m *mockReadCloser) Close() error {
	m.closeCalled = true
	return nil
}
