package io

import (
	"io"
	"sync"
)

type hookedReadCloser struct {
	base   io.ReadCloser
	hook   func()
	closed bool
	once   sync.Once
}

// Wrap `io.Reader`, and add hook which is called when the reader is closed.
//
// args:
//
//   - r: io.Reader to be wrapped.
//     If `r` is `io.ReadCloser`, `Close` is proxied to `r`.
//     Otherwise, `r` is wrapped in `io.NopCloser`.
//
//   - onClosed: hook function.
//     this is called only once.
//     If you `Close` twice (or more), onClosed hook is not called more.
//
//     You can pass `nil` as `onClosed`.
//     If you do so, this function does nothing when `r` is `io.ReadCloser`,
//     and is same as `io.NopCloser` when `r` is `io.Reader`.
func WithCloseHook(r io.Reader, onClosed func()) io.ReadCloser {
	var base io.ReadCloser
	switch b := r.(type) {
	case io.ReadCloser:
		base = b
	default:
		base = io.NopCloser(b)
	}
	if onClosed == nil {
		return base
	}

	return &hookedReadCloser{base: base, hook: onClosed}
}

func (hrc *hookedReadCloser) Read(p []byte) (int, error) {
	if hrc.closed {
		return 0, io.EOF
	}
	return hrc.base.Read(p)
}

func (hrc *hookedReadCloser) Close() error {
	if hrc.hook != nil {
		hrc.once.Do(hrc.hook)
	}
	return hrc.base.Close()
}
