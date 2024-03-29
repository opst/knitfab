package io

import (
	"io"
	"sync"
)

// Reader triggers callback on the end of the stream.
type TriggerReader interface {
	io.Reader
	OnEnd(func())
}

type triggerReader struct {
	base      io.Reader
	onEnd     []func()
	exhausted bool
	mux       sync.Mutex
}

func NewTriggerReader(base io.Reader) TriggerReader {
	return &triggerReader{base: base, exhausted: false}
}

func (t *triggerReader) Read(p []byte) (int, error) {
	n, err := t.base.Read(p)

	switch err {
	case nil:
		return n, nil
	case io.EOF:
		t.mux.Lock()
		defer t.mux.Unlock()
		if t.exhausted {
			break
		}
		t.exhausted = true
		for _, f := range t.onEnd {
			f()
		}
		t.onEnd = nil
	default: // pass
	}
	return n, err
}

func (t *triggerReader) OnEnd(callback func()) {
	t.mux.Lock()
	defer t.mux.Unlock()

	if t.exhausted {
		callback()
	}
	t.onEnd = append(t.onEnd, callback)
}
