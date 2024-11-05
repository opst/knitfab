package io_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	kio "github.com/opst/knitfab/pkg/utils/io"
)

func TestTriggerReader(t *testing.T) {
	t.Run("it calls callbacks at the end of stream.", func(t *testing.T) {
		message := "quick brown fox jumps over the lazy dog."
		buffer := bytes.NewBuffer([]byte(message))
		testee := kio.NewTriggerReader(buffer)

		chars := 0
		charsAtTheEnd := 0
		testee.OnEnd(func() {
			charsAtTheEnd = chars
		})

		actualReadOut := []byte{}

		for {
			buf := make([]byte, 1)
			l, err := testee.Read(buf)

			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatalf("fail to read stream.: %v", err)
			}

			chars += l
			actualReadOut = append(actualReadOut, buf...)
		}

		if charsAtTheEnd != len(message) {
			t.Errorf(
				"callback is not triggerd just before EOF. callback called at: %d bytes. expected at %d bytes)",
				charsAtTheEnd, len(message),
			)
		}

		actualContent := string(actualReadOut)
		if actualContent != message {
			t.Errorf(
				"TriggerReader does not preserve the content of stream. (actual, expected) = (%s, %s)",
				actualContent, message,
			)
		}
	})
}
