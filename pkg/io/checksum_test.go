package io

import (
	"bytes"
	enchex "encoding/hex"
	"io"
	"testing"
)

func fromhex(hexStr string) []byte {
	hash, err := enchex.DecodeString(hexStr)
	if err != nil {
		panic(err)
	}
	return hash
}

func TestMD5Writer(t *testing.T) {
	// md5 hash in expected is generated with BSD `md5` command.

	t.Run("when it is given nothing, it return hash of empty", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)
		testee := NewMD5Writer(buf)

		if !bytes.Equal(buf.Bytes(), []byte{}) {
			t.Errorf(
				"written content mismatch. actual != expected : %s != %s",
				buf.String(), string([]byte{}),
			)
		}

		expected := fromhex("d41d8cd98f00b204e9800998ecf8427e")
		if !bytes.Equal(testee.Sum(), expected) {
			t.Error("hashes do not match.")
		}
	})

	t.Run("when it given bytes, it produce MD5 hash", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)
		testee := NewMD5Writer(buf)

		payload := []byte("test text to be hashed")
		n, err := testee.Write(payload)
		if err != nil {
			t.Error("fail to write error buffer", err)
		}
		if n != len(payload) {
			t.Errorf(
				"length mismatch! payload != written actual : %d != %d",
				len(payload), n,
			)
		}

		expected := fromhex("a21436eeedcb3a89a5c9b4513655048f")
		if !bytes.Equal(testee.Sum(), expected) {
			t.Error(
				"hashes do not match. (actual v.s. expected)",
				testee.Sum(), expected,
			)
		}
	})
}

func TestMD5Reader(t *testing.T) {
	t.Run("when no bytes are read, it generates empty hash", func(t *testing.T) {
		source := bytes.NewBuffer(nil)
		testee := NewMD5Reader(source)

		dest := make([]byte, 2)
		n, err := testee.Read(dest)
		if err != nil && err != io.EOF {
			t.Fatal("unexpected error.", err)
		}
		if n != 0 {
			t.Fatal("something is read out", dest)
		}

		expected := fromhex("d41d8cd98f00b204e9800998ecf8427e")
		if !bytes.Equal(testee.Sum(), expected) {
			t.Error(
				"hases do not match. (actual v.s. expected)",
				testee.Sum(), expected,
			)
		}
	})

	t.Run("when bytes are read, it generates hash for them", func(t *testing.T) {
		message := []byte("test text to be hashed")
		source := bytes.NewBuffer(message)
		testee := NewMD5Reader(source)

		dest := make([]byte, len(message))
		_, err := testee.Read(dest)
		if err != nil && err != io.EOF {
			t.Fatal("unexpected error.", err)
		}
		if !bytes.Equal(dest, message) {
			t.Fatal("content is not match (actual v.s. expected)", dest, message)
		}

		expected := fromhex("a21436eeedcb3a89a5c9b4513655048f")
		if !bytes.Equal(testee.Sum(), expected) {
			t.Error(
				"hases do not match. (actual v.s. expected)",
				testee.Sum(), expected,
			)
		}
	})

	t.Run("when bytes are read , it generates hash for them", func(t *testing.T) {
		message := []byte("test text to be hashed")
		source := bytes.NewBuffer(message)
		testee := NewMD5Reader(source)

		head := 0
		dest := bytes.NewBuffer(nil)

		for {
			buf := make([]byte, 10)
			n, err := testee.Read(buf)
			if err != nil && err != io.EOF {
				t.Fatal("unexpected error.", err)
			}

			if !bytes.Equal(buf[:n], message[head:head+n]) {
				t.Error("content read do not match. (actual v.s. expected)", buf[:n], message[head:head+n])
			}
			dest.Write(buf[:n])
			head += n

			if err == io.EOF {
				break
			}
		}
		if !bytes.Equal(dest.Bytes(), message) {
			t.Error("messages does not match (actual v.s. expected)", dest.Bytes(), message)
		}

		expected := fromhex("a21436eeedcb3a89a5c9b4513655048f")
		if !bytes.Equal(testee.Sum(), expected) {
			t.Error(
				"hases do not match. (actual v.s. expected)",
				testee.Sum(), expected,
			)
		}
	})
}
