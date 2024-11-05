package io

import (
	"crypto/md5"
	"hash"
	"io"
)

type ChecksumWriter interface {
	io.Writer
	Sum() []byte
}

type ChecksumReader interface {
	io.Reader

	// Get Checksum calcurated from bytes have been read
	Sum() []byte
}

type MD5Writer struct {
	dest io.Writer
	md5  hash.Hash
}

func NewMD5Writer(dest io.Writer) ChecksumWriter {
	return &MD5Writer{
		dest: dest,
		md5:  md5.New(),
	}
}

func (mw *MD5Writer) Write(buf []byte) (int, error) {
	mw.md5.Write(buf)
	return mw.dest.Write(buf)
}

// Get MD5 Checksum.
func (mw *MD5Writer) Sum() []byte {
	return mw.md5.Sum(nil)
}

type MD5Reader struct {
	source io.Reader
	md5    hash.Hash
}

func NewMD5Reader(source io.Reader) ChecksumReader {
	return &MD5Reader{
		source: source,
		md5:    md5.New(),
	}
}

func (mr *MD5Reader) Read(p []byte) (int, error) {
	n, err := mr.source.Read(p)
	if 0 < n {
		mr.md5.Write(p[:n])
	}
	return n, err
}

func (mr *MD5Reader) Sum() []byte {
	return mr.md5.Sum(nil)
}
