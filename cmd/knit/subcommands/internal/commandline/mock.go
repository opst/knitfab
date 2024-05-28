package commandline

import (
	"io"

	"github.com/youta-t/flarc"
)

type MockCommandline[T any] struct {
	Fullname_ string

	Stdin_  io.Reader
	Stdout_ io.Writer
	Stderr_ io.Writer

	Flags_ T
	Args_  map[string][]string
}

var _ flarc.Commandline[struct{}] = &MockCommandline[struct{}]{}

func (t MockCommandline[T]) Fullname() string {
	return t.Fullname_
}

func (t MockCommandline[T]) Stdin() io.Reader {
	return t.Stdin_
}

func (t MockCommandline[T]) Stdout() io.Writer {
	return t.Stdout_
}

func (t MockCommandline[T]) Stderr() io.Writer {
	return t.Stderr_
}

func (t MockCommandline[T]) Flags() T {
	return t.Flags_
}

func (t MockCommandline[T]) Args() map[string][]string {
	return t.Args_
}
