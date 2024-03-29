// Provide error wrapper with created locaion.
//
// Usage:
//
// ```
// wrapped := xerrors.Wrap(err)
// ```
//
// returns new error object wraps `err`.
//
// `wrapped` knows filename, line, and the name of function where itself is created.
//
// When you read message of this, replace
//
//	s/<-/\n/
//
// and it gives you "stacks" of where you marks.
//

package errors

import (
	"errors"
	"fmt"
	"runtime"
)

type ErrWithCaller struct {
	file     string
	line     int
	funcname string
	note     string
	err      error
}

func (e *ErrWithCaller) File() string {
	return e.file
}

func (e *ErrWithCaller) Line() int {
	return e.line
}

func (e *ErrWithCaller) Error() string {
	if e.note == "" {
		return fmt.Sprintf(`@ %s "%s" l%d <- %s`, e.funcname, e.file, e.line, e.err.Error())
	} else {
		return fmt.Sprintf(`@ %s "%s" l%d (%s) <- %s`, e.funcname, e.file, e.line, e.note, e.err.Error())
	}
}

func (e *ErrWithCaller) Unwrap() error {
	return e.err
}

func New(text string) error {
	return wrap("", errors.New(text), 1)
}

func Wrap(err error) error {
	return wrap("", err, 1)
}

func WrapAsOuter(err error, depth int) error {
	return wrap("", err, depth+1)
}

func WrapWithNote(note string, err error) error {
	return wrap(note, err, 1)
}

func wrap(note string, err error, depth int) error {
	pc, file, line, ok := runtime.Caller(depth + 1)
	funcname := "(unknown func)"
	if !ok {
		file = "?"
		line = -1
	}
	fn := runtime.FuncForPC(pc)
	if fn != nil {
		funcname = fn.Name()
	}

	return &ErrWithCaller{
		funcname: funcname,
		file:     file,
		line:     line,
		note:     note,
		err:      err,
	}
}
