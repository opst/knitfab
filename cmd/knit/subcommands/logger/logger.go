package logger

import (
	"io"
	"log"
)

func Null() *log.Logger {
	return log.New(io.Discard, "", log.LstdFlags)
}

func Default() *log.Logger {
	return log.Default()
}
