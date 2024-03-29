package mocks

type CallLog[T any] []T

func (l CallLog[T]) Times() uint {
	return uint(len(l))
}
