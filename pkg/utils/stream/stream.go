package stream

// Streaming value operation
type Stream[T any] interface {
	// Filter stream and keep elements predicator matches.
	Filter(func(T) bool) Stream[T]

	// Append elements following this Stream.
	Concat(Stream[T]) Stream[T]

	// Drop elements as long as predicator matches with an element.
	//
	// for example,
	//
	//  s := FromSlice([]string{"a", "a", "b", "a", "b", "c"}).
	//           DropWhile(func(v string)bool { return v == "a"; }).
	//           Slice()
	//
	// gives []string{"b", "a", "b", "c"} .
	//
	// Node that the 4th element in input is remained in output,
	// because of the third in input is not matches.
	//
	DropWhile(func(T) bool) Stream[T]

	// Read Stream as channel
	Read() <-chan T

	// Convert whole of Stream into slice.
	//
	// Note that if the Stream is infinite, this method will never return.
	Slice() []T

	// // looking for that golang allows generic methods.
	// Map[U any](func (T) U) Stream[U]
}

type chanStream[T any] <-chan T

func FromSlice[T any](ts []T) Stream[T] {
	var ch = make(chan T, 1)
	go func() {
		defer close(ch)
		for _, t := range ts {
			ch <- t
		}
	}()
	return chanStream[T](ch)
}

func FromChan[T any](ch <-chan T) Stream[T] {
	return chanStream[T](ch)
}

func (c chanStream[T]) Read() <-chan T {
	return c
}

func (s chanStream[T]) Filter(f func(T) bool) Stream[T] {
	ch := make(chan T, 1)
	go func() {
		defer close(ch)
		for t := range s {
			if f(t) {
				ch <- t
			}
		}
	}()
	return FromChan(ch)
}

func (s chanStream[T]) Concat(u Stream[T]) Stream[T] {
	ch := make(chan T, 1)
	go func() {
		defer close(ch)
		for t := range s {
			ch <- t
		}
		for t := range u.Read() {
			ch <- t
		}
	}()
	return FromChan(ch)
}

func (s chanStream[T]) DropWhile(f func(T) bool) Stream[T] {
	ch := make(chan T, 1)
	go func() {
		defer close(ch)
		for v := range s.Read() {
			if f(v) {
				continue
			}
			ch <- v
			break
		}
		for v := range s.Read() {
			ch <- v
		}
	}()
	return FromChan(ch)
}

func (s chanStream[T]) Slice() []T {
	ret := []T{}
	for t := range s {
		ret = append(ret, t)
	}
	return ret
}
