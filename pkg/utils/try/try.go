package try

// something have method `Fatal`.
//
// For example in standard libraries: *testing.T, log.Logger
type Fataler interface {
	Fatal(...any)
}

// Wrapper of a pair of (T, error) .
//
// When error is nil, such Either is "ok", and T value is handled as valid.
//
// Otherwise, it is "no good", and T value is not valid.
type Either[T any] interface {

	// get value & error pair
	//
	// If the Either has value, return (value, nil).
	// Otherwize, return (zero-value, error).
	Get() (T, error)

	// When Either is "ok", it just return the T value.
	//
	// Otherwise, it calls ftl.Fatal(err) .
	// If ftl has "Helper()" method (like *testing.T), also that is called before `Fatal`.
	OrFatal(ftl Fataler) T

	OrDefault(T) T
}

// Convert value if the either has value.
func Map[T any, R any](try Either[T], mapper func(T) R) Either[R] {
	val, err := try.Get()
	if err != nil {
		return tryNg[R]{err}
	}
	return tryOk[R]{mapper(val)}
}

func Into[T, X, R any](a func(T) (X, error), b func(X) (R, error)) func(T) (R, error) {
	return func(t T) (R, error) {
		v, err := a(t)
		if err != nil {
			return *new(R), err
		}
		return b(v)
	}
}

func Done[T any](t T) (T, error) {
	return t, nil
}

// Convert value if the either has value.
//
// If the Either passed wraps error, it returns just error-wrapping Either.
//
// Otherwise, return To(mapper(value)), where value is something wrapped by Either.
func TryMap[T any, R any](try Either[T], mapper func(T) (R, error)) Either[R] {
	val, err := try.Get()
	if err != nil {
		return tryNg[R]{err}
	}
	return To(mapper(val))
}

func To[T any](ok T, ng error) Either[T] {
	if ng == nil {
		return tryOk[T]{ok}
	}
	return tryNg[T]{ng}
}

type tryOk[T any] struct {
	value T
}

type tryNg[T any] struct {
	err error
}

func (ok tryOk[T]) Get() (T, error) {
	return ok.value, nil
}

func (ng tryNg[T]) Get() (T, error) {
	return *new(T), ng.err
}

func (ok tryOk[T]) OrDefault(d T) T {
	return ok.value
}

func (ng tryNg[T]) OrDefault(d T) T {
	return d
}

func (ok tryOk[T]) OrFatal(Fataler) T {
	return ok.value
}

func (ng tryNg[T]) OrFatal(ftl Fataler) T {
	if hlp, ok := ftl.(interface{ Helper() }); ok {
		hlp.Helper() // think *testing.T
	}
	ftl.Fatal(ng.err)

	return *new(T)
}
