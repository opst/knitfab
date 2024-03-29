package args

type Adapter[T interface{ String() string }] struct {
	value  T
	parser func(string) (T, error)
	isSet  bool
}

func (i *Adapter[T]) String() string {
	if i.isSet {
		return i.value.String()
	}
	return ""
}

func (i *Adapter[T]) Set(s string) error {
	v, err := i.parser(s)
	if err != nil {
		return err
	}
	i.isSet = true
	i.value = v
	return nil
}

func (i Adapter[T]) Value() T {
	return i.value
}

func (i Adapter[T]) IsSet() bool {
	return i.isSet
}

func Parser[T interface{ String() string }](parser func(string) (T, error)) *Adapter[T] {
	return &Adapter[T]{parser: parser}
}
