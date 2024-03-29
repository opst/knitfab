package utils

func IfNotNil[T any, U any](t *T, mapper func(*T) *U) *U {
	if t == nil {
		return nil
	}
	return mapper(t)
}

func Default[T any](p *T, d T) T {
	if p != nil {
		return *p
	}
	return d
}

func ZeroUnless[T any](p *T) T {
	if p != nil {
		return *p
	}
	return *new(T)
}
