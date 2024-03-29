package tuple

import "fmt"

func PairOf[A, B any](a A, b B) Pair[A, B] {
	return Pair[A, B]{First: a, Second: b}
}

func PairAndError[A, B any](a A, b B, err error) (Pair[A, B], error) {
	return PairOf(a, b), err
}

type Pair[A, B any] struct {
	First  A
	Second B
}

func (p Pair[A, B]) Decompose() (A, B) {
	return p.First, p.Second
}

func (p Pair[A, B]) String() string {
	return fmt.Sprintf(`Pair{%v, %v}`, p.First, p.Second)
}

type Triple[A, B, C any] struct {
	First  A
	Second B
	Third  C
}

func TripleOf[A, B, C any](a A, b B, c C) Triple[A, B, C] {
	return Triple[A, B, C]{First: a, Second: b, Third: c}
}

func (t Triple[A, B, C]) Decompose() (A, B, C) {
	return t.First, t.Second, t.Third
}

func (t Triple[A, B, C]) String() string {
	return fmt.Sprintf(`Triple{%v, %v, %v}`, t.First, t.Second, t.Third)
}

func UnzipPair[A, B any](ps []Pair[A, B]) ([]A, []B) {
	as := make([]A, len(ps))
	bs := make([]B, len(ps))
	for i, p := range ps {
		as[i], bs[i] = p.Decompose()
	}
	return as, bs
}

func ToMap[A comparable, B any](ps []Pair[A, B]) map[A]B {
	ret := make(map[A]B, len(ps))
	for _, p := range ps {
		k, v := p.Decompose()
		ret[k] = v
	}

	return ret
}

func FromMap[A comparable, B any](m map[A]B) []Pair[A, B] {
	ret := make([]Pair[A, B], 0, len(m))
	for k, v := range m {
		ret = append(ret, PairOf(k, v))
	}
	return ret
}

func ToMultiMap[A comparable, B any](ps []Pair[A, B]) map[A][]B {
	ret := make(map[A][]B, len(ps))
	for _, p := range ps {
		k, v := p.Decompose()
		ret[k] = append(ret[k], v)
	}

	return ret
}
