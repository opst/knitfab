package flagtype

import "k8s.io/apimachinery/pkg/api/resource"

type Quantity resource.Quantity

func (q *Quantity) String() string {
	return (*resource.Quantity)(q).String()
}

func (q *Quantity) Set(expr string) error {
	parsed, err := resource.ParseQuantity(expr)
	if err != nil {
		return err
	}
	*q = (Quantity)(parsed)
	return nil
}

func (q *Quantity) AsResourceQuantity() *resource.Quantity {
	return (*resource.Quantity)(q)
}

func MustParse(expr string) *Quantity {
	q := Quantity{}
	if err := q.Set(expr); err != nil {
		panic(err)
	}
	return &q
}
