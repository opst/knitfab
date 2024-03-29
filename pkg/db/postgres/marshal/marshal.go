package marshal

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
)

type ResourceQuantity resource.Quantity

func (a *ResourceQuantity) Equal(b *ResourceQuantity) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	aq := resource.Quantity(*a)
	bq := resource.Quantity(*b)
	return aq.Equal(bq)
}

func (m ResourceQuantity) String() string {
	q := resource.Quantity(m)
	return q.String()
}

func (m ResourceQuantity) Value() (interface{}, error) {
	s := m.String()
	return s, nil
}

func (m *ResourceQuantity) Scan(src interface{}) error {
	expr, ok := src.(string)
	if !ok {
		return fmt.Errorf("Quantity.Scan: unexpected type: %T", src)
	}

	q, err := resource.ParseQuantity(expr)
	if err != nil {
		return err
	}
	*m = ResourceQuantity(q)
	return nil
}
