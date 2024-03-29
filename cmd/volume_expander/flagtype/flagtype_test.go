package flagtype_test

import (
	"flag"
	"testing"

	"github.com/opst/knitfab/cmd/volume_expander/flagtype"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestQuantity(t *testing.T) {
	q := &flagtype.Quantity{}
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.Var(q, "quantity", "quantity flag")
	fs.Parse([]string{"--quantity=1Gi"})

	expected := resource.MustParse("1Gi")

	if !q.AsResourceQuantity().Equal(expected) {
		t.Errorf("expected %v, got %v", expected, q)
	}
}
