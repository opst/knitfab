package mock

import (
	"context"
	"testing"

	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	kpgnom "github.com/opst/knitfab/pkg/domain/nomination/db/postgres"
)

type Call[T any] []T

func (c Call[T]) Times() int {
	return len(c)
}

func New(t *testing.T) *MockNominator {
	return &MockNominator{t: t}
}

type MockNominator struct {
	t    *testing.T
	Impl struct {
		NominateData        func(ctx context.Context, conn kpool.Tx, knitIds []string) error
		NominateMountpoints func(ctx context.Context, conn kpool.Tx, mountpointIds []int) error
		DropData            func(ctx context.Context, conn kpool.Tx, knitIds []string) error
	}
	Calls struct {
		NominateData        Call[[]string]
		NominateMountpoints Call[[]int]
		DropData            Call[[]string]
	}
}

var _ kpgnom.Nominator = &MockNominator{}

// cause test fail.
//
// calling this function leads `panic` or `t.Fatal`, so never returns.
func (n *MockNominator) panic(arg interface{}) {
	if n.t != nil {
		n.t.Fatal(arg)
		return
	}
	panic(arg)
}

func (n *MockNominator) NominateData(
	ctx context.Context, conn kpool.Tx, knitIds []string,
) error {
	if n.t != nil {
		n.t.Helper()
	}

	n.Calls.NominateData = append(n.Calls.NominateData, knitIds)
	if n.Impl.NominateData != nil {
		return n.Impl.NominateData(ctx, conn, knitIds)
	}

	n.panic("should not be called.")
	return nil
}

func (n *MockNominator) NominateMountpoints(ctx context.Context, conn kpool.Tx, mountpointIds []int) error {
	if n.t != nil {
		n.t.Helper()
	}

	n.Calls.NominateMountpoints = append(n.Calls.NominateMountpoints, mountpointIds)
	if n.Impl.NominateMountpoints != nil {
		return n.Impl.NominateMountpoints(ctx, conn, mountpointIds)
	}

	n.panic("should not be called")
	return nil
}

func (n *MockNominator) DropData(ctx context.Context, conn kpool.Tx, knitIds []string) error {
	if n.t != nil {
		n.t.Helper()
	}

	n.Calls.DropData = append(n.Calls.DropData, knitIds)
	if n.Impl.DropData != nil {
		return n.Impl.DropData(ctx, conn, knitIds)
	}

	n.panic("should not be called")
	return nil
}
