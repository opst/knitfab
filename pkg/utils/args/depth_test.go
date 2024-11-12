package args_test

import (
	"testing"

	"github.com/opst/knitfab/pkg/utils/args"
)

func TestDepth(t *testing.T) {
	t.Run("Set finite value", func(t *testing.T) {
		depth := new(args.Depth)
		err := depth.Set("10")
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if depth.Value() != 10 {
			t.Errorf("Expected 10, got %v", depth.Value())
		}
		if depth.IsInfinity() {
			t.Errorf("Expected false, got true")
		}
	})

	t.Run("Set infinity value", func(t *testing.T) {
		depth := new(args.Depth)
		err := depth.Set("all")
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if depth.Value() != 0 {
			t.Errorf("Expected 0, got %v", depth.Value())
		}
		if !depth.IsInfinity() {
			t.Errorf("Expected true, got false")
		}
	})

	t.Run("Set invalid value", func(t *testing.T) {
		depth := new(args.Depth)
		err := depth.Set("invalid")
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
	})

	t.Run("Add positive value", func(t *testing.T) {
		depth := args.NewDepth(10)
		depth = depth.Add(5)
		if depth.Value() != 15 {
			t.Errorf("Expected 15, got %v", depth.Value())
		}
	})

	t.Run("Add negative value", func(t *testing.T) {
		depth := args.NewDepth(10)
		depth = depth.Add(-5)
		if depth.Value() != 5 {
			t.Errorf("Expected 5, got %v", depth.Value())
		}
	})

	t.Run("Add negative value to zero", func(t *testing.T) {
		depth := args.NewDepth(3)
		depth = depth.Add(-5)
		if depth.Value() != 0 {
			t.Errorf("Expected 0, got %v", depth.Value())
		}
	})

	t.Run("Add positive value to infinity", func(t *testing.T) {
		depth := args.NewInfinityDepth()
		depth = depth.Add(5)
		if !depth.IsInfinity() {
			t.Errorf("Expected true, got false")
		}
	})

	t.Run("Add negative value to infinity", func(t *testing.T) {
		depth := args.NewInfinityDepth()
		depth = depth.Add(-5)
		if !depth.IsInfinity() {
			t.Errorf("Expected true, got false")
		}
	})
}
