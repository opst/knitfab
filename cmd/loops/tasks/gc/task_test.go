package gc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/opst/knitfab/pkg/domain"
	dbmock "github.com/opst/knitfab/pkg/domain/garbage/db/mock"
	k8smock "github.com/opst/knitfab/pkg/domain/garbage/k8s/mock"
)

func TestGarbageCollectionTask(t *testing.T) {
	t.Run("if a record is poped, it executes", func(t *testing.T) {
		mockK8sInterface := k8smock.New(t)
		mockK8sInterface.Impl.DestroyGarbage = func(ctx context.Context, g domain.Garbage) error {
			return nil
		}

		mockDbInterface := dbmock.NewMockGarbageInterface()
		mockDbInterface.Impl.Pop = func(ctx context.Context, callback func(domain.Garbage) error) (bool, error) {
			// does not implement callback function because the results of the pop method
			// according to the behavior of the callback function have been verified
			return true, nil
		}

		testee := Task(mockK8sInterface, mockDbInterface)
		_, pop, err := testee(
			context.Background(),
			Seed(), // first return value is not used in Garbage Collection.
		)

		if pop != true || err != nil {
			t.Errorf("(pop,err) = (%v, %v), want (%v, %v)", pop, err, true, nil)
		}
	})

	t.Run("if an error occurs while a record is popped, it makes error", func(t *testing.T) {
		mockK8sInterface := k8smock.New(t)
		mockK8sInterface.Impl.DestroyGarbage = func(ctx context.Context, g domain.Garbage) error {
			return nil
		}

		mockDbInterface := dbmock.NewMockGarbageInterface()
		expectedError := fmt.Errorf("expected error")
		mockDbInterface.Impl.Pop = func(ctx context.Context, f func(domain.Garbage) error) (bool, error) {
			return false, expectedError
		}

		testee := Task(mockK8sInterface, mockDbInterface)
		_, pop, err := testee(
			context.Background(),
			Seed(),
		)

		if pop || !errors.Is(err, expectedError) {
			t.Errorf("(pop,err) = (%v, %v), want (%v, %v)", pop, err, false, expectedError)
		}
	})

	t.Run("if nothing is poped, it executes", func(t *testing.T) {
		mockK8sInterface := k8smock.New(t)
		mockK8sInterface.Impl.DestroyGarbage = func(ctx context.Context, g domain.Garbage) error {
			return nil
		}
		GarbageInterface := dbmock.NewMockGarbageInterface()
		GarbageInterface.Impl.Pop = func(ctx context.Context, f func(domain.Garbage) error) (bool, error) {
			return false, nil
		}

		testee := Task(mockK8sInterface, GarbageInterface)
		_, pop, err := testee(
			context.Background(),
			Seed(),
		)

		if pop || err != nil {
			t.Errorf("(pop,err) = (%v, %v), want (%v, %v)", pop, err, false, nil)
		}
	})

	t.Run("if an error occurs while a DestroyGabage, it returns the error", func(t *testing.T) {
		mockK8sInterface := k8smock.New(t)
		expectedError := fmt.Errorf("expected error")
		mockK8sInterface.Impl.DestroyGarbage = func(ctx context.Context, g domain.Garbage) error {
			return expectedError
		}

		mockDbInterface := dbmock.NewMockGarbageInterface()
		mockDbInterface.Impl.Pop = func(ctx context.Context, f func(domain.Garbage) error) (bool, error) {
			err := f(domain.Garbage{})
			if !errors.Is(err, expectedError) {
				t.Errorf("err = %v, want %v", err, expectedError)
			}
			return false, err
		}

		testee := Task(mockK8sInterface, mockDbInterface)
		_, pop, err := testee(
			context.Background(),
			Seed(),
		)

		if pop || !errors.Is(err, expectedError) {
			t.Errorf("(pop,err) = (%v, %v), want (%v, %v)", pop, err, false, expectedError)
		}
	})
}
