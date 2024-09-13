package gc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	kdb "github.com/opst/knitfab/pkg/db"
	dbmock "github.com/opst/knitfab/pkg/db/mocks"
	k8smock "github.com/opst/knitfab/pkg/workloads/k8s/mock"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestGarbageCollectionTask(t *testing.T) {
	t.Run("if a record is poped, it executes", func(t *testing.T) {
		client := k8smock.NewMockClient()
		client.Impl.DeletePVC = func(ctx context.Context, namespace string, pvcname string) error {
			return nil
		}
		GarbageInterface := dbmock.NewMockGarbageInterface()
		GarbageInterface.Impl.Pop = func(ctx context.Context, callback func(kdb.Garbage) error) (bool, error) {
			// does not implement callback function because the results of the pop method
			// according to the behavior of the callback function have been verified
			return true, nil
		}

		namespace := "test-space"
		testee := Task
		_, pop, err := testee(
			client,
			namespace,
			GarbageInterface)(
			context.Background(),
			Seed(), // first return value is not used in Garbage Collection.
		)

		if pop != true || err != nil {
			t.Errorf("(pop,err) = (%v, %v), want (%v, %v)", pop, err, true, nil)
		}
	})

	t.Run("if an error occurs while a record is popped, it makes error", func(t *testing.T) {
		client := k8smock.NewMockClient()
		client.Impl.DeletePVC = func(ctx context.Context, namespace string, pvcname string) error {
			return nil
		}
		GarbageInterface := dbmock.NewMockGarbageInterface()
		expectedError := fmt.Errorf("expected error")
		GarbageInterface.Impl.Pop = func(ctx context.Context, f func(kdb.Garbage) error) (bool, error) {
			return false, expectedError
		}

		namespace := "test-space"
		testee := Task
		_, pop, err := testee(
			client,
			namespace,
			GarbageInterface,
		)(
			context.Background(),
			Seed(),
		)

		if pop || !errors.Is(err, expectedError) {
			t.Errorf("(pop,err) = (%v, %v), want (%v, %v)", pop, err, false, expectedError)
		}
	})

	t.Run("if an missing error occurs while a delete PVC, it does not makes error", func(t *testing.T) {
		client := k8smock.NewMockClient()
		client.Impl.DeletePVC = func(ctx context.Context, namespace string, pvcname string) error {
			return kubeerr.NewNotFound(schema.GroupResource{}, "not found")
		}
		GarbageInterface := dbmock.NewMockGarbageInterface()
		GarbageInterface.Impl.Pop = func(ctx context.Context, f func(kdb.Garbage) error) (bool, error) {
			return true, nil
		}

		namespace := "test-space"
		testee := Task
		_, pop, err := testee(
			client,
			namespace,
			GarbageInterface,
		)(
			context.Background(),
			Seed(),
		)

		if pop != true || err != nil {
			t.Errorf("(pop,err) = (%v, %v), want (%v, %v)", pop, err, true, nil)
		}
	})

	t.Run("if nothing is poped, it executes", func(t *testing.T) {
		client := k8smock.NewMockClient()
		client.Impl.DeletePVC = func(ctx context.Context, namespace string, pvcname string) error {
			return nil
		}
		GarbageInterface := dbmock.NewMockGarbageInterface()
		GarbageInterface.Impl.Pop = func(ctx context.Context, f func(kdb.Garbage) error) (bool, error) {
			return false, nil
		}

		namespace := "test-space"
		testee := Task
		_, pop, err := testee(
			client,
			namespace,
			GarbageInterface,
		)(
			context.Background(),
			Seed(),
		)

		if pop || err != nil {
			t.Errorf("(pop,err) = (%v, %v), want (%v, %v)", pop, err, false, nil)
		}

	})
}
