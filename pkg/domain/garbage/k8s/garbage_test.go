package k8s_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/garbage/k8s"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster/mock"
	kubecore "k8s.io/api/core/v1"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestInterface_DestroyGarbage(t *testing.T) {

	type When struct {
		garbage      domain.Garbage
		errDeletePVC error
	}

	type Then struct {
		err error
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			cluster, clientset := mock.NewCluster()

			clientset.Impl.DeletePVC = func(ctx context.Context, namespace, pvcname string) error {
				if pvcname != when.garbage.VolumeRef {
					t.Errorf("pvcname = %s, want %s", pvcname, when.garbage.VolumeRef)
				}

				return when.errDeletePVC
			}
			clientset.Impl.GetPVC = func(ctx context.Context, namespace, pvcname string) (*kubecore.PersistentVolumeClaim, error) {
				return nil, kubeerr.NewNotFound(schema.GroupResource{}, "not found")
			}

			testee := k8s.New(cluster)
			err := testee.DestroyGarbage(context.Background(), when.garbage)

			if then.err == nil {
				if err != nil {
					t.Errorf("err = %v, want nil", err)
				}
			} else {
				if err == nil {
					t.Errorf("err = nil, want %v", then.err)
				} else if !errors.Is(err, then.err) {
					t.Errorf("err = %v, want %v", err, then.err)
				}
			}
		}
	}

	t.Run("if a PVC is deleted successfully, it returns nil", theory(
		When{
			garbage: domain.Garbage{
				VolumeRef: "pvc-1",
			},
			errDeletePVC: nil,
		},
		Then{
			err: nil,
		},
	))

	t.Run("if a PVC is not found, it returns nil", theory(
		When{
			garbage: domain.Garbage{
				VolumeRef: "pvc-2",
			},
			errDeletePVC: kubeerr.NewNotFound(schema.GroupResource{}, "not found"),
		},
		Then{
			err: nil,
		},
	))

	wantErr := errors.New("fake error")
	t.Run("if a PVC is not found, it returns nil", theory(
		When{
			garbage: domain.Garbage{
				VolumeRef: "pvc-3",
			},
			errDeletePVC: wantErr,
		},
		Then{
			err: wantErr,
		},
	))

}
