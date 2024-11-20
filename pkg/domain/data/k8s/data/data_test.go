package data_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/data/k8s/data"
	k8smock "github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster/mock"
	kubecore "k8s.io/api/core/v1"
)

func TestCheckDataIsBound(t *testing.T) {
	type When struct {
		pvc *kubecore.PersistentVolumeClaim
		err error

		knitDataBody domain.KnitDataBody
	}

	type Then struct {
		want bool
		err  error
	}
	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			kcluster, clientset := k8smock.NewCluster()
			clientset.Impl.GetPVC = func(ctx context.Context, namespace, pvcname string) (*kubecore.PersistentVolumeClaim, error) {
				if pvcname != when.knitDataBody.VolumeRef {
					t.Errorf("expected pvc name %s, got %s", when.knitDataBody.VolumeRef, pvcname)
				}

				return when.pvc, when.err
			}

			got, err := data.CheckDataIsBound(context.Background(), kcluster, when.knitDataBody)

			if got != then.want {
				t.Errorf("expected %v, got %v", then.want, got)
			}

			if then.err == nil {
				if err != nil {
					t.Errorf("expected nil error, got %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected error %v, got nil", then.err)
				} else if !errors.Is(err, then.err) {
					t.Errorf("expected error %v, got %v", then.err, err)
				}
			}
		}
	}

	t.Run("pvc is bound", theory(
		When{
			pvc: &kubecore.PersistentVolumeClaim{
				Status: kubecore.PersistentVolumeClaimStatus{
					Phase: kubecore.ClaimBound,
				},
			},
			err: nil,
		},
		Then{
			want: true,
			err:  nil,
		},
	))

	t.Run("pvc is not bound", theory(
		When{
			pvc: &kubecore.PersistentVolumeClaim{
				Status: kubecore.PersistentVolumeClaimStatus{
					Phase: kubecore.ClaimPending,
				},
			},
			err: nil,
		},
		Then{
			want: false,
			err:  nil,
		},
	))

	wantErr := errors.New("fake error")
	t.Run("error", theory(
		When{
			pvc: nil,
			err: wantErr,
		},
		Then{
			want: false,
			err:  wantErr,
		},
	))

}
