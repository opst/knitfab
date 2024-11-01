package cluster_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/opst/knitfab-api-types/misc/rfctime"
	tenv "github.com/opst/knitfab/pkg/conn/k8s/testenv"
	k8serrors "github.com/opst/knitfab/pkg/domain/errors/k8serrors"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
	k8smock "github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster/mock"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/retry"
	"github.com/opst/knitfab/pkg/utils/slices"
	"github.com/opst/knitfab/pkg/utils/try"
	kubeapps "k8s.io/api/apps/v1"
	kubebatch "k8s.io/api/batch/v1"
	kubecore "k8s.io/api/core/v1"
	kubeevent "k8s.io/api/events/v1"
	kubeapierr "k8s.io/apimachinery/pkg/api/errors"
	kubeapiresource "k8s.io/apimachinery/pkg/api/resource"
	kubeapimeta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func ShouldBeError(err error) func(error) bool {
	return func(actual error) bool {
		return errors.Is(actual, err)
	}
}

// to relax test timeout, each `t.Run`s for Cluster are located sepalatedly.
func TestK8SCluster_Service(t *testing.T) {
	t.Run("it creates service", func(t *testing.T) {
		namespace := tenv.Namespace()
		if namespace == "" {
			t.Fatal("no namespace given.")
		}

		domain := tenv.Domain()

		ctx := context.Background()
		clientset := tenv.NewClient()
		testee := cluster.AttachCluster(cluster.WrapK8sClient(clientset), namespace, domain)

		serviceName := "test-service"
		expectedPortA := 34560
		expectedPortB := 34570
		selector := map[string]string{
			"test-selector-1": "sel-1",
		}
		serviceDef := &kubecore.Service{
			ObjectMeta: kubeapimeta.ObjectMeta{
				Name:      serviceName,
				Namespace: namespace,
				Labels: map[string]string{
					"test-label":  "label1",
					"test-label2": "label3",
				},
			},
			Spec: kubecore.ServiceSpec{
				Selector: selector,
				Ports: []kubecore.ServicePort{
					{
						Name:       "port-a",
						Port:       int32(expectedPortA),
						TargetPort: intstr.FromInt(expectedPortA + 2),
					},
					{
						Name:       "port-b",
						Port:       int32(expectedPortB),
						TargetPort: intstr.FromInt(expectedPortB + 2),
					},
				},
			},
		}

		result := <-testee.NewService(
			ctx,
			retry.StaticBackoff(200*time.Millisecond),
			serviceDef,
		)
		defer func() {
			clientset.CoreV1().
				Services(namespace).
				Delete(ctx, serviceName, kubeapimeta.DeleteOptions{})
		}()

		if result.Err != nil {
			t.Fatalf("fail to create service.: %v", result.Err)
		}

		value := result.Value
		svc, err := clientset.CoreV1().Services(namespace).Get(ctx, serviceName, kubeapimeta.GetOptions{})
		if err != nil {
			t.Errorf("cannot get the service. error = %v", err)
		}

		if value.Name() != serviceName {
			t.Errorf(
				"[return value] service name is wrong. (actual, expected) != (%s, %s)",
				value.Name(), serviceName,
			)
		}
		if svc.Name != serviceName {
			t.Errorf(
				"[k8s] service name is wrong. (actual, expected) != (%s, %s)",
				value.Name(), serviceName,
			)
		}

		expectedHost := fmt.Sprintf("test-service.%s.svc.%s", namespace, domain)
		if value.Host() != expectedHost {
			t.Errorf(
				"[return value] service host is wrong. (actual, expected) = (%s, %s)",
				value.Host(), expectedHost,
			)
		}

		if value.IP() != svc.Spec.ClusterIP {
			t.Errorf(
				"[k8s vs return value] service ip is wrong. (testee, k8s) = (%s, %s)",
				value.IP(), svc.Spec.ClusterIP,
			)
		}

		if value.Port("port-a") != int32(expectedPortA) {
			t.Errorf(
				"[return value] port-a of the service is wrong. (actual, expected) = (%d, %d)",
				value.Port("port-a"), expectedPortA,
			)
		}

		actualPortA, ok := slices.First(svc.Spec.Ports, func(p kubecore.ServicePort) bool { return p.Name == "port-a" })
		if !ok {
			t.Errorf("[k8s] port-a is not found in service.")
		} else if actualPortA.Port != int32(expectedPortA) {
			t.Errorf(
				"[k8s] port-a of the service is wrong. (acutual, expected) = (%d, %d)",
				actualPortA.Port, expectedPortA,
			)
		}

		if value.Port("port-b") != int32(expectedPortB) {
			t.Errorf(
				"[return value] port-b of the service is wrong. (actual, expected) = (%d, %d)",
				value.Port("port-b"), expectedPortB,
			)
		}

		actualPortB, ok := slices.First(svc.Spec.Ports, func(p kubecore.ServicePort) bool { return p.Name == "port-b" })
		if !ok {
			t.Error("[k8s] port-b is not found in service.")
		} else if actualPortB.Port != int32(expectedPortB) {
			t.Errorf(
				"[k8s] port-b of the service is wrong. (acutual, expected) = (%d, %d)",
				actualPortB.Port, expectedPortB,
			)
		}

		if !cmp.MapEq(svc.Spec.Selector, selector) {
			t.Errorf(
				"[k8s] selectors is wrong.: (actual, expected) = (%#v, %#v)",
				svc.Spec.Selector, selector,
			)
		}

		if err := value.Close(); err != nil {
			t.Errorf("failed to close Service: %v", err)
		}

		if _, err := clientset.CoreV1().
			Services(namespace).
			Get(ctx, serviceName, kubeapimeta.GetOptions{}); err == nil {
			t.Fatal("the service should be missing.")
		} else if !kubeapierr.IsNotFound(err) {
			t.Errorf("unexpected error. %v", err)
		}
	})

	type expected struct {
		Error func(err error) bool
	}

	type Testcase struct {
		client   *k8smock.MockClient
		ctx      context.Context
		expected expected
	}

	for label, m := range map[string]func() *Testcase{
		"It makes error if CreateService cause error": func() *Testcase {
			expectedErr := errors.New("fake error")
			client := k8smock.NewMockClient()
			client.Impl.CreateService = func(ctx context.Context, namespace string, svc *kubecore.Service) (*kubecore.Service, error) {
				return nil, expectedErr
			}

			return &Testcase{
				client: client,
				ctx:    context.Background(),
				expected: expected{
					Error: ShouldBeError(expectedErr),
				},
			}
		},
		"It makes error if CreateService is OK and GetService cause error": func() *Testcase {
			expectedErr := errors.New("fake error")
			client := k8smock.NewMockClient()
			client.Impl.CreateService = func(ctx context.Context, namespace string, svc *kubecore.Service) (*kubecore.Service, error) {
				// return "creating" Service. let it lack Spec.ClusterIP.
				return &kubecore.Service{}, nil
			}

			client.Impl.GetService = func(ctx context.Context, namespace string, svcname string) (*kubecore.Service, error) {
				return nil, expectedErr
			}

			return &Testcase{
				client: client,
				ctx:    context.Background(),
				expected: expected{
					Error: ShouldBeError(expectedErr),
				},
			}
		},
		"It makes error but not delete service if client.CreateService cause AlreadyExists error": func() *Testcase {
			client := k8smock.NewMockClient()
			client.Impl.CreateService = func(ctx context.Context, namespace string, svc *kubecore.Service) (*kubecore.Service, error) {
				return nil, kubeapierr.NewAlreadyExists(
					schema.GroupResource{Group: "testing", Resource: "test service"},
					"test-service",
				)
			}
			return &Testcase{
				client: client,
				ctx:    context.Background(),
				expected: expected{
					Error: k8serrors.AsConflict,
				},
			}
		},
		"It is cancelled if ctx is cancelled before CreateService is called": func() *Testcase {
			client := k8smock.NewMockClient()
			ctx, cancelled := context.WithCancel(context.Background())

			cancelled()
			return &Testcase{
				client: client,
				ctx:    ctx,
				expected: expected{
					Error: ShouldBeError(context.Canceled),
				},
			}
		},
		"It is cancelled if ctx is cancelled after CreateService is called": func() *Testcase {
			client := k8smock.NewMockClient()
			ctx, cancelled := context.WithCancel(context.Background())
			client.Impl.CreateService = func(ctx context.Context, namespace string, svc *kubecore.Service) (*kubecore.Service, error) {
				// return "creating" Service. let it lack Spec.ClusterIP.
				cancelled()
				return &kubecore.Service{}, nil
			}
			return &Testcase{
				client: client,
				ctx:    ctx,
				expected: expected{
					Error: ShouldBeError(context.Canceled),
				},
			}
		},
		"It is cancelled if ctx is cancelled after GetService is called": func() *Testcase {
			client := k8smock.NewMockClient()
			ctx, cancelled := context.WithCancel(context.Background())
			client.Impl.CreateService = func(ctx context.Context, namespace string, svc *kubecore.Service) (*kubecore.Service, error) {
				// return "creating" Service. let it lack Spec.ClusterIP.
				return &kubecore.Service{}, nil
			}

			client.Impl.GetService = func(ctx context.Context, namespace string, svcname string) (*kubecore.Service, error) {
				// return "creating" Service. let it lack Spec.ClusterIP.
				cancelled()
				return &kubecore.Service{}, nil
			}
			return &Testcase{
				client: client,
				ctx:    ctx,
				expected: expected{
					Error: ShouldBeError(context.Canceled),
				},
			}
		},
	} {
		t.Run(label, func(t *testing.T) {
			condition := m()
			client := condition.client
			ctx := condition.ctx
			expected := condition.expected

			namespace := tenv.Namespace()
			if namespace == "" {
				t.Fatal("no namespace given.")
			}

			domain := tenv.Domain()

			testee := cluster.AttachCluster(client, namespace, domain)

			result := <-testee.NewService(ctx, retry.StaticBackoff(200*time.Millisecond), &kubecore.Service{})
			if !expected.Error(result.Err) {
				t.Errorf("NewService does not causes expected error: %+v", result.Err)
			}
		})
	}
}

func TestK8SCluster_Deployment(t *testing.T) {
	t.Run("it creates deployment", func(t *testing.T) {
		namespace := tenv.Namespace()
		if namespace == "" {
			t.Fatal("no namespace given.")
		}

		domain := tenv.Domain()

		ctx := context.Background()
		clientset := tenv.NewClient()
		testee := cluster.AttachCluster(cluster.WrapK8sClient(clientset), namespace, domain)

		deploymentName := "test-deployment"

		replicas := int32(1)
		selector := map[string]string{
			"testing":  "true",
			"testcase": strings.ReplaceAll(t.Name(), "/", "."), // k8s label can not contain /. only /([a-z0-9]([-._a-z0-9]+[a-z0-9])?)?/i
		}
		container := kubecore.Container{
			Name:    "container-1",
			Image:   "busybox:1.34.1",
			Command: []string{"sh", "-c", "while : ; do sleep 10; done"},
			// make container just live
		}
		deplspec := &kubeapps.Deployment{
			ObjectMeta: kubeapimeta.ObjectMeta{
				Name:      deploymentName,
				Namespace: namespace,
				Labels: map[string]string{
					"testing": "true",
				},
			},
			Spec: kubeapps.DeploymentSpec{
				Replicas: &replicas,
				Selector: &kubeapimeta.LabelSelector{
					MatchLabels: selector,
				},
				Template: kubecore.PodTemplateSpec{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name:      "test-deployment-pod",
						Namespace: namespace,
						Labels:    selector,
					},
					Spec: kubecore.PodSpec{
						Containers: []kubecore.Container{container},
					},
				},
			},
		}

		result := <-testee.NewDeployment(
			ctx, retry.StaticBackoff(200*time.Millisecond), deplspec,
		)
		defer func() {
			clientset.AppsV1().Deployments(namespace).Delete(ctx, deploymentName, *kubeapimeta.NewDeleteOptions(0))
		}()

		if result.Err != nil {
			t.Fatalf("failed to create deployment.: %v", result.Err)
		}

		value := result.Value
		depl, err := clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, kubeapimeta.GetOptions{})
		if err != nil {
			t.Fatalf("cannot retreive the deployment.: %v", result.Err)
		}

		if value.Name() != deploymentName {
			t.Errorf(
				"[return value] deployment name is wrong. (actual, expected) = (%s, %s)",
				value.Name(), deploymentName,
			)
		}
		if depl.ObjectMeta.Name != deploymentName {
			t.Errorf(
				"[k8s] deployment name is wrong. (actual, expected) = (%s, %s)",
				depl.ObjectMeta.Name, deploymentName,
			)
		}

		if value.Namespace() != namespace {
			t.Errorf(
				"[return value] deploied in wrong namespace. (actual, expected) = (%s, %s)",
				value.Namespace(), namespace,
			)
		}
		if depl.ObjectMeta.Namespace != namespace {
			t.Errorf(
				"[k8s] deploied in wrong namespace. (actual, expected) = (%s, %s)",
				depl.ObjectMeta.Namespace, namespace,
			)
		}

		if len(depl.Spec.Template.Spec.Containers) != 1 {
			t.Errorf(
				"[k8s] containers is too many/less. %v",
				depl.Spec.Template.Spec.Containers,
			)
		}

		if !cmp.MapEq(depl.Spec.Template.ObjectMeta.Labels, selector) {
			t.Errorf(
				"[k8s] pod template is given wrong label. (actual, expected) = (%#v, %#v)",
				depl.Spec.Template.ObjectMeta.Labels, selector,
			)
		}

		if depl.Spec.Template.Spec.Containers[0].Image != container.Image {
			t.Errorf(
				"[k8s] container is set wrong image. (actual, expected) = (%s, %s)",
				depl.Spec.Template.Spec.Containers[0].Image,
				container.Image,
			)
		}

		if !cmp.SliceEq(depl.Spec.Template.Spec.Containers[0].Args, container.Args) {
			t.Errorf(
				"[k8s] container is set wrong args. (actual, expected) = (%v, %v)",
				depl.Spec.Template.Spec.Containers[0].Args,
				container.Args,
			)
		}

		{
			// create deployment meanwhile a deployment having same name exists.
			result := <-testee.NewDeployment(ctx, retry.StaticBackoff(200*time.Millisecond), deplspec)
			if !k8serrors.AsConflict(result.Err) {
				t.Errorf("NewDeployment does not causes conflict error: %v", result.Err)
			}
		}

		if err := value.Close(); err != nil {
			t.Fatalf("[k8s] fail to close deployment. %v", err)
		}

		if _, err := clientset.AppsV1().
			Deployments(namespace).
			Get(ctx, deploymentName, kubeapimeta.GetOptions{}); !kubeapierr.IsNotFound(err) {
			t.Errorf("[k8s] failed to delete deployment.: error = %v", err)
		}
	})

	type expected struct {
		Error func(err error) bool
	}
	type condition struct {
		client   *k8smock.MockClient
		ctx      context.Context
		expected expected
	}
	for label, m := range map[string]func() *condition{
		"It makes error if CreateDeployment cause error": func() *condition {
			expectedErr := errors.New("fake error")
			client := k8smock.NewMockClient()
			client.Impl.CreateDeployment = func(ctx context.Context, namespace string, depl *kubeapps.Deployment) (*kubeapps.Deployment, error) {
				return nil, expectedErr
			}

			return &condition{
				client: client,
				ctx:    context.Background(),
				expected: expected{
					Error: ShouldBeError(expectedErr),
				},
			}
		},
		"It makes error if CreateDeployment is OK and GetDeployment cause error": func() *condition {
			expectedError := errors.New("fake error")
			client := k8smock.NewMockClient()
			client.Impl.CreateDeployment = func(ctx context.Context, namespace string, svc *kubeapps.Deployment) (*kubeapps.Deployment, error) {
				// return "creating" Deployment. let it lack Status.Replica.
				return &kubeapps.Deployment{}, nil
			}

			client.Impl.GetDeployment = func(ctx context.Context, namespace string, deplname string) (*kubeapps.Deployment, error) {
				return nil, expectedError
			}
			return &condition{
				client: client,
				ctx:    context.Background(),
				expected: expected{
					Error: ShouldBeError(expectedError),
				},
			}
		},
		"It makes error but not delete Deployment if client.CreateDeployment causes AlreadyExists": func() *condition {
			client := k8smock.NewMockClient()
			client.Impl.CreateDeployment = func(ctx context.Context, namespace string, svc *kubeapps.Deployment) (*kubeapps.Deployment, error) {
				return nil, kubeapierr.NewAlreadyExists(schema.GroupResource{Group: "testing", Resource: "deployment"}, "test-deployment")
			}

			return &condition{
				client: client,
				ctx:    context.Background(),
				expected: expected{
					Error: k8serrors.AsConflict,
				},
			}
		},
		"It is cancelled if ctx is cancelled before CreateDeployment": func() *condition {
			client := k8smock.NewMockClient()
			ctx, cancelled := context.WithCancel(context.Background())

			cancelled()
			return &condition{
				client: client,
				ctx:    ctx,
				expected: expected{
					Error: ShouldBeError(context.Canceled),
				},
			}
		},
		"it is cancelled if ctx is cancelled after CreateDeployment is called": func() *condition {
			client := k8smock.NewMockClient()
			ctx, cancel := context.WithCancel(context.Background())

			client.Impl.CreateDeployment = func(ctx context.Context, namespace string, svc *kubeapps.Deployment) (*kubeapps.Deployment, error) {
				cancel()
				// return "creating" Deployment. let it lack Status.Replica.
				return &kubeapps.Deployment{}, nil
			}
			return &condition{
				client: client,
				ctx:    ctx,
				expected: expected{
					Error: ShouldBeError(context.Canceled),
				},
			}
		},
		"it is cancelled if ctx is cancelled after GetDeployment is called": func() *condition {
			client := k8smock.NewMockClient()
			ctx, cancel := context.WithCancel(context.Background())

			client.Impl.CreateDeployment = func(ctx context.Context, namespace string, svc *kubeapps.Deployment) (*kubeapps.Deployment, error) {
				// return "creating" Deployment. let it lack Status.Replica.
				return &kubeapps.Deployment{}, nil
			}

			client.Impl.GetDeployment = func(ctx context.Context, namespace string, svcname string) (*kubeapps.Deployment, error) {
				cancel()

				// return "creating" Service. let it lack Status.Replica.
				return &kubeapps.Deployment{}, nil
			}
			return &condition{
				client: client,
				ctx:    ctx,
				expected: expected{
					Error: ShouldBeError(context.Canceled),
				},
			}
		},
	} {
		t.Run(label, func(t *testing.T) {
			condition := m()
			client := condition.client
			ctx := condition.ctx
			expected := condition.expected

			namespace := tenv.Namespace()
			if namespace == "" {
				t.Fatal("no namespace given.")
			}

			domain := tenv.Domain()

			testee := cluster.AttachCluster(client, namespace, domain)

			result := <-testee.NewDeployment(
				ctx, retry.StaticBackoff(200*time.Millisecond), &kubeapps.Deployment{},
			)

			if !expected.Error(result.Err) {
				t.Errorf("NewDeployment does not cause expected error: %+v", result.Err)
			}
			if client.Impl.DeleteDeployment != nil {
				if client.Called.DeleteDeployment == 0 {
					t.Errorf("DeleteDeployment is not called")
				}
			}
		})
	}
}

func TestK8SCluster_PVC(t *testing.T) {
	t.Run("it creates PVC", func(t *testing.T) {
		namespace := tenv.Namespace()
		if namespace == "" {
			t.Fatal("no namespace given.")
		}

		domain := tenv.Domain()

		ctx := context.Background()
		clientset := tenv.NewClient()
		testee := cluster.AttachCluster(cluster.WrapK8sClient(clientset), namespace, domain)

		pvcname := "test-pvc"
		storageClassName := "knit-test-workloads-k8s-sc" // defined in chart "knit-test", template "pkg-workloads-k8s"
		capacity := kubeapiresource.MustParse("1Ki")
		pvcDefn := &kubecore.PersistentVolumeClaim{
			ObjectMeta: kubeapimeta.ObjectMeta{
				Name:      pvcname,
				Namespace: namespace,
			},
			Spec: kubecore.PersistentVolumeClaimSpec{
				StorageClassName: &storageClassName,
				AccessModes:      []kubecore.PersistentVolumeAccessMode{kubecore.ReadOnlyMany},
				Resources: kubecore.VolumeResourceRequirements{
					Requests: kubecore.ResourceList{
						kubecore.ResourceStorage: capacity,
					},
				},
			},
		}

		result := <-testee.NewPVC(
			ctx, retry.StaticBackoff(200*time.Millisecond), pvcDefn,
		)
		defer func() {
			clientset.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvcname, *kubeapimeta.NewDeleteOptions(0))
		}()

		if result.Err != nil {
			t.Fatalf("[result] cannot retreive PVC. %v", result.Err)
		}
		pvc, err := clientset.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcname, kubeapimeta.GetOptions{})
		if err != nil {
			t.Fatalf("[k8s] cannot retreive PVC. %v", err)
		}

		value := result.Value
		if !value.ClaimedCapacity().Equal(capacity) {
			t.Errorf(
				"[return value] claimed capacities are not met. (actual, expected) = (%v, %v)",
				value.ClaimedCapacity(), capacity,
			)
		}
		if !pvc.Spec.Resources.Requests.Storage().Equal(capacity) {
			t.Errorf(
				"[k8s] claimed capacities are not met. (actual, expected) = (%v, %v)",
				pvc.Spec.Resources.Requests.Storage(), capacity,
			)
		}

		if value.VolumeName() == "" {
			t.Errorf("[return value] PV name is not given.")
		}
		if pvc.Spec.VolumeName == "" {
			t.Errorf("[k8s] PV name is not given.")
		}

		{
			// create PVC meanwhile a PVC having same name exists.
			result := <-testee.NewPVC(ctx, retry.StaticBackoff(200*time.Millisecond), pvcDefn)
			if !k8serrors.AsConflict(result.Err) {
				t.Errorf("NewPVC does not causes conflict error: %v", result.Err)
			}
		}

		value.Close()
		removing, err := clientset.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcname, kubeapimeta.GetOptions{})

		// pass when err is "NotFound" OR removing is returned and DeletionTimestamp is recorded
		if !(kubeapierr.IsNotFound(err) || (removing != nil && removing.ObjectMeta.DeletionTimestamp != nil)) {
			t.Fatalf("[k8s] could retreive newly created PVC after closed. %v (pvc = %+v)", err, removing)
		}
	})

	type expected struct {
		Error func(err error) bool
	}
	type Testcase struct {
		client   *k8smock.MockClient
		ctx      context.Context
		expected expected
	}

	for label, m := range map[string]func() *Testcase{
		"NewPVC makes error if client.CreatePVC causes error": func() *Testcase {
			expectedErr := errors.New("fake error")
			client := k8smock.NewMockClient()
			client.Impl.CreatePVC = func(ctx context.Context, namespace string, pvc *kubecore.PersistentVolumeClaim) (*kubecore.PersistentVolumeClaim, error) {
				return nil, expectedErr
			}

			return &Testcase{
				client: client,
				ctx:    context.Background(),
				expected: expected{
					Error: ShouldBeError(expectedErr),
				},
			}
		},
		"NewPVC makes error if client.CreatePVC causes AlreadyExists error": func() *Testcase {
			client := k8smock.NewMockClient()
			client.Impl.CreatePVC = func(ctx context.Context, namespace string, pvc *kubecore.PersistentVolumeClaim) (*kubecore.PersistentVolumeClaim, error) {
				return nil, kubeapierr.NewAlreadyExists(schema.GroupResource{Group: "testing", Resource: "new pvc"}, "test-new-pvc")
			}

			return &Testcase{
				client: client,
				ctx:    context.Background(),
				expected: expected{
					Error: k8serrors.AsConflict,
				},
			}
		},
		"NewPVC makes error if client.GetPVC cause error": func() *Testcase {
			expectedError := errors.New("fake error")
			client := k8smock.NewMockClient()
			client.Impl.CreatePVC = func(ctx context.Context, namespace string, svc *kubecore.PersistentVolumeClaim) (*kubecore.PersistentVolumeClaim, error) {
				// return "creating" PVC. let it lack Spec.VolumeName.
				return &kubecore.PersistentVolumeClaim{}, nil
			}

			client.Impl.GetPVC = func(ctx context.Context, namespace string, pvcname string) (*kubecore.PersistentVolumeClaim, error) {
				return nil, expectedError
			}

			client.Impl.DeletePVC = func(ctx context.Context, namespace string, pvcname string) error {
				return nil
			}
			return &Testcase{
				client: client,
				ctx:    context.Background(),
				expected: expected{
					Error: ShouldBeError(expectedError),
				},
			}
		},
		"NewPVC is cancelled if ctx is cancelled before CreatePVC": func() *Testcase {
			client := k8smock.NewMockClient()
			ctx, cancelled := context.WithCancel(context.Background())

			cancelled()
			return &Testcase{
				client: client,
				ctx:    ctx,
				expected: expected{
					Error: ShouldBeError(context.Canceled),
				},
			}
		},
		"NewPVC is cancelled if ctx is cancelled after calling CreatePVC": func() *Testcase {
			client := k8smock.NewMockClient()
			ctx, cancel := context.WithCancel(context.Background())
			client.Impl.CreatePVC = func(ctx context.Context, namespace string, svc *kubecore.PersistentVolumeClaim) (*kubecore.PersistentVolumeClaim, error) {
				cancel()
				return &kubecore.PersistentVolumeClaim{}, nil
			}

			client.Impl.DeletePVC = func(ctx context.Context, namespace string, pvcname string) error {
				return nil
			}
			return &Testcase{
				client: client,
				ctx:    ctx,
				expected: expected{
					Error: ShouldBeError(context.Canceled),
				},
			}
		},
		"NewPVC is cancelled if ctx is cancelled after calling GetPVC": func() *Testcase {
			client := k8smock.NewMockClient()
			ctx, cancel := context.WithCancel(context.Background())
			client.Impl.CreatePVC = func(ctx context.Context, namespace string, svc *kubecore.PersistentVolumeClaim) (*kubecore.PersistentVolumeClaim, error) {
				return &kubecore.PersistentVolumeClaim{}, nil
			}

			client.Impl.GetPVC = func(ctx context.Context, namespace string, pvcname string) (*kubecore.PersistentVolumeClaim, error) {
				cancel()
				return &kubecore.PersistentVolumeClaim{}, nil
			}

			client.Impl.DeletePVC = func(ctx context.Context, namespace string, pvcname string) error {
				return nil
			}
			return &Testcase{
				client: client,
				ctx:    ctx,
				expected: expected{
					Error: ShouldBeError(context.Canceled),
				},
			}
		},
	} {
		t.Run(label, func(t *testing.T) {
			condition := m()
			client := condition.client
			ctx := condition.ctx
			expected := condition.expected

			namespace := tenv.Namespace()
			if namespace == "" {
				t.Fatal("no namespace given.")
			}

			domain := tenv.Domain()

			testee := cluster.AttachCluster(client, namespace, domain)

			result := <-testee.NewPVC(
				ctx, retry.StaticBackoff(200*time.Millisecond), &kubecore.PersistentVolumeClaim{},
			)

			if !expected.Error(result.Err) {
				t.Errorf("NewPVC does not cause expected error: %v", result.Err)
			}
		})
	}

	t.Run("GetPVC retreives PVC", func(t *testing.T) {
		namespace := tenv.Namespace()
		if namespace == "" {
			t.Fatal("no namespace given.")
		}

		domain := tenv.Domain()

		ctx := context.Background()
		clientset := tenv.NewClient()
		testee := cluster.AttachCluster(cluster.WrapK8sClient(clientset), namespace, domain)

		pvcname := "knit-test-workloads-k8s-get-pvc-retreives-pvc"
		{
			try.To(clientset.CoreV1().PersistentVolumeClaims(namespace).Create(
				ctx,
				&kubecore.PersistentVolumeClaim{
					ObjectMeta: kubeapimeta.ObjectMeta{Name: pvcname},
					Spec: kubecore.PersistentVolumeClaimSpec{
						StorageClassName: ref("knit-test-workloads-k8s-sc"),
						AccessModes:      []kubecore.PersistentVolumeAccessMode{kubecore.ReadOnlyMany},
						Resources: kubecore.VolumeResourceRequirements{
							Requests: kubecore.ResourceList{
								kubecore.ResourceStorage: kubeapiresource.MustParse("10Mi"),
							},
						},
					},
				},
				kubeapimeta.CreateOptions{},
			)).OrFatal(t)
			defer func() {
				clientset.CoreV1().PersistentVolumeClaims(namespace).Delete(
					ctx, pvcname,
					*kubeapimeta.NewDeleteOptions(0),
				)
			}()
		}

		result := <-testee.GetPVC(
			ctx, retry.StaticBackoff(200*time.Millisecond), pvcname,
		)
		if result.Err != nil {
			t.Fatalf("PVC is not found, unexpectedly. err: %v", result.Err)
		}

		value := result.Value
		if value.Name() != pvcname {
			t.Errorf("pvc name is wrong.: (actual, expected) = (%s, %s)", value.Name(), pvcname)
		}

		if value.VolumeName() == "" {
			t.Errorf("[return value] PV name is not given.")
		}

		tenMegiBytes := kubeapiresource.MustParse("10Mi")
		if !tenMegiBytes.Equal(value.ClaimedCapacity()) {
			t.Errorf("pvc claimed capacity unmatch. (actual, defined in yaml) = (%v, 10Mi)", value.ClaimedCapacity())
		}

		pvc, err := clientset.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcname, kubeapimeta.GetOptions{})
		if err != nil {
			t.Fatalf("[k8s] cannot retreive PVC. %v", err)
		}
		if pvc.ObjectMeta.Name != pvcname {
			t.Errorf("[k8s] pvc name is wrong (actual, expected) = (%s, %s)", pvc.ObjectMeta.Name, pvcname)
		}
		if pvc.Spec.VolumeName == "" {
			t.Errorf("[k8s] PV name is not given.")
		}

		if !value.ClaimedCapacity().Equal(tenMegiBytes) {
			t.Errorf(
				"[return value] claimed capacities are not met. (actual, expected) = (%v, %v)",
				value.ClaimedCapacity(), tenMegiBytes,
			)
		}
		if !pvc.Spec.Resources.Requests.Storage().Equal(tenMegiBytes) {
			t.Errorf(
				"[k8s] claimed capacities are not met. (actual, expected) = (%v, %v)",
				pvc.Spec.Resources.Requests.Storage(), tenMegiBytes,
			)
		}

		value.Close()
		for {
			if _, err := clientset.CoreV1().
				PersistentVolumeClaims(namespace).
				Get(ctx, pvcname, kubeapimeta.GetOptions{}); kubeapierr.IsNotFound(err) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	})

	for label, m := range map[string]func() *Testcase{
		"GetPVC makes error if client.GetPVC cause error": func() *Testcase {
			expectedError := errors.New("fake error")
			client := k8smock.NewMockClient()
			client.Impl.GetPVC = func(ctx context.Context, namespace string, pvcname string) (*kubecore.PersistentVolumeClaim, error) {
				return nil, expectedError
			}
			return &Testcase{
				client: client,
				ctx:    context.Background(),
				expected: expected{
					Error: ShouldBeError(expectedError),
				},
			}
		},
		"GetPVC is cancelled if ctx is cancelled before GetPVC": func() *Testcase {
			client := k8smock.NewMockClient()
			ctx, cancelled := context.WithCancel(context.Background())

			cancelled()
			return &Testcase{
				client: client,
				ctx:    ctx,
				expected: expected{
					Error: ShouldBeError(context.Canceled),
				},
			}
		},
		"GetPVC is cancelled if ctx is cancelled after calling first GetPVC": func() *Testcase {
			client := k8smock.NewMockClient()
			ctx, cancel := context.WithCancel(context.Background())

			client.Impl.GetPVC = func(ctx context.Context, namespace string, pvcname string) (*kubecore.PersistentVolumeClaim, error) {
				cancel()
				return &kubecore.PersistentVolumeClaim{}, nil
			}

			return &Testcase{
				client: client,
				ctx:    ctx,
				expected: expected{
					Error: ShouldBeError(context.Canceled),
				},
			}
		},
	} {
		t.Run(label, func(t *testing.T) {
			condition := m()
			client := condition.client
			ctx := condition.ctx
			expected := condition.expected

			namespace := tenv.Namespace()
			if namespace == "" {
				t.Fatal("no namespace given.")
			}

			domain := tenv.Domain()

			testee := cluster.AttachCluster(client, namespace, domain)

			result := <-testee.GetPVC(
				ctx, retry.StaticBackoff(200*time.Millisecond), "fake-pvc-name",
			)
			if !expected.Error(result.Err) {
				t.Errorf("GetPVC does not cause expected error: %v", result.Err)
			}
		})
	}

	t.Run("DeletePVC deletes PVC if exists", func(t *testing.T) {
		namespace := tenv.Namespace()
		if namespace == "" {
			t.Fatal("no namespace given.")
		}

		domain := tenv.Domain()

		ctx := context.Background()
		clientset := tenv.NewClient()
		testee := cluster.AttachCluster(cluster.WrapK8sClient(clientset), namespace, domain)

		pvcname := "knit-test-workloads-k8s-delete-pvc-deletes-pvc-if-exists"

		t.Cleanup(func() {
			clientset.CoreV1().
				PersistentVolumeClaims(namespace).
				Delete(context.Background(), pvcname, *kubeapimeta.NewDeleteOptions(0))

			for {
				// make sure PVC is removed.
				if _, err := clientset.CoreV1().
					PersistentVolumeClaims(namespace).
					Get(ctx, pvcname, kubeapimeta.GetOptions{}); kubeapierr.IsNotFound(err) {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
		})

		{
			try.To(
				clientset.CoreV1().
					PersistentVolumeClaims(namespace).
					Create(
						ctx,
						&kubecore.PersistentVolumeClaim{
							ObjectMeta: kubeapimeta.ObjectMeta{Name: pvcname},
							Spec: kubecore.PersistentVolumeClaimSpec{
								StorageClassName: ref("knit-test-workloads-k8s-sc"),
								AccessModes:      []kubecore.PersistentVolumeAccessMode{kubecore.ReadOnlyMany},
								Resources: kubecore.VolumeResourceRequirements{
									Requests: kubecore.ResourceList{
										kubecore.ResourceStorage: kubeapiresource.MustParse("10Mi"),
									},
								},
							},
						},
						kubeapimeta.CreateOptions{},
					),
			).
				OrFatal(t)

			for {
				// wait until PVC is created.
				if _, err := clientset.CoreV1().
					PersistentVolumeClaims(namespace).
					Get(ctx, pvcname, kubeapimeta.GetOptions{}); err == nil {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
		}

		ret := <-testee.DeletePVC(ctx, retry.StaticBackoff(50*time.Millisecond), pvcname)
		if err := ret.Err; err != nil {
			t.Fatalf("DeletePVC failed: %v", err)
		}

		if _, err := clientset.CoreV1().
			PersistentVolumeClaims(namespace).
			Get(ctx, pvcname, kubeapimeta.GetOptions{}); !kubeapierr.IsNotFound(err) {
			t.Errorf("PVC is not deleted: %v", err)
		}
	})

	t.Run("DeletePVC do nothing if PVC is not exists", func(t *testing.T) {
		namespace := tenv.Namespace()
		if namespace == "" {
			t.Fatal("no namespace given.")
		}

		domain := tenv.Domain()

		ctx := context.Background()
		clientset := tenv.NewClient()
		testee := cluster.AttachCluster(cluster.WrapK8sClient(clientset), namespace, domain)

		pvcname := "knit-test-workloads-k8s-delete-pvc-deletes-pvc-if-exists"

		clientset.CoreV1().
			PersistentVolumeClaims(namespace).
			Delete(context.Background(), pvcname, *kubeapimeta.NewDeleteOptions(0))

		for {
			// make sure PVC is not found.
			if _, err := clientset.CoreV1().
				PersistentVolumeClaims(namespace).
				Get(ctx, pvcname, kubeapimeta.GetOptions{}); kubeapierr.IsNotFound(err) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		ret := <-testee.DeletePVC(ctx, retry.StaticBackoff(50*time.Millisecond), pvcname)
		if err := ret.Err; err != nil {
			t.Fatalf("DeletePVC failed: %v", err)
		}

		if _, err := clientset.CoreV1().
			PersistentVolumeClaims(namespace).
			Get(ctx, pvcname, kubeapimeta.GetOptions{}); !kubeapierr.IsNotFound(err) {
			t.Errorf("PVC is not deleted: %v", err)
		}
	})
}

func TestK8SCluster_Pod(t *testing.T) {

	type Then struct {
		PodPhase kubecore.PodPhase
		Ports    map[string]int32

		// port name
		WantServe string
	}

	type When struct {
		PodName       string
		Container     kubecore.Container
		InitContainer []kubecore.Container
	}

	type Testcase struct {
		When When
		Then Then
	}

	theory := func(testcase Testcase) func(*testing.T) {
		return func(t *testing.T) {
			namespace := tenv.Namespace()
			if namespace == "" {
				t.Fatal("no namespace given.")
			}

			domain := tenv.Domain()

			ctx := context.Background()
			if dl, ok := t.Deadline(); ok {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, dl.Add(-10*time.Second))
				defer cancel()
			}

			clientset := tenv.NewClient()
			testee := cluster.AttachCluster(cluster.WrapK8sClient(clientset), namespace, domain)

			podDefn := &kubecore.Pod{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name:      testcase.When.PodName,
					Namespace: namespace,
					Labels:    map[string]string{"testing": "true"},
				},
				Spec: kubecore.PodSpec{
					InitContainers: testcase.When.InitContainer,
					Containers:     []kubecore.Container{testcase.When.Container},
					RestartPolicy:  kubecore.RestartPolicyNever,
				},
			}

			defer func() {
				clientset.CoreV1().
					Pods(namespace).
					Delete(ctx, testcase.When.PodName, *kubeapimeta.NewDeleteOptions(0))
			}()

			{
				// get pod before creation. it should be missing.
				result := <-testee.GetPod(
					ctx, retry.StaticBackoff(200*time.Millisecond), testcase.When.PodName,
				)
				if !k8serrors.AsMissingError(result.Err) {
					t.Errorf("GetPod does not causes expected error (ErrMissing): %v", result.Err)
				}
			}

			result := <-testee.NewPod(
				ctx, retry.StaticBackoff(200*time.Millisecond), podDefn,
				func(value cluster.WithEvents[*kubecore.Pod]) error {
					if value.Value.Status.Phase == testcase.Then.PodPhase {
						return nil
					}
					return retry.ErrRetry
				},
			)

			if result.Err != nil {
				t.Fatalf("failed to create pod.: %v", result.Err)
			}

			if result.Value.Name() != testcase.When.PodName {
				t.Errorf(
					"[return value] pod name is wrong. (actual, expected) = (%s, %s)",
					result.Value.Name(), testcase.When.PodName,
				)
			}

			if !cmp.MapEq(result.Value.Ports(), testcase.Then.Ports) {
				t.Errorf(
					"[return value] pod ports are wrong. (actual, expected) = (%v, %v)",
					result.Value.Ports(), testcase.Then.Ports,
				)
			}

			{
				result := <-testee.GetPod(
					ctx, retry.StaticBackoff(200*time.Millisecond), testcase.When.PodName,
					func(value cluster.WithEvents[*kubecore.Pod]) error {
						if value.Value.Status.Phase == testcase.Then.PodPhase {
							return nil
						}
						return retry.ErrRetry
					},
				)

				if result.Err != nil {
					t.Errorf("pod cannot be retreived: %v", result.Err)
				}

				if result.Value.Name() != testcase.When.PodName {
					t.Errorf(
						"[return value] pod name is wrong. (actual, expected) = (%s, %s)",
						result.Value.Name(), testcase.When.PodName,
					)
				}

				if !cmp.MapEq(result.Value.Ports(), testcase.Then.Ports) {
					t.Errorf(
						"[return value] pod ports are wrong. (actual, expected) = (%v, %v)",
						result.Value.Ports(), testcase.Then.Ports,
					)
				}

				{
					ev := result.Value.Events()
					if len(ev) == 0 {
						t.Errorf("pod events is not given.")
					}
					for _, e := range ev {
						if e.Regarding.Kind != "Pod" {
							t.Errorf("event regarding is not Pod: %v", e)
						}
						if e.Regarding.Name != testcase.When.PodName {
							t.Errorf("event regarding name is wrong: %v", e)
						}
					}
				}
			}

			{
				// create pod again meanwhile a pod having same name exists.
				result := <-testee.NewPod(
					ctx, retry.StaticBackoff(200*time.Millisecond), podDefn,
				)
				if !k8serrors.AsConflict(result.Err) {
					t.Errorf("NewPod does not causes conflict error: %v", result.Err)
				}
			}

			if testcase.Then.WantServe != "" {

				url := fmt.Sprintf(
					"http://%s:%d",
					result.Value.Host(), result.Value.Ports()[testcase.Then.WantServe],
				)

				curl := kubecore.Pod{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name:      fmt.Sprintf("curl-%s", testcase.When.PodName),
						Namespace: namespace,
						Labels:    map[string]string{"testing": "true"},
					},
					Spec: kubecore.PodSpec{
						Containers: []kubecore.Container{
							{
								Name:  "curl",
								Image: "curlimages/curl:8.4.0",
								Args:  []string{"-v", url},
							},
						},
						RestartPolicy: kubecore.RestartPolicyNever,
					},
				}
				defer func() {
					clientset.CoreV1().
						Pods(namespace).
						Delete(ctx, curl.Name, *kubeapimeta.NewDeleteOptions(0))
				}()

				try.To(
					clientset.CoreV1().
						Pods(namespace).
						Create(ctx, &curl, kubeapimeta.CreateOptions{}),
				).OrFatal(t)

			CURL:
				for {
					curl, err := clientset.CoreV1().
						Pods(namespace).
						Get(ctx, curl.Name, kubeapimeta.GetOptions{})
					if err != nil {
						t.Fatalf("cannot retreive the pod.: %v", result.Err)
					}

					switch curl.Status.Phase {
					case kubecore.PodSucceeded:
						break CURL
					case kubecore.PodFailed:
						resp, err := clientset.CoreV1().
							Pods(namespace).
							GetLogs(curl.Name, &kubecore.PodLogOptions{}).
							Do(ctx).
							Raw()

						if err != nil {
							t.Fatalf(
								"curl failed: %s (and cannot retreive log)\n=== error ===\n%+v",
								url, err,
							)
						}
						t.Fatalf(
							"curl failed: %s\n=== log ===\n%s",
							url, string(resp),
						)
					default:
						time.Sleep(50 * time.Millisecond)
					}
				}
			}

			if err := result.Value.Close(); err != nil {
				t.Fatal(err)
			}

			for {
				pod, err := clientset.CoreV1().
					Pods(namespace).
					Get(ctx, testcase.When.PodName, kubeapimeta.GetOptions{})
				if kubeapierr.IsNotFound(err) {
					break
				}
				t.Errorf("pod is not deleted: %+v", pod)
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

	t.Run("it starts nginx", theory(Testcase{
		When: When{
			PodName: "test-pod-nginx",
			Container: kubecore.Container{
				Name:  "container-1",
				Image: "nginx:1.25.3-alpine-slim",
				Ports: []kubecore.ContainerPort{
					{Name: "http", ContainerPort: 80, Protocol: "TCP"},
				},
			},
		},
		Then: Then{
			PodPhase:  kubecore.PodRunning,
			Ports:     map[string]int32{"http": 80},
			WantServe: "http",
		},
	}))

	t.Run("it stops as Failed", theory(Testcase{
		When: When{
			PodName: "test-pod-nginx",
			Container: kubecore.Container{
				Name:    "container-1",
				Image:   "busybox:1.35",
				Command: []string{"false"},
			},
		},
		Then: Then{
			PodPhase: kubecore.PodFailed,
		},
	}))

	t.Run("it stops as Succeeded", theory(Testcase{
		When: When{
			PodName: "test-pod",
			Container: kubecore.Container{
				Name:    "container-1",
				Image:   "busybox:1.35",
				Command: []string{"true"},
			},
		},
		Then: Then{
			PodPhase: kubecore.PodSucceeded,
		},
	}))

	t.Run("it is pending", theory(Testcase{
		When: When{
			PodName: "test-pod",
			InitContainer: []kubecore.Container{
				{
					Name:    "sleeper",
					Image:   "busybox:1.35",
					Command: []string{"sh", "-c", "while : ; do sleep 10; done"},
				},
			},
			Container: kubecore.Container{
				Name:    "container-1",
				Image:   "busybox:1.35",
				Command: []string{"true"},
			},
		},
		Then: Then{
			PodPhase: kubecore.PodPending,
		},
	}))
}

func TestK8sCluster_NewJob_and_GetJob_in_k8s(t *testing.T) {

	type When struct {
		Containers []kubecore.Container
		Volumes    []kubecore.Volume
	}

	type Then struct {
		Name                     string
		Namespace                string
		JobStatus                cluster.JobStatus
		IgnoreMessageOnJobStatus bool

		Log          string
		LogContainer string
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			if dl, ok := t.Deadline(); ok {
				_ctx, cancel := context.WithDeadline(
					ctx,
					dl.Add(-10*time.Second), // margin to cleanup
				)
				defer cancel()
				ctx = _ctx
			}

			namespace := tenv.Namespace()
			if namespace == "" {
				t.Fatal("no namespace is given.")
			}

			domain := tenv.Domain()

			clientset := tenv.NewClient()
			testee := cluster.AttachCluster(cluster.WrapK8sClient(clientset), namespace, domain)

			jobSpec := &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name: "test-job",
				},
				Spec: kubebatch.JobSpec{
					BackoffLimit: ref[int32](0),
					Template: kubecore.PodTemplateSpec{
						Spec: kubecore.PodSpec{
							TerminationGracePeriodSeconds: ref[int64](1), // to quick shutdown.
							RestartPolicy:                 kubecore.RestartPolicyNever,
							Containers:                    when.Containers,
							Volumes:                       when.Volumes,
						},
					},
				},
			}

			defer func() {
				delopt := kubeapimeta.NewDeleteOptions(0)
				delopt.PropagationPolicy = ref(kubeapimeta.DeletePropagationForeground)
				ctx := context.Background()

				clientset.BatchV1().
					Jobs(namespace).
					Delete(ctx, jobSpec.ObjectMeta.Name, *delopt)
				for {
					_, err := clientset.BatchV1().
						Jobs(namespace).
						Get(ctx, jobSpec.ObjectMeta.Name, kubeapimeta.GetOptions{})
					if kubeapierr.IsNotFound(err) {
						break
					}
					time.Sleep(50 * time.Millisecond)
				}
			}()

			got := <-testee.NewJob(
				ctx, retry.StaticBackoff(200*time.Millisecond), jobSpec,
			)
			if got.Err != nil {
				t.Fatal(got.Err)
			}

			{
				// test job existence
				_, err := clientset.BatchV1().
					Jobs(namespace).
					Get(ctx, jobSpec.ObjectMeta.Name, kubeapimeta.GetOptions{})
				if err != nil {
					t.Fatal("job is not created: ", err)
				}
			}

			// tests for returned value
			if gotName := got.Value.Name(); gotName != then.Name {
				t.Errorf("name: not match: (actual, expected) = (%s, %s)", gotName, then.Name)
			}

			if gotNamespace := got.Value.Namespace(); gotNamespace != then.Namespace {
				t.Errorf("namespace: not match: (actual, expected) = (%s, %s)", gotNamespace, then.Namespace)
			}

			for {
				time.Sleep(50 * time.Millisecond)
				select {
				case <-ctx.Done():
					t.Fatalf("timeout")
				default:
				}

				got := <-testee.GetJob(
					ctx, retry.StaticBackoff(200*time.Millisecond), jobSpec.ObjectMeta.Name,
				)

				if got.Err != nil {
					t.Fatalf("unexpected error: %v", got.Err)
				}

				if gotName := got.Value.Name(); gotName != then.Name {
					t.Errorf("name: not match: (actual, expected) = (%s, %s)", gotName, then.Name)
				}

				if gotNamespace := got.Value.Namespace(); gotNamespace != then.Namespace {
					t.Errorf("namespace: not match: (actual, expected) = (%s, %s)", gotNamespace, then.Namespace)
				}

				// wait condition met. If never, test will be timed	out.
				if gotStatus := got.Value.Status(ctx); gotStatus.Type != then.JobStatus.Type {
					continue
				} else {
					if gotStatus.Code != then.JobStatus.Code {
						t.Errorf("job status code: not match: (actual, expected) = (%d, %d)", gotStatus.Code, then.JobStatus.Code)
					}
					if !then.IgnoreMessageOnJobStatus && gotStatus.Message != then.JobStatus.Message {
						t.Errorf("job status message: not match: (actual, expected) = (%s, %s)", gotStatus.Message, then.JobStatus.Message)
					}
				}

				if lc := then.LogContainer; lc != "" {
					func() {
						rc := try.To(got.Value.Log(ctx, lc)).OrFatal(t)
						defer rc.Close()
						gotLog := try.To(io.ReadAll(rc)).OrFatal(t)
						if string(gotLog) != then.Log {
							t.Errorf("log: not match: (actual, expected) = (%s, %s)", gotLog, then.Log)
						}
					}()
				}
				break
			}
		}
	}

	wantNamespace := tenv.Namespace()
	t.Run("successing job with single line log", theory(
		When{
			Containers: []kubecore.Container{
				{
					Name:    "main",
					Image:   "busybox:1.35",
					Command: []string{"sh", "-c", "echo 'hello world'"},
				},
			},
		},
		Then{
			Name:      "test-job",
			Namespace: wantNamespace,
			JobStatus: cluster.JobStatus{Type: cluster.Succeeded},

			LogContainer: "main",
			Log:          "hello world\n",
		},
	))

	t.Run("successing job with multiple line log", theory(
		When{
			Containers: []kubecore.Container{
				{
					Name:    "main",
					Image:   "busybox:1.35",
					Command: []string{"sh", "-c", "echo 'hello world'; sleep 1; echo 'goodbye world'"},
				},
			},
		},
		Then{
			Name:      "test-job",
			Namespace: wantNamespace,
			JobStatus: cluster.JobStatus{Type: cluster.Succeeded},

			LogContainer: "main",
			Log: `hello world
goodbye world
`,
		},
	))

	t.Run("failing job with single line log", theory(
		When{
			Containers: []kubecore.Container{
				{
					Name:    "main",
					Image:   "busybox:1.35",
					Command: []string{"sh", "-c", "echo 'hello world' && exit 1"},
				},
			},
		},
		Then{
			Name:      "test-job",
			Namespace: wantNamespace,
			JobStatus: cluster.JobStatus{Type: cluster.Failed, Code: 1, Message: "(container main) Error"},

			LogContainer: "main",
			Log: `hello world
`,
		},
	))

	t.Run("successing job with multiple container", theory(
		When{
			Containers: []kubecore.Container{
				{
					Name:    "main",
					Image:   "busybox:1.35",
					Command: []string{"sh", "-c", "echo 'hello world'"},
				},
				{
					Name:    "sub",
					Image:   "busybox:1.35",
					Command: []string{"sh", "-c", "echo 'goodbye world'"},
				},
			},
		},
		Then{
			Name:      "test-job",
			Namespace: wantNamespace,
			JobStatus: cluster.JobStatus{Type: cluster.Succeeded},

			LogContainer: "main",
			Log: `hello world
`,
		},
	))

	t.Run("failing job with multiple container", theory(
		When{
			Containers: []kubecore.Container{
				{
					Name:    "main",
					Image:   "busybox:1.35",
					Command: []string{"sh", "-c", "echo 'hello world'"},
				},
				{
					Name:    "sub",
					Image:   "busybox:1.35",
					Command: []string{"sh", "-c", "echo 'goodbye world' && exit 1"},
				},
			},
		},
		Then{
			Name:      "test-job",
			Namespace: wantNamespace,
			JobStatus: cluster.JobStatus{Type: cluster.Failed, Code: 1, Message: "(container sub) Error"},

			LogContainer: "sub",
			Log: `goodbye world
`,
		},
	))
}

func TestK8sCluster_GetJob_with_mock(t *testing.T) {
	type When struct {
		Job       *kubebatch.Job
		GetJobErr error

		Pods        []kubecore.Pod
		FindPodsErr error

		Log      string
		LogError error

		DeleteJobErr error

		GetEvents    []kubeevent.Event
		GetEventsErr error
	}

	type Then struct {
		Name      string
		Namespace string
		Status    cluster.JobStatus

		LogSourcePodName string
	}

	domain := tenv.Domain()
	namespace := tenv.Namespace()
	if namespace == "" {
		t.Fatal("no namespace is given.")
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			jobName := "fake-job"

			mockClient := k8smock.NewMockClient()
			mockClient.Impl.GetJob = func(ctx context.Context, ns string, n string) (*kubebatch.Job, error) {
				if ns != namespace {
					t.Errorf("unexpected namespace: (got, want) = (%s, %s)", ns, namespace)
				}
				if n != jobName {
					t.Errorf("unexpected job name: (got, want) = (%s, %s)", n, jobName)
				}
				return when.Job, when.GetJobErr
			}

			if when.GetJobErr == nil {
				mockClient.Impl.FindPods = func(ctx context.Context, ns string, ls cluster.LabelSelector) ([]kubecore.Pod, error) {
					if ns != namespace {
						t.Errorf("unexpected namespace: (got, want) = (%s, %s)", ns, namespace)
					}

					if want := cluster.LabelsToSelecor(when.Job.Spec.Selector.MatchLabels); !cmp.MapEqWith(ls, want, cluster.SelectorElement.Equal) {
						t.Errorf("unexpected label selector: (got, want) = (%v, %v)", ls, want)
					}

					return when.Pods, when.FindPodsErr
				}
				mockClient.Impl.Log = func(ctx context.Context, ns string, n string, c string) (io.ReadCloser, error) {
					if ns != namespace {
						t.Errorf("unexpected namespace: (got, want) = (%s, %s)", ns, namespace)
					}
					if n != then.LogSourcePodName {
						t.Errorf("unexpected pod name: (got, want) = (%s, %s)", n, then.LogSourcePodName)
					}
					if c != "main" {
						t.Errorf("unexpected container name: (got, want) = (%s, %s)", c, "main")
					}
					return io.NopCloser(strings.NewReader(when.Log)), when.LogError
				}
				mockClient.Impl.GetEvents = func(ctx context.Context, kind string, target kubeapimeta.ObjectMeta) ([]kubeevent.Event, error) {
					if kind != "Pod" {
						t.Errorf("unexpected kind: (got, want) = (%s, %s)", kind, "Pod")
					}
					if target.Namespace != namespace {
						t.Errorf("unexpected namespace: (got, want) = (%s, %s)", target.Namespace, namespace)
					}
					return when.GetEvents, when.GetEventsErr
				}

				deleteJobHasBeenCalled := false
				defer func() {
					if !deleteJobHasBeenCalled {
						t.Errorf("DeleteJob has not been called.")
					}
				}()
				mockClient.Impl.DeleteJob = func(ctx context.Context, ns string, n string) error {
					deleteJobHasBeenCalled = true
					if ns != namespace {
						t.Errorf("unexpected namespace: (got, want) = (%s, %s)", ns, namespace)
					}
					if n != jobName {
						t.Errorf("unexpected job name: (got, want) = (%s, %s)", n, jobName)
					}
					return when.DeleteJobErr
				}
			}
			testee := cluster.AttachCluster(mockClient, namespace, domain)

			got := <-testee.GetJob(
				ctx, retry.StaticBackoff(200*time.Millisecond), jobName,
			)
			if want := when.GetJobErr; want != nil {
				if got.Err == nil {
					t.Fatalf("error is expected, but got nil")
				} else if !errors.Is(got.Err, want) {
					t.Fatalf("unexpected error: %v", got.Err)
				}
				return
			} else if got.Err != nil {
				t.Fatalf("unexpected error: %v", got.Err)
			}

			if gotName := got.Value.Name(); gotName != then.Name {
				t.Errorf("name: not match: (got, want) = (%s, %s)", gotName, then.Name)
			}

			if gotNamespace := got.Value.Namespace(); gotNamespace != then.Namespace {
				t.Errorf("namespace: not match: (got, want) = (%s, %s)", gotNamespace, then.Namespace)
			}

			if gotStatus := got.Value.Status(ctx); gotStatus != then.Status {
				t.Errorf("status: not match: (got, want) = (%+v, %+v)", gotStatus, then.Status)
			}

			gotLog, err := got.Value.Log(ctx, "main")
			if when.LogError != nil {
				if err == nil {
					t.Fatalf("error is expected, but got nil")
				} else if !errors.Is(err, when.LogError) {
					t.Fatalf("unexpected error: %v", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			} else {
				gotLogBytes := try.To(io.ReadAll(gotLog)).OrFatal(t)
				if string(gotLogBytes) != when.Log {
					t.Errorf("log: not match: (got, want) = (%s, %s)", string(gotLogBytes), when.Log)
				}
			}

			if gotErr := got.Value.Close(); gotErr != nil {
				if when.DeleteJobErr == nil {
					t.Fatalf("unexpected error: %v", gotErr)
				} else if !errors.Is(gotErr, when.DeleteJobErr) {
					t.Fatalf("unexpected error: %v", gotErr)
				}
			}
		}
	}

	t.Run("successed job", theory(
		When{
			Job: &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name:      "fake-job",
					Namespace: namespace,
				},
				Spec: kubebatch.JobSpec{
					Selector: &kubeapimeta.LabelSelector{
						MatchLabels: map[string]string{
							"controller":   "fake-job",
							"custom-label": "condition",
						},
					},
				},
				Status: kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{
						{
							Status: "False", // should be ignored
							Type:   kubebatch.JobFailed,
						},
						{
							Status: "True",
							Type:   kubebatch.JobComplete,
						},
					},
				},
			},
			Pods: []kubecore.Pod{
				{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name:      "fake-job-pod-1",
						Namespace: namespace,
					},
					Status: kubecore.PodStatus{
						Phase: kubecore.PodSucceeded,
						ContainerStatuses: []kubecore.ContainerStatus{
							{
								Name: "main",
								State: kubecore.ContainerState{
									Terminated: &kubecore.ContainerStateTerminated{ExitCode: 0},
								},
							},
						},
					},
				},
			},
			Log: `hello world
this is succeeded pod`,
		},
		Then{
			Name:      "fake-job",
			Namespace: namespace,
			Status:    cluster.JobStatus{Type: cluster.Succeeded},

			LogSourcePodName: "fake-job-pod-1",
		},
	))

	t.Run("failed job", theory(
		When{
			Job: &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name:      "fake-job",
					Namespace: namespace,
				},
				Spec: kubebatch.JobSpec{
					Selector: &kubeapimeta.LabelSelector{
						MatchLabels: map[string]string{
							"controller":   "fake-job",
							"custom-label": "condition",
						},
					},
				},
				Status: kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{
						{
							Status: "True",
							Type:   kubebatch.JobFailed,
						},
						{
							Status: "False", // should be ignored
							Type:   kubebatch.JobComplete,
						},
					},
				},
			},
			Pods: []kubecore.Pod{
				{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name:      "fake-job-pod-1",
						Namespace: namespace,
					},
					Status: kubecore.PodStatus{
						Phase: kubecore.PodFailed,
						ContainerStatuses: []kubecore.ContainerStatus{
							{
								Name: "main",
								State: kubecore.ContainerState{
									Terminated: &kubecore.ContainerStateTerminated{ExitCode: 1, Reason: "Crashed"},
								},
							},
						},
					},
				},
			},
			Log: `hello world
this is failed pod
`,
		},
		Then{
			Name:             "fake-job",
			Namespace:        namespace,
			Status:           cluster.JobStatus{Type: cluster.Failed, Code: 1, Message: "(container main) Crashed"},
			LogSourcePodName: "fake-job-pod-1",
		},
	))

	t.Run("Pending job: no pods", theory(
		When{
			Job: &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name:      "fake-job",
					Namespace: namespace,
				},
				Spec: kubebatch.JobSpec{
					Selector: &kubeapimeta.LabelSelector{
						MatchLabels: map[string]string{
							"controller":   "fake-job",
							"custom-label": "condition",
						},
					},
				},
				Status: kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{
						{
							Status: "False", // should be ignored
							Type:   kubebatch.JobFailed,
						},
						{
							Status: "False", // should be ignored
							Type:   kubebatch.JobComplete,
						},
					},
				},
			},
			Pods:     []kubecore.Pod{}, // empty
			LogError: cluster.ErrJobHasNoPods,
		},
		Then{
			Name:      "fake-job",
			Namespace: namespace,
			Status:    cluster.JobStatus{Type: cluster.Pending},
		},
	))

	t.Run("Pending job: no pods are found since error", theory(
		When{
			Job: &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name:      "fake-job",
					Namespace: namespace,
				},
				Spec: kubebatch.JobSpec{
					Selector: &kubeapimeta.LabelSelector{
						MatchLabels: map[string]string{
							"controller":   "fake-job",
							"custom-label": "condition",
						},
					},
				},
			},
			FindPodsErr: errors.New("fake error"),
			LogError:    cluster.ErrJobHasNoPods,
		},
		Then{
			Name:      "fake-job",
			Namespace: namespace,
			Status:    cluster.JobStatus{Type: cluster.Pending},
		},
	))

	t.Run("Pending job: there are pods which is not started", theory(
		When{
			Job: &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name:      "fake-job",
					Namespace: namespace,
				},
				Spec: kubebatch.JobSpec{
					Selector: &kubeapimeta.LabelSelector{
						MatchLabels: map[string]string{
							"controller":   "fake-job",
							"custom-label": "condition",
						},
					},
				},
				Status: kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{
						{
							Status: "False", // should be ignored
							Type:   kubebatch.JobFailed,
						},
						{
							Status: "False", // should be ignored
							Type:   kubebatch.JobComplete,
						},
					},
				},
			},
			Pods: []kubecore.Pod{
				{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name:      "fake-job-pod-1",
						Namespace: namespace,
					},
					Status: kubecore.PodStatus{
						Phase: kubecore.PodPending,
					},
				},
				{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name:      "fake-job-pod-2",
						Namespace: namespace,
					},
					Status: kubecore.PodStatus{
						Phase: kubecore.PodPending,
					},
				},
			},
			GetEvents: []kubeevent.Event{
				{
					EventTime: kubeapimeta.NewMicroTime(try.To(rfctime.ParseRFC3339DateTime(
						"2024-09-15T12:13:14+09:00",
					)).OrFatal(t).Time()),
					ReportingController: "fake-scheduler",
					Reason:              "FailedScheduling",
					Note:                "0/1 nodes are available: 1 Insufficient memory",
					Type:                "Warning",
				},
				{
					EventTime: kubeapimeta.NewMicroTime(try.To(rfctime.ParseRFC3339DateTime(
						"2024-09-15T12:13:15+09:00",
					)).OrFatal(t).Time()),
					ReportingController: "fake-scheduler",
					Reason:              "ScheduleSuccess",
					Note:                "Pod scheduled",
					Type:                "Normal", // overtake the previous Warning event
				},
			},
			Log: `hello world
this is pending pod`,
		},
		Then{
			Name:             "fake-job",
			Namespace:        namespace,
			Status:           cluster.JobStatus{Type: cluster.Pending},
			LogSourcePodName: "fake-job-pod-1",
		},
	))

	t.Run("Pending job: All pods are not scheduled", theory(
		When{
			Job: &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name:      "fake-job",
					Namespace: namespace,
				},
				Spec: kubebatch.JobSpec{
					Selector: &kubeapimeta.LabelSelector{
						MatchLabels: map[string]string{
							"controller":   "fake-job",
							"custom-label": "condition",
						},
					},
				},
				Status: kubebatch.JobStatus{},
			},
			Pods: []kubecore.Pod{
				{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name:      "fake-job-pod-1",
						Namespace: namespace,
					},
					Status: kubecore.PodStatus{
						Phase: kubecore.PodPending,
						Conditions: []kubecore.PodCondition{
							{
								Type:   kubecore.PodScheduled,
								Status: "False",
							},
						},
					},
				},
			},
			GetEvents: []kubeevent.Event{
				{
					EventTime: kubeapimeta.NewMicroTime(try.To(rfctime.ParseRFC3339DateTime(
						"2024-09-15T12:13:14+09:00",
					)).OrFatal(t).Time()),
					ReportingController: "fake-scheduler",
					Reason:              "FailedScheduling",
					Note:                "0/1 nodes are available: 1 Insufficient memory",
					Type:                "Warning",
				},
			},
		},
		Then{
			Name:             "fake-job",
			Namespace:        namespace,
			Status:           cluster.JobStatus{Type: cluster.Pending},
			LogSourcePodName: "fake-job-pod-1",
		},
	))

	t.Run("Stucking job: some Pod is not started AND Warning event is reported", theory(
		When{
			Job: &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name:      "fake-job",
					Namespace: namespace,
				},
				Spec: kubebatch.JobSpec{
					Selector: &kubeapimeta.LabelSelector{
						MatchLabels: map[string]string{
							"controller":   "fake-job",
							"custom-label": "condition",
						},
					},
				},
				Status: kubebatch.JobStatus{},
			},
			Pods: []kubecore.Pod{
				{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name:      "fake-job-pod-1",
						Namespace: namespace,
					},
					Status: kubecore.PodStatus{
						Phase: kubecore.PodPending,
						Conditions: []kubecore.PodCondition{
							{
								Type:   kubecore.PodScheduled,
								Status: "True",
							},
						},
					},
				},
				{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name:      "fake-job-pod-2",
						Namespace: namespace,
					},
					Status: kubecore.PodStatus{
						Phase: kubecore.PodRunning,
						Conditions: []kubecore.PodCondition{
							{
								Type:   kubecore.PodScheduled,
								Status: "True",
							},
						},
					},
				},
			},
			GetEvents: []kubeevent.Event{
				{
					EventTime: kubeapimeta.NewMicroTime(try.To(rfctime.ParseRFC3339DateTime(
						"2024-09-15T12:13:14+09:00",
					)).OrFatal(t).Time()),
					ReportingController: "fake-scheduler",
					Reason:              "VolumeMountFailure",
					Note:                "Permission denied",
					Type:                "Warning",
				},
			},
		},
		Then{
			Name:      "fake-job",
			Namespace: namespace,
			Status: cluster.JobStatus{
				Type: cluster.Stucking, Code: 255,
				Message: "(pod fake-job-pod-1) [VolumeMountFailure] Permission denied",
			},
			LogSourcePodName: "fake-job-pod-1",
		},
	))

	t.Run("Running job: some Pod is not started AND Warning event is not reported", theory(
		When{
			Job: &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name:      "fake-job",
					Namespace: namespace,
				},
				Spec: kubebatch.JobSpec{
					Selector: &kubeapimeta.LabelSelector{
						MatchLabels: map[string]string{
							"controller":   "fake-job",
							"custom-label": "condition",
						},
					},
				},
				Status: kubebatch.JobStatus{},
			},
			Pods: []kubecore.Pod{
				{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name:      "fake-job-pod-1",
						Namespace: namespace,
					},
					Status: kubecore.PodStatus{
						Phase: kubecore.PodPending,
						Conditions: []kubecore.PodCondition{
							{
								Type:   kubecore.PodScheduled,
								Status: "True",
							},
						},
					},
				},
				{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name:      "fake-job-pod-2",
						Namespace: namespace,
					},
					Status: kubecore.PodStatus{
						Phase: kubecore.PodRunning,
						Conditions: []kubecore.PodCondition{
							{
								Type:   kubecore.PodScheduled,
								Status: "True",
							},
						},
					},
				},
			},
		},
		Then{
			Name:             "fake-job",
			Namespace:        namespace,
			Status:           cluster.JobStatus{Type: cluster.Running},
			LogSourcePodName: "fake-job-pod-1",
		},
	))

	t.Run("Error: GetJob returns error", theory(
		When{
			GetJobErr: errors.New("fake error"),
		},
		Then{
			Name:      "fake-job",
			Namespace: namespace,
		},
	))
}

func ref[T any](t T) *T {
	return &t
}
