package k8s_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/retry"
	"github.com/opst/knitfab/pkg/utils/try"
	wl "github.com/opst/knitfab/pkg/workloads"
	k8s "github.com/opst/knitfab/pkg/workloads/k8s"
	k8smock "github.com/opst/knitfab/pkg/workloads/k8s/mock"
	kubeapps "k8s.io/api/apps/v1"
	kubebatch "k8s.io/api/batch/v1"
	kubecore "k8s.io/api/core/v1"
	kubeapierr "k8s.io/apimachinery/pkg/api/errors"
	kubeapiresource "k8s.io/apimachinery/pkg/api/resource"
	kubeapimeta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"

	tenv "github.com/opst/knitfab/pkg/workloads/k8s/testenv"
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
		testee := k8s.AttachCluster(k8s.WrapK8sClient(clientset), namespace, domain)

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

		actualPortA, ok := utils.First(svc.Spec.Ports, func(p kubecore.ServicePort) bool { return p.Name == "port-a" })
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

		actualPortB, ok := utils.First(svc.Spec.Ports, func(p kubecore.ServicePort) bool { return p.Name == "port-b" })
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
					Error: wl.AsConflict,
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

			testee := k8s.AttachCluster(client, namespace, domain)

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
		testee := k8s.AttachCluster(k8s.WrapK8sClient(clientset), namespace, domain)

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
			if !wl.AsConflict(result.Err) {
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
					Error: wl.AsConflict,
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

			testee := k8s.AttachCluster(client, namespace, domain)

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
		testee := k8s.AttachCluster(k8s.WrapK8sClient(clientset), namespace, domain)

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
			if !wl.AsConflict(result.Err) {
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
					Error: wl.AsConflict,
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

			testee := k8s.AttachCluster(client, namespace, domain)

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
		testee := k8s.AttachCluster(k8s.WrapK8sClient(clientset), namespace, domain)

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

			testee := k8s.AttachCluster(client, namespace, domain)

			result := <-testee.GetPVC(
				ctx, retry.StaticBackoff(200*time.Millisecond), "fake-pvc-name",
			)
			if !expected.Error(result.Err) {
				t.Errorf("GetPVC does not cause expected error: %v", result.Err)
			}
		})
	}
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
			testee := k8s.AttachCluster(k8s.WrapK8sClient(clientset), namespace, domain)

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
				if !wl.AsMissingError(result.Err) {
					t.Errorf("GetPod does not causes expected error (ErrMissing): %v", result.Err)
				}
			}

			result := <-testee.NewPod(
				ctx, retry.StaticBackoff(200*time.Millisecond), podDefn,
				func(value *kubecore.Pod) error {
					if value.Status.Phase == testcase.Then.PodPhase {
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
					func(value *kubecore.Pod) error {
						if value.Status.Phase == testcase.Then.PodPhase {
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
				if !wl.AsConflict(result.Err) {
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

func TestK8sCluster_Job(t *testing.T) {
	newDeleteOptions := func(opts ...func(*kubeapimeta.DeleteOptions) *kubeapimeta.DeleteOptions) *kubeapimeta.DeleteOptions {
		d := kubeapimeta.NewDeleteOptions(0)
		for _, o := range opts {
			d = o(d)
		}
		return d
	}

	cascadeForeground := func(do *kubeapimeta.DeleteOptions) *kubeapimeta.DeleteOptions {
		pp := kubeapimeta.DeletePropagationForeground
		do.PropagationPolicy = &pp
		return do
	}

	for name, testcase := range map[string]struct {
		Cmd       []string
		JobStatus k8s.JobStatus
		WantLog   string
	}{
		"when the passed job spec is one to be succeeded is passed, it creates a k8s job which is completed as succeeded": {
			Cmd:       []string{"sh", "-c", "exit 0"},
			JobStatus: k8s.Succeeded,
			WantLog:   ``,
		},
		"when the passed job spec is one to be succeeded is passed, it creates a k8s job which is completed as succeeded with log (stdout)": {
			Cmd:       []string{"sh", "-c", "echo line 1; echo line 2; echo line 3; exit 0"},
			JobStatus: k8s.Succeeded,
			WantLog: `line 1
line 2
line 3
`,
		},
		"when the passed job spec is one to be succeeded is passed, it creates a k8s job which is completed as succeeded with log (stderr)": {
			Cmd:       []string{"sh", "-c", "echo line 1 >&2; echo line 2 >&2; echo line 3 >&2; exit 0"},
			JobStatus: k8s.Succeeded,
			WantLog: `line 1
line 2
line 3
`,
		},
		"when the passed job spec is one to be failed is passed, it creates a k8s job which is completed as Failed": {
			Cmd:       []string{"sh", "-c", "exit 1"},
			JobStatus: k8s.Failed,
			WantLog:   ``,
		},
		"when the passed job spec is one to be failed is passed, it creates a k8s job which is completed as Failed with log (stdout)": {
			Cmd:       []string{"sh", "-c", "echo line A; echo line B; echo line C; exit 1"},
			JobStatus: k8s.Failed,
			WantLog: `line A
line B
line C
`,
		},
		"when the passed job spec is one to be failed is passed, it creates a k8s job which is completed as Failed with log (stderr)": {
			Cmd:       []string{"sh", "-c", "echo line A >&2; echo line B >&2; echo line C >&2; exit 1"},
			JobStatus: k8s.Failed,
			WantLog: `line A
line B
line C
`,
		},
		"when the passed job spec is taking long time to be done, it creates a k8s job and the job is running": {
			Cmd:       []string{"sh", "-c", "while : ; do sleep 10; done"},
			JobStatus: k8s.Running,
		},
	} {
		t.Run("[in k8s] "+name, func(t *testing.T) {
			jobSpec := &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name: "test-job",
				},
				Spec: kubebatch.JobSpec{
					BackoffLimit: ref[int32](0),
					Template: kubecore.PodTemplateSpec{
						Spec: kubecore.PodSpec{
							RestartPolicy:                 kubecore.RestartPolicyNever,
							TerminationGracePeriodSeconds: ref[int64](1), // to quick shutdown
							Containers: []kubecore.Container{
								{
									Name: "main", Image: "busybox:1.35",
									Command: testcase.Cmd,
								},
							},
						},
					},
				},
			}

			namespace := tenv.Namespace()
			if namespace == "" {
				t.Fatal("no namespace is given.")
			}

			domain := tenv.Domain()

			ctx := context.Background()
			clientset := tenv.NewClient()
			testee := k8s.AttachCluster(k8s.WrapK8sClient(clientset), namespace, domain)

			defer func() {
				clientset.BatchV1().
					Jobs(namespace).
					Delete(
						ctx, jobSpec.ObjectMeta.Name,
						*newDeleteOptions(cascadeForeground),
					)
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

			{
				result := <-testee.GetJob(
					ctx, retry.StaticBackoff(200*time.Millisecond), jobSpec.ObjectMeta.Name,
				)
				if !wl.AsMissingError(result.Err) {
					t.Errorf("GetJob does not causes expected error (ErrMissing): %v", result.Err)
				}
			}

			result := <-testee.NewJob(
				ctx, retry.StaticBackoff(200*time.Millisecond), jobSpec,
			)

			if result.Err != nil {
				t.Fatalf("Job is not found, unexpectedly. err: %v", result.Err)
			}

			// tests for returned value
			actual := result.Value
			if actual == nil {
				t.Fatalf("Job is nil, unexpectedly")
			}

			if actual.Name() != jobSpec.ObjectMeta.Name {
				t.Errorf("name: not match: (actual, expected) = (%s, %s)", actual.Name(), jobSpec.ObjectMeta.Name)
			}

			if actual.Namespace() != namespace {
				t.Errorf("namespace: not match: (actual, expected) = (%s, %s)", actual.Namespace(), namespace)
			}

			{
				result := <-testee.NewJob(
					ctx, retry.StaticBackoff(200*time.Millisecond), jobSpec,
				)
				if !wl.AsConflict(result.Err) {
					t.Errorf("NewJob does not causes conflict error: %v", result.Err)
				}
			}

			_ctx := ctx
			if deadline, ok := t.Deadline(); ok {
				var cancel func()
				_ctx, cancel = context.WithDeadline(ctx, deadline.Add(-time.Second))
				defer cancel()
			}
			// Waiting the job status is updated...
			var job k8s.Job
			for {
				result := <-testee.GetJob(
					_ctx, retry.StaticBackoff(200*time.Millisecond), jobSpec.ObjectMeta.Name,
				)

				if result.Err != nil {
					t.Fatal("unexpected error: ", result.Err)
				}
				job = result.Value

				if job.Name() != jobSpec.ObjectMeta.Name {
					t.Errorf("name: not match: (actual, expected) = (%s, %s)", actual.Name(), jobSpec.ObjectMeta.Name)
				}
				if job.Namespace() != namespace {
					t.Errorf("namespace: not match: (actual, expected) = (%s, %s)", actual.Namespace(), namespace)
				}
				if job.Status() == testcase.JobStatus {
					break
				} // otherwise, it gets into infinity loop and shall fail with time-out.
			}

			if job.Status() == k8s.Failed || job.Status() == k8s.Succeeded {
				logs := try.To(job.Log(ctx, "main")).OrFatal(t)
				defer logs.Close()

				logContent := new(strings.Builder)
				if _, err := io.Copy(logContent, logs); err != nil {
					t.Fatal(err)
				}

				if logContent.String() != testcase.WantLog {
					t.Errorf(
						"logs: not match:\n===actual===\n%s\n===expected===\n%s",
						logContent.String(), testcase.WantLog,
					)
				}
			}
		})
	}

	t.Run("[in k8s] when a job comes from NewJob is closed, the job should be removed from k8s", func(t *testing.T) {

		jobSpec := &kubebatch.Job{
			ObjectMeta: kubeapimeta.ObjectMeta{
				Name: "test-job",
			},
			Spec: kubebatch.JobSpec{
				BackoffLimit: ref[int32](0),
				Template: kubecore.PodTemplateSpec{
					Spec: kubecore.PodSpec{
						RestartPolicy:                 kubecore.RestartPolicyNever,
						TerminationGracePeriodSeconds: ref[int64](1), // to quick shutdown.
						Containers: []kubecore.Container{
							{
								Name: "main", Image: "busybox:1.35",
								Command: []string{"sh", "-c", "while : ; do sleep 100; done"},
							},
						},
					},
				},
			},
		}

		namespace := tenv.Namespace()
		if namespace == "" {
			t.Fatal("no namespace is given.")
		}

		domain := tenv.Domain()

		ctx := context.Background()
		clientset := tenv.NewClient()
		testee := k8s.AttachCluster(k8s.WrapK8sClient(clientset), namespace, domain)

		defer func() {
			clientset.BatchV1().
				Jobs(namespace).
				Delete(
					ctx, jobSpec.ObjectMeta.Name,
					*newDeleteOptions(cascadeForeground),
				)
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

		result := <-testee.NewJob(
			ctx, retry.StaticBackoff(200*time.Millisecond), jobSpec,
		)

		if result.Err != nil {
			t.Fatalf("Job is not found, unexpectedly. err: %v", result.Err)
		}

		// tests for returned value
		actual := result.Value

		if _, err := clientset.BatchV1().
			Jobs(namespace).
			Get(ctx, jobSpec.ObjectMeta.Name, kubeapimeta.GetOptions{}); err != nil {
			t.Fatal("job is not created: ", err)
		}

		if err := actual.Close(); err != nil {
			t.Fatal("unexpected error: ", err)
		}

		_ctx := ctx
		if deadline, ok := t.Deadline(); ok {
			var cancel func()
			_ctx, cancel = context.WithDeadline(ctx, deadline.Add(-time.Second))
			defer cancel()
		}

		for {
			_, err := clientset.BatchV1().
				Jobs(namespace).
				Get(_ctx, jobSpec.ObjectMeta.Name, kubeapimeta.GetOptions{})
			if kubeapierr.IsNotFound(err) {
				break // ok
			}
			// otherwise, fail with timeout
			time.Sleep(50 * time.Millisecond)
		}
	})

	t.Run("[in k8s] when a job comes from GetJob is closed, the job should be removed from k8s", func(t *testing.T) {

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
						Containers: []kubecore.Container{
							{
								Name: "main", Image: "busybox:1.35",
								Command: []string{"sh", "-c", "while : ; do sleep 100; done"},
							},
						},
					},
				},
			},
		}

		namespace := tenv.Namespace()
		if namespace == "" {
			t.Fatal("no namespace is given.")
		}

		domain := tenv.Domain()

		ctx := context.Background()
		clientset := tenv.NewClient()
		testee := k8s.AttachCluster(k8s.WrapK8sClient(clientset), namespace, domain)

		defer func() {
			clientset.BatchV1().
				Jobs(namespace).
				Delete(
					ctx, jobSpec.ObjectMeta.Name,
					*newDeleteOptions(cascadeForeground),
				)
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

		if r := <-testee.NewJob(
			ctx, retry.StaticBackoff(200*time.Millisecond), jobSpec,
		); r.Err != nil {
			t.Fatalf("Job is not found, unexpectedly. err: %v", r.Err)
		}

		if _, err := clientset.BatchV1().
			Jobs(namespace).
			Get(ctx, jobSpec.ObjectMeta.Name, kubeapimeta.GetOptions{}); err != nil {
			t.Fatal("job is not created: ", err)
		}

		// tests for returned value
		result := <-testee.GetJob(
			ctx, retry.StaticBackoff(200*time.Millisecond), jobSpec.ObjectMeta.Name,
		)

		if result.Err != nil {
			t.Fatal("unexpected error: ", result.Err)
		}

		actual := result.Value
		if err := actual.Close(); err != nil {
			t.Fatal("unexpected error: ", err)
		}

		for {
			_, err := clientset.BatchV1().
				Jobs(namespace).
				Get(ctx, jobSpec.ObjectMeta.Name, kubeapimeta.GetOptions{})
			if kubeapierr.IsNotFound(err) {
				break // ok
			}
			// otherwise, fail with timeout

			time.Sleep(50 * time.Millisecond)
		}
	})

	// mocked test: testing behaviour of return value
	type when struct {
		job  *kubebatch.JobStatus
		pods []kubecore.Pod
	}
	type then struct {
		Name      string
		Namespace string
		Status    k8s.JobStatus
	}
	for name, testcase := range map[string]struct {
		when
		then
	}{
		`when the new job does NOT have Status="True" in Conditions and there are no pods for the job, it reports the job is Pending`: {
			when{
				job: &kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{{Status: kubecore.ConditionFalse}},
				},
				pods: []kubecore.Pod{},
			},
			then{
				Name:      "fake-job",
				Namespace: "fake-namespace",
				Status:    k8s.Pending,
			},
		},
		`when the new job does NOT have Status="True" in Conditions and the pod of the job is Running, it reports the job is Running`: {
			when{
				job: &kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{}, // empty slice has no conditions with status="True"
				},
				pods: []kubecore.Pod{
					{Status: kubecore.PodStatus{Phase: kubecore.PodRunning}},
				},
			},
			then{
				Name:      "fake-job",
				Namespace: "fake-namespace",
				Status:    k8s.Running,
			},
		},
		`when the new job does NOT have Status="True" in Conditions and the pod of the job is Pending, it reports the job is Pending`: {
			when{
				job: &kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{{Status: kubecore.ConditionFalse}},
				},
				pods: []kubecore.Pod{
					{Status: kubecore.PodStatus{Phase: kubecore.PodPending}},
				},
			},
			then{
				Name:      "fake-job",
				Namespace: "fake-namespace",
				Status:    k8s.Pending,
			},
		},
		`when the new job does NOT have Status="True" in Conditions and pods of the job is Pending AND Running, it reports the job is Running`: {
			when{
				job: &kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{{Status: kubecore.ConditionFalse}},
				},
				pods: []kubecore.Pod{
					{Status: kubecore.PodStatus{Phase: kubecore.PodPending}},
					{Status: kubecore.PodStatus{Phase: kubecore.PodRunning}},
				},
			},
			then{
				Name:      "fake-job",
				Namespace: "fake-namespace",
				Status:    k8s.Running,
			},
		},
		`when the new job does NOT have Status="True" in Conditions and there are pod is Succeeded, it reports the job is Running`: {
			when{
				job: &kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{{Status: kubecore.ConditionFalse}},
				},
				pods: []kubecore.Pod{
					{Status: kubecore.PodStatus{Phase: kubecore.PodSucceeded}},
				},
			},
			then{
				Name:      "fake-job",
				Namespace: "fake-namespace",
				Status:    k8s.Running,
			},
		},
		`when the new job does NOT have Status="True" in Conditions and there are pod is Failed, it reports the job is Running`: {
			when{
				job: &kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{{Status: kubecore.ConditionFalse}},
				},
				pods: []kubecore.Pod{
					{Status: kubecore.PodStatus{Phase: kubecore.PodFailed}},
				},
			},
			then{
				Name:      "fake-job",
				Namespace: "fake-namespace",
				Status:    k8s.Running,
			},
		},
		`when the new job does NOT have Status="True" in Conditions and there are pod is Succeeded and Pending, it reports the job is Running`: {
			when{
				job: &kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{{Status: kubecore.ConditionFalse}},
				},
				pods: []kubecore.Pod{
					{Status: kubecore.PodStatus{Phase: kubecore.PodSucceeded}},
					{Status: kubecore.PodStatus{Phase: kubecore.PodPending}},
				},
			},
			then{
				Name:      "fake-job",
				Namespace: "fake-namespace",
				Status:    k8s.Running,
			},
		},
		`when the new job does NOT have Status="True" in Conditions and there are pod is Failed and Pending, it reports the job is Running`: {
			when{
				job: &kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{{Status: kubecore.ConditionFalse}},
				},
				pods: []kubecore.Pod{
					{Status: kubecore.PodStatus{Phase: kubecore.PodFailed}},
					{Status: kubecore.PodStatus{Phase: kubecore.PodPending}},
				},
			},
			then{
				Name:      "fake-job",
				Namespace: "fake-namespace",
				Status:    k8s.Running,
			},
		},
		`when the new job has Status="True" & Type="Complete" in Conditions, it reports the job is Succeeded`: {
			when{
				job: &kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{
						{Status: kubecore.ConditionTrue, Type: kubebatch.JobComplete},
					},
				},
				pods: []kubecore.Pod{}, // do not care
			},
			then{
				Name:      "fake-job",
				Namespace: "fake-namespace",
				Status:    k8s.Succeeded,
			},
		},
		`when the new job has Status="True" & Type="Failed" in Conditions, it reports the job is Failed`: {
			when{
				job: &kubebatch.JobStatus{
					Conditions: []kubebatch.JobCondition{
						{Status: kubecore.ConditionTrue, Type: kubebatch.JobFailed},
					},
				},
				pods: []kubecore.Pod{}, // do not care
			},
			then{
				Name:      "fake-job",
				Namespace: "fake-namespace",
				Status:    k8s.Failed,
			},
		},
	} {
		t.Run("[mock / NewJob]"+name, func(t *testing.T) {
			when, then := testcase.when, testcase.then

			namespace := tenv.Namespace()
			if namespace == "" {
				t.Fatal("no namespace given.")
			}

			domain := tenv.Domain()

			ctx := context.Background()
			clientset := k8smock.NewMockClient()

			fakeJob := &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name: "test-job",
				},
				Spec: kubebatch.JobSpec{
					BackoffLimit: ref[int32](0),
					Template: kubecore.PodTemplateSpec{
						Spec: kubecore.PodSpec{
							RestartPolicy: kubecore.RestartPolicyNever,
							Containers: []kubecore.Container{
								{
									Name:  "main",
									Image: "busybox:1.35",
									Command: []string{
										"sh", "-c", "echo 'hello world'",
									},
								},
							},
						},
					},
				},
			}

			matchLabels := map[string]string{
				"controller":   "fake-job",
				"custom-label": "condition",
			}

			clientset.Impl.CreateJob = func(ctx context.Context, ns string, j *kubebatch.Job) (*kubebatch.Job, error) {
				if ns != namespace {
					t.Errorf("unexpected namespace: (actual, expected) = (%s, %s)", ns, namespace)
				}
				if j != fakeJob {
					t.Errorf("unexpected job: (actual, expected) = (%v, %v)", *j, *fakeJob)
				}

				job := &kubebatch.Job{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name:      "fake-job",
						Namespace: "fake-namespace",
					},
					Spec: kubebatch.JobSpec{
						BackoffLimit: fakeJob.Spec.BackoffLimit,
						Selector: &kubeapimeta.LabelSelector{
							MatchLabels: matchLabels,
						},
						Template: fakeJob.Spec.Template,
					},
					Status: *when.job,
				}
				return job, nil
			}
			clientset.Impl.FindPods = func(ctx context.Context, ns string, ls k8s.LabelSelector) ([]kubecore.Pod, error) {
				if ns != namespace {
					t.Errorf("unexpected namespace: (actual, expected) = (%s, %s)", ns, namespace)
				}
				if !cmp.MapEqWith(ls, k8s.LabelsToSelecor(matchLabels), k8s.SelectorElement.Equal) {
					t.Errorf("unexpected label selector")
				}

				return when.pods, nil
			}
			testee := k8s.AttachCluster(clientset, namespace, domain)

			result := <-testee.NewJob(
				ctx, retry.StaticBackoff(200*time.Millisecond), fakeJob,
			)

			if result.Err != nil {
				t.Fatalf("unexpected error is retured: %s", result.Err)
			}

			// tests for returned value
			actual := result.Value
			if actual == nil {
				t.Fatalf("Job is nil, unexpectedly")
			}

			if actual.Name() != then.Name {
				t.Errorf("name: not match: (actual, expected) = (%s, %s)", actual.Name(), then.Name)
			}

			if actual.Namespace() != then.Namespace {
				t.Errorf("namespace: not match: (actual, expected) = (%s, %s)", actual.Namespace(), then.Namespace)
			}

			if actual.Status() != then.Status {
				t.Errorf("job status: not match: (actual, expected) = (%s, %s)", actual.Status(), then.Status)
			}
		})

		t.Run("[mock / GetJob]"+name, func(t *testing.T) {
			when, then := testcase.when, testcase.then

			namespace := tenv.Namespace()
			if namespace == "" {
				t.Fatal("no namespace given.")
			}

			domain := tenv.Domain()

			ctx := context.Background()
			clientset := k8smock.NewMockClient()

			jobName := "fake-job"

			matchLabels := map[string]string{
				"controller":   "fake-job",
				"custom-label": "condition",
			}

			clientset.Impl.GetJob = func(ctx context.Context, ns string, n string) (*kubebatch.Job, error) {
				if ns != namespace {
					t.Errorf("unexpected namespace: (actual, expected) = (%s, %s)", ns, namespace)
				}
				if n != jobName {
					t.Errorf("unexpected job name: (actual, expected) = (%s, %s)", n, jobName)
				}

				job := &kubebatch.Job{
					ObjectMeta: kubeapimeta.ObjectMeta{
						Name:      "fake-job",
						Namespace: "fake-namespace",
					},
					Spec: kubebatch.JobSpec{
						Selector: &kubeapimeta.LabelSelector{
							MatchLabels: matchLabels,
						},
					},
					Status: *when.job,
				}
				return job, nil
			}
			clientset.Impl.FindPods = func(ctx context.Context, ns string, ls k8s.LabelSelector) ([]kubecore.Pod, error) {
				if ns != namespace {
					t.Errorf("unexpected namespace: (actual, expected) = (%s, %s)", ns, namespace)
				}
				if !cmp.MapEqWith(ls, k8s.LabelsToSelecor(matchLabels), k8s.SelectorElement.Equal) {
					t.Errorf("unexpected label selector")
				}

				return when.pods, nil
			}
			testee := k8s.AttachCluster(clientset, namespace, domain)

			result := <-testee.GetJob(
				ctx, retry.StaticBackoff(200*time.Millisecond), "fake-job",
			)

			if result.Err != nil {
				t.Fatalf("unexpected error is retured: %s", result.Err)
			}

			// tests for returned value
			actual := result.Value
			if actual == nil {
				t.Fatalf("Job is nil, unexpectedly")
			}

			if actual.Name() != then.Name {
				t.Errorf("name: not match: (actual, expected) = (%s, %s)", actual.Name(), then.Name)
			}

			if actual.Namespace() != then.Namespace {
				t.Errorf("namespace: not match: (actual, expected) = (%s, %s)", actual.Namespace(), then.Namespace)
			}

			if actual.Status() != then.Status {
				t.Errorf("job status: not match: (actual, expected) = (%s, %s)", actual.Status(), then.Status)
			}
		})
	}

	t.Run("[mock / NewJob] when it fails to create a new job, it returns error", func(t *testing.T) {
		namespace := tenv.Namespace()
		if namespace == "" {
			t.Fatal("no namespace given.")
		}

		domain := tenv.Domain()

		ctx := context.Background()
		clientset := k8smock.NewMockClient()

		errorForJob := errors.New("fake error")
		clientset.Impl.CreateJob = func(ctx context.Context, ns string, j *kubebatch.Job) (*kubebatch.Job, error) {
			if ns != namespace {
				t.Errorf("unexpected namespace: (actual, expected) = (%s, %s)", ns, namespace)
			}

			return nil, errorForJob
		}
		clientset.Impl.FindPods = func(ctx context.Context, ns string, ls k8s.LabelSelector) ([]kubecore.Pod, error) {
			t.Errorf("should not be called")

			return nil, errors.New("should not be called")
		}
		testee := k8s.AttachCluster(clientset, namespace, domain)

		result := <-testee.NewJob(
			ctx, retry.StaticBackoff(200*time.Millisecond),
			&kubebatch.Job{}, // anything ok. k8s client is mocked & hadnling JobSpec is tested in other testcase.
		)

		if err := result.Err; !errors.Is(err, errorForJob) {
			t.Fatalf("unexpected error is retured: %s", result.Err)
		}
	})

	t.Run("[mock / NewJob] when it fails to get pods of the new job without Conditions, it returns a job as pending", func(t *testing.T) {
		namespace := tenv.Namespace()
		if namespace == "" {
			t.Fatal("no namespace given.")
		}

		domain := tenv.Domain()

		ctx := context.Background()
		clientset := k8smock.NewMockClient()

		clientset.Impl.CreateJob = func(ctx context.Context, ns string, j *kubebatch.Job) (*kubebatch.Job, error) {
			job := &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name:      "fake-job",
					Namespace: "fake-namespace",
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
					Conditions: []kubebatch.JobCondition{},
				},
			}
			return job, nil
		}

		errorForPod := errors.New("fake error")
		clientset.Impl.FindPods = func(ctx context.Context, ns string, ls k8s.LabelSelector) ([]kubecore.Pod, error) {
			if ns != namespace {
				t.Errorf("unexpected namespace: (actual, expected) = (%s, %s)", ns, namespace)
			}

			return nil, errorForPod
		}
		testee := k8s.AttachCluster(clientset, namespace, domain)

		result := <-testee.NewJob(
			ctx, retry.StaticBackoff(200*time.Millisecond),
			&kubebatch.Job{}, // anything ok. k8s client is mocked & hadnling JobSpec is tested in other testcase.
		)

		if result.Err != nil {
			t.Fatalf("unexpected error is retured: %s", result.Err)
		}

		// tests for returned value
		actual := result.Value
		if actual == nil {
			t.Fatalf("Job is nil, unexpectedly")
		}

		if actual.Status() != k8s.Pending {
			t.Errorf("job status: not match: (actual, expected) = (%s, %s)", actual.Status(), k8s.Pending)
		}
	})

	t.Run("[mock / GetJob] when it fails to get a new job, it returns error", func(t *testing.T) {
		namespace := tenv.Namespace()
		if namespace == "" {
			t.Fatal("no namespace given.")
		}

		domain := tenv.Domain()

		ctx := context.Background()
		clientset := k8smock.NewMockClient()

		errorForJob := errors.New("fake error")
		clientset.Impl.GetJob = func(ctx context.Context, ns string, n string) (*kubebatch.Job, error) {
			if ns != namespace {
				t.Errorf("unexpected namespace: (actual, expected) = (%s, %s)", ns, namespace)
			}

			return nil, errorForJob
		}
		clientset.Impl.FindPods = func(ctx context.Context, ns string, ls k8s.LabelSelector) ([]kubecore.Pod, error) {
			t.Errorf("should not be called")

			return nil, errors.New("should not be called")
		}
		testee := k8s.AttachCluster(clientset, namespace, domain)

		result := <-testee.GetJob(
			ctx, retry.StaticBackoff(200*time.Millisecond), "fake-job",
		)

		if err := result.Err; !errors.Is(err, errorForJob) {
			t.Fatalf("unexpected error is retured: %s", result.Err)
		}
	})

	t.Run("[mock / GetJob] when it fails to get pods of the new job without Conditions, it returns a job as pending", func(t *testing.T) {
		namespace := tenv.Namespace()
		if namespace == "" {
			t.Fatal("no namespace given.")
		}

		domain := tenv.Domain()

		ctx := context.Background()
		clientset := k8smock.NewMockClient()

		clientset.Impl.GetJob = func(ctx context.Context, ns string, n string) (*kubebatch.Job, error) {
			job := &kubebatch.Job{
				ObjectMeta: kubeapimeta.ObjectMeta{
					Name: n, Namespace: "fake-namespace",
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
					Conditions: []kubebatch.JobCondition{},
				},
			}
			return job, nil
		}

		errorForPod := errors.New("fake error")
		clientset.Impl.FindPods = func(ctx context.Context, ns string, ls k8s.LabelSelector) ([]kubecore.Pod, error) {
			if ns != namespace {
				t.Errorf("unexpected namespace: (actual, expected) = (%s, %s)", ns, namespace)
			}

			return nil, errorForPod
		}
		testee := k8s.AttachCluster(clientset, namespace, domain)

		result := <-testee.GetJob(
			ctx, retry.StaticBackoff(200*time.Millisecond), "fake-job",
		)

		if result.Err != nil {
			t.Fatalf("unexpected error is retured: %s", result.Err)
		}

		// tests for returned value
		actual := result.Value
		if actual == nil {
			t.Fatalf("Job is nil, unexpectedly")
		}

		if actual.Status() != k8s.Pending {
			t.Errorf("job status: not match: (actual, expected) = (%s, %s)", actual.Status(), k8s.Pending)
		}
	})
}

func ref[T any](t T) *T {
	return &t
}
