package mock

import (
	"context"
	"errors"
	"io"
	"regexp"
	"strings"
	"testing"

	k8s "github.com/opst/knitfab/pkg/workloads/k8s"
	kubeapps "k8s.io/api/apps/v1"
	kubebatch "k8s.io/api/batch/v1"
	kubecore "k8s.io/api/core/v1"
	kubeevents "k8s.io/api/events/v1"
	kubeapimeta "k8s.io/apimachinery/pkg/apis/meta/v1"
	applyconfigurations "k8s.io/client-go/applyconfigurations/core/v1"
)

// get mocked k8s.Cluster
//
// # returns
//
//   - k8s.Cluser : using *testutils.MockClient as base client
//   - *testutils.MockClient : mock object.
//     you can fake k8s behaviours or spy its usage.
func NewCluster() (k8s.Cluster, *MockClient) {
	clientset := NewMockClient()

	namespace := "fake-namespace"
	domain := "fake.local"

	return k8s.AttachCluster(clientset, namespace, domain), clientset
}

var reNonAcceptableInLabelValue = regexp.MustCompile("[^-.a-zA-Z0-9]")

const k8slabel_maxlen int = 63

// find minimum number.
func min[T interface {
	// integral full-ordering numbers
	//
	// to generalize to floating point, you should take care NaN,
	// expecially case of a = NaN
	int | int8 | int16 | int32 | int64 |
		uint | uint8 | uint16 | uint32 | uint64
}](a T, b ...T) T {
	m := a

	for _, x := range b {
		if x < m {
			m = x
		}
	}

	return m
}

// convert test name to k8s-label compliant string.
//
// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
func LabelValue(t *testing.T, limit ...int) string {
	name := t.Name()
	name = strings.ToLower(name)
	name = reNonAcceptableInLabelValue.ReplaceAllString(name, "-")
	name = strings.TrimLeft(name, "-._")
	name = strings.TrimRight(name, "-._")

	limit = append(limit, len(name))
	name = name[:min(k8slabel_maxlen, limit...)]
	name = strings.TrimLeft(name, "-._")
	name = strings.TrimRight(name, "-._")

	return name
}

type MockClient struct {
	Impl struct {
		GetService    func(ctx context.Context, namespace string, svcname string) (*kubecore.Service, error)
		CreateService func(ctx context.Context, namespace string, svc *kubecore.Service) (*kubecore.Service, error)
		DeleteService func(ctx context.Context, namespace string, svcname string) error

		GetPVC    func(ctx context.Context, namespace string, pvcname string) (*kubecore.PersistentVolumeClaim, error)
		CreatePVC func(ctx context.Context, namespace string, pvc *kubecore.PersistentVolumeClaim) (*kubecore.PersistentVolumeClaim, error)
		DeletePVC func(ctx context.Context, namespace string, pvcname string) error

		GetDeployment    func(ctx context.Context, namespace string, deplname string) (*kubeapps.Deployment, error)
		CreateDeployment func(ctx context.Context, namespace string, depl *kubeapps.Deployment) (*kubeapps.Deployment, error)
		DeleteDeployment func(ctx context.Context, namespace string, deplname string) error

		GetJob    func(ctx context.Context, namespace string, name string) (*kubebatch.Job, error)
		CreateJob func(ctx context.Context, namespace string, job *kubebatch.Job) (*kubebatch.Job, error)
		DeleteJob func(ctx context.Context, namespace string, name string) error

		CreatePod func(ctx context.Context, namespace string, pod *kubecore.Pod) (*kubecore.Pod, error)
		GetPod    func(ctx context.Context, namespace string, name string) (*kubecore.Pod, error)
		DeletePod func(ctx context.Context, namespace string, name string) error
		FindPods  func(ctx context.Context, namespace string, ls k8s.LabelSelector) ([]kubecore.Pod, error)

		UpsertSecret func(ctx context.Context, namespace string, spec *applyconfigurations.SecretApplyConfiguration) (*kubecore.Secret, error)
		GetSecret    func(ctx context.Context, namespace string, name string) (*kubecore.Secret, error)
		DeleteSecret func(ctx context.Context, namespace string, name string) error

		GetEvents func(ctx context.Context, kind string, target kubeapimeta.ObjectMeta) ([]kubeevents.Event, error)

		Log func(ctx context.Context, namespace string, pod string, container string) (io.ReadCloser, error)
	}
	Called struct {
		GetService    uint64
		CreateService uint64
		DeleteService uint64

		GetPVC    uint64
		CreatePVC uint64
		DeletePVC uint64

		GetDeployment    uint64
		CreateDeployment uint64
		DeleteDeployment uint64

		GetJob    uint64
		CreateJob uint64
		DeleteJob uint64

		CreatePod uint64
		GetPod    uint64
		DeletePod uint64
		FindPods  uint64

		UpsertSecret uint64
		GetSecret    uint64
		DeleteSecret uint64

		GetEvents uint64

		Log uint64
	}
}

// MockClient implements wl.K8sClient
var _ k8s.K8sClient = &MockClient{}

func (m *MockClient) GetService(ctx context.Context, namespace string, svcname string) (*kubecore.Service, error) {
	m.Called.GetService += 1
	if m.Impl.GetService == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.GetService(ctx, namespace, svcname)
}
func (m *MockClient) CreateService(ctx context.Context, namespace string, svc *kubecore.Service) (*kubecore.Service, error) {
	m.Called.CreateService += 1
	if m.Impl.CreateService == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.CreateService(ctx, namespace, svc)
}
func (m *MockClient) DeleteService(ctx context.Context, namespace string, svcname string) error {
	m.Called.DeleteService += 1
	if m.Impl.DeleteService == nil {
		return errors.New("[MOCK] not implemented")
	}
	return m.Impl.DeleteService(ctx, namespace, svcname)
}
func (m *MockClient) GetPVC(ctx context.Context, namespace string, pvcname string) (*kubecore.PersistentVolumeClaim, error) {
	m.Called.GetPVC += 1
	if m.Impl.GetPVC == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.GetPVC(ctx, namespace, pvcname)
}
func (m *MockClient) CreatePVC(ctx context.Context, namespace string, pvc *kubecore.PersistentVolumeClaim) (*kubecore.PersistentVolumeClaim, error) {
	m.Called.CreatePVC += 1
	if m.Impl.CreatePVC == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.CreatePVC(ctx, namespace, pvc)
}
func (m *MockClient) DeletePVC(ctx context.Context, namespace string, pvcname string) error {
	m.Called.DeletePVC += 1
	if m.Impl.DeletePVC == nil {
		return errors.New("[MOCK] not implemented")
	}
	return m.Impl.DeletePVC(ctx, namespace, pvcname)
}
func (m *MockClient) GetDeployment(ctx context.Context, namespace string, deplname string) (*kubeapps.Deployment, error) {
	m.Called.GetDeployment += 1

	if m.Impl.GetDeployment == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.GetDeployment(ctx, namespace, deplname)
}
func (m *MockClient) CreateDeployment(ctx context.Context, namespace string, depl *kubeapps.Deployment) (*kubeapps.Deployment, error) {
	m.Called.CreateDeployment += 1

	if m.Impl.CreateDeployment == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.CreateDeployment(ctx, namespace, depl)
}
func (m *MockClient) DeleteDeployment(ctx context.Context, namespace string, deplname string) error {
	m.Called.DeleteDeployment += 1

	if m.Impl.DeleteDeployment == nil {
		return errors.New("[MOCK] not implemented")
	}
	return m.Impl.DeleteDeployment(ctx, namespace, deplname)
}
func (m *MockClient) GetJob(ctx context.Context, namespace string, name string) (*kubebatch.Job, error) {
	m.Called.GetJob += 1

	if m.Impl.GetJob == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.GetJob(ctx, namespace, name)
}
func (m *MockClient) CreateJob(ctx context.Context, namespace string, job *kubebatch.Job) (*kubebatch.Job, error) {
	m.Called.CreateJob += 1

	if m.Impl.CreateJob == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.CreateJob(ctx, namespace, job)
}
func (m *MockClient) DeleteJob(ctx context.Context, namespace string, name string) error {
	m.Called.DeleteJob += 1

	if m.Impl.DeleteJob == nil {
		return errors.New("[MOCK] not implemented")
	}
	return m.Impl.DeleteJob(ctx, namespace, name)
}
func (m *MockClient) CreatePod(ctx context.Context, namespace string, pod *kubecore.Pod) (*kubecore.Pod, error) {
	m.Called.CreatePod += 1

	if m.Impl.CreatePod == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.CreatePod(ctx, namespace, pod)
}
func (m *MockClient) GetPod(ctx context.Context, namespace string, name string) (*kubecore.Pod, error) {
	m.Called.GetPod += 1

	if m.Impl.GetPod == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.GetPod(ctx, namespace, name)
}
func (m *MockClient) DeletePod(ctx context.Context, namespace string, name string) error {
	m.Called.DeletePod += 1

	if m.Impl.DeletePod == nil {
		return errors.New("[MOCK] not implemented")
	}
	return m.Impl.DeletePod(ctx, namespace, name)
}
func (m *MockClient) FindPods(ctx context.Context, namespace string, ls k8s.LabelSelector) ([]kubecore.Pod, error) {
	m.Called.FindPods += 1

	if m.Impl.FindPods == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.FindPods(ctx, namespace, ls)
}
func (m *MockClient) Log(ctx context.Context, namespace string, pod string, container string) (io.ReadCloser, error) {
	m.Called.Log += 1

	if m.Impl.Log == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.Log(ctx, namespace, pod, container)
}

func (m *MockClient) UpsertSecret(ctx context.Context, namespace string, spec *applyconfigurations.SecretApplyConfiguration) (*kubecore.Secret, error) {
	m.Called.UpsertSecret += 1

	if m.Impl.UpsertSecret == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.UpsertSecret(ctx, namespace, spec)
}

func (m *MockClient) GetSecret(ctx context.Context, namespace string, name string) (*kubecore.Secret, error) {
	m.Called.GetSecret += 1

	if m.Impl.GetSecret == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.GetSecret(ctx, namespace, name)
}

func (m *MockClient) DeleteSecret(ctx context.Context, namespace string, name string) error {
	m.Called.DeleteSecret += 1

	if m.Impl.DeleteSecret == nil {
		return errors.New("[MOCK] not implemented")
	}
	return m.Impl.DeleteSecret(ctx, namespace, name)
}

func (m *MockClient) GetEvents(ctx context.Context, kind string, target kubeapimeta.ObjectMeta) ([]kubeevents.Event, error) {
	m.Called.GetEvents += 1

	if m.Impl.GetEvents == nil {
		return nil, errors.New("[MOCK] not implemented")
	}
	return m.Impl.GetEvents(ctx, kind, target)
}

func NewMockClient() *MockClient {
	return &MockClient{}
}
