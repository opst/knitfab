package testenv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	kos "github.com/opst/knitfab/pkg/utils/os"
	"github.com/opst/knitfab/pkg/workloads/k8s"
	kubeapps "k8s.io/api/apps/v1"
	kubebatch "k8s.io/api/batch/v1"
	kubecore "k8s.io/api/core/v1"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
	kubeapimeta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

const (
	ENV_KNIT_TEST_KUBECONFIG = "KNIT_TEST_KUBECONFIG"
	ENV_KNIT_TEST_KUBECTX    = "KNIT_TEST_KUBECTX"
	ENV_KNIT_TEST_NAMESPACE  = "KNIT_TEST_NAMESPACE"
	ENV_KNIT_TEST_DOMAIN     = "KNIT_TEST_DOMAIN"

	ENV_IMAGE_DATAAGT = "KNIT_TEST_DATAAGT_IMAGENAME"
	ENV_IMAGE_NURSE   = "KNIT_TEST_NURSE_IMAGENAME"
	ENV_IMAGE_EMPTY   = "KNIT_TEST_EMPTY_IMAGENAME"

	DEFAULT_DOMAIN     = "cluster.local"
	STORAGE_CLASS_NAME = "knit-test-workloads-dataagt-sc"
)

// image names for test
//
// In case of switching image via envvar, it evaluates envvar for each times.
func Images() struct {
	Dataagt string
	Nurse   string
	Empty   string
} {
	return struct {
		Dataagt string
		Nurse   string
		Empty   string
	}{
		Dataagt: kos.GetEnvOr(ENV_IMAGE_DATAAGT, "knit-dataagt:TEST"),
		Nurse:   kos.GetEnvOr(ENV_IMAGE_NURSE, "knit-nurse:TEST"),
		Empty:   kos.GetEnvOr(ENV_IMAGE_EMPTY, "knit-empty:TEST"),
	}
}

func Namespace() string {
	return os.Getenv(ENV_KNIT_TEST_NAMESPACE)
}

func Domain() string {
	return kos.GetEnvOr(ENV_KNIT_TEST_DOMAIN, DEFAULT_DOMAIN)
}

func getConfig() (*rest.Config, error) {
	kubeconfig := os.Getenv(ENV_KNIT_TEST_KUBECONFIG)
	context := os.Getenv(ENV_KNIT_TEST_KUBECTX)

	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: context},
	).ClientConfig()
}

func NewClient() *kubernetes.Clientset {
	config, err := getConfig()
	if err != nil {
		panic(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	return clientset
}

// port-forwarding to k8s pod in service.
type Portforwarding interface {
	// local address of the port-forwarding
	LocalAddr() string

	// cased error on port-forwarding.
	//
	// If it is not nil, port-forwarding is no longer available.
	Err() error
}

type portforwarding struct {
	localAddr string
	err       error
}

func (pf *portforwarding) LocalAddr() string {
	return pf.localAddr
}

func (pf *portforwarding) Err() error {
	return pf.err
}

type pfoption struct {
	log interface{ Log(...any) }
}

type PortForwardOption func(*pfoption) *pfoption

// Subscribe log from port-forwarding and send to Log method.
//
// If this option is not given, logs will be discarded.
//
// If you pass this option multiple times, only the last one will be used.
//
// # Args
//
// - l interface{ Log(...any) } : log receiver, like *testing.T
func WithLog(l interface{ Log(...any) }) PortForwardOption {
	return func(o *pfoption) *pfoption {
		o.log = l
		return o
	}
}

// port-forwarding to k8s pod in service.
//
// # Args
//
// - ctx context.Context : context for port-forwarding
// It should be canceled when the port-forwarding is no longer needed.
//
// - namespace string : namespace of the pod
//
// - svcName string : name of the service
//
// - portName string : name of the port
//
// # Returns
//
// - string : address of the port-forwarding
//
// - error : error if any
func Portforward(
	ctx context.Context, namespace, svcName string, portName string,
	opts ...PortForwardOption,
) (Portforwarding, error) {

	opt := &pfoption{}
	for _, o := range opts {
		opt = o(opt)
	}

	client := NewClient()
	svc, err := client.CoreV1().Services(namespace).Get(ctx, svcName, kubeapimeta.GetOptions{})
	if err != nil {
		return nil, err
	}

	var podPort string
	for _, p := range svc.Spec.Ports {
		if p.Name != portName {
			continue
		}
		podPort = p.TargetPort.String()
		break
	}

	pods, err := client.CoreV1().Pods(namespace).List(ctx, kubeapimeta.ListOptions{
		LabelSelector: kubeapimeta.FormatLabelSelector(
			&kubeapimeta.LabelSelector{MatchLabels: svc.Spec.Selector},
		),
	})
	if err != nil {
		return nil, err
	}

	if len(pods.Items) <= 0 {
		return nil, errors.New("no pods found")
	}
	pod := pods.Items[0]

	config, err := getConfig()
	if err != nil {
		return nil, err
	}
	roundTripper, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return nil, err
	}

	host := config.Host
	if _, h, ok := strings.Cut(host, "//"); ok { // remove scheme
		host = h
	}

	d := spdy.NewDialer(
		upgrader, &http.Client{Transport: roundTripper},
		http.MethodPost,
		&url.URL{
			Scheme: "https",
			Host:   host,
			Path: fmt.Sprintf(
				"/api/v1/namespaces/%s/pods/%s/portforward", namespace, pod.Name,
			),
		},
	)

	readyChan := make(chan struct{})
	out := io.Discard
	if opt.log != nil {
		o := new(bytes.Buffer)
		out = o

		go func() {
			header := "port-forwarding>"
			line := new(strings.Builder)
			defer func() {
				if line.String() != "" {
					opt.log.Log(header, line)
				}
			}()

			buf := make([]byte, 1024)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				n, err := o.Read(buf)
				if errors.Is(err, io.EOF) {
					return
				}
				s := string(buf[:n])
				tail, body, ok := strings.Cut(s, "\n")
				line.WriteString(tail)
				if ok {
					opt.log.Log(header, line.String())
					line = new(strings.Builder)
					line.WriteString(body)
				}
			}
		}()
	}

	forwarder, err := portforward.New(
		d, []string{"0:" + podPort}, ctx.Done(), readyChan, out, out,
	)
	if err != nil {
		return nil, err
	}

	pf := &portforwarding{}

	go func() {
		if err := forwarder.ForwardPorts(); err != nil {
			pf.err = err
		}
	}()

	<-readyChan
	ports, err := forwarder.GetPorts()
	if err != nil {
		return nil, err
	}

	pf.localAddr = fmt.Sprintf("localhost:%d", ports[0].Local)
	return pf, nil
}

// get external endpoint (host:port) directing k8s service.
func DetectExternalAddr(ctx context.Context, svcName string, portName string) (string, error) {
	clientset := NewClient()

	namespace := os.Getenv(ENV_KNIT_TEST_NAMESPACE)
	if namespace == "" {
		return "", errors.New("no namespace are given")
	}

	svc, err := clientset.CoreV1().Services(namespace).Get(ctx, svcName, kubeapimeta.GetOptions{})
	if err != nil {
		return "", err
	}
	var port *int32
	for _, p := range svc.Spec.Ports {
		if p.Name != portName {
			continue
		}
		port = &p.NodePort
	}

	nodes, err := clientset.CoreV1().Nodes().List(ctx, kubeapimeta.ListOptions{})
	if err != nil {
		return "", err
	}
	if len(nodes.Items) <= 0 {
		return "", errors.New("no node found")
	}
	n := nodes.Items[0]
	for _, addr := range n.Status.Addresses {
		if addr.Type != kubecore.NodeInternalIP {
			continue
		}
		return fmt.Sprintf("%s:%d", addr.Address, *port), nil
	}
	return "", errors.New("no addresses are found")
}

// get k8s.Cluster of test envirionment.
//
// Each k8s resource which are crated in a testcase, will be deleted after the testcase.
//
// this function reqiures envitonment variables below:
//
// - KNIT_TEST_KUBECONFIG : filepath to kubeconfig file knows test environment
//
// - KNIT_TEST_NAMESPACE : k8s namespace for testing
//
// - KNIT_TEST_KUBECTX : k8s context for testing. (should be found in KNIT_TEST_KUBECONFIG file)
//
// and optionally,
//
//   - KNIT_TEST_DOMAIN : k8s in-cluster second-level domain.
//     If you custom your testing cluster, you should set it.
//     (default: "cluster.local")
//
// # returns
//
// - k8s.Cluster
// - *kubernetes.Clientset : base client of the above k8s.Cluster.
func NewCluster(t *testing.T) (k8s.Cluster, *kubernetes.Clientset) {
	t.Helper()

	namespace := Namespace()
	if namespace == "" {
		t.Fatal("no namespace given.")
	}

	domain := Domain()

	clientset := NewClient()
	c := &k8sclient{
		base: k8s.WrapK8sClient(clientset),
		t:    t,
	}
	return k8s.AttachCluster(c, namespace, domain), clientset
}

type k8sclient struct {
	base k8s.K8sClient
	// // DO NOT embed this directly like
	//
	// k8s.K8sClient
	//
	// // this is to detect missing `t.Cleanup` in Create* method comming in future.

	t *testing.T
}

var _ k8s.K8sClient = &k8sclient{}

func (kc *k8sclient) CreateService(ctx context.Context, namespace string, svc *kubecore.Service) (*kubecore.Service, error) {
	name := svc.ObjectMeta.Name

	r, err := kc.base.CreateService(ctx, namespace, svc)

	if err == nil {
		kc.t.Cleanup(func() {
			err := kc.base.DeleteService(context.Background(), namespace, name)
			if err == nil || kubeerr.IsNotFound(err) {
				return
			}
			kc.t.Fatal(err)
		})
	}

	return r, err
}

func (kc *k8sclient) GetService(ctx context.Context, namespace string, svcname string) (*kubecore.Service, error) {
	return kc.base.GetService(ctx, namespace, svcname)
}
func (kc *k8sclient) DeleteService(ctx context.Context, namespace string, svcname string) error {
	return kc.base.DeleteService(ctx, namespace, svcname)
}

func (kc *k8sclient) CreatePVC(ctx context.Context, namespace string, pvc *kubecore.PersistentVolumeClaim) (*kubecore.PersistentVolumeClaim, error) {
	name := pvc.ObjectMeta.Name

	r, err := kc.base.CreatePVC(ctx, namespace, pvc)

	if err == nil {
		kc.t.Cleanup(func() {
			err := kc.base.DeletePVC(context.Background(), namespace, name)
			if err == nil || kubeerr.IsNotFound(err) {
				return
			}
			kc.t.Fatal(err)
		})
	}

	return r, err
}
func (kc *k8sclient) GetPVC(ctx context.Context, namespace string, pvcname string) (*kubecore.PersistentVolumeClaim, error) {
	return kc.base.GetPVC(ctx, namespace, pvcname)
}
func (kc *k8sclient) DeletePVC(ctx context.Context, namespace string, pvcname string) error {
	return kc.base.DeletePVC(ctx, namespace, pvcname)
}

func (kc *k8sclient) CreateDeployment(ctx context.Context, namespace string, depl *kubeapps.Deployment) (*kubeapps.Deployment, error) {
	name := depl.ObjectMeta.Name

	r, err := kc.base.CreateDeployment(ctx, namespace, depl)

	if err == nil {
		kc.t.Cleanup(func() {
			err := kc.base.DeleteDeployment(context.Background(), namespace, name)
			if err == nil || kubeerr.IsNotFound(err) {
				return
			}
			kc.t.Fatal(err)
		})
	}

	return r, err
}
func (kc *k8sclient) GetDeployment(ctx context.Context, namespace string, deplname string) (*kubeapps.Deployment, error) {
	return kc.base.GetDeployment(ctx, namespace, deplname)
}
func (kc *k8sclient) DeleteDeployment(ctx context.Context, namespace string, deplname string) error {
	return kc.base.DeleteDeployment(ctx, namespace, deplname)
}

func (kc *k8sclient) CreateJob(ctx context.Context, namespace string, spec *kubebatch.Job) (*kubebatch.Job, error) {
	name := spec.ObjectMeta.Name

	r, err := kc.base.CreateJob(ctx, namespace, spec)

	if err == nil {
		kc.t.Cleanup(func() {
			err := kc.base.DeleteJob(context.Background(), namespace, name)
			if err == nil || kubeerr.IsNotFound(err) {
				return
			}
			kc.t.Fatal(err)
		})
	}

	return r, err
}
func (kc *k8sclient) GetJob(ctx context.Context, namespace string, name string) (*kubebatch.Job, error) {
	return kc.base.GetJob(ctx, namespace, name)
}
func (kc *k8sclient) DeleteJob(ctx context.Context, namespace string, name string) error {
	return kc.base.DeleteJob(ctx, namespace, name)
}

func (kc *k8sclient) CreatePod(ctx context.Context, namespace string, pod *kubecore.Pod) (*kubecore.Pod, error) {
	name := pod.ObjectMeta.Name

	r, err := kc.base.CreatePod(ctx, namespace, pod)

	if err == nil {
		kc.t.Cleanup(func() {
			err := kc.base.DeletePod(context.Background(), namespace, name)
			if err == nil || kubeerr.IsNotFound(err) {
				return
			}
			kc.t.Fatal(err)
		})
	}

	return r, err
}

func (kc *k8sclient) GetPod(ctx context.Context, namespace string, name string) (*kubecore.Pod, error) {
	return kc.base.GetPod(ctx, namespace, name)
}

func (kc *k8sclient) DeletePod(ctx context.Context, namespace string, name string) error {
	return kc.base.DeletePod(ctx, namespace, name)
}

func (kc *k8sclient) FindPods(ctx context.Context, namespace string, labelSelector k8s.LabelSelector) ([]kubecore.Pod, error) {
	return kc.base.FindPods(ctx, namespace, labelSelector)
}

func (kc *k8sclient) Log(ctx context.Context, namespace, podname, containerName string) (io.ReadCloser, error) {
	return kc.base.Log(ctx, namespace, podname, containerName)
}
