package k8s

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	kubeapps "k8s.io/api/apps/v1"
	kubebatch "k8s.io/api/batch/v1"
	kubecore "k8s.io/api/core/v1"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
	kubeapiresouce "k8s.io/apimachinery/pkg/api/resource"
	kubeapimeta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"

	"github.com/opst/knitfab/pkg/utils/retry"
	wl "github.com/opst/knitfab/pkg/workloads"
)

// subset of k8s.Clientset
type K8sClient interface {
	GetService(ctx context.Context, namespace string, svcname string) (*kubecore.Service, error)
	CreateService(ctx context.Context, namespace string, svc *kubecore.Service) (*kubecore.Service, error)
	DeleteService(ctx context.Context, namespace string, svcname string) error

	GetPVC(ctx context.Context, namespace string, pvcname string) (*kubecore.PersistentVolumeClaim, error)
	CreatePVC(ctx context.Context, namespace string, pvc *kubecore.PersistentVolumeClaim) (*kubecore.PersistentVolumeClaim, error)
	DeletePVC(ctx context.Context, namespace string, pvcname string) error

	GetDeployment(ctx context.Context, namespace string, deplname string) (*kubeapps.Deployment, error)
	CreateDeployment(ctx context.Context, namespace string, depl *kubeapps.Deployment) (*kubeapps.Deployment, error)
	DeleteDeployment(ctx context.Context, namespace string, deplname string) error

	GetJob(ctx context.Context, namespace string, name string) (*kubebatch.Job, error)
	CreateJob(ctx context.Context, namespace string, spec *kubebatch.Job) (*kubebatch.Job, error)
	DeleteJob(ctx context.Context, namespace string, name string) error

	CreatePod(ctx context.Context, namespace string, spec *kubecore.Pod) (*kubecore.Pod, error)
	GetPod(ctx context.Context, namespace string, name string) (*kubecore.Pod, error)
	DeletePod(ctx context.Context, namespace string, name string) error
	FindPods(ctx context.Context, namespace string, labelSelector LabelSelector) ([]kubecore.Pod, error)

	Log(ctx context.Context, namespace string, podname string, container string) (io.ReadCloser, error)
}

// A wrapper for the type k8s.Clientset; because it does not prefer method chain-style invocations of that type.
type k8sClient struct {
	client *k8s.Clientset
}

// type check: k8sClient implements K8sClient
var _ K8sClient = &k8sClient{}

func (k *k8sClient) CreateService(ctx context.Context, namespace string, svc *kubecore.Service) (*kubecore.Service, error) {
	return k.client.CoreV1().Services(namespace).Create(ctx, svc, kubeapimeta.CreateOptions{})
}

func (k *k8sClient) GetService(ctx context.Context, namespace string, svcname string) (*kubecore.Service, error) {
	return k.client.CoreV1().Services(namespace).Get(ctx, svcname, kubeapimeta.GetOptions{})
}

func (k *k8sClient) DeleteService(ctx context.Context, namespace string, svcname string) error {
	return k.client.CoreV1().Services(namespace).Delete(ctx, svcname, *kubeapimeta.NewDeleteOptions(0))
}

func (k *k8sClient) CreatePVC(ctx context.Context, namespace string, pvc *kubecore.PersistentVolumeClaim) (*kubecore.PersistentVolumeClaim, error) {
	return k.client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, kubeapimeta.CreateOptions{})
}

func (k *k8sClient) GetPVC(ctx context.Context, namespace string, pvcname string) (*kubecore.PersistentVolumeClaim, error) {
	return k.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcname, kubeapimeta.GetOptions{})
}

func (k *k8sClient) DeletePVC(ctx context.Context, namespace string, pvcname string) error {
	return k.client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvcname, *kubeapimeta.NewDeleteOptions(0))
}

func (k *k8sClient) CreateDeployment(ctx context.Context, namespace string, depl *kubeapps.Deployment) (*kubeapps.Deployment, error) {
	return k.client.AppsV1().Deployments(namespace).Create(ctx, depl, kubeapimeta.CreateOptions{})
}

func (k *k8sClient) GetDeployment(ctx context.Context, namespace string, deplname string) (*kubeapps.Deployment, error) {
	return k.client.AppsV1().Deployments(namespace).Get(ctx, deplname, kubeapimeta.GetOptions{})
}

func (k *k8sClient) DeleteDeployment(ctx context.Context, namespace string, deplname string) error {
	return k.client.AppsV1().Deployments(namespace).Delete(ctx, deplname, *kubeapimeta.NewDeleteOptions(0))
}

func (k *k8sClient) CreateJob(ctx context.Context, namespace string, job *kubebatch.Job) (*kubebatch.Job, error) {
	return k.client.BatchV1().Jobs(namespace).Create(ctx, job, kubeapimeta.CreateOptions{})
}

func (k *k8sClient) GetJob(ctx context.Context, namespace string, name string) (*kubebatch.Job, error) {
	return k.client.BatchV1().Jobs(namespace).Get(ctx, name, kubeapimeta.GetOptions{})
}

func (k *k8sClient) Log(ctx context.Context, namespace string, podname string, container string) (io.ReadCloser, error) {
	return k.client.
		CoreV1().
		Pods(namespace).
		GetLogs(podname, &kubecore.PodLogOptions{Container: container, Follow: true}).
		Stream(ctx)
}

func (k *k8sClient) DeleteJob(ctx context.Context, namespace string, name string) error {
	foreground := kubeapimeta.DeletePropagationForeground
	zero := int64(0)
	return k.client.BatchV1().Jobs(namespace).Delete(ctx, name, kubeapimeta.DeleteOptions{
		GracePeriodSeconds: &zero,
		PropagationPolicy:  &foreground,
	})
}

func (k *k8sClient) CreatePod(ctx context.Context, namespace string, pod *kubecore.Pod) (*kubecore.Pod, error) {
	return k.client.CoreV1().Pods(namespace).Create(ctx, pod, kubeapimeta.CreateOptions{})
}

func (k *k8sClient) GetPod(ctx context.Context, namespace string, name string) (*kubecore.Pod, error) {
	return k.client.CoreV1().Pods(namespace).Get(ctx, name, kubeapimeta.GetOptions{})
}

func (k *k8sClient) DeletePod(ctx context.Context, namespace string, podname string) error {
	return k.client.CoreV1().Pods(namespace).Delete(ctx, podname, *kubeapimeta.NewDeleteOptions(0))
}

func (k *k8sClient) FindPods(ctx context.Context, namespace string, labels LabelSelector) ([]kubecore.Pod, error) {
	resp, err := k.client.CoreV1().Pods(namespace).List(ctx, kubeapimeta.ListOptions{
		LabelSelector: labels.QueryString(),
	})
	if err != nil {
		return nil, err
	}
	return resp.Items, nil
}

func WrapK8sClient(c *k8s.Clientset) K8sClient {
	return &k8sClient{client: c}
}

type service struct {
	resource *kubecore.Service
	domain   string
	close    func() error
}

// Abstraction of k8s Service
type Service interface {
	Namespace() string
	Name() string

	// get service domain name.
	Host() string

	// get service cluster IP
	IP() string

	// get named port number.
	Port(name string) int32

	// release resources.
	//
	// Delete service.
	Close() error
}

func (s *service) Namespace() string {
	return s.resource.GetNamespace()
}

func (s *service) Name() string {
	return s.resource.GetName()
}

func (s *service) IP() string {
	return s.resource.Spec.ClusterIP
}

func (s *service) Host() string {
	return fmt.Sprintf("%s.%s.svc.%s", s.Name(), s.Namespace(), s.domain)
}

// Get port number named as parameter `name`
//
// If not found, return `0`.
func (s *service) Port(name string) int32 {
	for _, p := range s.resource.Spec.Ports {
		if p.Name == name {
			return p.Port
		}
	}
	return 0
}

func (s *service) Close() error {
	return s.close()
}

type deployment struct {
	resource *kubeapps.Deployment
	onClose  func() error
}

// Abstraction of k8s Deployment
type Deployment interface {
	Name() string
	Namespace() string

	// release resources.
	//
	// Delete deployment and related pods
	Close() error
}

func (d *deployment) Namespace() string {
	return d.resource.GetNamespace()
}

func (d *deployment) Name() string {
	return d.resource.GetName()
}

func (d *deployment) Close() error {
	return d.onClose()
}

// Abstraction of Persistent Volume Claim
type PVC interface {
	Name() string
	Namespace() string
	VolumeName() string

	// Capacity in claim.
	ClaimedCapacity() kubeapiresouce.Quantity

	// destroy PVC if it is created as this instance.
	Close() error
}

type pvc struct {
	resource *kubecore.PersistentVolumeClaim
	onClose  func() error
}

func (p *pvc) Name() string {
	return p.resource.GetName()
}

func (p *pvc) Namespace() string {
	return p.resource.GetNamespace()
}

func (p *pvc) VolumeName() string {
	return p.resource.Spec.VolumeName
}

func (p *pvc) ClaimedCapacity() kubeapiresouce.Quantity {
	return p.resource.Spec.Resources.Requests["storage"]
}

func (p *pvc) Close() error {
	if p.onClose == nil {
		return nil
	}
	return p.onClose()
}

type JobStatus string

const (
	// no pods have been started.
	Pending JobStatus = "Pending"

	// at least one pod has started, and the job has not completed.
	Running JobStatus = "Running"

	// the job is succeeded.
	//
	// In case of parallel > 1, some pods can be failed.
	Succeeded JobStatus = "Succeeded"

	// the job is failed.
	//
	// In case of parallel, some pods can be succeeded.
	Failed JobStatus = "Failed"
)

// abstraction of k8s job.
type Job interface {
	// the name of the job
	Name() string

	// the namespace where the job is placed in
	Namespace() string

	// how does the job progress, at least
	//
	// This value is just a SNAPSHOT of the job when you get the instance.
	// To refresh, you should get a new instance of `Job` with `K8sClient.GetJob`.
	//
	// # return
	//
	// - Succeeded, Failed : it is succeeded or failed as a job.
	// In case of parallel jobs, some pods can be failed/succeeded inspite of the Status().
	//
	// - Running : (At least) one pod has been started.
	// It can be no pods are running if some pods have run to be terminated
	// and more pods are pending to be started.
	//
	// - Pending : no pods have been started.
	Status() JobStatus

	//	ExitCode returns the exit code of the main container of job
	//
	// # Return
	//
	// - exitCode : the exit code of the main container.
	//
	// - reason: the reason of the termination.
	//
	// - ok : true if the job has been stopped, false otherwise.
	ExitCode(container string) (uint8, string, bool)

	// Log get log stream of the job
	//
	// # Args
	//
	// - ctx context.Context
	//
	// - containerName string: name of container to get log
	//
	// # Return
	//
	// - io.ReadCloser: the log stream of the container.
	//
	// - error : error if any.
	Log(ctx context.Context, containerName string) (io.ReadCloser, error)

	// destroy the job. If the job is running or pending, it can be aborted.
	Close() error
}

type job struct {
	job    *kubebatch.Job
	pods   []kubecore.Pod
	client K8sClient
	close  func() error
}

var _ Job = &job{}

func (j *job) Name() string {
	return j.job.Name
}

func (j *job) Namespace() string {
	return j.job.Namespace
}

func (j *job) Status() JobStatus {
	for _, sc := range j.job.Status.Conditions {
		if sc.Status != "True" {
			continue
		}
		switch sc.Type {
		case kubebatch.JobComplete:
			return Succeeded
		case kubebatch.JobFailed:
			return Failed
		}
	}

	for _, p := range j.pods {
		// if at least one pod has been run, the job has been run.
		switch p.Status.Phase {
		case kubecore.PodRunning, kubecore.PodSucceeded, kubecore.PodFailed:
			return Running
		}
	}

	return Pending
}

func (j *job) Log(ctx context.Context, containerName string) (io.ReadCloser, error) {
	if len(j.pods) == 0 {
		return nil, errors.New("no pods")
	}
	pod := j.pods[0]
	return j.client.Log(ctx, pod.Namespace, pod.Name, containerName)
}

func (j *job) ExitCode(container string) (uint8, string, bool) {
	for _, p := range j.pods {
		for _, c := range p.Status.ContainerStatuses {
			if c.Name != container {
				continue
			}
			if term := c.State.Terminated; term != nil {
				return uint8(term.ExitCode), term.Reason, true
			}
			break
		}
	}
	return 0, "", false
}

func (j *job) Close() error {
	if j.close == nil {
		return nil
	}
	return j.close()
}

type PodPhase kubecore.PodPhase

var (
	PodPending   PodPhase = PodPhase(kubecore.PodPending)
	PodRunning   PodPhase = PodPhase(kubecore.PodRunning)
	PodSucceeded PodPhase = PodPhase(kubecore.PodSucceeded)
	PodFailed    PodPhase = PodPhase(kubecore.PodFailed)
	PodUnknown   PodPhase = PodPhase(kubecore.PodUnknown)
)

type Pod interface {
	Name() string
	Status() PodPhase
	Host() string
	Ports() map[string]int32
	Close() error
}

type pod struct {
	description kubecore.Pod
	onClose     func() error
}

func (p *pod) Name() string {
	return p.description.Name
}

func (p *pod) Status() PodPhase {
	return PodPhase(p.description.Status.Phase)
}

func (p *pod) Host() string {
	return p.description.Status.PodIP
}

func (p *pod) Ports() map[string]int32 {
	ports := map[string]int32{}
	for _, c := range p.description.Spec.Containers {
		for _, p := range c.Ports {
			ports[p.Name] = p.ContainerPort
		}
	}
	return ports
}

func (p *pod) Close() error {
	if p.onClose == nil {
		return nil
	}
	return p.onClose()
}

type Cluster interface {
	Namespace() string
	Domain() string

	// Create new Service and wait for it to satisfy all requirements.
	//
	// Args
	//
	// - ctx context.Context
	//
	// - backoff poll.Backoff: backoff policy to wait for Service satisfy all requirements.
	//
	// - svcconf *Service: spec of wanted Service
	//
	// - requirements ...Requirement[Service]: requirements for the Service.
	// If not given, ServiceIsReady is used as default.
	//
	// Return
	//
	// - retry.Promise[Service]
	//
	// Promise which is resolved when the Service is created & satisfied requirements.
	//
	// The Promise may have Error below:
	//
	// - workloads.ErrConflict: Service is already created.
	//
	// - workloads.ErrMissing: Service is missing after created until meets requirements.
	//
	// - other errors come from Requirements and context.Context
	//
	// Whether or not the Promise has Error, service can be created.
	// So, you may need to Close() it.
	NewService(context.Context, retry.Backoff, *kubecore.Service, ...Requirement[*kubecore.Service]) retry.Promise[Service]

	// Create new Deployment
	//
	// Args
	//
	// - ctx context.Context
	//
	// - backoff poll.Backoff: backoff policy to wait for Deployment satisfy all requirements.
	//
	// - dplconf *Deployment: spec of wanted Deployment
	//
	// - requirements ...Requirement[Deployment]: requirements for the Deployment.
	// If not given, EnoughReplicas is used as default.
	//
	// Return
	//
	// - retry.Promise[Deployment]
	//
	// Promise which is resolved when the Deployment is created & satisfied requirements.
	//
	// The Promise may have Error below:
	//
	// - workloads.ErrConflict: Deployment is already created.
	//
	// - workloads.ErrMissing: Deployment is missing after created until meets requirements.
	//
	// - other errors come from Requirements and context.Context
	//
	// Whether or not the Promise has Error, deployment can be created.
	// So, you may need to Close() it.
	NewDeployment(context.Context, retry.Backoff, *kubeapps.Deployment, ...Requirement[*kubeapps.Deployment]) retry.Promise[Deployment]

	// Create new PVC
	//
	// Args
	// - ctx context.Context
	//
	// - backoff poll.Backoff: backoff policy to wait for PVC satisfy all requirements.
	//
	// - pvcconf *PersistentVolumeClaim: spec of wanted PVC
	//
	// - requirements ...Requirement[PVC]: requirements for the PVC.
	// If not given, PVCIsBound is used as default.
	//
	// Return
	//
	// - retry.Promise[PVC]
	// Promise which is resolved when the PVC is created & satisfied requirements.
	//
	// The Promise may have Error below:
	//
	// - workloads.ErrConflict: PVC is already created.
	//
	// - workloads.ErrMissing: PVC is missing after created until meets requirements.
	//
	// - other errors come from Requirements and context.Context
	//
	// Whether or not the Promise has Error, PVC can be created.
	// So, you may need to Close() it.
	NewPVC(context.Context, retry.Backoff, *kubecore.PersistentVolumeClaim, ...Requirement[*kubecore.PersistentVolumeClaim]) retry.Promise[PVC]

	// Get Existing PVC.
	//
	// Args
	//
	// - context.Context
	//
	// - backoff poll.Backoff: backoff policy to wait for PVC satisfy all requirements.
	//
	// - string: name of PVC
	//
	// - requirements ...Requirement[PVC]: requirements for the PVC.
	// If not given, PVCIsBound is used as default.
	//
	// Return
	//
	// - retry.Promise[PVC]:
	//
	// Promise which is resolved when the PVC is created & satisfied requirements.
	//
	// The Promise may have Error below:
	//
	// - workloads.ErrMissing: PVC is not found.
	//
	// - other errors come from Requirements and context.Context
	//
	// Whether or not the Promise has Error, PVC can be found.
	// So, you may need to Close() it.
	//
	GetPVC(context.Context, retry.Backoff, string, ...Requirement[*kubecore.PersistentVolumeClaim]) retry.Promise[PVC]

	// Create new k8s job
	//
	// Args
	//
	// - context.Context
	//
	// - backoff poll.Backoff: backoff policy to wait for Job satisfy all requirements.
	//
	// - *Job: job specification
	//
	// - requirements ...Requirement[Job]: requirements for the Job.
	// If not given, JobIsReady is used as default.
	//
	// Return
	//
	// - retry.Promise[Job]
	//
	// Promise which is resolved when the Job is created & satisfied requirements.
	//
	// The Promise may have Error below:
	//
	// - workloads.ErrConflict: Job is already created.
	//
	// - workloads.ErrMissing: Job is missing after created until meets requirements.
	//
	// - other errors come from Requirements and context.Context
	//
	// Whether or not the Promise has Error, Job can be created.
	// So, you may need to Close() it.
	NewJob(context.Context, retry.Backoff, *kubebatch.Job, ...Requirement[*kubebatch.Job]) retry.Promise[Job]

	// Get existing k8s job
	//
	// Args
	//
	// - context.Context
	//
	// - backoff poll.Backoff: backoff policy to wait for Job satisfy all requirements.
	//
	// - string: name of job
	//
	// - ...requirements ...Requirement[Job]: requirements for the Job.
	// If not given, JobIsReady is used as default.
	//
	// Return
	//
	// - retry.Promise[Job]
	//
	// Promise which is resolved when the Job is created & satisfied requirements.
	//
	// The Promise may have Error below:
	//
	// - workloads.ErrMissing: Job is not found.
	//
	// - other errors come from Requirements and context.Context
	//
	// Whether or not the Promise has Error, Job can be found.
	// So, you may need to Close() it.
	GetJob(context.Context, retry.Backoff, string, ...Requirement[*kubebatch.Job]) retry.Promise[Job]

	// Create new Pod
	//
	// Args
	//
	// - context.Context
	//
	// - backoff poll.Backoff: backoff policy to wait for Pod satisfy all requirements.
	//
	// - *Pod: pod specification
	//
	// - requirements ...Requirement[Pod]: requirements for the Pod.
	// If not given, PodIsReady is used as default.
	//
	// Return
	//
	// - retry.Promise[Pod]
	//
	// Promise which is resolved when the Pod is created & satisfied requirements.
	//
	// The Promise may have Error below:
	//
	// - workloads.ErrConflict: Pod is already created.
	//
	// - workloads.ErrMissing: Pod is missing after created until meets requirements.
	//
	// - other errors come from Requirements and context.Context
	//
	// Whether or not the Promise has Error, Pod can be created.
	// So, you may need to Close() it.
	NewPod(context.Context, retry.Backoff, *kubecore.Pod, ...Requirement[*kubecore.Pod]) retry.Promise[Pod]

	//	Get existing Pod
	//
	// Args
	//
	// - context.Context
	//
	// - backoff poll.Backoff: backoff policy to wait for Pod satisfy all requirements.
	//
	// - string: name of pod
	//
	// - requirements ...Requirement[Pod]: requirements for the Pod.
	// If not given, PodHasBeenRunning is used as default.
	//
	// Return
	//
	// - retry.Promise[Pod]
	//
	// Promise which is resolved when the Pod is created & satisfied requirements.
	//
	// The Promise may have Error below:
	//
	// - workloads.ErrMissing: Pod is not found.
	//
	// - other errors come from Requirements and context.Context
	//
	// Whether or not the Promise has Error, Pod can be found.
	// So, you may need to Close() it.
	GetPod(context.Context, retry.Backoff, string, ...Requirement[*kubecore.Pod]) retry.Promise[Pod]
}

type k8sCluster struct {
	client    K8sClient
	namespace string
	domain    string
}

// Requirement is a function that checks if creating k8s resource satisfies the requirement.
//
// # Return
//
// - error: When the value satisfies the requirement, return nil.
// If it is waiting to satisfy the requirement, return `retry.ErrRetry`.
// Otherwise, return error.
type Requirement[T any] func(value T) error

func WithCheckpoint[T any](requirement Requirement[T], deadline time.Time) Requirement[T] {
	satisfied := false
	return func(value T) error {
		if satisfied {
			return nil
		}
		if time.Now().After(deadline) {
			return wl.ErrDeadlineExceeded
		}

		err := requirement(value)
		if err != nil {
			return err
		}

		satisfied = true
		return nil
	}
}

func satisfyAll[T any](value T, req []Requirement[T]) error {
	for _, r := range req {
		if err := r(value); err != nil {
			return err
		}
	}
	return nil
}

// type check: k8scluster implements Cluster
var _ Cluster = &k8sCluster{}

// Attch kubernetes cluster.
//
// args:
//   - client: k8s clientset
//   - namespace: k8s namespace
//   - domain: k8s-internal domain name. If empty string is passed, it uses`"cluster.local"` as default.
func AttachCluster(client K8sClient, namespace string, domain string) Cluster {
	if domain == "" {
		domain = "cluster.local"
	}
	return &k8sCluster{client: client, namespace: namespace, domain: domain}
}

func (c *k8sCluster) Namespace() string {
	return c.namespace
}

func (c *k8sCluster) Domain() string {
	return c.domain
}

var ServiceIsReady Requirement[*kubecore.Service] = func(value *kubecore.Service) error {
	if value.Spec.ClusterIP != "" {
		return nil
	}
	return retry.ErrRetry
}

func (c *k8sCluster) NewService(
	ctx context.Context, backoff retry.Backoff, svcconf *kubecore.Service,
	requirements ...Requirement[*kubecore.Service],
) retry.Promise[Service] {
	if len(requirements) == 0 {
		requirements = []Requirement[*kubecore.Service]{ServiceIsReady}
	}

	select {
	case <-ctx.Done():
		return retry.Failed[Service](ctx.Err())
	default:
	}

	svc, err := c.client.CreateService(ctx, c.namespace, svcconf)
	if err != nil {
		if kubeerr.IsAlreadyExists(err) {
			return retry.Failed[Service](wl.NewConflictCausedBy("", err))
		}
		return retry.Failed[Service](err)
	}
	_close := func() error {
		return c.client.DeleteService(
			context.Background(), // close should run if given has closed.
			c.namespace,
			svcconf.ObjectMeta.Name,
		)
	}
	if err := satisfyAll(svc, requirements); err == nil {
		return retry.Ok[Service](&service{resource: svc, domain: c.domain, close: _close})
	} else if !errors.Is(err, retry.ErrRetry) {
		return retry.Failed[Service](err)
	}

	return retry.Go(ctx, backoff, func() (Service, error) {
		svc, err := c.client.GetService(ctx, c.namespace, svcconf.ObjectMeta.Name)
		ret := &service{resource: svc, domain: c.domain, close: _close}
		if err != nil {
			return ret, err
		}
		return ret, satisfyAll(svc, requirements)
	})
}

var EnoughReplicas Requirement[*kubeapps.Deployment] = func(value *kubeapps.Deployment) error {
	replicas := int32(1)
	if value.Spec.Replicas != nil {
		replicas = *value.Spec.Replicas
	}
	if replicas <= value.Status.AvailableReplicas {
		return nil
	}
	return retry.ErrRetry
}

func (c *k8sCluster) NewDeployment(
	ctx context.Context, backoff retry.Backoff, dplconf *kubeapps.Deployment,
	requirements ...Requirement[*kubeapps.Deployment],
) retry.Promise[Deployment] {
	if len(requirements) == 0 {
		requirements = []Requirement[*kubeapps.Deployment]{EnoughReplicas}
	}

	select {
	case <-ctx.Done():
		return retry.Failed[Deployment](ctx.Err())
	default:
	}

	dpl, err := c.client.CreateDeployment(ctx, c.namespace, dplconf)
	if err != nil {
		if kubeerr.IsAlreadyExists(err) {
			return retry.Failed[Deployment](wl.NewConflictCausedBy("", err))
		}
		return retry.Failed[Deployment](err)
	}
	_close := func() error {
		return c.client.DeleteDeployment(
			context.Background(), // close should run if given has closed.
			c.namespace,
			dplconf.ObjectMeta.Name,
		)
	}

	if err := satisfyAll(dpl, requirements); err == nil {
		return retry.Ok[Deployment](&deployment{resource: dpl})
	} else if !errors.Is(err, retry.ErrRetry) {
		return retry.Failed[Deployment](err)
	}

	return retry.Go(ctx, backoff, func() (Deployment, error) {
		dpl, err := c.client.GetDeployment(ctx, c.namespace, dplconf.ObjectMeta.Name)
		ret := &deployment{resource: dpl, onClose: _close}
		if err != nil {
			return ret, err
		}
		return ret, satisfyAll(dpl, requirements)
	})
}

var PVCIsBound Requirement[*kubecore.PersistentVolumeClaim] = func(value *kubecore.PersistentVolumeClaim) error {
	if value.Status.Phase == kubecore.ClaimBound {
		return nil
	}
	return retry.ErrRetry
}

func (c *k8sCluster) NewPVC(
	ctx context.Context, backoff retry.Backoff, pvcconf *kubecore.PersistentVolumeClaim,
	requirements ...Requirement[*kubecore.PersistentVolumeClaim],
) retry.Promise[PVC] {
	if len(requirements) == 0 {
		requirements = []Requirement[*kubecore.PersistentVolumeClaim]{PVCIsBound}
	}

	select {
	case <-ctx.Done():
		return retry.Failed[PVC](ctx.Err())
	default:
	}

	_pvc, err := c.client.CreatePVC(ctx, c.namespace, pvcconf)

	if err != nil {
		if kubeerr.IsAlreadyExists(err) {
			return retry.Failed[PVC](wl.NewConflictCausedBy("", err))
		}
		return retry.Failed[PVC](err)
	}

	_close := func() error {
		return c.client.DeletePVC(
			context.Background(),
			c.namespace,
			pvcconf.ObjectMeta.Name,
		)
	}
	if err := satisfyAll(_pvc, requirements); err == nil {
		ret := &pvc{resource: _pvc, onClose: _close}
		return retry.Ok[PVC](ret)
	} else if !errors.Is(err, retry.ErrRetry) {
		return retry.Failed[PVC](err)
	}

	return c.GetPVC(ctx, backoff, pvcconf.ObjectMeta.Name, requirements...)
}

func (c *k8sCluster) GetPVC(
	ctx context.Context, backoff retry.Backoff, pvcname string,
	requirements ...Requirement[*kubecore.PersistentVolumeClaim],
) retry.Promise[PVC] {
	if len(requirements) == 0 {
		requirements = []Requirement[*kubecore.PersistentVolumeClaim]{PVCIsBound}
	}

	_close := func() error {
		return c.client.DeletePVC(context.Background(), c.namespace, pvcname)
	}
	return retry.Go(ctx, backoff, func() (PVC, error) {
		_pvc, err := c.client.GetPVC(ctx, c.namespace, pvcname)
		ret := &pvc{resource: _pvc, onClose: _close}
		if err != nil {
			return ret, err
		}
		return ret, satisfyAll(_pvc, requirements)
	})
}

var JobHaveBeenCreated Requirement[*kubebatch.Job] = func(value *kubebatch.Job) error {
	return nil
}

func (c *k8sCluster) NewJob(
	ctx context.Context, p retry.Backoff, j *kubebatch.Job,
	requirements ...Requirement[*kubebatch.Job],
) retry.Promise[Job] {
	if len(requirements) == 0 {
		requirements = []Requirement[*kubebatch.Job]{JobHaveBeenCreated}
	}

	select {
	case <-ctx.Done():
		return retry.Failed[Job](ctx.Err())
	default:
	}
	_job, err := c.client.CreateJob(ctx, c.namespace, j)
	if err != nil {
		if kubeerr.IsAlreadyExists(err) {
			return retry.Failed[Job](wl.NewConflictCausedBy("", err))
		}
		return retry.Failed[Job](err)
	}
	_close := func() error {
		return c.client.DeleteJob(
			context.Background(), c.namespace, _job.ObjectMeta.Name,
		)
	}

	if err := satisfyAll(_job, requirements); err == nil {
		pods, err := c.client.FindPods(
			ctx, c.namespace,
			LabelsToSelecor(_job.Spec.Selector.MatchLabels),
		)
		if err != nil {
			pods = []kubecore.Pod{}
		}
		return retry.Ok[Job](&job{job: _job, pods: pods, close: _close})
	} else if !errors.Is(err, retry.ErrRetry) {
		return retry.Failed[Job](err)
	}

	return c.GetJob(ctx, p, _job.ObjectMeta.Name, requirements...)
}

func (c *k8sCluster) GetJob(
	ctx context.Context, p retry.Backoff, name string,
	requirements ...Requirement[*kubebatch.Job],
) retry.Promise[Job] {
	if len(requirements) == 0 {
		requirements = []Requirement[*kubebatch.Job]{JobHaveBeenCreated}
	}
	_close := func() error {
		return c.client.DeleteJob(context.Background(), c.namespace, name)
	}

	return retry.Go(ctx, p, func() (Job, error) {
		_job, err := c.client.GetJob(ctx, c.namespace, name)
		ret := &job{
			job: _job, close: _close, client: c.client,
		}

		if err != nil {
			if kubeerr.IsNotFound(err) {
				return ret, wl.NewMissingCausedBy("", err)
			}
			return ret, err
		}

		if err := satisfyAll(_job, requirements); err != nil {
			return ret, err
		}

		pods, err := c.client.FindPods(
			ctx, c.namespace,
			LabelsToSelecor(_job.Spec.Selector.MatchLabels),
		)
		if err == nil {
			ret.pods = pods
		}
		return ret, nil
	})
}

var PodHasBeenRunning Requirement[*kubecore.Pod] = func(p *kubecore.Pod) error {
	switch p.Status.Phase {
	case kubecore.PodRunning, kubecore.PodFailed, kubecore.PodSucceeded:
		return nil
	default:
		return retry.ErrRetry
	}
}

var PodHasBeenPending Requirement[*kubecore.Pod] = func(p *kubecore.Pod) error {
	switch p.Status.Phase {
	case kubecore.PodPending, kubecore.PodRunning, kubecore.PodFailed, kubecore.PodSucceeded:
		return nil
	default:
		return retry.ErrRetry
	}
}

func (c *k8sCluster) NewPod(
	ctx context.Context, r retry.Backoff, p *kubecore.Pod,
	requirements ...Requirement[*kubecore.Pod],
) retry.Promise[Pod] {
	if len(requirements) == 0 {
		requirements = []Requirement[*kubecore.Pod]{PodHasBeenRunning}
	}
	select {
	case <-ctx.Done():
		return retry.Failed[Pod](ctx.Err())
	default:
	}

	_close := func() error {
		ctx := context.Background()
		return c.client.DeletePod(ctx, c.namespace, p.ObjectMeta.Name)
	}

	_pod, err := c.client.CreatePod(ctx, c.namespace, p)
	if err != nil {
		if kubeerr.IsAlreadyExists(err) {
			return retry.Failed[Pod](wl.NewConflictCausedBy("", err))
		}
		return retry.Failed[Pod](err)
	}
	if err := satisfyAll(_pod, requirements); err == nil {
		return retry.Ok[Pod](&pod{description: *_pod, onClose: _close})
	} else if !errors.Is(err, retry.ErrRetry) {
		return retry.Failed[Pod](err)
	}

	return c.GetPod(ctx, r, _pod.ObjectMeta.Name, requirements...)
}

func (c *k8sCluster) GetPod(
	ctx context.Context, r retry.Backoff, name string,
	requirements ...Requirement[*kubecore.Pod],
) retry.Promise[Pod] {
	if len(requirements) == 0 {
		requirements = []Requirement[*kubecore.Pod]{PodHasBeenRunning}
	}
	_close := func() error {
		ctx := context.Background()
		return c.client.DeletePod(ctx, c.namespace, name)
	}

	return retry.Go(ctx, r, func() (Pod, error) {
		_pod, err := c.client.GetPod(ctx, c.namespace, name)
		ret := &pod{description: *_pod, onClose: _close}
		if err != nil {
			if kubeerr.IsNotFound(err) {
				return ret, wl.NewMissingCausedBy("", err)
			}
			return ret, err
		}
		return ret, satisfyAll(_pod, requirements)
	})
}
