package pvcs

import (
	"context"
	"reflect"

	"github.com/opst/knitfab/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

type Mount struct {
	ReadOnly bool
}

type PVCStatus struct {
	MountedOn map[string]Mount
}

func (p PVCStatus) ReadOnly() bool {
	for _, m := range p.MountedOn {
		if !m.ReadOnly {
			return false
		}
	}
	return true
}

type Observer interface {
	// StatusOf returns current stats of PVCs.
	//
	// # Returns
	//
	// - map[string]PVCStat: a map of PVC name and its stats.
	StatusOf(string) (PVCStatus, bool)

	// stop ovserving
	Close()

	Closed() bool
}

type observer struct {
	stats  map[string]PVCStatus
	cancel func()
	closed bool
}

func (o *observer) StatusOf(pvcName string) (PVCStatus, bool) {
	s, ok := o.stats[pvcName]
	return s, ok
}

func (o *observer) Close() {
	o.cancel()
}

func (o *observer) Closed() bool {
	return o.closed
}

func forContext[T any](ctx context.Context, ch <-chan T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-ch:
				if !ok {
					return
				}
				out <- v
			}
		}
	}()
	return out
}

// merge recv channels into one.
func merge[T any](ch ...<-chan T) <-chan T {
	out := make(chan T)

	cases := utils.Map(
		ch,
		func(c <-chan T) reflect.SelectCase {
			return reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(c),
			}
		},
	)

	go func() {
		defer close(out)
		for 0 < len(cases) {
			i, v, ok := reflect.Select(cases)
			if !ok {
				cases = append(cases[:i], cases[i+1:]...)
				continue
			}
			out <- v.Interface().(T)
		}
	}()

	return out
}

func ObserveOnNode(
	ctx context.Context,
	cs *kubernetes.Clientset,
	nodeName string,
	namespace ...string,
) (Observer, error) {
	ctx, cancel := context.WithCancel(ctx)

	stoppers := []interface{ Stop() }{}

	var events <-chan watch.Event
	{
		recv := []<-chan watch.Event{}
		for i := range namespace {
			ns := namespace[i]
			watcher, err := cs.CoreV1().Pods(ns).Watch(
				ctx,
				metav1.ListOptions{
					FieldSelector: "spec.nodeName=" + nodeName,
				},
			)
			if err != nil {
				for _, s := range stoppers {
					s.Stop()
				}
				cancel()
				return nil, err
			}
			stoppers = append(stoppers, watcher)
			recv = append(recv, forContext(ctx, watcher.ResultChan()))
		}
		events = merge(recv...)
	}

	obs := &observer{
		stats:  map[string]PVCStatus{},
		cancel: cancel,
	}
	go func() {
		defer func() {
			for _, s := range stoppers {
				s.Stop()
			}
			obs.closed = true
			cancel()
		}()

		for ev := range events {
			pod, ok := ev.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			switch ev.Type {
			case watch.Added:
				nameMap := map[string]string{} // volume name -> pvc name
				for _, v := range pod.Spec.Volumes {
					if v.PersistentVolumeClaim == nil {
						continue
					}
					pvcName := v.PersistentVolumeClaim.ClaimName
					if _, ok := obs.stats[pvcName]; !ok {
						obs.stats[pvcName] = PVCStatus{
							MountedOn: map[string]Mount{},
						}
					}
					obs.stats[pvcName].MountedOn[pod.Name] = Mount{
						ReadOnly: v.PersistentVolumeClaim.ReadOnly,
					}
					nameMap[v.Name] = pvcName
				}
				for _, c := range pod.Spec.Containers {
					for _, v := range c.VolumeMounts {
						pvcName, ok := nameMap[v.Name]
						if !ok {
							continue
						}
						if _, ok := obs.stats[pvcName].MountedOn[pod.Name]; !ok {
							obs.stats[pvcName].MountedOn[pod.Name] = Mount{}
						}
						readOnly := obs.stats[pvcName].MountedOn[pod.Name].ReadOnly
						obs.stats[pvcName].MountedOn[pod.Name] = Mount{
							ReadOnly: readOnly || v.ReadOnly,
						}
					}
				}
			case watch.Deleted:
				for _, v := range pod.Spec.Volumes {
					if v.PersistentVolumeClaim == nil {
						continue
					}
					pvcName := v.PersistentVolumeClaim.ClaimName
					delete(obs.stats[pvcName].MountedOn, pod.Name)
					if len(obs.stats[pvcName].MountedOn) == 0 {
						delete(obs.stats, pvcName)
					}
				}
			default:
			}
		}
	}()

	return obs, nil
}
