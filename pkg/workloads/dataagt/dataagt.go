package dataagt

import (
	"context"
	"fmt"
	"sync"
	"time"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	kdb "github.com/opst/knitfab/pkg/db"
	xe "github.com/opst/knitfab/pkg/errors"
	"github.com/opst/knitfab/pkg/utils/retry"
	k8s "github.com/opst/knitfab/pkg/workloads/k8s"
	kubecore "k8s.io/api/core/v1"
)

const dataagtPortName = "dataagt-port"

type dataagt struct {
	namespace string
	knitId    string
	mode      kdb.DataAgentMode
	pod       k8s.Pod
	pvc       k8s.PVC

	mux sync.Mutex
}

type Dataagt interface {
	Name() string

	// API Listening port
	APIPort() int32

	// URL pointing to data agent api
	URL() string

	// Read or Write ?
	Mode() kdb.DataAgentMode

	// KnitID
	KnitID() string

	// PVC Name bound with this Datadgt
	VolumeRef() string

	// convert to string describing this object
	String() string

	Close() error
}

func (agt *dataagt) Name() string {
	return agt.pod.Name()
}

func (agt *dataagt) Host() string {
	return agt.pod.Host()
}

func (agt *dataagt) Port(name string) int32 {
	return agt.pod.Ports()[name]
}

func (agt *dataagt) APIPort() int32 {
	return agt.Port(dataagtPortName)
}

// URL
func (agt *dataagt) URL() string {
	return fmt.Sprintf("http://%s:%d/", agt.Host(), agt.APIPort())
}

func (agt *dataagt) Mode() kdb.DataAgentMode {
	return agt.mode
}

func (agt *dataagt) KnitID() string {
	return agt.knitId
}

func (agt *dataagt) VolumeRef() string {
	return agt.pvc.Name()
}

func (agt *dataagt) Close() error {
	if agt == nil {
		// silently ignore. nothing to do.
		return nil
	}

	agt.mux.Lock() // lock for commit state
	defer agt.mux.Unlock()

	if agt.pod != nil {
		err := agt.pod.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (agt *dataagt) String() string {
	return fmt.Sprintf("Dataagt(%s knitId: %s)", agt.mode, agt.knitId)
}

// Spawn new Dataagt.
//
// # Args
//
// - ctx: context
//
// - kconf: configuration of the cluster that knit is placed in.
//
// - kcluster: Cluster
//
// - kdb.DataAgent: data agent to be spawned
//
// - pendingDeadline: deadline until DataAgent get be pending
//
// # Returns
//
// - Dataagt: data agent
//
// - error
//
// error may be:
//
// - workloads.ErrConflict: Data Agent is already created.
//
// - workloads.ErrMissing: Data Agent is missing after created until started.
//
// - workloads.ErrDeadlineExceeded: Data Agent is not started until deadline.
//
// - other errors come from context.Context
func Spawn(
	ctx context.Context,
	kconf *bconf.KnitClusterConfig,
	kcluster k8s.Cluster,
	da kdb.DataAgent,
	pendingDeadline time.Time,
) (Dataagt, error) {
	builder, err := Of(da)
	if err != nil {
		return nil, err
	}
	spec := builder.Build(kconf)

	// apply
	var promPVC retry.Promise[k8s.PVC]
	switch spec.Mode {
	case kdb.DataAgentWrite:
		promPVC = kcluster.NewPVC(
			ctx,
			retry.StaticBackoff(200*time.Millisecond),
			spec.PVC,
		)
	case kdb.DataAgentRead:
		promPVC = kcluster.GetPVC(
			ctx,
			retry.StaticBackoff(200*time.Millisecond),
			spec.PVC.Name,
		)
	default:
		return nil, fmt.Errorf("unknwon data agent mode: %s", spec.Mode)
	}

	promPod := kcluster.NewPod(
		ctx, retry.StaticBackoff(200*time.Millisecond), spec.Pod,
		k8s.WithCheckpoint(k8s.PodHasBeenPending, pendingDeadline),
		func(value k8s.WithEvents[*kubecore.Pod]) error {
			scheduled := false
			for _, c := range value.Value.Status.Conditions {
				if c.Status != kubecore.ConditionTrue {
					continue
				}
				if c.Type == kubecore.PodScheduled {
					scheduled = true
					break
				}
			}
			if !scheduled {
				return retry.ErrRetry
			}

			sigev := value.SignificantEvent()
			if sigev == nil {
				return nil
			}
			if sigev.Type == "Warning" {
				return fmt.Errorf("data agent cannot start: %s", sigev.Reason)
			}
			return nil
		},
		k8s.PodHasBeenRunning,
	)

	drop := true
	var pod k8s.Pod
	var pvc k8s.PVC

	defer func() {
		if !drop {
			return
		}
		if pod != nil {
			pod.Close()
		}
		if pvc != nil && spec.Mode == kdb.DataAgentWrite {
			pvc.Close()
		}
	}()

	// wait for them to be created
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case p := <-promPVC: // sanity check: if deployment is ready, PVC & PV should be ready.
		if p.Err != nil {
			return nil, xe.Wrap(p.Err)
		}
		pvc = p.Value
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case p := <-promPod:
		if p.Err != nil {
			return nil, xe.Wrap(p.Err)
		}
		pod = p.Value
	}

	drop = false
	dagt := dataagt{
		namespace: kcluster.Namespace(),
		knitId:    spec.KnitId,
		mode:      spec.Mode,
		pod:       pod,
		pvc:       pvc,
	}

	return &dagt, nil
}
