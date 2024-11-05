package dataagt

import (
	"context"
	"fmt"
	"sync"
	"time"

	bconf "github.com/opst/knitfab/pkg/configs/backend"
	"github.com/opst/knitfab/pkg/domain"
	data "github.com/opst/knitfab/pkg/domain/data/k8s/data"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
	xe "github.com/opst/knitfab/pkg/errors"
	"github.com/opst/knitfab/pkg/utils/retry"
	kubecore "k8s.io/api/core/v1"
)

const dataagtPortName = "dataagt-port"

type dataagt struct {
	namespace string
	knitId    string
	mode      domain.DataAgentMode
	pod       cluster.Pod
	pvc       cluster.PVC

	mux sync.Mutex
}

type DataAgent interface {
	Name() string

	// API Listening port
	APIPort() int32

	// URL pointing to DataAgent api
	URL() string

	// Read or Write ?
	Mode() domain.DataAgentMode

	// KnitID
	KnitID() string

	// PVC Name bound with this DataAgent
	VolumeRef() string

	PodPhase() cluster.PodPhase

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

func (agt *dataagt) Mode() domain.DataAgentMode {
	return agt.mode
}

func (agt *dataagt) KnitID() string {
	return agt.knitId
}

func (agt *dataagt) PodPhase() cluster.PodPhase {
	return agt.pod.Status()
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
	kcluster cluster.Cluster,
	da domain.DataAgent,
	pendingDeadline time.Time,
) (DataAgent, error) {
	podbuilder, err := Of(da)
	if err != nil {
		return nil, err
	}
	podspec := podbuilder.Build(kconf)

	pvcbuilder, err := data.Of(da.KnitDataBody)
	if err != nil {
		return nil, err
	}
	pvcspec := pvcbuilder.Build(kconf)

	// apply
	var promPVC retry.Promise[cluster.PVC]
	switch da.Mode {
	case domain.DataAgentWrite:
		promPVC = kcluster.NewPVC(
			ctx,
			retry.StaticBackoff(200*time.Millisecond),
			pvcspec,
		)
	case domain.DataAgentRead:
		promPVC = kcluster.GetPVC(
			ctx,
			retry.StaticBackoff(200*time.Millisecond),
			pvcspec.ObjectMeta.Name,
		)
	default:
		return nil, fmt.Errorf("unknwon data agent mode: %s", da.Mode)
	}

	promPod := kcluster.NewPod(
		ctx, retry.StaticBackoff(200*time.Millisecond), podspec,
		cluster.WithCheckpoint(cluster.PodHasBeenPending, pendingDeadline),
		func(value cluster.WithEvents[*kubecore.Pod]) error {
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
		cluster.PodHasBeenRunning,
	)

	drop := true
	var pod cluster.Pod
	var pvc cluster.PVC

	defer func() {
		if !drop {
			return
		}
		if pod != nil {
			pod.Close()
		}
		if pvc != nil && da.Mode == domain.DataAgentWrite {
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
		knitId:    da.KnitDataBody.KnitId,
		mode:      da.Mode,
		pod:       pod,
		pvc:       pvc,
	}

	return &dagt, nil
}

func Find(
	ctx context.Context,
	kcluster cluster.Cluster,
	da domain.DataAgent,
) (DataAgent, error) {
	pvcspec, err := data.Of(da.KnitDataBody)
	if err != nil {
		return nil, err
	}

	podret := kcluster.GetPod(
		ctx, retry.StaticBackoff(50*time.Millisecond), da.Name,
		func(cluster.WithEvents[*kubecore.Pod]) error { return nil }, // everything is fine
	)

	pvcret := kcluster.GetPVC(
		ctx, retry.StaticBackoff(50*time.Millisecond), pvcspec.Instance(),
	)

	ppod := <-podret
	if err := ppod.Err; err != nil {
		return nil, err
	}
	pod := ppod.Value

	ppvc := <-pvcret
	if err := ppvc.Err; err != nil {
		return nil, err
	}
	pvc := ppvc.Value

	return &dataagt{
		namespace: kcluster.Namespace(),
		knitId:    da.KnitDataBody.KnitId,
		mode:      da.Mode,
		pod:       pod,
		pvc:       pvc,
	}, nil
}
