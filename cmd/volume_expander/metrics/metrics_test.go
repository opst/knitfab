package metrics_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/opst/knitfab/cmd/volume_expander/metrics"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/opst/knitfab/pkg/workloads/k8s/testenv"
	io_prometheus_client "github.com/prometheus/client_model/go"
	kubecore "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEventsFromNode(t *testing.T) {

	ctx := context.Background()
	cs := testenv.NewClient()

	nodes := try.To(cs.CoreV1().Nodes().List(ctx, kubecore.ListOptions{})).OrFatal(t)
	node := nodes.Items[0].Name

	testee := metrics.FromNode(cs, node, 1*time.Second)

	cctx, cancel := context.WithCancel(ctx)
	testee.Subscribe(metrics.Anything(), func(e *io_prometheus_client.Metric) error {
		cancel()
		return nil
	})
	err := testee.Start(cctx)
	if errors.Is(err, context.Canceled) {
		return
	}
	t.Errorf("expected context.Canceled, got %v", err)
}
