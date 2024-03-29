//go:generate go run github.com/Songmu/gocredits/cmd/gocredits@v0.3.0 -w
package main

// Periodicaly, it check PV's avaliable capacity in kubelet metrics
// If available capacity < requirement, run resize & switch to Wait mode

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/opst/knitfab/cmd/volume_expander/flagtype"
	"github.com/opst/knitfab/cmd/volume_expander/metrics"
	"github.com/opst/knitfab/cmd/volume_expander/pvcs"
	"github.com/opst/knitfab/pkg/commandline/flag/flagger"
	"github.com/opst/knitfab/pkg/kubeutil"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/try"
	dto "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

//go:embed CREDITS
var CREDITS string

type Flags struct {
	NodeName string             `flag:",help=required. node name to be monitored for resizing"`
	Interval time.Duration      `flag:""`
	Margin   *flagtype.Quantity `flag:",help=storage margin capacity in SI unit"`
	Delta    *flagtype.Quantity `flag:",help=increasing delta of resizing PV in SI unit"`
	License  bool               `flag:",help=show licenses of dependencies"`
}

func main() {
	ctx, cancel := signal.NotifyContext(
		context.Background(), os.Interrupt, os.Kill,
	)
	defer cancel()
	logger := log.Default()

	flags := flagger.New(Flags{
		NodeName: os.Getenv("NODE_NAME"),
		Interval: 1 * time.Second,
		Margin:   flagtype.MustParse(envFallback("MARGIN", "500Mi")),
		Delta:    flagtype.MustParse(envFallback("DELTA", "500Mi")),
	})

	try.To(flags.SetFlags(flag.CommandLine)).
		OrFatal(logger).
		Parse(os.Args[1:])

	targetNamespaces := map[string]struct{}{}
	for _, ns := range flag.Args() {
		targetNamespaces[ns] = struct{}{}
	}

	values := flags.Values

	if values.License {
		log.Println(CREDITS)
		return
	}

	if values.NodeName == "" {
		log.Println("flag `--node-name` (or, envvar NODE_NAME) is required")
		flag.PrintDefaults()
		os.Exit(2)
	}

	cs := kubeutil.ConnectToK8s()

	pvcsOnNode, err := pvcs.ObserveOnNode(ctx, cs, values.NodeName, utils.KeysOf(targetNamespaces)...)
	if err != nil {
		log.Fatal("failed to start observing PVCs:", err)
	}
	defer pvcsOnNode.Close()

	m := metrics.FromNode(cs, values.NodeName, values.Interval)
	m.Subscribe(
		metrics.ForKey("kubelet_volume_stats_used_bytes"),
		func(m *dto.Metric) error {
			var namespace, pvcName string
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case "namespace":
					namespace = l.GetValue()
					if _, ok := targetNamespaces[namespace]; !ok {
						return nil
					}
				case "persistentvolumeclaim":
					pvcName = l.GetValue()
				default:
					continue
				}

				if pvcName != "" && namespace != "" {
					break
				}
			}
			if pvcName == "" || namespace == "" {
				return nil // skip it
			}

			usedQuantity := resource.NewQuantity(
				int64(m.GetGauge().GetValue()),
				resource.BinarySI,
			)

			if pvcsOnNode.Closed() {
				return errors.New("pvc observer is stopped")
			}
			if s, ok := pvcsOnNode.StatusOf(pvcName); !ok || s.ReadOnly() {
				return nil
			}

			pvc, err := cs.CoreV1().
				PersistentVolumeClaims(namespace).
				Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				return nil
			}
			pvcCapacity := maxQuantity(
				pvc.Spec.Resources.Requests["storage"],
				pvc.Status.Capacity["storage"],
			).DeepCopy()

			if HasEnoughCapacity(
				*usedQuantity, *values.Margin.AsResourceQuantity(), pvcCapacity,
			) {
				return nil // skip it
			}

			newQuantity := pointer.Ref(
				maxQuantity(pvcCapacity, *usedQuantity).DeepCopy(),
			)
			newQuantity.Add(*values.Delta.AsResourceQuantity())
			newQuantity.RoundUp(resource.Giga)

			logger.Printf(
				"request resize PVC: namespace/%s/persistentvolumeclaims/%s (used: %dG) capacity: %dG -> %dG",
				namespace, pvcName,
				usedQuantity.ScaledValue(resource.Giga),
				pvcCapacity.ScaledValue(resource.Giga),
				newQuantity.ScaledValue(resource.Giga),
			)
			newPvc := GrowPVC(ctx, cs, pvc, *newQuantity)

			_, err = cs.CoreV1().
				PersistentVolumeClaims(newPvc.Namespace).
				Update(ctx, newPvc, metav1.UpdateOptions{})

			return err
		},
	)

	if err := m.Start(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Fatal("unexpected error:", err)
		}
	}
}

func envFallback(envname string, defaultVal string) string {
	if value := os.Getenv(envname); value != "" {
		return value
	}

	return defaultVal
}

// select max value from a and b
func maxQuantity(a resource.Quantity, b ...resource.Quantity) resource.Quantity {
	max := a
	for _, q := range b {
		if q.Cmp(max) > 0 {
			max = q
		}
	}
	return max
}

// GrowPVC grows PVC's storage capacity request by delta.
func GrowPVC(
	ctx context.Context,
	cs *kubernetes.Clientset,
	pvc *v1.PersistentVolumeClaim,
	newQuantity resource.Quantity,
) *v1.PersistentVolumeClaim {
	newPvc := pvc.DeepCopy()
	newPvc.Spec.Resources.Requests["storage"] = newQuantity
	return newPvc
}

// HasEnoughCapacity returns true if (used + margin) < limit
func HasEnoughCapacity(used, margin, limit resource.Quantity) bool {
	u := used.DeepCopy()
	uu := &u
	uu.Add(margin)
	return (*uu).Cmp(limit) < 0
}
