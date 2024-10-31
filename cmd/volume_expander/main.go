//go:generate go run github.com/Songmu/gocredits/cmd/gocredits@v0.3.0 -w
package main

// Periodicaly, it check PV's avaliable capacity in kubelet metrics
// If available capacity < requirement, run resize & switch to Wait mode

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/opst/knitfab/cmd/volume_expander/flagtype"
	"github.com/opst/knitfab/cmd/volume_expander/metrics"
	"github.com/opst/knitfab/cmd/volume_expander/pvcs"
	connk8s "github.com/opst/knitfab/pkg/conn/k8s"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/try"
	dto "github.com/prometheus/client_model/go"
	"github.com/youta-t/flarc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

//go:embed CREDITS
var CREDITS string

const NAMESPACE = "NAMESPACE"

type Flags struct {
	NodeName string             `flag:"node-name" help:"required. node name to be monitored for resizing"`
	Interval time.Duration      `flag:"interval"`
	Margin   *flagtype.Quantity `flag:"margin" help:"storage margin capacity in SI unit"`
	Delta    *flagtype.Quantity `flag:"delta" help:"increasing delta of resizing PV in SI unit"`
	License  bool               `flag:"license" help:"show licenses of dependencies"`
}

func main() {
	ctx, cancel := signal.NotifyContext(
		context.Background(), os.Interrupt, os.Kill,
	)
	defer cancel()
	logger := log.Default()

	cmd := try.To(
		flarc.NewCommand(
			"Periodically check PV's available capacity and resize if needed",
			Flags{
				NodeName: os.Getenv("NODE_NAME"),
				Interval: 1 * time.Second,
				Margin:   flagtype.MustParse(envFallback("MARGIN", "500Mi")),
				Delta:    flagtype.MustParse(envFallback("DELTA", "500Mi")),
			},
			flarc.Args{
				{
					Name: NAMESPACE, Required: false, Repeatable: true,
					Help: "target namespaces of PVC to be monitored for resizing",
				},
			},
			func(ctx context.Context, c flarc.Commandline[Flags], _ []any) error {
				flags := c.Flags()
				if flags.License {
					fmt.Fprintln(c.Stdout(), CREDITS)
					return nil
				}

				return Vex(ctx, logger, c.Flags(), c.Args()[NAMESPACE])
			},
		),
	).OrFatal(logger)

	os.Exit(flarc.Run(ctx, cmd))
}

func Vex(ctx context.Context, logger *log.Logger, flags Flags, namespaces []string) error {
	targetNamespaces := map[string]struct{}{}
	for _, ns := range namespaces {
		targetNamespaces[ns] = struct{}{}
	}

	if flags.NodeName == "" {
		return fmt.Errorf(
			"%w: flag `--node-name` (or, envvar NODE_NAME) is required", flarc.ErrUsage,
		)
	}

	cs := connk8s.ConnectToK8s()

	pvcsOnNode, err := pvcs.ObserveOnNode(ctx, cs, flags.NodeName, utils.KeysOf(targetNamespaces)...)
	if err != nil {
		return fmt.Errorf("failed to start observing PVCs: %w", err)
	}
	defer pvcsOnNode.Close()

	m := metrics.FromNode(cs, flags.NodeName, flags.Interval)
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
				return err
			}
			if anManage := pvc.GetAnnotations()["vex.knitfab/managed"]; anManage == "false" {
				// this PVC is not managed by vex
				return nil
			}

			pvcCapacity := maxQuantity(
				pvc.Spec.Resources.Requests["storage"],
				pvc.Status.Capacity["storage"],
			).DeepCopy()

			if HasEnoughCapacity(
				*usedQuantity, *flags.Margin.AsResourceQuantity(), pvcCapacity,
			) {
				return nil // skip it
			}

			newQuantity := pointer.Ref(
				maxQuantity(pvcCapacity, *usedQuantity).DeepCopy(),
			)
			newQuantity.Add(*flags.Delta.AsResourceQuantity())
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
			return err
		}
	}

	return nil
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
