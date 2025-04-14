package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"time"

	"github.com/opst/knitfab-api-types/v2/runs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func handler(l *log.Logger, planId string, outputPath string, inst Instrument) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		detail := &runs.Detail{}
		err := json.NewDecoder(r.Body).Decode(detail)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Invalid request body"))
			return
		}

		if detail.Status != "done" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
			l.Println("# This run is not done yet:", detail.Status)
			return
		}

		tempdir, err := os.MkdirTemp("", "*")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Failed to create temp directory"))
			l.Println("# Failed to create temp directory:", err)
			return
		}
		defer os.RemoveAll(tempdir)

		if detail.Plan.PlanId != planId {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK -- this plan is not the one we are looking for"))
			l.Printf("# This plan is not the one we are looking for: %s\n", detail.Plan.PlanId)
			return
		}

		l.Printf("Received Run: %s\n", detail.RunId)
		inst.RunReceived.Inc()

		var targetKnitId string

		l.Println("  Checking Outputs:")
		for _, output := range detail.Outputs {
			l.Printf("    - %s\n", output.Mountpoint.Path)
			if output.Mountpoint.Path != outputPath {
				continue
			}
			targetKnitId = output.KnitId
			l.Println("    [!] Found target output: knit#id = ", targetKnitId)
			break
		}

		if targetKnitId == "" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK -- no target output found"))
			l.Println("# No target output found")
			return
		}

		// download outputs from the detail with `knit` command
		{
			before := time.Now()
			cmd := exec.Command("knit", "data", "pull", "-x", targetKnitId, tempdir)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Failed to download output"))
				l.Println("# Failed to download output:", err)
				return
			}
			after := time.Now()
			dur := after.Sub(before)
			l.Printf("  - Download output took %s\n", dur)
			inst.KnitPullDuration.Set(float64(dur.Milliseconds()))
		}

		// purge all outputs
		{
			before := time.Now()
			for _, output := range detail.Outputs {
				cmd := exec.Command("knit", "data", "purge", output.KnitId)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				if err := cmd.Run(); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte("Failed to purge output"))
					return
				}
			}
			after := time.Now()
			dur := after.Sub(before)
			l.Printf("  - Purge output took %s\n", dur)
			inst.KnitPurgeDuration.Set(float64(dur.Milliseconds()))
		}

		// push the output to the target path
		{
			before := time.Now()
			args := []string{
				"data", "push", "-t", "type:input", "-t", "project:knitfab-dev-cluster-endurance-test"}
			args = append(args, path.Join(tempdir, targetKnitId))
			cmd := exec.Command("knit", args...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Failed to push output"))
				return
			}
			after := time.Now()
			dur := after.Sub(before)
			l.Printf("  - Push output took %s\n", dur)
			inst.KnitPushDuration.Set(float64(dur.Milliseconds()))
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
}

type Instrument struct {
	RunReceived       prometheus.Counter
	KnitPullDuration  prometheus.Gauge
	KnitPurgeDuration prometheus.Gauge
	KnitPushDuration  prometheus.Gauge
}

// This server is used to Knitfab after Hook.
func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	planId := flag.String("planId", "", "plan id")
	outputPath := flag.String("outputPath", "", "output path")

	flag.Parse()

	if *planId == "" {
		panic("planId is required")
	}
	if *outputPath == "" {
		panic("outputPath is required")
	}

	l := log.New(os.Stdout, "hook-server: ", log.LstdFlags)

	mux := http.NewServeMux()
	mux.HandleFunc(
		"/",
		handler(
			l, *planId, *outputPath,
			Instrument{
				RunReceived: promauto.NewCounter(prometheus.CounterOpts{
					Namespace: "knitfab_endurance_test",
					Name:      "run_received",
					Help:      "Count of runs to be monitored",
				}),
				KnitPullDuration: promauto.NewGauge(prometheus.GaugeOpts{
					Namespace: "knitfab_endurance_test",
					Name:      "knit_pull_duration",
					Help:      "Duration of knit pull",
				}),
				KnitPurgeDuration: promauto.NewGauge(prometheus.GaugeOpts{
					Namespace: "knitfab_endurance_test",
					Name:      "knit_purge_duration",
					Help:      "Duration of knit purge",
				}),
				KnitPushDuration: promauto.NewGauge(prometheus.GaugeOpts{
					Namespace: "knitfab_endurance_test",
					Name:      "knit_push_duration",
					Help:      "Duration of knit push",
				}),
			},
		),
	)
	mux.Handle("/metrics", promhttp.Handler())

	// Create a new HTTP server
	server := &http.Server{
		Addr:    *addr,
		Handler: mux,
	}

	// Start the server
	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}
