//go:generate go run github.com/Songmu/gocredits/cmd/gocredits@v0.3.0 -w
package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	configs "github.com/opst/knitfab/pkg/configs/backend"
	"github.com/opst/knitfab/pkg/kubeutil"

	knit "github.com/opst/knitfab/pkg"
	kpg "github.com/opst/knitfab/pkg/db/postgres"
)

//go:embed CREDITS
var CREDITS string

func main() {

	pconfig := flag.String(
		"config", os.Getenv("KNIT_BACKEND_CONFIG"), "path to config file",
	)
	plic := flag.Bool("license", false, "show licenses of dependencies")
	loglevel := flag.String("loglevel", "warn", "log level. debug|info|warn|error|off")

	flag.Parse()

	if *plic {
		fmt.Println(CREDITS)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go startTraps(ctx, cancel)

	conf, err := configs.LoadBackendConfig(*pconfig)
	if err != nil {
		panic(err)
	}

	clientset := kubeutil.ConnectToK8s()

	db, err := kpg.New(ctx, conf.Cluster().Database())
	if err != nil {
		panic(err)
	}

	knitCluster := knit.AttachKnitCluster(clientset, conf.Cluster(), db)

	server := BuildServer(knitCluster, *loglevel)
	for _, r := range server.Routes() {
		server.Logger.Debugf("- mount handler: %s %s", strings.ToUpper(r.Method), r.Path)
	}

	ch := make(chan error, 1)
	go func() {
		defer close(ch)
		if err := server.Start(fmt.Sprintf(":%d", conf.Port())); err != nil && err != http.ErrServerClosed {
			ch <- err
		}
		ch <- nil
	}()

	select {
	case <-ctx.Done(): // wait
	case err := <-ch:
		server.Logger.Error("server stops with error:", err)
	}
	server.Logger.Info("shutting down...")
	qctx, qcancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer qcancel()
	if err := server.Shutdown(qctx); err != nil {
		server.Logger.Fatalf("Shutdown with error. %+v", err)
	}

	os.Exit(0)
}

func startTraps(ctx context.Context, cancel func()) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT)

	select {
	case s := <-sig:
		fmt.Printf("Signal received: %s \n", s.String())
		cancel()
	case <-ctx.Done():
		// cancel with others.
	}
}
