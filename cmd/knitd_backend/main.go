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
	schemaRepo := flag.String("schema-repo", os.Getenv("KNIT_SCHEMA"), "schema repository path")
	loglevel := flag.String("loglevel", "warn", "log level. debug|info|warn|error|off")

	flag.Parse()

	if *plic {
		fmt.Println(CREDITS)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	conf, err := configs.LoadBackendConfig(*pconfig)
	if err != nil {
		panic(err)
	}

	clientset := kubeutil.ConnectToK8s()

	db, err := kpg.New(ctx, conf.Cluster().Database(), kpg.WithSchemaRepository(*schemaRepo))
	if err != nil {
		panic(err)
	}
	{
		ctx_, ccan := db.Schema().Context(ctx)
		defer ccan()
		ctx = ctx_
	}

	knitCluster := knit.AttachKnitCluster(clientset, conf.Cluster(), db)

	server := BuildServer(knitCluster, *loglevel)
	for _, r := range server.Routes() {
		server.Logger.Debugf("- mount handler: %s %s", strings.ToUpper(r.Method), r.Path)
	}

	ch := make(chan error, 1)
	defer close(ch)
	go func() {
		defer close(ch)
		if err := server.Start(fmt.Sprintf(":%d", conf.Port())); err != nil && err != http.ErrServerClosed {
			ch <- err
		}
		ch <- nil
	}()

	exit := 0
	select {
	case <-ctx.Done(): // wait
		if err := ctx.Err(); err != nil {
			server.Logger.Infof("context has been done: %s, cause: %s", err, context.Cause(ctx))
			exit = 1
		}
	case err := <-ch:
		if err != nil {
			server.Logger.Error("server stops with error:", err)
			exit = 1
		}
	}

	{
		server.Logger.Info("shutting down...")
		qctx, qcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer qcancel()

		if err := server.Shutdown(qctx); err != nil {
			server.Logger.Fatalf("Shutdown with error. %+v", err)
			os.Exit(1)
		}
		os.Exit(exit)
	}
}
