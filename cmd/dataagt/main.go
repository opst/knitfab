//go:generate go run github.com/Songmu/gocredits/cmd/gocredits@v0.3.0 -w
package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/opst/knitfab/cmd/dataagt/server"
	"github.com/opst/knitfab/pkg/utils/try"
)

//go:embed CREDITS
var CREDITS string

func main() {
	pmode := flag.String("mode", "", "read | write")
	plic := flag.Bool("license", false, "show licenses of dependencies")
	ppath := flag.String("path", "./contents", "path to directory to be served/written")
	pdeadline := flag.Int("deadline", 180, "deadline duration (in seconds) before receiving first request")
	pport := flag.Int("port", 8080, "port number where dataagt serves on")
	flag.Parse()

	if *plic {
		fmt.Println(CREDITS)
		return
	}

	logger := log.Default()

	mode := try.To(server.ModeFromString(*pmode)).OrFatal(logger)
	root := *ppath
	port := *pport
	deadline := time.Second * time.Duration(*pdeadline)

	endpoint := try.To(mode.Expose("/", root)).OrFatal(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	s := server.Start(
		ctx,
		server.OnPort(port), endpoint,
		server.WithDeadline(deadline),
	)
	logger.Printf(
		"starting dataagt server on port %d, serving %s in %s mode. deadline = %s.",
		port, root, mode, deadline,
	)

	select {
	case <-ctx.Done():
		logger.Println("server stops by interrupt signal")
	case err := <-s.ServerStop:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("server stops by error:\n%+v", err)
		} else {
			logger.Println("server stops...")
		}
		return
	}
	logger.Println("bye")
}
