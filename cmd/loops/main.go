//go:generate go run github.com/Songmu/gocredits/cmd/gocredits@v0.3.0 -w
package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/opst/knitfab/cmd/loops/loop/recurring"
	configs "github.com/opst/knitfab/pkg/configs/backend"
	cfg_hook "github.com/opst/knitfab/pkg/configs/hook"
	"github.com/opst/knitfab/pkg/domain"
	knitfab "github.com/opst/knitfab/pkg/domain/knitfab"
	"github.com/opst/knitfab/pkg/utils/args"
	"github.com/opst/knitfab/pkg/utils/filewatch"
	"github.com/opst/knitfab/pkg/utils/try"
)

//go:embed CREDITS
var CREDITS string

func main() {
	logger := log.Default()
	ctx, cancel := signal.NotifyContext(
		context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM,
	)
	// call cancel() when this function exits
	defer cancel()

	// define command line flags
	//-- path to config file
	pconfig := flag.String(
		"config", os.Getenv("KNIT_BACKEND_CONFIG"), "path to config file",
	)
	pSchemaRepo := flag.String(
		"schema-repo", os.Getenv("KNIT_SCHEMA"), "schema repository path",
	)
	phooks := flag.String(
		"hooks", os.Getenv("KNIT_HOOK_CONFIG"), "path to hook config file",
	)
	//-- which loop type to run
	loopType := args.Parser(domain.AsLoopType)
	flag.Var(loopType, "type", "one of loop type")
	//-- loop policy
	policy := args.Parser(recurring.ParsePolicy)
	flag.Var(
		policy, "policy",
		`loop policy (syntax: forever[:COOLDOWN]|backlog).`+
			` "forever[:COOLDOWN]" = run forever until error. When backlog is over, `+
			`wait COOLDOWN (optional duration. default: 0) as inteval.`+
			` "backlog" = run until error or backlog is over.`,
	)
	plic := flag.Bool("license", false, "show licenses of dependencies")
	// parse command line flags
	flag.Parse()

	if *plic {
		logger.Println(CREDITS)
		return
	}

	{
		// watch config & hooks
		wctx, cancel, err := filewatch.UntilModifyContext(ctx, *pconfig, *phooks)
		if err != nil {
			logger.Fatal(err)
		}
		defer cancel()
		ctx = wctx
	}

	conf := try.To(configs.LoadBackendConfig(*pconfig)).OrFatal(logger)

	options := []knitfab.Option{}
	if *pSchemaRepo != "" {
		options = append(options, knitfab.WithSchemaRepository(*pSchemaRepo))
	}

	kcluster := try.To(knitfab.Default(ctx, conf.Cluster(), options...)).OrFatal(logger)

	{
		ctx_, ccan := kcluster.Schema().Database().Context(ctx)
		defer ccan()
		ctx = ctx_
	}

	hooks := cfg_hook.Config{}
	if hookPath := *phooks; hookPath != "" {
		hooks = try.To(cfg_hook.Load(hookPath)).OrFatal(logger)
	}

	logger.Printf(
		`start loop "%s" /w policy "%s"`,
		loopType.Value().String(), policy.Value().String(),
	)

	manifest := LoopManifest{Policy: policy.Value(), Hooks: hooks}
	var err error
	switch loopType.Value() {
	case domain.Projection:
		err = StartProjectionLoop(ctx, logger, kcluster, manifest)
	case domain.Initialize:
		err = StartInitializeLoop(ctx, logger, kcluster, manifest)
	case domain.RunManagement:
		err = StartRunManagementLoop(ctx, logger, kcluster, manifest)
	case domain.Finishing:
		err = StartFinishingLoop(ctx, logger, kcluster, manifest)
	case domain.GarbageCollection:
		err = StartGarbageCollectionLoop(ctx, logger, kcluster, manifest)
	case domain.Housekeeping:
		err = StartHousekeepingLoop(ctx, logger, kcluster, manifest)
	default:
		err = fmt.Errorf("unsupported loop type: %s", loopType.Value())
	}

	if err == nil {
		return
	} else if errors.Is(err, context.Canceled) {
		logger.Fatal(err, "(loop context is cancelled by:", context.Cause(ctx), ")")
	}

	if ctx.Err() != nil {
		logger.Fatal(err)
	}
}
