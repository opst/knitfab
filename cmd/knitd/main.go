//go:generate go run github.com/Songmu/gocredits/cmd/gocredits@v0.3.0 -w
package main

import (
	"context"
	_ "embed"
	"flag"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	kcx "github.com/opst/knitfab/pkg/configs/extras"
	kcf "github.com/opst/knitfab/pkg/configs/frontend"
	kpg "github.com/opst/knitfab/pkg/domain/knitfab/db/postgres"
	"github.com/opst/knitfab/pkg/utils/echoutil"
	"github.com/opst/knitfab/pkg/utils/filewatch"
	kstrings "github.com/opst/knitfab/pkg/utils/strings"

	"github.com/opst/knitfab/cmd/knitd/handlers"
)

//go:embed CREDITS
var CREDITS string

func main() {

	configPath := flag.String("config-path", "", "frontend config path")
	extraConfigPath := flag.String("extra-apis-config", "", "path to extra api config file")
	schemaRepo := flag.String("schema-repo", os.Getenv("KNIT_SCHEMA"), "schema repository path")
	loglevel := flag.String("loglevel", "info", "log level. debug|info|warn|error|off")
	pcert := flag.String("cert", "", "certification file for TLS")
	pkey := flag.String("certkey", "", "key of certification file for TLS")
	ppub := flag.String("public", os.Getenv("KNIT_PUBLIC"), "expose public directory. default is $KNIT_PUBLIC")
	plic := flag.Bool("license", false, "show licenses of dependencies")
	flag.Parse()

	if *plic {
		log.Println(CREDITS)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	e := echo.New()
	e.Pre(middleware.AddTrailingSlash())

	// set log
	echoutil.SetLevel(e, *loglevel)
	e.HTTPErrorHandler = func(err error, ctx echo.Context) {
		e.DefaultHTTPErrorHandler(err, ctx)
		e.Logger.Error(err)
	}
	e.Use(echoutil.LogHandlerFunc)

	// read configfile
	conf, err := kcf.LoadFrontendConfig(*configPath)
	if err != nil {
		log.Fatalf("can not read configration: %s", err)
	}

	extraApis := kcx.Config{}
	if extraConfigPath != nil {
		x, err := kcx.Load(*extraConfigPath)
		if err != nil {
			log.Fatalf("can not read configration: %s", err)
		}
		extraApis = x

		ctx, cancel, err := filewatch.UntilModifyContext(context.Background(), *extraConfigPath)
		if err != nil {
			log.Fatalf("can not watch configration: %s", err)
		}
		defer cancel()
		context.AfterFunc(ctx, func() {
			log.Panicln("extra API config file is updated. quit to restart server.")
			graceful, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := e.Shutdown(graceful); err != nil {
				log.Printf("error on shutdown by extra API config update: %s", err)
			}
		})
	}

	// set public directory
	if *ppub != "" {
		pub := *ppub
		e.Static("/", pub)

		index := path.Join(pub, "index.html")
		if _, err := os.Stat(index); err == nil {
			e.File("/", index)
		}
	}

	api, err := root("/api")
	if err != nil {
		log.Fatalf("api root /api is invalid url or path: %s", err)
	}
	backendApi, err := root(kstrings.SuppySuffix(conf.BackendApiRoot, "/") + "api/backend")
	if err != nil {
		log.Fatalf(
			"backend api root %s is invalid url oe path: %s",
			conf.BackendApiRoot+"/api/backend", err,
		)
	}

	// get dbaccesor
	db, err := kpg.New(ctx, conf.DBURI, kpg.WithSchemaRepository(*schemaRepo))
	if err != nil {
		log.Fatalf("can not read configration: %s", err.Error())
	}
	defer db.Close()

	{
		ctx_, ccan := db.Schema().Context(ctx)
		defer ccan()
		ctx = ctx_
	}

	// handlers
	{
		knitid := "knitid"
		proxy := func(c echo.Context) error {
			url := backendApi("data", c.Param(knitid))

			return echoutil.Proxy(&c, url)
		}
		e.GET(
			api("data"),
			handlers.GetDataForDataHandler(db.Data()),
		)
		e.POST(api("data"), proxy)

		e.GET(api("data/:knitid/"), proxy)
		e.PUT(api("data/:knitid/"), handlers.PutTagForDataHandler(db.Data(), knitid))
	}

	{
		e.GET(
			api("plans"),
			handlers.FindPlanHandler(db.Plan()),
		)
		e.POST(api("plans"), handlers.PlanRegisterHandler(db.Plan()))

		e.GET(api("plans/:planId/"), handlers.GetPlanHandler(db.Plan()))

		e.PUT(api("plans/:planId/active"), handlers.PutPlanForActivate(db.Plan(), true))
		e.DELETE(api("plans/:planId/active"), handlers.PutPlanForActivate(db.Plan(), false))
		e.PUT(api("plans/:planId/resources"), handlers.PutPlanResource(db.Plan(), "planId"))
		e.PUT(api("plans/:planId/annotations"), handlers.PutPlanAnnotations(db.Plan(), "planId"))
		e.PUT(api("plans/:planId/serviceaccount"), handlers.PutPlanServiceAccount(db.Plan(), "planId"))
		e.DELETE(api("plans/:planId/serviceaccount"), handlers.DeletePlanServiceAccount(db.Plan(), "planId"))
	}

	{
		runId := "runid"
		e.GET(api("runs"), handlers.FindRunHandler(db.Run()))
		e.GET(api("runs/:runId/"), handlers.GetRunHandler(db.Run()))
		e.PUT(api("runs/:runId/abort"), handlers.AbortRunHandler(db.Run(), "runId"))
		e.PUT(api("runs/:runId/tearoff"), handlers.TearoffRunHandler(db.Run(), "runId"))
		e.PUT(api("runs/:runId/retry"), handlers.RetryRunHandler(db.Run(), "runId"))

		e.DELETE(api("runs/:runId/"), handlers.DeleteRunHandler(db.Run()))

		e.GET(api("runs/:runid/log"), func(c echo.Context) error {
			url := backendApi("runs", c.Param(runId), "log")
			if rq := c.Request().URL.RawQuery; rq != "" {
				url += "?" + rq
			}

			return echoutil.Proxy(&c, url)
		})
	}
	log.Println("registred routes:")
	for _, r := range e.Routes() {
		log.Println(r.Method, r.Path)
	}

	{
		// register extra APIs
		for _, ex := range extraApis.Endpoints {
			log.Printf("register extra api: %s => %s", ex.Path, ex.ProxyTo)
			if ex.Path == "/api" || strings.HasPrefix(ex.Path, "/api/") {
				log.Fatalf("/api/... is reserved by Knitfab: %s", ex.Path)
			}
			if err := handlers.ExtraAPI(e, ex, echoutil.Proxy); err != nil {
				log.Fatalf("can not set extra api: %s", err)
			}
		}
	}

	quitch := make(chan error, 1)
	defer close(quitch)

	cert, key := *pcert, *pkey

	// watch certs. When cert or key is updated, stop server. (and k8s will restart it)
	if cert != "" {
		_ctx, _cancel, err := filewatch.UntilModifyContext(ctx, cert)
		if err != nil {
			log.Fatalf("can not watch cert file: %s", err)
		}
		ctx = _ctx
		defer _cancel()
	}

	if key != "" {
		_ctx, _cancel, err := filewatch.UntilModifyContext(ctx, key)
		if err != nil {
			log.Fatalf("can not watch key file: %s", err)
		}
		ctx = _ctx
		defer _cancel()
	}

	go func() {
		var err error
		if cert != "" && key != "" {
			err = e.StartTLS(":"+conf.ServerPort, cert, key)
		} else {
			err = e.Start(":" + conf.ServerPort)
		}
		quitch <- err
	}()

	exit := 0
	select {
	case <-ctx.Done():
		if err := ctx.Err(); err != nil {
			log.Printf("context has been done: %s, cause: %s", err, context.Cause(ctx))
			exit = 1
		}
	case err := <-quitch:
		if err != nil {
			log.Printf("server stops with error: %s", err)
			exit = 1
		}
	}

	{
		log.Println("shutting down...")
		graceful, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := e.Shutdown(graceful); err != nil {
			log.Fatalf("Shutdown with error. %+v", err)
			os.Exit(1)
		}
		os.Exit(exit)
	}
}

// create api URL factory
//
// args:
//   - root: api root
//
// return:
// - func: it receive relative path from root, and returns full-path of URL.
func root(r string) (func(...string) string, error) {
	//    when r is https://example.org:8080/api/root/path
	origin := "" // https://example.org:8080/ . "/" terminated. if r is path only, this is empty.
	base := ""   // /api/root/path
	{
		b, err := url.Parse(r)
		if err != nil {
			return nil, err
		}
		base = b.Path
		if b.Host != "" || b.Scheme != "" {
			_r := *b
			r := &_r
			r.RawPath = ""
			r.Path = ""
			r.RawQuery = ""
			r.Fragment = ""
			origin = r.String()
		}
	}
	origin = kstrings.SuppySuffix(origin, "/")

	return func(s ...string) string {
		parts := make([]string, len(s)+1)
		parts[0] = base
		copy(parts[1:], s)
		path := path.Join(parts...)
		path = kstrings.TrimPrefixAll(path, "/")

		return kstrings.SuppySuffix(origin+path, "/")
	}, nil
}
