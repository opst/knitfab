//go:generate go run github.com/Songmu/gocredits/cmd/gocredits@v0.3.0 -w
package main

import (
	"context"
	_ "embed"
	"flag"
	"log"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	kcx "github.com/opst/knitfab/pkg/configs/extras"
	kcf "github.com/opst/knitfab/pkg/configs/frontend"
	kdb "github.com/opst/knitfab/pkg/db"
	kpg "github.com/opst/knitfab/pkg/db/postgres"
	"github.com/opst/knitfab/pkg/echoutil"
	"github.com/opst/knitfab/pkg/utils/filewatch"
	kstrings "github.com/opst/knitfab/pkg/utils/strings"

	"github.com/opst/knitfab/cmd/knitd/handlers"
)

//go:embed CREDITS
var CREDITS string

func main() {

	configPath := flag.String("config-path", "", "frontend config path")
	extraConfigPath := flag.String("extra-apis-config", "", "path to extra api config file")
	loglevel := flag.String("loglevel", "info", "log level. debug|info|warn|error|off")
	pcert := flag.String("cert", "", "certification file for TLS")
	pkey := flag.String("certkey", "", "key of certification file for TLS")
	plic := flag.Bool("license", false, "show licenses of dependencies")
	flag.Parse()

	if *plic {
		log.Println(CREDITS)
		return
	}

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
	ctx := context.Background()
	db, err := getDBAccesor(ctx, conf.DBURI)
	if err != nil {
		log.Fatalf("can not read configration: %s", err.Error())
	}
	defer db.Close()

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
	}

	{
		runId := "runid"
		e.GET(api("runs"), handlers.FindRunHandler(db.Runs()))
		e.GET(api("runs/:runId/"), handlers.GetRunHandler(db.Runs()))
		e.PUT(api("runs/:runId/abort"), handlers.AbortRunHandler(db.Runs(), "runId"))
		e.PUT(api("runs/:runId/tearoff"), handlers.TearoffRunHandler(db.Runs(), "runId"))
		e.PUT(api("runs/:runId/retry"), handlers.RetryRunHandler(db.Runs(), "runId"))

		e.DELETE(api("runs/:runId/"), handlers.DeleteRunHandler(db.Runs()))

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

	cert, key := *pcert, *pkey
	if cert != "" && key != "" {
		e.Logger.Fatal(e.StartTLS(":"+conf.ServerPort, cert, key))
	} else {
		e.Logger.Fatal(e.Start(":" + conf.ServerPort))
	}
}

func getDBAccesor(ctx context.Context, dburi string) (kdb.KnitDatabase, error) {
	return kpg.New(ctx, dburi)
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
