package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/labstack/gommon/log"
	handlers "github.com/opst/knitfab/cmd/knitd_backend/handlers"
	keyprovider "github.com/opst/knitfab/cmd/knitd_backend/provider/keyProvider"
	"github.com/opst/knitfab/pkg/domain"
	keychain "github.com/opst/knitfab/pkg/domain/keychain/k8s"
	"github.com/opst/knitfab/pkg/domain/keychain/k8s/key"
	knit "github.com/opst/knitfab/pkg/domain/knitfab"
	"github.com/opst/knitfab/pkg/domain/run/k8s/worker"
)

var API_ROOT = "/api/backend"

func api(subpath string) string {
	if !strings.HasSuffix(subpath, "/") {
		subpath += "/"
	}
	return fmt.Sprintf("%s/%s", API_ROOT, subpath)
}

func BuildServer(knit knit.Knitfab, loglevel string) *echo.Echo {

	e := echo.New()

	switch strings.ToLower(loglevel) {
	case "debug":
		e.Logger.SetLevel(log.DEBUG)
	case "info":
		e.Logger.SetLevel(log.INFO)
	case "warn":
	case "":
		e.Logger.SetLevel(log.WARN)
	case "error":
		e.Logger.SetLevel(log.ERROR)
	case "off":
		e.Logger.SetLevel(log.OFF)
	default:
		e.Logger.SetLevel(log.WARN)
		e.Logger.Warnf("unknown loglevel: %s . fall-backed to warn")
	}

	e.HTTPErrorHandler = func(err error, ctx echo.Context) {
		e.DefaultHTTPErrorHandler(err, ctx)
		e.Logger.Error(err)
	}

	e.Pre(middleware.AddTrailingSlash())

	// logging for server-side latency.
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			meth := c.Request().Method
			path := c.Request().URL
			BEGIN := time.Now()
			c.Logger().Infof(
				"< request @[%s] %s %s", BEGIN, meth, path,
			)

			var err error

			defer func() {
				END := time.Now()
				c.Logger().Infof(
					"> response @[%s] status = %s (for request @[%s] %s %s) in %v / error = %+v",
					END, c.Response().Status, BEGIN, meth, path, END.Sub(BEGIN), err,
				)
			}()

			err = next(c)
			return err
		}
	})

	e.POST(api("data"), handlers.PostDataHandler(
		knit.Data().Database(),
		knit.Run().Database(),
		knit.Data().K8s().SpawnDatAgent,
	))

	e.GET(api("data/:knitId"), handlers.GetDataHandler(
		knit.Data().Database(),
		knit.Data().K8s().SpawnDatAgent,
		"knitId",
	))

	keyProviderForImportToken := keyprovider.New(
		knit.Keychain().Database(),
		func(ctx context.Context) (keychain.Keychain, error) {
			return knit.Keychain().K8s().Get(
				ctx,
				knit.Config().Keychains().SignKeyForImportToken().Name(),
			)
		},
		keyprovider.WithPolicy(key.HS256(3*time.Hour, 2048/8)),
	)
	e.POST(api("data/import/begin"), handlers.ImportDataBeginHandler(
		keyProviderForImportToken,
		knit.Run().Database(),
	))

	e.POST(api("data/import/end"), handlers.ImportDataEndHandler(
		knit.Cluster(),
		keyProviderForImportToken,
		knit.Run().Database(),
		knit.Data().Database(),
	))

	e.GET(api("runs/:runid/log"), handlers.GetRunLogHandler(
		knit.Run().Database(),
		knit.Data().Database(),
		knit.Data().K8s().SpawnDatAgent,
		func(ctx context.Context, r domain.Run) (worker.Worker, error) {
			return knit.Run().K8s().FindWorker(ctx, r.RunBody)
		},
		"runid",
	))

	return e
}
