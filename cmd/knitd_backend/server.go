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
	knit "github.com/opst/knitfab/pkg"
	"github.com/opst/knitfab/pkg/workloads/keychain"
	"github.com/opst/knitfab/pkg/workloads/keychain/key"
)

var API_ROOT = "/api/backend"

func api(subpath string) string {
	if !strings.HasSuffix(subpath, "/") {
		subpath += "/"
	}
	return fmt.Sprintf("%s/%s", API_ROOT, subpath)
}

func BuildServer(knit knit.KnitCluster, loglevel string) *echo.Echo {

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
		knit.Database().Data(),
		knit.Database().Runs(),
		knit.SpawnDataAgent,
	))

	e.GET(api("data/:knitId"), handlers.GetDataHandler(
		knit.Database().Data(),
		knit.SpawnDataAgent,
		"knitId",
	))

	signKeyProviderForImportToken := keyprovider.New(
		knit.Config().Keychains().SignKeyForImportToken().Name(),
		knit.Database().Keychain(),
		func(ctx context.Context, s string) (keychain.Keychain, error) {
			return keychain.Get(ctx, knit.BaseCluster(), s)
		},
		keyprovider.WithPolicy(key.HS256(3*time.Hour, 2048/8)),
	)
	e.POST(api("data/import/begin"), handlers.ImportDataBeginHandler(
		signKeyProviderForImportToken,
		knit.Database().Runs(),
	))

	e.POST(api("data/import/end"), handlers.ImportDataEndHandler(
		knit.BaseCluster(),
		signKeyProviderForImportToken,
		knit.Database().Runs(),
		knit.Database().Data(),
	))

	e.GET(api("runs/:runid/log"), handlers.GetRunLogHandler(
		knit.Database().Runs(),
		knit.Database().Data(),
		knit.SpawnDataAgent,
		knit.GetWorker,
		"runid",
	))

	return e
}
