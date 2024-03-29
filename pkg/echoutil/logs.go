package echoutil

import (
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

func LogHandlerFunc(next echo.HandlerFunc) echo.HandlerFunc {
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
}

func SetLevel(e *echo.Echo, loglevel string) {
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
}
