package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/opst/knitfab/pkg/utils/retry"
)

type Mode string

const (
	Read  Mode = "read"
	Write Mode = "write"
)

func ModeFromString(s string) (Mode, error) {
	switch Mode(s) {
	case Read:
		return Read, nil
	case Write:
		return Write, nil
	default:
		return "", fmt.Errorf("unknown mode %s", s)
	}
}

func (m Mode) String() string {
	return string(m)
}

func (m Mode) Expose(urlpath string, filepath string) (Endpoint, error) {

	{
		if dir, err := os.Lstat(filepath); err != nil {
			return Endpoint{}, fmt.Errorf(`given path "%s" is something wrong: %+v`, filepath, err)
		} else if !dir.IsDir() {
			return Endpoint{}, fmt.Errorf(`given path "%s" is not directory`, filepath)
		}
	}

	switch m {
	case Read:
		return Endpoint{Method: http.MethodGet, Path: urlpath, Handler: Reader(filepath)}, nil
	case Write:
		return Endpoint{Method: http.MethodPost, Path: urlpath, Handler: Writer(filepath)}, nil
	default:
		return Endpoint{}, fmt.Errorf("unknown mode %s", m)
	}
}

type Endpoint struct {
	Method  string
	Path    string
	Handler echo.HandlerFunc
}

type server struct {
	silent         bool
	deadline       time.Duration
	gracefulPeriod time.Duration
}

func defaultServerConfig() server {
	return server{
		deadline:       180 * time.Second,
		gracefulPeriod: 30 * time.Second,
	}
}

type Option func(*server) *server

// set deadline duration before receiving first request.
//
// Deadline is 180 seconds by deafult.
func WithDeadline(d time.Duration) Option {
	return func(s *server) *server {
		s.deadline = d
		return s
	}
}

// set graceful period for shutdown.
//
// GracefulPeriod is 30 seconds by deafult.
func WithGracefulPeriod(d time.Duration) Option {
	return func(s *server) *server {
		s.gracefulPeriod = d
		return s
	}
}

func Silent() Option {
	return func(s *server) *server {
		s.silent = true
		return s
	}
}

type Starter func(*echo.Echo) error

// start server on port number to start server.
func OnPort(p int) Starter {
	return func(e *echo.Echo) error {
		if err := e.Start(fmt.Sprintf(":%d", p)); err != nil {
			return err
		}
		return nil
	}
}

// start server on port number to start server.
//
// listen on localhost only.
func OnLocalPort(p int) Starter {
	return func(e *echo.Echo) error {
		if err := e.Start(fmt.Sprintf("localhost:%d", p)); err != nil {
			return err
		}
		return nil
	}
}

type Server struct {
	Port       int
	ServerStop <-chan error
}

// start server. Its port and path are 8080 and "/" by default.
//
// # Params
//
// - ctx context.Context: context to be used for server.
// To stop the server, cancel this context.
//
// - starter Starter: starter to be used for server.
//
// - handler Handler: handler to be registered to server.
//
// - opts ...ServerOption: options to configure server.
func Start(ctx context.Context, starter Starter, handler Endpoint, opts ...Option) Server {
	ctx, cancelContext := context.WithCancel(ctx)
	serverConfig := defaultServerConfig()
	for _, opt := range opts {
		serverConfig = *opt(&serverConfig)
	}

	e := echo.New()
	if serverConfig.silent {
		e.HideBanner = true
		e.HidePort = true
	}
	closeServer := func() func() {
		o := sync.Once{}
		return func() {
			o.Do(func() {
				if 0 < serverConfig.gracefulPeriod {
					_ctx, _cancel := context.WithTimeout(ctx, serverConfig.gracefulPeriod)
					defer _cancel()
					e.Shutdown(_ctx) // try to shutdown gracefully
				}
				e.Close() // close forcefully
			})
		}
	}()
	watchdog := time.AfterFunc(serverConfig.deadline, cancelContext)
	go func() {
		select {
		case <-ctx.Done():
			if !watchdog.Stop() {
				// drain timer
				select {
				case <-watchdog.C:
				default:
				}
			}
		case <-watchdog.C:
			cancelContext()
		}

		closeServer()
	}()

	e.Add(
		handler.Method, handler.Path, handler.Handler,
		func(next echo.HandlerFunc) echo.HandlerFunc {
			mu := sync.Mutex{}
			return func(c echo.Context) error {
				if err := func() error {
					mu.Lock()
					defer mu.Unlock()
					if !watchdog.Stop() {
						return echo.ErrNotFound
					}
					return nil
				}(); err != nil {
					return err
				}

				rctx := c.Request().Context()
				defer func() {
					go func() {
						<-rctx.Done()
						cancelContext()
					}()
				}()
				if err := next(c); err != nil {
					return err
				}
				c.Response().Flush()
				return nil
			}
		},
	)

	ch := make(chan error, 1)
	go func() {
		defer close(ch)
		ch <- starter(e)
	}()

	port, _ := retry.Blocking[int](
		ctx, retry.StaticBackoff(100*time.Millisecond),
		func() (int, error) {
			if e.Listener == nil {
				return 0, retry.ErrRetry
			}
			return e.Listener.Addr().(*net.TCPAddr).Port, nil
		},
	)

	return Server{Port: port, ServerStop: ch}
}
