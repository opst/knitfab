package server_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/opst/knitfab/cmd/dataagt/server"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestServer(t *testing.T) {
	t.Run("it accepts request before deadline is exceeded", func(t *testing.T) {
		payloadLine := "response!!"
		expectedPayload := strings.Repeat(payloadLine, 1024)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		svr := server.Start(
			ctx,
			server.OnLocalPort(0), server.Endpoint{
				Method: http.MethodGet, Path: "/",
				Handler: func(c echo.Context) error {
					resp := c.Response()
					resp.WriteHeader(http.StatusOK)

					// sending large chunked response...
					for i := 0; i < 512; i++ {
						fmt.Fprint(resp, payloadLine)
					}
					time.Sleep(300 * time.Millisecond) // exceeding deadline in handler is safe
					for i := 0; i < 512; i++ {
						fmt.Fprint(resp, payloadLine)
					}
					return nil
				},
			},
			server.WithDeadline(300*time.Millisecond),
			server.WithGracefulPeriod(0),
			server.Silent(),
		)

		resp := try.To(
			http.Get(fmt.Sprintf("http://localhost:%d", svr.Port)),
		).OrFatal(t)
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
		}

		payload := []byte{}
		// read slowly...
		for {
			buf := make([]byte, 512)
			n, err := resp.Body.Read(buf)
			payload = append(payload, buf[:n]...)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatalf("unexpected error: %+v", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if string(payload) != expectedPayload {
			t.Errorf(
				"payload (length: want %d, got %d):\nexpected payload: %s\ngot: %s",
				len(expectedPayload), len(payload),
				payload, expectedPayload,
			)
		}

		var err error
		var serverHasStopped bool
		select {
		case err = <-svr.ServerStop:
			serverHasStopped = true
		default:
		}
		if !serverHasStopped {
			t.Errorf("server has not stopped")
		}

		if !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("server stops by unexpected error: %+v", err)
		}
	})

	t.Run("it accepts request only once", func(t *testing.T) {
		expectedPayload := "response!"

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		svr := server.Start(
			ctx,
			server.OnLocalPort(0), server.Endpoint{
				Method: http.MethodGet, Path: "/",
				Handler: func(c echo.Context) error {
					resp := c.Response()
					resp.WriteHeader(http.StatusOK)

					// exceeding deadline in handler is safe
					time.Sleep(300 * time.Millisecond)

					fmt.Fprint(resp, expectedPayload)
					return nil
				},
			},
			server.WithDeadline(300*time.Millisecond),
			server.WithGracefulPeriod(100*time.Millisecond),
			server.Silent(),
		)

		resp1 := try.To(
			http.Get(fmt.Sprintf("http://localhost:%d", svr.Port)),
		).OrFatal(t)
		defer resp1.Body.Close()
		if resp1.StatusCode != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, resp1.StatusCode)
		}
		{
			resp2, err := http.Get(fmt.Sprintf("http://localhost:%d", svr.Port))
			if err == nil {
				resp2.Body.Close()
			}
			if resp2 != nil {
				if resp2.StatusCode != http.StatusNotFound {
					t.Errorf(
						"responses twice:\n- status: got %d, want 0 or %d\n",
						resp2.StatusCode, http.StatusNotFound,
					)
				}
			}
		}
	})

	t.Run("it stops when no requests come before deadline is exceeded", func(t *testing.T) {
		expectedPayload := "response!"

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		svr := server.Start(
			ctx,
			server.OnLocalPort(0), server.Endpoint{
				Method: http.MethodGet, Path: "/",
				Handler: func(c echo.Context) error {
					resp := c.Response()
					resp.WriteHeader(http.StatusOK)
					fmt.Fprint(resp, expectedPayload)
					return nil
				},
			},
			server.WithDeadline(100*time.Millisecond),
			server.WithGracefulPeriod(0),
			server.Silent(),
		)
		select {
		case err := <-svr.ServerStop:
			t.Errorf("server stops too early: %+v", err)
		default:
		}

		time.Sleep(200 * time.Millisecond) // wait for deadline x2

		select {
		case err := <-svr.ServerStop:
			if !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("server stops by unexpected error: %+v", err)
			}
		default:
			t.Errorf("server has not stopped")
		}
	})

	t.Run("it stops when given context is cancelled", func(t *testing.T) {
		expectedPayload := "response!"
		deadline := time.Hour // longer than test timeout

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		svr := server.Start(
			ctx,
			server.OnLocalPort(0), server.Endpoint{
				Method: http.MethodGet, Path: "/",
				Handler: func(c echo.Context) error {
					resp := c.Response()
					resp.WriteHeader(http.StatusOK)
					fmt.Fprint(resp, expectedPayload)
					return nil
				},
			},
			server.WithDeadline(deadline),
			server.WithGracefulPeriod(0),
			server.Silent(),
		)

		before := time.Now()
		cancel()
		err := <-svr.ServerStop
		after := time.Now()

		if !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("server stops by unexpected error: %+v", err)
		}
		if !(after.Sub(before) < deadline) {
			t.Errorf("server stops after deadline is exceeded")
		}
	})
}
