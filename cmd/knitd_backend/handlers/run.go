package handlers

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	apierr "github.com/opst/knitfab/pkg/api/types/errors"
	"github.com/opst/knitfab/pkg/archive"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/echoutil"
	"github.com/opst/knitfab/pkg/workloads"
	"github.com/opst/knitfab/pkg/workloads/dataagt"
	"github.com/opst/knitfab/pkg/workloads/worker"
)

func GetRunLogHandler(
	run kdb.RunInterface,
	dbData kdb.DataInterface,
	spawnDataAgent func(context.Context, kdb.DataAgent, time.Time) (dataagt.Dataagt, error),
	getWorker func(context.Context, kdb.Run) (worker.Worker, error),
	runIdKey string,
) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()

		runId := c.Param(runIdKey)

		var runInfo kdb.Run
		if rs, err := run.Get(ctx, []string{runId}); err != nil {
			return apierr.InternalServerError(err)
		} else if r, ok := rs[runId]; !ok || r.Status.Invalidated() {
			return apierr.NotFound()
		} else {
			runInfo = r
		}

		if l := runInfo.Log; l == nil {
			return apierr.NotFound()
		}

		var data kdb.KnitDataBody
		switch runInfo.Status {
		case kdb.Deactivated, kdb.Waiting, kdb.Ready:
			// = before create container. started means "contaienr runs",
			// but there are contaienrs a bit before that.
			return apierr.ServiceUnavailable("please retry later.", nil)
		case kdb.Starting, kdb.Running:
			if !c.QueryParams().Has("follow") {
				data = runInfo.Log.KnitDataBody
			} else {
				// follow mode!
				worker, err := getWorker(ctx, runInfo)
				if err != nil {
					return apierr.InternalServerError(err)
				}
				stream, err := worker.Log(ctx)
				if err != nil {
					return apierr.InternalServerError(err)
				}
				defer stream.Close()

				lr := &lineReader{
					r: stream,
					callback: func() {
						fmt.Println("flush")
						c.Response().Flush()
					},
				}
				return c.Stream(http.StatusOK, "text/plain", lr)
			}
		case kdb.Completing, kdb.Aborting, kdb.Done, kdb.Failed:
			data = runInfo.Log.KnitDataBody
		}

		timeout := 30 * time.Second
		deadline := time.Now().Add(timeout)
		daRecord, err := dbData.NewAgent(ctx, data.KnitId, kdb.DataAgentRead, timeout)
		if err != nil {
			if errors.Is(err, kdb.ErrMissing) {
				return apierr.NotFound()
			}
			return apierr.InternalServerError(err)
		}

		dagt, err := spawnDataAgent(ctx, daRecord, deadline)
		if err != nil {
			if errors.Is(err, workloads.ErrDeadlineExceeded) {
				return apierr.ServiceUnavailable("please retry later", err)
			}
			return apierr.InternalServerError(err)
		}
		defer func() {
			if err := dagt.Close(); err != nil {
				return
			}
			dbData.RemoveAgent(ctx, daRecord.Name)
		}()

		bresp, err := echoutil.CopyRequest(ctx, dagt.URL(), c.Request())
		if err != nil {
			return apierr.InternalServerError(err)
		}
		defer bresp.Body.Close()

		resp := c.Response()
		hdr := resp.Header()

		if bresp.StatusCode != http.StatusOK {
			return echoutil.CopyResponse(&c, bresp)
		}

		echoutil.CopyHeader(&hdr, &bresp.Header)
		hdr.Set("Content-Type", "plain/text")
		resp.WriteHeader(bresp.StatusCode)
		if err := archive.TarGzWalk(bresp.Body, func(h *tar.Header, f io.Reader, err error) error {
			if err != nil {
				return err
			}
			io.Copy(resp.Writer, f)
			return nil
		}); err != nil {
			return apierr.InternalServerError(err)
		}

		return nil
	}
}

type lineReader struct {
	r        io.Reader
	callback func()
}

func (lr *lineReader) Read(p []byte) (n int, err error) {
	n, err = lr.r.Read(p)
	if n > 0 {
		if bytes.Contains(p[:n], []byte{'\n'}) {
			lr.callback()
		}
	}
	return
}
