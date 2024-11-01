package handlers

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	apierr "github.com/opst/knitfab/pkg/api-types-binding/errors"
	"github.com/opst/knitfab/pkg/domain"
	kdbdata "github.com/opst/knitfab/pkg/domain/data/db"
	k8sdata "github.com/opst/knitfab/pkg/domain/data/k8s"
	kerr "github.com/opst/knitfab/pkg/domain/errors"
	k8serrors "github.com/opst/knitfab/pkg/domain/errors/k8serrors"
	kdbrun "github.com/opst/knitfab/pkg/domain/run/db"
	k8srun "github.com/opst/knitfab/pkg/domain/run/k8s"
	"github.com/opst/knitfab/pkg/utils/archive"
	"github.com/opst/knitfab/pkg/utils/echoutil"
)

func GetRunLogHandler(
	iRunDB kdbrun.Interface,
	iDataDB kdbdata.DataInterface,
	iDataK8s k8sdata.Interface,
	iRunK8s k8srun.Interface,
	runIdKey string,
) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()

		runId := c.Param(runIdKey)

		var runInfo domain.Run
		if rs, err := iRunDB.Get(ctx, []string{runId}); err != nil {
			return apierr.InternalServerError(err)
		} else if r, ok := rs[runId]; !ok || r.Status.Invalidated() {
			return apierr.NotFound()
		} else {
			runInfo = r
		}

		if l := runInfo.Log; l == nil {
			return apierr.NotFound()
		}

		var data domain.KnitDataBody
		switch runInfo.Status {
		case domain.Deactivated, domain.Waiting, domain.Ready:
			// = before create container. started means "contaienr runs",
			// but there are contaienrs a bit before that.
			return apierr.ServiceUnavailable("please retry later.", nil)
		case domain.Starting, domain.Running:
			if !c.QueryParams().Has("follow") {
				data = runInfo.Log.KnitDataBody
			} else {
				// follow mode!
				worker, err := iRunK8s.FindWorker(ctx, runInfo.RunBody)
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
		case domain.Completing, domain.Aborting, domain.Done, domain.Failed:
			data = runInfo.Log.KnitDataBody
		}

		timeout := 30 * time.Second
		deadline := time.Now().Add(timeout)
		daRecord, err := iDataDB.NewAgent(ctx, data.KnitId, domain.DataAgentRead, timeout)
		if err != nil {
			if errors.Is(err, kerr.ErrMissing) {
				return apierr.NotFound()
			}
			return apierr.InternalServerError(err)
		}

		dagt, err := iDataK8s.SpawnDataAgent(ctx, daRecord, deadline)
		if err != nil {
			if errors.Is(err, k8serrors.ErrDeadlineExceeded) {
				return apierr.ServiceUnavailable("please retry later", err)
			}
			return apierr.InternalServerError(err)
		}
		defer func() {
			if err := dagt.Close(); err != nil {
				return
			}
			iDataDB.RemoveAgent(ctx, daRecord.Name)
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
