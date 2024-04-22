package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	apidata "github.com/opst/knitfab/pkg/api/types/data"
	apierr "github.com/opst/knitfab/pkg/api/types/errors"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/echoutil"
	"github.com/opst/knitfab/pkg/workloads"
	"github.com/opst/knitfab/pkg/workloads/dataagt"
)

func PostDataHandler(
	dbData kdb.DataInterface,
	dbRun kdb.RunInterface,
	spawnDataAgent func(context.Context, kdb.DataAgent, time.Time) (dataagt.Dataagt, error),
) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()

		finished := false
		deadline := time.Now().Add(30 * time.Second)

		runId, err := dbRun.NewPseudo(ctx, kdb.Uploaded, time.Until(deadline))
		if err != nil {
			return apierr.InternalServerError(err)
		}
		defer func() {
			if finished {
				return
			}
			ctx := context.Background()
			dbRun.SetStatus(ctx, runId, kdb.Aborting)
			dbRun.Finish(ctx, runId)
		}()

		runs, err := dbRun.Get(ctx, []string{runId})
		if err != nil {
			return apierr.InternalServerError(err)
		}
		run, ok := runs[runId]
		if !ok {
			return apierr.InternalServerError(
				errors.New("failed to get the newly created run detail"),
			)
		}
		out := run.Outputs
		if len(out) != 1 {
			return apierr.InternalServerError(
				fmt.Errorf("plan %s requires %d data, not 1", kdb.Uploaded, len(out)),
			)
		}

		daRecord, err := dbData.NewAgent(
			ctx, out[0].KnitDataBody.KnitId, kdb.DataAgentWrite, time.Until(deadline),
		)
		if err != nil {
			return apierr.InternalServerError(err)
		}

		da, err := spawnDataAgent(ctx, daRecord, deadline)
		if err != nil {
			if workloads.AsConflict(err) || errors.Is(err, workloads.ErrDeadlineExceeded) {
				return apierr.ServiceUnavailable("please retry later", err)
			}
			return apierr.InternalServerError(err)
		}
		defer func() {
			if err := da.Close(); err != nil {
				return
			}
			dbData.RemoveAgent(ctx, daRecord.Name)
		}()

		bresp, err := echoutil.CopyRequest(ctx, da.URL(), c.Request())
		if err != nil {
			return apierr.InternalServerError(err)
		}
		defer bresp.Body.Close()

		newStatus := kdb.Aborting
		if 200 <= bresp.StatusCode && bresp.StatusCode < 300 {
			newStatus = kdb.Completing
		}
		if err := dbRun.SetStatus(ctx, runId, newStatus); err != nil {
			return apierr.InternalServerError(err)
		}
		if err := dbRun.Finish(ctx, runId); err != nil {
			return apierr.InternalServerError(err)
		}
		finished = true

		if newStatus != kdb.Completing {
			// proxy dataagt response.  -- fixme: check & reword error message.
			echoutil.CopyResponse(&c, bresp)
			return nil
		}

		resultSet, err := dbData.Get(ctx, []string{da.KnitID()})
		if err != nil {
			return apierr.InternalServerError(err)
		}
		data, ok := resultSet[da.KnitID()]
		if !ok {
			return apierr.InternalServerError(errors.New(`uploaded data "%s" is lost`))
		}

		return c.JSON(
			http.StatusOK,
			apidata.ComposeDetail(data),
		)
	}
}

func GetDataHandler(
	data kdb.DataInterface,
	spawnDataAgent func(context.Context, kdb.DataAgent, time.Time) (dataagt.Dataagt, error),
	knitIdKey string,
) echo.HandlerFunc {
	return func(c echo.Context) error {

		ctx := c.Request().Context()
		knitId := c.Param(knitIdKey)

		timeout := 30 * time.Second
		deadline := time.Now().Add(timeout)

		// There are no guarantee that clocks are syncronized betweebn knitd-backend and database.
		// So, we should keep that deadline in database comes later than the deadline in knitd-backend.
		// In order to do that, we should set the deadline in knitd-backend first.
		daRecord, err := data.NewAgent(ctx, knitId, kdb.DataAgentRead, timeout)
		if err != nil {
			if errors.Is(err, kdb.ErrMissing) {
				return apierr.NotFound()
			}
			return apierr.InternalServerError(err)
		}

		dagt, err := spawnDataAgent(ctx, daRecord, deadline)
		if errors.Is(err, workloads.ErrDeadlineExceeded) {
			return apierr.ServiceUnavailable("please retry later", err)
		} else if err != nil {
			return apierr.InternalServerError(err)
		}
		defer func() {
			if err := dagt.Close(); err != nil {
				return
			}
			data.RemoveAgent(ctx, daRecord.Name)
		}()

		bresp, err := echoutil.CopyRequest(ctx, dagt.URL(), c.Request())
		if err != nil {
			return apierr.InternalServerError(err)
		}

		return echoutil.CopyResponse(&c, bresp)
	}
}
