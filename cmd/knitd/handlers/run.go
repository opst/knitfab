package handlers

import (
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	apiruns "github.com/opst/knitfab-api-types/runs"
	binderr "github.com/opst/knitfab/pkg/api-types-binding/errors"
	bindrun "github.com/opst/knitfab/pkg/api-types-binding/runs"
	kdb "github.com/opst/knitfab/pkg/db"
	kstrings "github.com/opst/knitfab/pkg/utils/strings"
)

func FindRunHandler(dbRun kdb.RunInterface) echo.HandlerFunc {

	return func(c echo.Context) error {
		c.Response().Header().Add("Content-Type", "application/json")
		query, err := func(c echo.Context) (kdb.RunFindQuery, error) {

			result := kdb.RunFindQuery{
				PlanId:       kstrings.SplitIfNotEmpty(c.QueryParam("plan"), ","),
				InputKnitId:  kstrings.SplitIfNotEmpty(c.QueryParam("knitIdInput"), ","),
				OutputKnitId: kstrings.SplitIfNotEmpty(c.QueryParam("knitIdOutput"), ","),
				Status:       []kdb.KnitRunStatus{},
				UpdatedSince: nil,
				UpdatedUntil: nil,
			}

			for _, p := range kstrings.SplitIfNotEmpty(c.QueryParam("status"), ",") {
				s, err := kdb.AsKnitRunStatus(p)
				if err != nil || s == kdb.Invalidated {
					return kdb.RunFindQuery{}, binderr.BadRequest(
						`"status" should be one of "waiting", "deactivated", "starting", "running", "done" or "failed"`,
						nil,
					)
				}
				result.Status = append(result.Status, s)
			}

			since := c.QueryParam("since")
			if since != "" {
				t, err := rfctime.ParseRFC3339DateTime(since)
				if err != nil {
					return kdb.RunFindQuery{}, binderr.BadRequest(
						`"since" should be a RFC3339 date-time format`,
						err,
					)
				}
				_t := t.Time()
				result.UpdatedSince = &_t
			}

			duration := c.QueryParam("duration")
			if duration != "" {
				d, err := time.ParseDuration(duration)
				if err != nil {
					return kdb.RunFindQuery{}, binderr.BadRequest(
						`"duration" should be a Go duration format`,
						err,
					)
				}
				_t := result.UpdatedSince.Add(d)
				result.UpdatedUntil = &_t
			}

			return result, nil
		}(c)

		if err != nil {
			return err
		}
		ctx := c.Request().Context()

		runIds, err := dbRun.Find(ctx, query)
		if err != nil {
			return binderr.InternalServerError(err)
		}

		result, err := dbRun.Get(ctx, runIds)
		if err != nil {
			return binderr.InternalServerError(err)
		}

		resp := make([]apiruns.Detail, 0, len(result))
		for _, r := range runIds {
			resp = append(resp, bindrun.ComposeDetail(result[r]))
		}

		c.JSON(http.StatusOK, resp)

		return nil
	}

}

func GetRunHandler(dbrun kdb.RunInterface) echo.HandlerFunc {

	return func(c echo.Context) error {
		c.Response().Header().Add("Content-Type", "application/json")
		runId := c.Param("runId")
		ctx := c.Request().Context()

		runs, err := dbrun.Get(ctx, []string{runId})
		if err != nil {
			return binderr.InternalServerError(err)
		}

		run, ok := runs[runId]
		if !ok {
			return binderr.NotFound()
		}

		c.JSON(http.StatusOK, bindrun.ComposeDetail(run))

		return nil
	}
}

func AbortRunHandler(dbrun kdb.RunInterface, paramnRunId string) echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Response().Header().Add("Content-Type", "application/json")
		runId := c.Param(paramnRunId)
		ctx := c.Request().Context()

		if err := dbrun.SetStatus(ctx, runId, kdb.Aborting); err != nil {
			if errors.Is(err, kdb.ErrMissing) {
				return binderr.NotFound()
			} else if errors.Is(err, kdb.ErrInvalidRunStateChanging) {
				return binderr.Conflict("prohibited operation", binderr.WithError(err))
			}
			return binderr.InternalServerError(err)
		}

		if err := dbrun.SetExit(ctx, runId, kdb.RunExit{
			Code:    253,
			Message: "aborted by user",
		}); err != nil {
			return binderr.InternalServerError(err)
		}

		runs, err := dbrun.Get(ctx, []string{runId})
		if err != nil {
			return binderr.InternalServerError(err)
		}

		if r, ok := runs[runId]; !ok {
			return binderr.NotFound()
		} else {
			c.JSON(http.StatusOK, bindrun.ComposeDetail(r))
		}

		return nil
	}
}

func TearoffRunHandler(dbrun kdb.RunInterface, paramnRunId string) echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Response().Header().Add("Content-Type", "application/json")
		runId := c.Param(paramnRunId)
		ctx := c.Request().Context()

		if err := dbrun.SetStatus(ctx, runId, kdb.Completing); err != nil {
			if errors.Is(err, kdb.ErrMissing) {
				return binderr.NotFound()
			} else if errors.Is(err, kdb.ErrInvalidRunStateChanging) {
				return binderr.Conflict("prohibited operation", binderr.WithError(err))
			}
			return binderr.InternalServerError(err)
		}
		if err := dbrun.SetExit(ctx, runId, kdb.RunExit{
			Code:    0,
			Message: "stopped by user",
		}); err != nil {
			return binderr.InternalServerError(err)
		}

		runs, err := dbrun.Get(ctx, []string{runId})
		if err != nil {
			return binderr.InternalServerError(err)
		}

		if r, ok := runs[runId]; !ok {
			return binderr.NotFound()
		} else {
			c.JSON(http.StatusOK, bindrun.ComposeDetail(r))
		}

		return nil
	}
}

func DeleteRunHandler(dbrun kdb.RunInterface) echo.HandlerFunc {
	return func(c echo.Context) error {

		c.Response().Header().Add("Content-Type", "application/json")
		ctx := c.Request().Context()

		runId := c.Param("runId")

		if err := dbrun.Delete(ctx, runId); err != nil {
			if errors.Is(err, kdb.ErrMissing) {
				return binderr.NotFound()
			} else if errors.Is(err, kdb.ErrRunIsProtected) {
				return binderr.Conflict("output of the run is in use", binderr.WithError(err))
			}
			return binderr.InternalServerError(err)
		}

		c.Response().WriteHeader(http.StatusNoContent)

		return nil
	}
}

func RetryRunHandler(dbrun kdb.RunInterface, paramRunId string) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()
		runId := c.Param(paramRunId)

		err := dbrun.Retry(ctx, runId)
		if errors.Is(err, kdb.ErrMissing) {
			return binderr.NotFound()
		}
		if errors.Is(err, kdb.ErrInvalidRunStateChanging) {
			return binderr.Conflict(
				"the run have not finished yet",
				binderr.WithError(err),
				binderr.WithAdvice("Wait for the run to finish, or abort it"),
			)
		}
		if errors.Is(err, kdb.ErrRunIsProtected) {
			message := "prohibited operation"
			options := []binderr.ErrorMessageOption{binderr.WithError(err)}
			if errors.Is(err, kdb.ErrRunHasDownstreams) {
				message = "output of the run is in use"
				options = append(
					options,
					binderr.WithAdvice("Delete all downstreams of the run first"),
				)
			} else if errors.Is(err, kdb.ErrWorkerActive) {
				message = "the run may not be finished"
				options = append(
					options,
					binderr.WithAdvice("Wait for the run to finish, or abort it"),
				)
			} else {
				options = append(
					options,
					binderr.WithAdvice("Root run cannot be retried"),
				)
			}

			return binderr.Conflict(message, options...)
		}
		if err != nil {
			return binderr.InternalServerError(err)
		}

		return nil
	}
}
