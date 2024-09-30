package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	keyprovider "github.com/opst/knitfab/cmd/knitd_backend/provider/keyProvider"
	binddata "github.com/opst/knitfab/pkg/api-types-binding/data"
	binderr "github.com/opst/knitfab/pkg/api-types-binding/errors"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/echoutil"
	"github.com/opst/knitfab/pkg/utils/retry"
	"github.com/opst/knitfab/pkg/workloads"
	"github.com/opst/knitfab/pkg/workloads/dataagt"
	"github.com/opst/knitfab/pkg/workloads/k8s"
	"github.com/opst/knitfab/pkg/workloads/keychain"
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
			return binderr.InternalServerError(err)
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
			return binderr.InternalServerError(err)
		}
		run, ok := runs[runId]
		if !ok {
			return binderr.InternalServerError(
				errors.New("failed to get the newly created run detail"),
			)
		}
		out := run.Outputs
		if len(out) != 1 {
			return binderr.InternalServerError(
				fmt.Errorf("plan %s requires %d data, not 1", kdb.Uploaded, len(out)),
			)
		}

		daRecord, err := dbData.NewAgent(
			ctx, out[0].KnitDataBody.KnitId, kdb.DataAgentWrite, time.Until(deadline),
		)
		if err != nil {
			return binderr.InternalServerError(err)
		}

		da, err := spawnDataAgent(ctx, daRecord, deadline)
		if err != nil {
			if workloads.AsConflict(err) || errors.Is(err, workloads.ErrDeadlineExceeded) {
				return binderr.ServiceUnavailable("please retry later", err)
			}
			return binderr.InternalServerError(err)
		}
		defer func() {
			if err := da.Close(); err != nil {
				return
			}
			dbData.RemoveAgent(ctx, daRecord.Name)
		}()

		bresp, err := echoutil.CopyRequest(ctx, da.URL(), c.Request())
		if err != nil {
			return binderr.InternalServerError(err)
		}
		defer bresp.Body.Close()

		newStatus := kdb.Aborting
		if 200 <= bresp.StatusCode && bresp.StatusCode < 300 {
			newStatus = kdb.Completing
		}
		if err := dbRun.SetStatus(ctx, runId, newStatus); err != nil {
			return binderr.InternalServerError(err)
		}
		if err := dbRun.Finish(ctx, runId); err != nil {
			return binderr.InternalServerError(err)
		}
		finished = true

		if newStatus != kdb.Completing {
			// proxy dataagt response.  -- fixme: check & reword error message.
			echoutil.CopyResponse(&c, bresp)
			return nil
		}

		resultSet, err := dbData.Get(ctx, []string{da.KnitID()})
		if err != nil {
			return binderr.InternalServerError(err)
		}
		data, ok := resultSet[da.KnitID()]
		if !ok {
			return binderr.InternalServerError(errors.New(`uploaded data "%s" is lost`))
		}

		return c.JSON(
			http.StatusOK,
			binddata.ComposeDetail(data),
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
				return binderr.NotFound()
			}
			return binderr.InternalServerError(err)
		}

		dagt, err := spawnDataAgent(ctx, daRecord, deadline)
		if errors.Is(err, workloads.ErrDeadlineExceeded) {
			return binderr.ServiceUnavailable("please retry later", err)
		} else if err != nil {
			return binderr.InternalServerError(err)
		}
		defer func() {
			if err := dagt.Close(); err != nil {
				return
			}
			data.RemoveAgent(ctx, daRecord.Name)
		}()

		bresp, err := echoutil.CopyRequest(ctx, dagt.URL(), c.Request())
		if err != nil {
			return binderr.InternalServerError(err)
		}

		return echoutil.CopyResponse(&c, bresp)
	}
}

func ImportDataBeginHandler(
	kp keyprovider.KeyProvider,
	dbRun kdb.RunInterface,
) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()

		deadline := time.Now().Add(30 * time.Minute)

		runId, err := dbRun.NewPseudo(ctx, kdb.Imported, time.Until(deadline))
		if err != nil {
			return binderr.InternalServerError(err)
		}

		runs, err := dbRun.Get(ctx, []string{runId})
		if err != nil {
			return binderr.InternalServerError(err)
		}
		run, ok := runs[runId]
		if !ok {
			return binderr.InternalServerError(
				errors.New("failed to get the newly created run"),
			)
		}

		out := run.Outputs
		if len(out) != 1 {
			return binderr.InternalServerError(
				fmt.Errorf("plan %s requires %d data, not 1", kdb.Imported, len(out)),
			)
		}
		data := out[0]

		kid, key, err := kp.Provide(ctx, keychain.WithExpAfter(deadline))
		if err != nil {
			return binderr.InternalServerError(err)
		}

		token, err := keychain.NewJWS(
			kid, key,
			DataImportClaim{
				RegisteredClaims: jwt.RegisteredClaims{
					// jti
					ID: uuid.NewString(),

					// sub
					Subject: data.KnitDataBody.VolumeRef,
				},

				// private claims
				KnitId: data.KnitDataBody.KnitId,
				RunId:  runId,
			},
		)
		if err != nil {
			return binderr.InternalServerError(err)
		}

		resp := c.Response()
		resp.Header().Set("Content-Type", "application/jwt")
		resp.WriteHeader(http.StatusOK)
		_, err = resp.Write([]byte(token))
		return err
	}
}

func ImportDataEndHandler(
	cluster k8s.Cluster,
	kp keyprovider.KeyProvider,
	dbRun kdb.RunInterface,
	dbData kdb.DataInterface,
) echo.HandlerFunc {
	return func(c echo.Context) error {
		req := c.Request()
		ctx := req.Context()

		if req.Header.Get("Content-Type") != "application/jwt" {
			return binderr.BadRequest(`"Content-Type" should be "application/jwt"`, nil)
		}
		if req.Body == nil {
			return binderr.BadRequest(`token given by "import/begin" is required in Body`, nil)
		}

		payload, err := io.ReadAll(req.Body)
		if err != nil {
			return binderr.InternalServerError(err)
		}

		kc, err := kp.GetKeychain(ctx)
		if err != nil {
			return binderr.InternalServerError(err)
		}

		claims, err := keychain.VerifyJWS[*DataImportClaim](kc, string(payload))
		if err != nil {
			if errors.Is(err, keychain.ErrInvalidToken) {
				return binderr.Unauthorized("invalid token", err)
			}
			return binderr.InternalServerError(err)
		}

		knitId := claims.KnitId
		data, err := dbData.Get(ctx, []string{knitId})
		if err != nil {
			return binderr.InternalServerError(err)
		}

		if _, ok := data[knitId]; !ok {
			return binderr.InternalServerError(errors.New("data not found"))
		}

		if err := func() error {
			_ctx, cancel := context.WithTimeout(ctx, 3*time.Second) // we expects that PVC has been bound.
			defer cancel()
			result := <-cluster.GetPVC(
				_ctx, retry.StaticBackoff(1*time.Second), data[knitId].KnitDataBody.VolumeRef,
				k8s.PVCIsBound,
			)
			return result.Err
		}(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || workloads.AsMissingError(err) {
				return binderr.BadRequest(
					fmt.Sprintf("retry after that PVC %s is bound", data[knitId].KnitDataBody.VolumeRef),
					err,
				)
			}
			return binderr.InternalServerError(err)
		}

		runId := claims.RunId
		if err := dbRun.SetStatus(ctx, runId, kdb.Completing); err != nil {
			if errors.Is(err, kdb.ErrInvalidRunStateChanging) {
				return binderr.Conflict("", binderr.WithError(err))
			}
			if errors.Is(err, kdb.ErrMissing) {
				return binderr.Conflict("missing Run", binderr.WithError(err))
			}
			return binderr.InternalServerError(err)
		}

		return c.JSON(http.StatusOK, binddata.ComposeDetail(data[knitId]))
	}
}

type DataImportClaim struct {
	jwt.RegisteredClaims

	// private claims
	KnitId string `json:"knitfab/knitId"`
	RunId  string `json:"knitfab/runId"`
}
