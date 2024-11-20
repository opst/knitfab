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
	"github.com/opst/knitfab/pkg/domain"
	kdbdata "github.com/opst/knitfab/pkg/domain/data/db"
	k8sdata "github.com/opst/knitfab/pkg/domain/data/k8s"
	kerr "github.com/opst/knitfab/pkg/domain/errors"
	k8serrors "github.com/opst/knitfab/pkg/domain/errors/k8serrors"
	keychain "github.com/opst/knitfab/pkg/domain/keychain/k8s"
	kdbrun "github.com/opst/knitfab/pkg/domain/run/db"
	"github.com/opst/knitfab/pkg/utils/echoutil"
)

func PostDataHandler(
	dbData kdbdata.DataInterface,
	dbRun kdbrun.Interface,
	k8sData k8sdata.Interface,
) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()

		finished := false
		deadline := time.Now().Add(30 * time.Second)

		runId, err := dbRun.NewPseudo(ctx, domain.Uploaded, time.Until(deadline))
		if err != nil {
			return binderr.InternalServerError(err)
		}
		defer func() {
			if finished {
				return
			}
			ctx := context.Background()
			dbRun.SetStatus(ctx, runId, domain.Aborting)
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
				fmt.Errorf("plan %s requires %d data, not 1", domain.Uploaded, len(out)),
			)
		}

		daRecord, err := dbData.NewAgent(
			ctx, out[0].KnitDataBody.KnitId, domain.DataAgentWrite, time.Until(deadline),
		)
		if err != nil {
			return binderr.InternalServerError(err)
		}

		da, err := k8sData.SpawnDataAgent(ctx, daRecord, deadline)
		if err != nil {
			if k8serrors.AsConflict(err) || errors.Is(err, k8serrors.ErrDeadlineExceeded) {
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

		newStatus := domain.Aborting
		if 200 <= bresp.StatusCode && bresp.StatusCode < 300 {
			newStatus = domain.Completing
		}
		if err := dbRun.SetStatus(ctx, runId, newStatus); err != nil {
			return binderr.InternalServerError(err)
		}
		if err := dbRun.Finish(ctx, runId); err != nil {
			return binderr.InternalServerError(err)
		}
		finished = true

		if newStatus != domain.Completing {
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
	data kdbdata.DataInterface,
	iDataK8s k8sdata.Interface,
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
		daRecord, err := data.NewAgent(ctx, knitId, domain.DataAgentRead, timeout)
		if err != nil {
			if errors.Is(err, kerr.ErrMissing) {
				return binderr.NotFound()
			}
			return binderr.InternalServerError(err)
		}

		dagt, err := iDataK8s.SpawnDataAgent(ctx, daRecord, deadline)
		if errors.Is(err, k8serrors.ErrDeadlineExceeded) {
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
	dbRun kdbrun.Interface,
) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()

		deadline := time.Now().Add(30 * time.Minute)

		runId, err := dbRun.NewPseudo(ctx, domain.Imported, time.Until(deadline))
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
				fmt.Errorf("plan %s requires %d data, not 1", domain.Imported, len(out)),
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
	k8sData k8sdata.Interface,
	kp keyprovider.KeyProvider,
	dbRun kdbrun.Interface,
	dbData kdbdata.DataInterface,
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

		if ok, err := k8sData.CheckDataIsBound(ctx, data[knitId].KnitDataBody); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || k8serrors.AsMissingError(err) {
				return binderr.BadRequest(
					fmt.Sprintf("retry after that PVC %s is bound", data[knitId].KnitDataBody.VolumeRef),
					err,
				)
			}
			return binderr.InternalServerError(err)
		} else if !ok {
			return binderr.BadRequest(
				fmt.Sprintf("retry after that PVC %s is bound", data[knitId].KnitDataBody.VolumeRef),
				nil,
			)
		}

		runId := claims.RunId
		if err := dbRun.SetStatus(ctx, runId, domain.Completing); err != nil {
			if errors.Is(err, domain.ErrInvalidRunStateChanging) {
				return binderr.Conflict("", binderr.WithError(err))
			}
			if errors.Is(err, kerr.ErrMissing) {
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
