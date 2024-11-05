package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	apitags "github.com/opst/knitfab-api-types/tags"
	binddata "github.com/opst/knitfab/pkg/api-types-binding/data"
	binderr "github.com/opst/knitfab/pkg/api-types-binding/errors"
	"github.com/opst/knitfab/pkg/domain"
	kdbdata "github.com/opst/knitfab/pkg/domain/data/db"
	kerr "github.com/opst/knitfab/pkg/domain/errors"
)

func GetDataForDataHandler(dbData kdbdata.DataInterface) echo.HandlerFunc {
	return func(c echo.Context) error {

		ctx := c.Request().Context()
		paramMap := c.QueryParams()
		paramTag := paramMap["tag"]

		// Extract knitId of data that contains all the tags specified in the query parameters
		tags, err := queryParamToTags(paramTag)
		if err != nil {
			if errors.Is(err, errIncorrectQueryTag) {
				return binderr.BadRequest(`each tag should be formatted as KEY:VALUE`, err)
			}
			return binderr.InternalServerError(err)
		}

		var since *time.Time
		if paramSicne := c.QueryParam("since"); paramSicne != "" {
			t, err := rfctime.ParseRFC3339DateTime(paramSicne)
			if err != nil {
				return binderr.BadRequest(
					`"since" should be a RFC3339 date-time format`,
					err,
				)
			}
			_t := t.Time()
			since = &_t
		}

		var until *time.Time
		if paramDuration := c.QueryParam("duration"); paramDuration != "" {
			d, err := time.ParseDuration(paramDuration)
			if err != nil {
				return binderr.BadRequest(
					`"duration" should be a Go duration format`,
					err,
				)
			}
			_t := since.Add(d)
			until = &_t
		}

		knitIds, err := dbData.Find(ctx, tags, since, until)
		if err != nil {
			return binderr.InternalServerError(err)
		}
		if len(knitIds) == 0 {
			return c.JSON(http.StatusOK, []data.Detail{})
		}

		d, err := dbData.Get(ctx, knitIds)
		if err != nil {
			return binderr.InternalServerError(err)
		}

		found := make([]data.Detail, 0, len(d))
		for _, id := range knitIds {
			if v, ok := d[id]; ok {
				found = append(found, binddata.ComposeDetail(v))
			}
		}

		return c.JSON(http.StatusOK, found)
	}
}

// converts query parameter array to Tag arrays
//
// [project:projectX, type:trainingData, knit#timestamp:2022-10-12T01:05:12+00:00, knit#id:knit-test-id-1]
// ---> {project, projectX}
//
//	{type, trainingData}
//	{knit#timestamp, 2022-10-12T01:05:12+00:00}
//	...
//
// When queryparam is empty, it assumes no tag is specified and returns an empty list.
func queryParamToTags(queryParam []string) ([]domain.Tag, error) {

	tags := make([]domain.Tag, len(queryParam))

	for nth, p := range queryParam {
		var found bool
		tags[nth].Key, tags[nth].Value, found = strings.Cut(p, ":")
		if !found {
			return nil, fmt.Errorf(
				`%w: "%s" is not formatted as KEY:VALUE`, errIncorrectQueryTag, p,
			)
		}
	}

	return tags, nil
}

var errIncorrectQueryTag = errors.New("incorrect query tag")

func PutTagForDataHandler(dbData kdbdata.DataInterface, paramKey string) echo.HandlerFunc {

	return func(c echo.Context) error {
		ctx := c.Request().Context()
		knitId := c.Param(paramKey)

		// read request body
		change := apitags.Change{}
		decoder := json.NewDecoder(c.Request().Body)
		decoder.DisallowUnknownFields()

		if err := decoder.Decode(&change); err != nil {
			return binderr.NewErrorMessage(
				http.StatusBadRequest,
				"format error",
				binderr.WithAdvice(err.Error()),
				binderr.WithError(err),
			)
		}

		delta := domain.TagDelta{
			RemoveKey: change.RemoveKey,
		}
		for _, tag := range change.AddTags {
			if t, err := domain.NewTag(tag.Key, tag.Value); err != nil {
				binderr.BadRequest(fmt.Sprintf("bad tag: %s", tag), err)
			} else {
				delta.Add = append(delta.Add, t)
			}
		}
		for _, tag := range change.RemoveTags {
			if t, err := domain.NewTag(tag.Key, tag.Value); err != nil {
				binderr.BadRequest(fmt.Sprintf("bad tag: %s", tag), err)
			} else {
				delta.Remove = append(delta.Remove, t)
			}
		}

		if err := dbData.UpdateTag(ctx, knitId, delta); errors.Is(err, kerr.ErrMissing) {
			return binderr.NewErrorMessage(http.StatusNotFound, "correspontind data is missing")
		} else if err != nil {
			return binderr.InternalServerError(err)
		}

		resultSet, err := dbData.Get(ctx, []string{knitId})
		if err != nil {
			return binderr.InternalServerError(err)
		}

		d, ok := resultSet[knitId]
		if !ok {
			return binderr.InternalServerError(errors.New("data not found; the data was updated tag just now"))
		}

		return c.JSON(http.StatusOK, binddata.ComposeDetail(d))
	}
}
