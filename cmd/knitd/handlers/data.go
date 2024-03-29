package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	apidata "github.com/opst/knitfab/pkg/api/types/data"
	apierr "github.com/opst/knitfab/pkg/api/types/errors"
	apitags "github.com/opst/knitfab/pkg/api/types/tags"
	kdb "github.com/opst/knitfab/pkg/db"
)

func GetDataForDataHandler(dbData kdb.DataInterface) echo.HandlerFunc {
	return func(c echo.Context) error {

		ctx := c.Request().Context()
		paramMap := c.QueryParams()
		paramTag := paramMap["tag"]

		// Extract knitId of data that contains all the tags specified in the query parameters
		tags, err := queryParamToTags(paramTag)
		if err != nil {
			if errors.Is(err, errIncorrectQueryTag) {
				return apierr.BadRequest(`each tag should be formatted as KEY:VALUE`, err)
			}
			return apierr.InternalServerError(err)
		}

		knitIds, err := dbData.GetKnitIdByTags(ctx, tags)
		if err != nil {
			return apierr.InternalServerError(err)
		}
		if len(knitIds) == 0 {
			return c.JSON(http.StatusOK, []apidata.Detail{})
		}

		data, err := dbData.Get(ctx, knitIds)
		if err != nil {
			return apierr.InternalServerError(err)
		}

		found := make([]apidata.Detail, 0, len(data))
		for _, d := range knitIds {
			if v, ok := data[d]; ok {
				found = append(found, apidata.ComposeDetail(v))
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
func queryParamToTags(queryParam []string) ([]kdb.Tag, error) {

	tags := make([]kdb.Tag, len(queryParam))

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

func PutTagForDataHandler(dbData kdb.DataInterface, paramKey string) echo.HandlerFunc {

	return func(c echo.Context) error {
		ctx := c.Request().Context()
		knitId := c.Param(paramKey)

		// read request body
		change := apitags.Change{}
		decoder := json.NewDecoder(c.Request().Body)
		decoder.DisallowUnknownFields()

		if err := decoder.Decode(&change); err != nil {
			return apierr.NewErrorMessage(
				http.StatusBadRequest,
				"format error",
				apierr.WithAdvice(err.Error()),
				apierr.WithError(err),
			)
		}

		delta := kdb.TagDelta{}
		for _, tag := range change.AddTags {
			if t, err := kdb.NewTag(tag.Key, tag.Value); err != nil {
				apierr.BadRequest(fmt.Sprintf("bad tag: %s", tag), err)
			} else {
				delta.Add = append(delta.Add, t)
			}
		}
		for _, tag := range change.RemoveTags {
			if t, err := kdb.NewTag(tag.Key, tag.Value); err != nil {
				apierr.BadRequest(fmt.Sprintf("bad tag: %s", tag), err)
			} else {
				delta.Remove = append(delta.Remove, t)
			}
		}

		if err := dbData.UpdateTag(ctx, knitId, delta); errors.Is(err, kdb.ErrMissing) {
			return apierr.NewErrorMessage(http.StatusNotFound, "correspontind data is missing")
		} else if err != nil {
			return apierr.InternalServerError(err)
		}

		resultSet, err := dbData.Get(ctx, []string{knitId})
		if err != nil {
			return apierr.InternalServerError(err)
		}

		d, ok := resultSet[knitId]
		if !ok {
			return apierr.InternalServerError(errors.New("data not found; the data was updated tag just now"))
		}

		return c.JSON(http.StatusOK, apidata.ComposeDetail(d))
	}
}
