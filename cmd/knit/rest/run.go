package rest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/runs"
)

func (c *client) GetRun(ctx context.Context, runId string) (runs.Detail, error) {
	resp, err := c.httpclient.Get(c.apipath("runs", runId))
	if err != nil {
		return runs.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas runs.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("runId:%v is not found", runId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return runs.Detail{}, err
	}
	return dataMetas, nil
}

func (c *client) GetRunLog(ctx context.Context, runId string, follow bool) (io.ReadCloser, error) {
	followQuery := ""
	if follow {
		followQuery = "?follow"
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, c.apipath("runs", runId, "log")+followQuery, nil,
	)

	if err != nil {
		return nil, err
	}

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return nil, err
	}

	r, err := unmarshalStreamResponse(
		resp,
		MessageFor{
			Status4xx: fmt.Sprintf("cannot get log of runId:%v", runId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	)
	if err != nil {
		resp.Body.Close()
		return nil, err
	}
	return r, nil
}

func (c *client) FindRun(
	ctx context.Context,
	query FindRunParameter,
) ([]runs.Detail, error) {

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apipath("runs"), nil)
	if err != nil {
		return nil, err
	}

	// set query values
	q := req.URL.Query()
	paramMap := map[string][]string{
		"plan":         query.PlanId,
		"knitIdInput":  query.KnitIdIn,
		"knitIdOutput": query.KnitIdOut,
		"status":       query.Status,
	}

	if query.Since != nil {
		paramMap["since"] = []string{query.Since.Format(rfctime.RFC3339DateTimeFormatZ)}
	}

	if query.Duration != nil {
		paramMap["duration"] = []string{query.Duration.String()}
	}

	for key, value := range paramMap {
		if len(value) > 0 {
			q.Add(key, strings.Join(value, ","))
		}
	}
	req.URL.RawQuery = q.Encode()

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	dataMetas := make([]runs.Detail, 0, 5)
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("[BUG] client is not compatible with the server (status code = %d)", resp.StatusCode),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return nil, err
	}

	return dataMetas, nil
}

func (c *client) Tearoff(ctx context.Context, runId string) (runs.Detail, error) {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPut, c.apipath("runs", runId, "tearoff"), nil,
	)
	if err != nil {
		return runs.Detail{}, err
	}

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return runs.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas runs.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("runId:%v cannot be teared off", runId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return runs.Detail{}, err
	}
	return dataMetas, nil
}

func (c *client) Abort(ctx context.Context, runId string) (runs.Detail, error) {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPut, c.apipath("runs", runId, "abort"), nil,
	)
	if err != nil {
		return runs.Detail{}, err
	}

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return runs.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas runs.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("runId:%v cannot be aborted", runId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return runs.Detail{}, err
	}
	return dataMetas, nil
}

func (c *client) DeleteRun(ctx context.Context, runId string) error {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodDelete, c.apipath("runs", runId), nil,
	)

	if err != nil {
		return err
	}

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := unmarshalResponseDiscardingPayload(
		resp,
		MessageFor{
			Status4xx: fmt.Sprintf("runId:%v cannot be deleted", runId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return err
	}

	return nil
}

func (c *client) Retry(ctx context.Context, runId string) error {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPut, c.apipath("runs", runId, "retry"), nil,
	)

	if err != nil {
		return err
	}

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := unmarshalResponseDiscardingPayload(
		resp,
		MessageFor{
			Status4xx: fmt.Sprintf("cannot retry runId:%v", runId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return err
	}

	return nil
}
