package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	apiplans "github.com/opst/knitfab/pkg/api/types/plans"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils/logic"
)

func (c *client) GetPlans(ctx context.Context, planId string) (apiplans.Detail, error) {
	resp, err := c.httpclient.Get(c.apipath("plans", planId))
	if err != nil {
		return apiplans.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas apiplans.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("planId:%v is not found", planId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return apiplans.Detail{}, err
	}
	return dataMetas, nil
}

func (c *client) PutPlanForActivate(ctx context.Context, planId string, isActive bool) (apiplans.Detail, error) {
	method := http.MethodPut
	if !isActive {
		method = http.MethodDelete
	}
	req, err := http.NewRequest(method, c.apipath("plans", planId, "active"), nil)
	if err != nil {
		return apiplans.Detail{}, err
	}

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return apiplans.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas apiplans.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("planId:%v is not found", planId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return apiplans.Detail{}, err
	}
	return dataMetas, nil
}

func (c *client) RegisterPlan(ctx context.Context, spec apiplans.PlanSpec) (apiplans.Detail, error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return apiplans.Detail{}, err
	}

	resp, err := c.httpclient.Post(c.apipath("plans"), "application/json", bytes.NewBuffer(b))
	if err != nil {
		return apiplans.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas apiplans.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: "invalid request",
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return apiplans.Detail{}, err
	}
	return dataMetas, nil
}

func (c *client) FindPlan(
	ctx context.Context,
	active logic.Ternary,
	imageVer kdb.ImageIdentifier,
	inTags []apitag.Tag,
	outTags []apitag.Tag,
) ([]apiplans.Detail, error) {

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apipath("plans"), nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()

	switch active {
	case logic.True:
		q.Add("active", "true")
	case logic.False:
		q.Add("active", "false")
	case logic.Indeterminate:
		// add nothing
	}

	if imageVer.Image != "" {
		q.Add("image", fmt.Sprintf("%s:%s", imageVer.Image, imageVer.Version))
	} else if imageVer.Version != "" {
		return nil, err
	}

	inTagCount := len(inTags)
	if 0 < inTagCount {
		for _, t := range inTags {
			q.Add("in_tag", fmt.Sprintf("%s:%s", t.Key, t.Value))
		}
	}

	outTagCount := len(outTags)
	if 0 < outTagCount {
		for _, t := range outTags {
			q.Add("out_tag", fmt.Sprintf("%s:%s", t.Key, t.Value))
		}
	}

	req.URL.RawQuery = q.Encode()
	resp, err := c.httpclient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	dataMetas := make([]apiplans.Detail, 0, 5)
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

func (c *client) UpdateResources(ctx context.Context, planId string, res apiplans.ResourceLimitChange) (apiplans.Detail, error) {
	b, err := json.Marshal(res)
	if err != nil {
		return apiplans.Detail{}, err
	}

	req, err := http.NewRequest(http.MethodPut, c.apipath("plans", planId, "resources"), bytes.NewBuffer(b))
	if err != nil {
		return apiplans.Detail{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return apiplans.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas apiplans.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("planId:%v cannot be updated", planId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return apiplans.Detail{}, err
	}
	return dataMetas, nil
}
