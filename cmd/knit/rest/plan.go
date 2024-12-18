package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/logic"
)

func (c *client) GetPlans(ctx context.Context, planId string) (plans.Detail, error) {
	resp, err := c.httpclient.Get(c.apipath("plans", planId))
	if err != nil {
		return plans.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas plans.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("planId:%v is not found", planId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return plans.Detail{}, err
	}
	return dataMetas, nil
}

func (c *client) PutPlanForActivate(ctx context.Context, planId string, isActive bool) (plans.Detail, error) {
	method := http.MethodPut
	if !isActive {
		method = http.MethodDelete
	}
	req, err := http.NewRequest(method, c.apipath("plans", planId, "active"), nil)
	if err != nil {
		return plans.Detail{}, err
	}

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return plans.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas plans.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("planId:%v is not found", planId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return plans.Detail{}, err
	}
	return dataMetas, nil
}

func (c *client) RegisterPlan(ctx context.Context, spec plans.PlanSpec) (plans.Detail, error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return plans.Detail{}, err
	}

	resp, err := c.httpclient.Post(c.apipath("plans"), "application/json", bytes.NewBuffer(b))
	if err != nil {
		return plans.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas plans.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: "invalid request",
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return plans.Detail{}, err
	}
	return dataMetas, nil
}

func (c *client) FindPlan(
	ctx context.Context,
	active logic.Ternary,
	imageVer *domain.ImageIdentifier,
	inTags []tags.Tag,
	outTags []tags.Tag,
) ([]plans.Detail, error) {

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

	if imageVer != nil {
		q.Add("image", imageVer.String())
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

	dataMetas := make([]plans.Detail, 0, 5)
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

func (c *client) UpdateResources(ctx context.Context, planId string, res plans.ResourceLimitChange) (plans.Detail, error) {
	b, err := json.Marshal(res)
	if err != nil {
		return plans.Detail{}, err
	}

	req, err := http.NewRequest(http.MethodPut, c.apipath("plans", planId, "resources"), bytes.NewBuffer(b))
	if err != nil {
		return plans.Detail{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return plans.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas plans.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("planId:%v cannot be updated", planId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return plans.Detail{}, err
	}
	return dataMetas, nil
}

func (c *client) UpdateAnnotations(ctx context.Context, planId string, change plans.AnnotationChange) (plans.Detail, error) {
	b, err := json.Marshal(change)
	if err != nil {
		return plans.Detail{}, err
	}

	req, err := http.NewRequest(http.MethodPut, c.apipath("plans", planId, "annotations"), bytes.NewBuffer(b))
	if err != nil {
		return plans.Detail{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return plans.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas plans.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("planId:%v cannot be updated", planId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return plans.Detail{}, err
	}
	return dataMetas, nil
}

func (c *client) SetServiceAccount(ctx context.Context, planId string, setServiceAccount plans.SetServiceAccount) (plans.Detail, error) {
	b, err := json.Marshal(setServiceAccount)
	if err != nil {
		return plans.Detail{}, err
	}

	req, err := http.NewRequest(http.MethodPut, c.apipath("plans", planId, "serviceaccount"), bytes.NewBuffer(b))
	if err != nil {
		return plans.Detail{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return plans.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas plans.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("planId:%v cannot be updated", planId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return plans.Detail{}, err
	}
	return dataMetas, nil
}

func (c *client) UnsetServiceAccount(ctx context.Context, planId string) (plans.Detail, error) {
	req, err := http.NewRequest(http.MethodDelete, c.apipath("plans", planId, "serviceaccount"), nil)
	if err != nil {
		return plans.Detail{}, err
	}

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return plans.Detail{}, err
	}
	defer resp.Body.Close()

	var dataMetas plans.Detail
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("planId:%v cannot be updated", planId),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return plans.Detail{}, err
	}
	return dataMetas, nil
}
