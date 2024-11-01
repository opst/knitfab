package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	apiplans "github.com/opst/knitfab-api-types/plans"
	apitags "github.com/opst/knitfab-api-types/tags"
	binderr "github.com/opst/knitfab/pkg/api-types-binding/errors"
	bindplan "github.com/opst/knitfab/pkg/api-types-binding/plans"
	"github.com/opst/knitfab/pkg/domain"
	kerr "github.com/opst/knitfab/pkg/domain/errors"
	kdbplan "github.com/opst/knitfab/pkg/domain/plan/db"
	"github.com/opst/knitfab/pkg/utils/logic"
	"github.com/opst/knitfab/pkg/utils/nils"
	"github.com/opst/knitfab/pkg/utils/slices"
	"k8s.io/apimachinery/pkg/api/resource"
)

func PlanRegisterHandler(dbplan kdbplan.PlanInterface) echo.HandlerFunc {
	return func(c echo.Context) error {
		req := c.Request()
		ctx := req.Context()
		if strings.ToLower(req.Header.Get("content-type")) != "application/json" {
			return binderr.BadRequest(
				"unexpected content type. it shoule be application/json", nil,
			)
		}

		specInReq := new(apiplans.PlanSpec)
		if err := json.NewDecoder(req.Body).Decode(specInReq); err != nil {
			return binderr.BadRequest(
				"can not understand the requested json", err,
			)
		}

		plan, err := func() (*domain.Plan, error) {
			params := domain.PlanParam{
				Image:      specInReq.Image.Repository,
				Version:    specInReq.Image.Tag,
				Active:     nils.Default(specInReq.Active, true),
				Entrypoint: specInReq.Entrypoint,
				Args:       specInReq.Args,
				Inputs: slices.Map(
					specInReq.Inputs,
					func(mp apiplans.Mountpoint) domain.MountPointParam {
						return domain.MountPointParam{
							Path: mp.Path,
							Tags: domain.NewTagSet(
								slices.Map(mp.Tags, func(reqtag apitags.Tag) domain.Tag {
									return domain.Tag{Key: reqtag.Key, Value: reqtag.Value}
								}),
							),
						}
					},
				),
				Resources: specInReq.Resources,
				Outputs: slices.Map(
					specInReq.Outputs,
					func(mp apiplans.Mountpoint) domain.MountPointParam {
						return domain.MountPointParam{
							Path: mp.Path,
							Tags: domain.NewTagSet(
								slices.Map(mp.Tags, func(reqtag apitags.Tag) domain.Tag {
									return domain.Tag{Key: reqtag.Key, Value: reqtag.Value}
								}),
							),
						}
					},
				),
				ServiceAccount: specInReq.ServiceAccount,
				Annotations: slices.Map(specInReq.Annotations, func(a apiplans.Annotation) domain.Annotation {
					return domain.Annotation{Key: a.Key, Value: a.Value}
				}),
			}

			if params.Resources == nil {
				params.Resources = map[string]resource.Quantity{}
			}
			if _, ok := params.Resources["cpu"]; !ok {
				params.Resources["cpu"] = resource.MustParse("1")
			}
			if _, ok := params.Resources["memory"]; !ok {
				params.Resources["memory"] = resource.MustParse("1Gi")
			}

			if l := specInReq.Log; l != nil {
				params.Log = &domain.LogParam{
					Tags: domain.NewTagSet(
						slices.Map(l.Tags, func(reqtag apitags.Tag) domain.Tag {
							return domain.Tag{Key: reqtag.Key, Value: reqtag.Value}
						}),
					),
				}
			}

			if on := specInReq.OnNode; on != nil {
				onNode := []domain.OnNode{}
				for _, may := range on.May {
					onNode = append(
						onNode,
						domain.OnNode{Mode: domain.MayOnNode, Key: may.Key, Value: may.Value},
					)
				}
				for _, prefer := range on.Prefer {
					onNode = append(
						onNode,
						domain.OnNode{Mode: domain.PreferOnNode, Key: prefer.Key, Value: prefer.Value},
					)
				}
				for _, must := range on.Must {
					onNode = append(
						onNode,
						domain.OnNode{Mode: domain.MustOnNode, Key: must.Key, Value: must.Value},
					)
				}
				params.OnNode = onNode
			}

			spec, err := params.Validate()
			if err != nil {
				return nil, err
			}

			planId, err := dbplan.Register(ctx, spec)
			if err != nil {
				return nil, err
			}

			plans, err := dbplan.Get(ctx, []string{planId})
			if err != nil {
				return nil, err
			}
			return plans[planId], nil
		}()

		if err != nil {
			if errors.Is(err, domain.ErrConflictingPlan) {
				if planEx := new(domain.ErrEquivPlanExists); errors.As(err, &planEx) {
					return binderr.Conflict(
						"there are equiverent plan", binderr.WithSee(planEx.PlanId),
					)
				}
				return binderr.Conflict("plan spec conflics with others", binderr.WithError(err))
			}
			if errors.Is(err, domain.ErrInvalidPlan) {
				return binderr.BadRequest(err.Error(), err)
			}

			return binderr.InternalServerError(err)
		}

		resp := c.Response()
		resp.Header().Add("Content-Type", "application/json")

		return c.JSON(
			http.StatusOK,
			bindplan.ComposeDetail(*plan),
		)
	}
}

func FindPlanHandler(dbplan kdbplan.PlanInterface) echo.HandlerFunc {

	type FindArgs struct {
		Active   logic.Ternary
		ImageVer domain.ImageIdentifier
		InTag    []domain.Tag
		OutTag   []domain.Tag
	}

	return func(c echo.Context) error {
		c.Response().Header().Add("Content-Type", "application/json")

		args, err := func(c echo.Context) (*FindArgs, error) {

			result := FindArgs{}

			paramMap := c.QueryParams()
			paramActive := c.QueryParam("active")
			paramInTag := paramMap["in_tag"]
			paramOutTag := paramMap["out_tag"]
			paramImage := c.QueryParam("image")

			if paramActive == "" {
				result.Active = logic.Indeterminate
			} else if paramActive == "true" {
				result.Active = logic.True
			} else if paramActive == "false" {
				result.Active = logic.False
			} else {
				return nil, errIncorrectQueryActive
			}

			inTag, err := queryParamToTags(paramInTag)
			if err != nil {
				return nil, errIncorrectQueryInTag
			}
			result.InTag = inTag
			outTag, err := queryParamToTags(paramOutTag)
			if err != nil {
				return nil, errIncorrectQueryOutTag
			}
			result.OutTag = outTag

			if paramImage != "" {
				image, version, _ := strings.Cut(paramImage, ":")

				if image == "" {
					return nil, errIncorrectQueryImageVersion
				}
				result.ImageVer.Image = image
				result.ImageVer.Version = version
			}

			return &result, nil
		}(c)

		if err != nil {
			return binderr.BadRequest("query specification is incorrect", err)
		}
		ctx := c.Request().Context()

		planIds, err := dbplan.Find(ctx, args.Active, args.ImageVer, args.InTag, args.OutTag)
		if err != nil {
			return binderr.InternalServerError(err)
		}

		ps, err := dbplan.Get(ctx, planIds)
		if err != nil {
			return binderr.InternalServerError(err)
		}

		resp := make([]apiplans.Detail, 0, len(ps))
		for _, planId := range planIds {
			resp = append(resp, bindplan.ComposeDetail(*ps[planId]))
		}

		return c.JSON(http.StatusOK, resp)
	}
}

func GetPlanHandler(dbplan kdbplan.PlanInterface) echo.HandlerFunc {

	return func(c echo.Context) error {

		c.Response().Header().Add("Content-Type", "application/json")

		planId := c.Param("planId")
		ctx := c.Request().Context()

		result, err := dbplan.Get(ctx, []string{planId})
		if err != nil {
			return binderr.InternalServerError(err)
		}

		plan, ok := result[planId]
		if !ok {
			return binderr.NotFound()
		}

		return c.JSON(http.StatusOK, bindplan.ComposeDetail(*plan))
	}
}

var (
	// Find method parameter error
	errIncorrectQueryActive       = errors.New("incorrect query param active")
	errIncorrectQueryImageVersion = errors.New("incorrect query param image version")
	errIncorrectQueryInTag        = errors.New("incorrect query param in-tag")
	errIncorrectQueryOutTag       = errors.New("incorrect query param out-tag")
)

func PutPlanForActivate(dbPlan kdbplan.PlanInterface, isActive bool) echo.HandlerFunc {

	return func(c echo.Context) error {
		ctx := c.Request().Context()
		planId := c.Param("planId")

		if err := dbPlan.Activate(ctx, planId, isActive); errors.Is(err, kerr.ErrMissing) {
			return binderr.NotFound()
		} else if err != nil {
			return binderr.InternalServerError(err)
		}

		plans, err := dbPlan.Get(ctx, []string{planId})
		if err != nil {
			return binderr.InternalServerError(err)
		}

		if p, ok := plans[planId]; ok {
			return c.JSON(http.StatusOK, bindplan.ComposeDetail(*p))
		} else {
			return binderr.NotFound()
		}
	}
}

func Deref[T any, R any](f func(T) R) func(*T) R {
	return func(t *T) R {
		return f(*t)
	}
}

func PutPlanResource(dbPlan kdbplan.PlanInterface, planIdParam string) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()
		planId := c.Param(planIdParam)

		req := new(apiplans.ResourceLimitChange)
		if err := c.Bind(req); err != nil {
			return binderr.BadRequest("can not understand the requested json", err)
		}

		if err := dbPlan.SetResourceLimit(ctx, planId, req.Set); err != nil {
			if errors.Is(err, kerr.ErrMissing) {
				return binderr.NotFound()
			}
			return binderr.InternalServerError(err)
		}

		if err := dbPlan.UnsetResourceLimit(ctx, planId, req.Unset); err != nil {
			if errors.Is(err, kerr.ErrMissing) {
				return binderr.NotFound()
			}
			return binderr.InternalServerError(err)
		}

		plans, err := dbPlan.Get(ctx, []string{planId})
		if err != nil {
			if errors.Is(err, kerr.ErrMissing) {
				return binderr.NotFound()
			}
			return binderr.InternalServerError(err)
		}

		if p, ok := plans[planId]; ok {
			return c.JSON(http.StatusOK, bindplan.ComposeDetail(*p))
		} else {
			return binderr.NotFound()
		}
	}
}

func PutPlanAnnotations(dbPlan kdbplan.PlanInterface, planIdParam string) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()
		planId := c.Param(planIdParam)

		req := new(apiplans.AnnotationChange)
		if err := c.Bind(req); err != nil {
			return binderr.BadRequest("can not understand the request", err)
		}

		delta := domain.AnnotationDelta{
			Add: slices.Map(req.Add, func(a apiplans.Annotation) domain.Annotation {
				return domain.Annotation{Key: a.Key, Value: a.Value}
			}),
			Remove: slices.Map(req.Remove, func(a apiplans.Annotation) domain.Annotation {
				return domain.Annotation{Key: a.Key, Value: a.Value}
			}),
			RemoveKey: req.RemoveKey,
		}

		if err := dbPlan.UpdateAnnotations(ctx, planId, delta); err != nil {
			if errors.Is(err, kerr.ErrMissing) {
				return binderr.NotFound()
			}
			return binderr.InternalServerError(err)
		}

		plans, err := dbPlan.Get(ctx, []string{planId})
		if err != nil {
			return binderr.InternalServerError(err)
		}

		if p, ok := plans[planId]; ok {
			return c.JSON(http.StatusOK, bindplan.ComposeDetail(*p))
		} else {
			return binderr.NotFound()
		}
	}
}

func PutPlanServiceAccount(dbPlan kdbplan.PlanInterface, planIdParam string) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()
		planId := c.Param(planIdParam)

		req := new(apiplans.SetServiceAccount)
		if err := c.Bind(req); err != nil {
			return binderr.BadRequest("can not understand the request", err)
		}

		if err := dbPlan.SetServiceAccount(ctx, planId, req.ServiceAccount); err != nil {
			if errors.Is(err, kerr.ErrMissing) {
				return binderr.NotFound()
			}
			return binderr.InternalServerError(err)
		}

		plans, err := dbPlan.Get(ctx, []string{planId})
		if err != nil {
			return binderr.InternalServerError(err)
		}

		if p, ok := plans[planId]; ok {
			return c.JSON(http.StatusOK, bindplan.ComposeDetail(*p))
		} else {
			return binderr.NotFound()
		}
	}
}

func DeletePlanServiceAccount(dbPlan kdbplan.PlanInterface, planIdParam string) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()
		planId := c.Param(planIdParam)

		if err := dbPlan.UnsetServiceAccount(ctx, planId); err != nil {
			if errors.Is(err, kerr.ErrMissing) {
				return binderr.NotFound()
			}
			return binderr.InternalServerError(err)
		}

		plans, err := dbPlan.Get(ctx, []string{planId})
		if err != nil {
			return binderr.InternalServerError(err)
		}

		if p, ok := plans[planId]; ok {
			return c.JSON(http.StatusOK, bindplan.ComposeDetail(*p))
		} else {
			return binderr.NotFound()
		}
	}
}
