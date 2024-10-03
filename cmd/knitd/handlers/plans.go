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
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/logic"
	"k8s.io/apimachinery/pkg/api/resource"
)

func PlanRegisterHandler(dbplan kdb.PlanInterface) echo.HandlerFunc {
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

		plan, err := func() (*kdb.Plan, error) {
			params := kdb.PlanParam{
				Image:   specInReq.Image.Repository,
				Version: specInReq.Image.Tag,
				Active:  utils.Default(specInReq.Active, true),
				Inputs: utils.Map(
					specInReq.Inputs,
					func(mp apiplans.Mountpoint) kdb.MountPointParam {
						return kdb.MountPointParam{
							Path: mp.Path,
							Tags: kdb.NewTagSet(
								utils.Map(mp.Tags, func(reqtag apitags.Tag) kdb.Tag {
									return kdb.Tag{Key: reqtag.Key, Value: reqtag.Value}
								}),
							),
						}
					},
				),
				Resources: specInReq.Resources,
				Outputs: utils.Map(
					specInReq.Outputs,
					func(mp apiplans.Mountpoint) kdb.MountPointParam {
						return kdb.MountPointParam{
							Path: mp.Path,
							Tags: kdb.NewTagSet(
								utils.Map(mp.Tags, func(reqtag apitags.Tag) kdb.Tag {
									return kdb.Tag{Key: reqtag.Key, Value: reqtag.Value}
								}),
							),
						}
					},
				),
				ServiceAccount: specInReq.ServiceAccount,
				Annotations: utils.Map(specInReq.Annotations, func(a apiplans.Annotation) kdb.Annotation {
					return kdb.Annotation{Key: a.Key, Value: a.Value}
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
				params.Log = &kdb.LogParam{
					Tags: kdb.NewTagSet(
						utils.Map(l.Tags, func(reqtag apitags.Tag) kdb.Tag {
							return kdb.Tag{Key: reqtag.Key, Value: reqtag.Value}
						}),
					),
				}
			}

			if on := specInReq.OnNode; on != nil {
				onNode := []kdb.OnNode{}
				for _, may := range on.May {
					onNode = append(
						onNode,
						kdb.OnNode{Mode: kdb.MayOnNode, Key: may.Key, Value: may.Value},
					)
				}
				for _, prefer := range on.Prefer {
					onNode = append(
						onNode,
						kdb.OnNode{Mode: kdb.PreferOnNode, Key: prefer.Key, Value: prefer.Value},
					)
				}
				for _, must := range on.Must {
					onNode = append(
						onNode,
						kdb.OnNode{Mode: kdb.MustOnNode, Key: must.Key, Value: must.Value},
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
			if errors.Is(err, kdb.ErrConflictingPlan) {
				if planEx := new(kdb.ErrEquivPlanExists); errors.As(err, &planEx) {
					return binderr.Conflict(
						"there are equiverent plan", binderr.WithSee(planEx.PlanId),
					)
				}
				return binderr.Conflict("plan spec conflics with others", binderr.WithError(err))
			}
			if errors.Is(err, kdb.ErrInvalidPlan) {
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

func FindPlanHandler(dbplan kdb.PlanInterface) echo.HandlerFunc {

	type FindArgs struct {
		Active   logic.Ternary
		ImageVer kdb.ImageIdentifier
		InTag    []kdb.Tag
		OutTag   []kdb.Tag
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

func GetPlanHandler(dbplan kdb.PlanInterface) echo.HandlerFunc {

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

func PutPlanForActivate(dbPlan kdb.PlanInterface, isActive bool) echo.HandlerFunc {

	return func(c echo.Context) error {
		ctx := c.Request().Context()
		planId := c.Param("planId")

		if err := dbPlan.Activate(ctx, planId, isActive); errors.Is(err, kdb.ErrMissing) {
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

func PutPlanResource(dbPlan kdb.PlanInterface, planIdParam string) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()
		planId := c.Param(planIdParam)

		req := new(apiplans.ResourceLimitChange)
		if err := c.Bind(req); err != nil {
			return binderr.BadRequest("can not understand the requested json", err)
		}

		if err := dbPlan.SetResourceLimit(ctx, planId, req.Set); err != nil {
			if errors.Is(err, kdb.ErrMissing) {
				return binderr.NotFound()
			}
			return binderr.InternalServerError(err)
		}

		if err := dbPlan.UnsetResourceLimit(ctx, planId, req.Unset); err != nil {
			if errors.Is(err, kdb.ErrMissing) {
				return binderr.NotFound()
			}
			return binderr.InternalServerError(err)
		}

		plans, err := dbPlan.Get(ctx, []string{planId})
		if err != nil {
			if errors.Is(err, kdb.ErrMissing) {
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

func PutPlanAnnotations(dbPlan kdb.PlanInterface, planIdParam string) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()
		planId := c.Param(planIdParam)

		req := new(apiplans.AnnotationChange)
		if err := c.Bind(req); err != nil {
			return binderr.BadRequest("can not understand the requested json", err)
		}

		delta := kdb.AnnotationDelta{
			Add: utils.Map(req.Add, func(a apiplans.Annotation) kdb.Annotation {
				return kdb.Annotation{Key: a.Key, Value: a.Value}
			}),
			Remove: utils.Map(req.Remove, func(a apiplans.Annotation) kdb.Annotation {
				return kdb.Annotation{Key: a.Key, Value: a.Value}
			}),
		}

		if err := dbPlan.UpdateAnnotations(ctx, planId, delta); err != nil {
			if errors.Is(err, kdb.ErrMissing) {
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
