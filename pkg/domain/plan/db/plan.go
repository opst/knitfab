package db

import (
	"context"

	types "github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/logic"
	"k8s.io/apimachinery/pkg/api/resource"
)

type PlanInterface interface {
	// Retreive Plans by its id
	//
	// Args
	// - context.Context:
	//
	// - ...string: plan id to be searched
	//
	// Returns
	// - map[string][]*Plan : mapping plan id to a found Plan.
	// each value should not be nil.
	//
	// - error
	Get(context.Context, []string) (map[string]*types.Plan, error)

	// register new plan
	//
	// Args
	//
	// - context.Context
	//
	// - *PlanSpec: specification of plan to be created
	//
	// Return
	//
	// - string: Registered Plan Id
	//
	// - error: ErrInvalidPlan or ErrConflictingPlan
	Register(context.Context, *types.PlanSpec) (string, error)

	// Activate Plans by its id
	//
	// This method SHOULD NOT effect to pseudo plan.
	//
	// If plan id is pseudo plan's, this method returns MissingError.
	//
	// Args
	//
	// - context.Context:
	//
	// - string: plan id to be searched
	//
	// - bool: when true, the plan should be active. otherwise, be inactive.
	//
	// Returns
	//
	// - error
	Activate(context.Context, string, bool) error

	// SetResouceLimit set (create or update) resource limit for plan.
	//
	// Args
	//
	// - context.Context
	//
	// - string: plan id to be set
	//
	// - map[string]resource.Quantity: resource name to its quantity.
	// If a resource name in this map is already set, it will be updated.
	// If a resource name in this map is not set, it will be created.
	//
	// Returns
	//
	// - error
	SetResourceLimit(ctx context.Context, planId string, rlimits map[string]resource.Quantity) error

	// UnsetResourceLimit unset resource limit for plan.
	//
	// Args
	//
	// - context.Context
	//
	// - string: plan id to be unset
	//
	// - []string: resource names to be unset
	//
	// Returns
	//
	// - error
	UnsetResourceLimit(ctx context.Context, planId string, types []string) error

	// Retreive plans that satisfies all the conditions specified in the following items:
	// tags specified in mountpoint, container image and its version, activity status of plan.
	//
	// Args
	//
	// - context.Context
	//
	// - Ternary : activity status of plan to extract
	//     False         ... deactive
	//     Indeterminate ... both
	//     True          ... active
	//
	// - ImageIdentifier : image and its version
	//
	// - TagSet : tags specified in input mountpoint
	//  retreive plans with input mountpoint containing all of the specified tags.
	//
	// - TagSet : tags specified in output mountpoint
	//  retreive plans with input mountpoint containing all of the specified tags.
	//
	// Returns
	//
	// - []string : found plan ids
	//
	// - error
	Find(context.Context, logic.Ternary, types.ImageIdentifier, []types.Tag, []types.Tag) ([]string, error)

	// UpdateAnnotations updates Annotations of a Plan.
	//
	// Args
	//
	// - context.Context
	//
	// - string : Plan ID
	//
	// - AnnotationDelta : Annotations to be added and removed
	//
	// Returns
	//
	// - error
	UpdateAnnotations(context.Context, string, types.AnnotationDelta) error

	// SetServiceAccount sets the service account for a Plan.
	//
	// Args
	//
	// - context.Context
	//
	// - string : Plan ID
	//
	// - string : Service Account name
	//
	// Returns
	//
	// - error
	SetServiceAccount(ctx context.Context, planId, serviceAccount string) error

	// UnsetServiceAccount unsets the service account for a Plan.
	//
	// Args
	//
	// - context.Context
	//
	// - string : Plan ID
	//
	// Returns
	//
	// - error
	UnsetServiceAccount(ctx context.Context, planId string) error
}
