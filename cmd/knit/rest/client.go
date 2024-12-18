package rest

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/logic"
	"github.com/opst/knitfab/pkg/utils/slices"
)

// meaningless value
type Unit interface{}

// struct that contains the arguments for FindRun
type FindRunParameter struct {
	// planId which run to be found has
	PlanId []string
	// knitId which run to be found has as input
	KnitIdIn []string
	// knitId which run to be found has as output
	KnitIdOut []string
	// status which run to be found is
	Status []string
	// time which updated time of run to be found is equal or later than
	Since *time.Time
	// duration which updated time of run to be found is within
	Duration *time.Duration
}

var ValUnit Unit = struct{}{}

type KnitClient interface {
	// register a new data into knit
	//
	// Args
	//
	// - context.Context
	//
	// - string: path to directory to be registered
	//
	// Returns
	//
	// - *apidata.Detail: metadata of created data
	//
	// - error
	PostData(ctx context.Context, source string, dereference bool) Progress[*data.Detail]

	// set/remove tags to a data in knit.
	//
	// Args
	//
	// - string: knitId to be (un)tagged.
	//
	// - apitag.Change: adding/removing tags.
	//
	// Returns
	//
	// - *apidata.Detail: metadata of updated data
	//
	// - error
	PutTagsForData(knitId string, tags tags.Change) (*data.Detail, error)

	// Download Data from knitfab and verify checksum.
	//
	// Args
	//
	// - knitId: identifier of data to be downloaded
	//
	// - handler: function to be called for raw stream.
	// If handler returns an error, downloading is stopped and the error is returned.
	//
	// Returns
	//
	// - error: error occured when starting downloading.
	//
	GetDataRaw(ctx context.Context, knitId string, handler func(io.Reader) error) error

	// Extract Data from knitfab and verify checksum.
	//
	// Args
	//
	// - knitId: identifier of data to be downloaded
	//
	// - handler: function to be called for each files in the data.
	// If handler returns an error, downloading is stopped and the error is returned.
	//
	// Returns
	//
	// - error: error occured when starting downloading.
	//
	GetData(ctx context.Context, knitId string, handler func(FileEntry) error) error

	// FindData find data with given tags.
	//
	// Args
	//
	// - context.Context
	//
	// - []apitag.Tag: tags which data to be found has.
	//
	// - time.Time: The updated time of the run to be found is later.
	//
	// - time.duration: duration which updated time of run to be found is within
	//
	// Returns
	//
	// - []apidata.Detail: metadata of found data
	//
	// - error
	FindData(ctx context.Context, tag []tags.Tag, since *time.Time, duration *time.Duration) ([]data.Detail, error)

	// GetPlan get plan detail with given planId.
	//
	// Args
	//
	// - context.Context
	//
	// - string: planId to be found
	//
	// Returns
	//
	// - apiplans.Detail: metadata of found plan
	//
	// - error
	GetPlans(ctx context.Context, planId string) (plans.Detail, error)

	// FindPlan find plan with given status, image and tags.
	//
	// Args
	//
	// - context.Context
	//
	// - logic.Ternary: true if plan to be found is activated,
	//                  false if plan to be found is deactivated,
	//                  indeterminate if plan to be found is either activated or deactivated.
	//
	// - kdb.ImageIdentifier: image which plan to be found has.
	// Pass nil when ImageIdentifier is not specified.
	//
	// - []apitag.Tag: tags which plan to be found has as input
	//
	// - []apitag.Tag: tags which plan to be found has as output
	//
	// Returns
	//
	// - []apiplans.Detail: metadata of found plan
	//
	// - error
	FindPlan(
		ctx context.Context, active logic.Ternary, imageVer *domain.ImageIdentifier,
		inTags []tags.Tag, outTags []tags.Tag,
	) ([]plans.Detail, error)

	// Activate or deactivate a plan.
	//
	// Args
	//
	// - context.Context
	//
	// - string: planId to be activated/deactivated
	//
	// - bool: true if plan is to be activated, false if plan is to be deactivated.
	//
	// Returns
	//
	// - apiplans.Detail: metadata of updated plan
	//
	// - error
	PutPlanForActivate(ctx context.Context, planId string, isActive bool) (plans.Detail, error)

	// Register a new plan into knitfab.
	//
	// Args
	//
	// - context.Context
	//
	// - apiplans.PlanSpec: spec of plan to be registered
	//
	// Returns
	//
	// - apiplans.Detail: metadata of created plan
	//
	// - error
	RegisterPlan(ctx context.Context, spec plans.PlanSpec) (plans.Detail, error)

	// SetResources set (or unset) resource limits of plan with given planId.
	//
	// Args
	//
	// - context.Context
	//
	// - string: planId to be updated
	//
	// - apiplans.ResourceLimitChange: change of resource limits
	//
	// Returns
	//
	// - apiplans.Detail: metadata of updated plan
	//
	// - error
	//
	UpdateResources(ctx context.Context, planId string, res plans.ResourceLimitChange) (plans.Detail, error)

	// UpdateAnnotations update annotations of plan with given planId.
	//
	// Args
	//
	// - context.Context
	//
	// - string: planId to be updated
	//
	// - apiplans.AnnotationChange: change of annotations
	//
	// Returns
	//
	// - apiplans.Detail: metadata of updated plan
	//
	// - error
	//
	UpdateAnnotations(ctx context.Context, planId string, change plans.AnnotationChange) (plans.Detail, error)

	// SetServiceAccount set service account of plan with given planId.
	//
	// Args
	//
	// - context.Context
	//
	// - string: planId to be updated
	//
	// - string: service account to be set
	//
	// Returns
	//
	// - apiplans.Detail: metadata of updated plan
	//
	// - error
	//
	SetServiceAccount(ctx context.Context, planId string, serviceAccount plans.SetServiceAccount) (plans.Detail, error)

	// UnsetServiceAccount unset service account of plan with given planId.
	//
	// Args
	//
	// - context.Context
	//
	// - string: planId to be updated
	//
	// Returns
	//
	// - apiplans.Detail: metadata of updated plan
	//
	// - error
	//
	UnsetServiceAccount(ctx context.Context, planId string) (plans.Detail, error)

	// GetRun get run detail with given runId.
	//
	// Args
	//
	// - context.Context
	//
	// - string: runId to be found
	//
	// Returns
	//
	// - apirun.Detail: metadata of found run
	//
	// - error
	GetRun(ctx context.Context, runId string) (runs.Detail, error)

	// GetRunLog get log of run with given runId.
	//
	// Args
	//
	// - context.Context
	//
	// - string: runId to be found
	//
	// Returns
	//
	// - io.ReadCloser: stream of log
	//
	// - error
	GetRunLog(ctx context.Context, runId string, follow bool) (io.ReadCloser, error)

	// FindRun find run with FindRunParameter.

	// Args
	//
	// - context.Context
	//
	// - FindRunParameter
	//
	// Returns
	//
	// - []apirun.Detail: metadata of found run
	//
	// - error
	FindRun(context.Context, FindRunParameter) ([]runs.Detail, error)

	// Abort abort run with given runId.
	//
	// Args
	//
	// - context.Context
	//
	// - string: runId to be aborted
	//
	// Returns
	//
	// - apirun.Detail: metadata of aborted run
	//
	// - error
	Abort(ctx context.Context, runId string) (runs.Detail, error)

	// Tearoff stop run with given runId gently.
	//
	// The run will be "completing" status after this operation.
	//
	// Args
	//
	// - context.Context
	//
	// - string: runId to be teared off
	//
	// Returns
	//
	// - apirun.Detail: metadata of run teared off
	//
	// - error
	Tearoff(ctx context.Context, runId string) (runs.Detail, error)

	// DeleteRun delete run with given runId.
	//
	// Args
	//
	// - context.Context
	//
	// - string: runId to be deleted
	//
	// Returns
	//
	// - error
	DeleteRun(ctx context.Context, runId string) error

	// Retry retry run with given runId.
	//
	// Args
	//
	// - context.Context
	//
	// - string: runId to be retried
	//
	// Returns
	//
	// - error
	Retry(ctx context.Context, runId string) error
}

type client struct {
	httpclient *http.Client
	api        string
}

// create new knit client for KnitProfile
//
// # Args
//
// - *kconf.KnitProfile
//
// # Return
//
// - KnitClient: created client
//
// - error: If given profile is invalid, ErrProfileInvalid is returned.
func NewClient(prof *kprof.KnitProfile) (KnitClient, error) {
	if err := prof.Verify(); err != nil {
		return nil, err
	}
	httpclient := new(http.Client)

	if prof.Cert.CA != "" {
		hc, err := trustCa(httpclient, []string{prof.Cert.CA})
		if err != nil {
			return nil, err
		}
		httpclient = hc
	}

	c := &client{
		httpclient: httpclient,
		api:        strings.TrimSuffix(prof.ApiRoot, "/"),
	}

	return c, nil
}

// build URL with path
func (c *client) apipath(path ...string) string {
	path = slices.Map(path, func(p string) string {
		return strings.TrimPrefix(strings.TrimSuffix(p, "/"), "/")
	})

	return strings.Join(append([]string{c.api}, path...), "/")
}

func trustCa(hc *http.Client, cacerts []string) (*http.Client, error) {
	if len(cacerts) <= 0 {
		return hc, nil
	}

	if hc.Transport == nil {
		hc.Transport = http.DefaultTransport
	}

	tran, ok := hc.Transport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("failed to add ca cert")
	}
	tran = tran.Clone()

	tcc := tran.TLSClientConfig.Clone()
	if tcc == nil {
		tcc = &tls.Config{}
	}

	rootcas := tcc.RootCAs
	if rootcas == nil {
		rootcas = x509.NewCertPool()
		tcc.RootCAs = rootcas
	}
	for _, ca := range cacerts {
		bin, err := base64.StdEncoding.DecodeString(ca)
		if err != nil {
			return nil, err
		}

		if !rootcas.AppendCertsFromPEM(bin) {
			return nil, fmt.Errorf("failed to add cert")
		}
	}

	tran.TLSClientConfig = tcc
	hc.Transport = tran
	return hc, nil
}
