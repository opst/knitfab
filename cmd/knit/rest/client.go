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

	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	apidata "github.com/opst/knitfab/pkg/api/types/data"
	apiplans "github.com/opst/knitfab/pkg/api/types/plans"
	apirun "github.com/opst/knitfab/pkg/api/types/runs"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/logic"
)

// meaningless value
type Unit interface{}

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
	PostData(ctx context.Context, source string, dereference bool) Progress[*apidata.Detail]

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
	PutTagsForData(knitId string, tags apitag.Change) (*apidata.Detail, error)

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
	// Returns
	//
	// - []apidata.Detail: metadata of found data
	//
	// - error
	FindData(ctx context.Context, tag []apitag.Tag) ([]apidata.Detail, error)

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
	GetPlans(ctx context.Context, planId string) (apiplans.Detail, error)

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
	// - kdb.ImageIdentifier: image which plan to be found has
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
		ctx context.Context, active logic.Ternary, imageVer kdb.ImageIdentifier,
		inTags []apitag.Tag, outTags []apitag.Tag,
	) ([]apiplans.Detail, error)

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
	PutPlanForActivate(ctx context.Context, planId string, isActive bool) (apiplans.Detail, error)

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
	RegisterPlan(ctx context.Context, spec apiplans.PlanSpec) (apiplans.Detail, error)

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
	GetRun(ctx context.Context, runId string) (apirun.Detail, error)

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

	// FindRun find run with given planId, knitId and status.

	// Args
	//
	// - context.Context
	//
	// - []string: planId which run to be found has
	//
	// - []string: knitId which run to be found has as input
	//
	// - []string: knitId which run to be found has as output
	//
	// - []string: status which run to be found is
	//
	// Returns
	//
	// - []apirun.Detail: metadata of found run
	//
	// - error
	FindRun(
		ctx context.Context, planId []string, knitIdIn []string, knitIdOut []string, status []string,
	) ([]apirun.Detail, error)

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
	Abort(ctx context.Context, runId string) (apirun.Detail, error)

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
	Tearoff(ctx context.Context, runId string) (apirun.Detail, error)

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

	// SetResources set (or unset) resource limits of plan with given planId.
	UpdateResources(ctx context.Context, planId string, res apiplans.ResourceLimitChange) (apiplans.Detail, error)
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
	path = utils.Map(path, func(p string) string {
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
