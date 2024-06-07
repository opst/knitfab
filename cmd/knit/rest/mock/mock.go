package mock

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/opst/knitfab/cmd/knit/rest"
	apidata "github.com/opst/knitfab/pkg/api/types/data"
	apiplans "github.com/opst/knitfab/pkg/api/types/plans"
	apirun "github.com/opst/knitfab/pkg/api/types/runs"
	apitags "github.com/opst/knitfab/pkg/api/types/tags"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils/logic"
)

type PostDataArgs struct {
	Source string
}

type PutTagsForDataArgs struct {
	KnitId string
	Tags   apitags.Change
}

type FindPlanArgs struct {
	Active   logic.Ternary
	ImageVer kdb.ImageIdentifier
	InTags   []apitags.Tag
	OutTags  []apitags.Tag
}

type FindDataArgs struct {
	Tags     []apitags.Tag
	since    *time.Time
	duration *time.Duration
}

type FindRunArgs struct {
	planId    []string
	KnitIdIn  []string
	KnitIdOut []string
	status    []string
	since     time.Time
	duration  time.Duration
}

func New(t *testing.T) *mockKnitClient {
	return &mockKnitClient{t: t}
}

type MockedPostDataProgress struct {
	EstimatedTotalSize_ int64

	ProgressedSize_ int64

	ProgressingFile_ string

	Error_ error

	Result_ *apidata.Detail

	ResultOk_ bool

	Done_ <-chan struct{}

	Sent_ <-chan struct{}
}

func (m *MockedPostDataProgress) EstimatedTotalSize() int64 {
	return m.EstimatedTotalSize_
}

func (m *MockedPostDataProgress) ProgressedSize() int64 {
	return m.ProgressedSize_
}

func (m *MockedPostDataProgress) ProgressingFile() string {
	return m.ProgressingFile_
}

func (m *MockedPostDataProgress) Result() (*apidata.Detail, bool) {
	return m.Result_, m.ResultOk_
}

func (m *MockedPostDataProgress) Error() error {
	return m.Error_
}

func (m *MockedPostDataProgress) Done() <-chan struct{} {
	return m.Done_
}

func (m *MockedPostDataProgress) Sent() <-chan struct{} {
	return m.Sent_
}

type mockKnitClient struct {
	t    *testing.T
	Impl struct {
		PostData       func(ctx context.Context, source string, dereference bool) rest.Progress[*apidata.Detail]
		PutTagsForData func(knitId string, tags apitags.Change) (*apidata.Detail, error)
		GetDataRaw     func(context.Context, string, func(io.Reader) error) error
		GetData        func(context.Context, string, func(rest.FileEntry) error) error
		FindData       func(ctx context.Context, tags []apitags.Tag, since *time.Time, duration *time.Duration) ([]apidata.Detail, error)
		GetPlans       func(ctx context.Context, planId string) (apiplans.Detail, error)
		FindPlan       func(
			ctx context.Context, active logic.Ternary, imageVer kdb.ImageIdentifier,
			inTags []apitags.Tag, outTags []apitags.Tag,
		) ([]apiplans.Detail, error)
		PutPlanForActivate func(ctx context.Context, planId string, isActive bool) (apiplans.Detail, error)
		UpdateResources    func(ctx context.Context, runId string, resources apiplans.ResourceLimitChange) (apiplans.Detail, error)
		RegisterPlan       func(ctx context.Context, spec apiplans.PlanSpec) (apiplans.Detail, error)
		GetRun             func(ctx context.Context, runId string) (apirun.Detail, error)
		GetRunLog          func(ctx context.Context, runId string, follow bool) (io.ReadCloser, error)
		FindRun            func(ctx context.Context, query rest.FindRunParameter) ([]apirun.Detail, error)
		Abort              func(ctx context.Context, runId string) (apirun.Detail, error)
		Tearoff            func(ctx context.Context, runId string) (apirun.Detail, error)
		DeleteRun          func(ctx context.Context, runId string) error
		Retry              func(ctx context.Context, runId string) error
	}
	Calls struct {
		PostData           []PostDataArgs
		PutTagsForData     []PutTagsForDataArgs
		GetDataRaw         []string
		GetData            []string
		FindData           []FindDataArgs
		GetPlans           []string
		Findplan           []FindPlanArgs
		PutPlanForActivate []string
		UpdateResources    []struct {
			PlanId    string
			Resources apiplans.ResourceLimitChange
		}
		RegisterPlan []apiplans.PlanSpec
		GetRun       []string
		GetRunLog    []struct {
			RunId  string
			Follow bool
		}
		FindRun   []FindRunArgs
		Tearoff   []string
		Abort     []string
		DeleteRun []string
		Retry     []string
	}
}

var _ rest.KnitClient = &mockKnitClient{}

func (m *mockKnitClient) PostData(ctx context.Context, src string, dereference bool) rest.Progress[*apidata.Detail] {
	m.t.Helper()

	m.Calls.PostData = append(m.Calls.PostData, PostDataArgs{Source: src})
	if m.Impl.PostData == nil {
		m.t.Fatal("PostData is not ready to be called")
	}
	return m.Impl.PostData(ctx, src, dereference)
}

func (m *mockKnitClient) PutTagsForData(knitId string, argtags apitags.Change) (*apidata.Detail, error) {
	m.t.Helper()

	m.Calls.PutTagsForData = append(m.Calls.PutTagsForData, PutTagsForDataArgs{KnitId: knitId, Tags: argtags})

	if m.Impl.PutTagsForData == nil {
		m.t.Fatal("PutTagsForData is not ready to be called")
	}
	return m.Impl.PutTagsForData(knitId, argtags)
}

func (m *mockKnitClient) GetDataRaw(ctx context.Context, knitId string, handler func(io.Reader) error) error {
	m.t.Helper()

	m.Calls.GetDataRaw = append(m.Calls.GetDataRaw, knitId)
	if m.Impl.GetDataRaw == nil {
		m.t.Fatal("GetDataRaw is not ready to be called")
	}
	return m.Impl.GetDataRaw(ctx, knitId, handler)
}

func (m *mockKnitClient) GetData(ctx context.Context, knitId string, handler func(rest.FileEntry) error) error {
	m.t.Helper()

	m.Calls.GetData = append(m.Calls.GetData, knitId)
	if m.Impl.GetData == nil {
		m.t.Fatal("GetData it not ready to be called")
	}
	return m.Impl.GetData(ctx, knitId, handler)
}

func (m *mockKnitClient) FindData(ctx context.Context, tags []apitags.Tag, since *time.Time, duration *time.Duration) ([]apidata.Detail, error) {
	m.t.Helper()

	m.Calls.FindData = append(
		m.Calls.FindData,
		FindDataArgs{tags, since, duration},
	)

	if m.Impl.FindData == nil {
		m.t.Fatal("FindData is not ready to be called")
	}
	return m.Impl.FindData(ctx, tags, since, duration)
}

func (m *mockKnitClient) GetPlans(ctx context.Context, planId string) (apiplans.Detail, error) {
	m.t.Helper()

	m.Calls.GetPlans = append(m.Calls.GetPlans, planId)
	if m.Impl.GetPlans == nil {
		m.t.Fatal("GetPlans is not ready to be called")
	}
	return m.Impl.GetPlans(ctx, planId)
}

func (m *mockKnitClient) FindPlan(
	ctx context.Context,
	active logic.Ternary,
	imageVer kdb.ImageIdentifier,
	inTags []apitags.Tag,
	outTags []apitags.Tag,
) ([]apiplans.Detail, error) {
	m.t.Helper()

	m.Calls.Findplan = append(m.Calls.Findplan, FindPlanArgs{active, imageVer, inTags, outTags})
	if m.Impl.FindPlan == nil {
		m.t.Fatal("FindPlan is not ready to be called")
	}
	return m.Impl.FindPlan(ctx, active, imageVer, inTags, outTags)
}

func (m *mockKnitClient) PutPlanForActivate(ctx context.Context, planId string, isActive bool) (apiplans.Detail, error) {
	m.t.Helper()

	m.Calls.PutPlanForActivate = append(m.Calls.PutPlanForActivate, planId)
	if m.Impl.PutPlanForActivate == nil {
		m.t.Fatal("PutPlanForActivate is not ready to be called")
	}
	return m.Impl.PutPlanForActivate(ctx, planId, isActive)
}

func (m *mockKnitClient) RegisterPlan(ctx context.Context, spec apiplans.PlanSpec) (apiplans.Detail, error) {
	m.t.Helper()

	m.Calls.RegisterPlan = append(m.Calls.RegisterPlan, spec)
	if m.Impl.RegisterPlan == nil {
		m.t.Fatal("RegisterPlan is not ready to be called")
	}
	return m.Impl.RegisterPlan(ctx, spec)
}

func (m *mockKnitClient) UpdateResources(ctx context.Context, runId string, resources apiplans.ResourceLimitChange) (apiplans.Detail, error) {
	m.t.Helper()

	m.Calls.UpdateResources = append(m.Calls.UpdateResources, struct {
		PlanId    string
		Resources apiplans.ResourceLimitChange
	}{PlanId: runId, Resources: resources})
	if m.Impl.UpdateResources == nil {
		m.t.Fatal("UpdateResources is not ready to be called")
	}
	return m.Impl.UpdateResources(ctx, runId, resources)
}

func (m *mockKnitClient) GetRun(ctx context.Context, runId string) (apirun.Detail, error) {
	m.t.Helper()

	m.Calls.GetRun = append(m.Calls.GetRun, runId)
	if m.Impl.GetRun == nil {
		m.t.Fatal("GetRun is not ready to be called")
	}
	return m.Impl.GetRun(ctx, runId)
}

func (m *mockKnitClient) GetRunLog(ctx context.Context, runId string, follow bool) (io.ReadCloser, error) {
	m.t.Helper()

	m.Calls.GetRunLog = append(m.Calls.GetRunLog, struct {
		RunId  string
		Follow bool
	}{
		RunId:  runId,
		Follow: follow,
	})
	if m.Impl.GetRunLog == nil {
		m.t.Fatal("GetRunLog is not ready to be called")
	}
	return m.Impl.GetRunLog(ctx, runId, follow)
}

func (m *mockKnitClient) FindRun(
	ctx context.Context,
	query rest.FindRunParameter,
) ([]apirun.Detail, error) {
	m.t.Helper()

	m.Calls.FindRun = append(
		m.Calls.FindRun,
		FindRunArgs{query.PlanId, query.KnitIdIn, query.KnitIdOut, query.Status, *query.Since, *query.Duration},
	)
	if m.Impl.FindRun == nil {
		m.t.Fatal("FindRun is not ready to be called")
	}
	return m.Impl.FindRun(ctx, query)
}

func (m *mockKnitClient) Abort(ctx context.Context, runId string) (apirun.Detail, error) {
	m.t.Helper()

	m.Calls.Abort = append(m.Calls.Abort, runId)
	if m.Impl.Abort == nil {
		m.t.Fatal("Abort is not ready to be called")
	}
	return m.Impl.Abort(ctx, runId)
}

func (m *mockKnitClient) Tearoff(ctx context.Context, runId string) (apirun.Detail, error) {
	m.t.Helper()

	m.Calls.Tearoff = append(m.Calls.Tearoff, runId)
	if m.Impl.Tearoff == nil {
		m.t.Fatal("Tearoff is not ready to be called")
	}
	return m.Impl.Tearoff(ctx, runId)
}

func (m *mockKnitClient) DeleteRun(ctx context.Context, runId string) error {
	m.t.Helper()

	m.Calls.DeleteRun = append(m.Calls.DeleteRun, runId)
	if m.Impl.DeleteRun == nil {
		m.t.Fatal("DeleteRun is not ready to be called")
	}
	return m.Impl.DeleteRun(ctx, runId)
}

func (m *mockKnitClient) Retry(ctx context.Context, runId string) error {
	m.t.Helper()

	m.Calls.Retry = append(m.Calls.Retry, runId)
	if m.Impl.Retry == nil {
		m.t.Fatal("Retry is not ready to be called")
	}
	return m.Impl.Retry(ctx, runId)
}
