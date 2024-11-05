package mock

import (
	"context"
	"errors"
	"time"

	"github.com/opst/knitfab/pkg/domain"
	dbmock "github.com/opst/knitfab/pkg/domain/internal/db/mock"
	kdb "github.com/opst/knitfab/pkg/domain/run/db"
)

type RunInterface struct {
	Impl struct {
		NewPseudo        func(ctx context.Context, planName domain.PseudoPlanName, lifecyclSuspend time.Duration) (string, error)
		New              func(context.Context) ([]string, *domain.ProjectionTrigger, error)
		Finish           func(ctx context.Context, runId string) error
		Find             func(ctx context.Context, query domain.RunFindQuery) ([]string, error)
		Get              func(ctx context.Context, runId []string) (map[string]domain.Run, error)
		SetStatus        func(ctx context.Context, runId string, newStatus domain.KnitRunStatus) error
		SetExit          func(ctx context.Context, runId string, exit domain.RunExit) error
		PickAndSetStatus func(ctx context.Context, cursor domain.RunCursor, callback func(domain.Run) (domain.KnitRunStatus, error)) (domain.RunCursor, bool, error)
		Delete           func(ctx context.Context, runId string) error
		DeleteWorker     func(ctx context.Context, runId string) error
		Retry            func(ctx context.Context, runId string) error
	}

	Calls struct {
		NewPseudo dbmock.CallLog[struct {
			planName         domain.PseudoPlanName
			lifecycleSuspend time.Duration
		}]
		New       dbmock.CallLog[struct{}]
		Finish    dbmock.CallLog[string]
		Find      dbmock.CallLog[domain.RunFindQuery]
		Get       dbmock.CallLog[[]string]
		SetStatus dbmock.CallLog[struct {
			RunId     string
			NewStatus domain.KnitRunStatus
		}]
		SetExit dbmock.CallLog[struct {
			RunId string
			Exit  domain.RunExit
		}]
		PickAndSetStatus dbmock.CallLog[domain.RunCursor]
		Delete           dbmock.CallLog[string]
		DeleteWorker     dbmock.CallLog[string]
	}
}

func NewRunInterface() *RunInterface {
	return &RunInterface{}
}

var _ kdb.Interface = &RunInterface{}

func (m *RunInterface) NewPseudo(ctx context.Context, planName domain.PseudoPlanName, lifecycleSuspend time.Duration) (string, error) {
	m.Calls.NewPseudo = append(m.Calls.NewPseudo, struct {
		planName         domain.PseudoPlanName
		lifecycleSuspend time.Duration
	}{planName: planName, lifecycleSuspend: lifecycleSuspend})
	if m.Impl.NewPseudo != nil {
		return m.Impl.NewPseudo(ctx, planName, lifecycleSuspend)
	}

	panic(errors.New("it should no be called"))
}

func (m *RunInterface) New(ctx context.Context) ([]string, *domain.ProjectionTrigger, error) {
	m.Calls.New = append(m.Calls.New, struct{}{})
	if m.Impl.New != nil {
		return m.Impl.New(ctx)
	}

	panic(errors.New("it should not be called"))
}

func (m *RunInterface) Finish(ctx context.Context, runId string) error {
	m.Calls.Finish = append(m.Calls.Finish, runId)
	if m.Impl.Finish != nil {
		return m.Impl.Finish(ctx, runId)
	}
	panic(errors.New("it should no be called"))
}

func (m *RunInterface) SetStatus(ctx context.Context, runId string, newStatus domain.KnitRunStatus) error {
	m.Calls.SetStatus = append(m.Calls.SetStatus, struct {
		RunId     string
		NewStatus domain.KnitRunStatus
	}{
		RunId:     runId,
		NewStatus: newStatus,
	})
	if m.Impl.SetStatus != nil {
		return m.Impl.SetStatus(ctx, runId, newStatus)
	}

	panic(errors.New("it should no be called"))
}

func (m *RunInterface) PickAndSetStatus(
	ctx context.Context,
	cursor domain.RunCursor,
	callback func(domain.Run) (domain.KnitRunStatus, error),
) (domain.RunCursor, bool, error) {
	m.Calls.PickAndSetStatus = append(m.Calls.PickAndSetStatus, cursor)
	if m.Impl.PickAndSetStatus != nil {
		return m.Impl.PickAndSetStatus(ctx, cursor, callback)
	}

	panic(errors.New("it should no be called"))
}

func (m *RunInterface) SetExit(ctx context.Context, runId string, exit domain.RunExit) error {
	m.Calls.SetExit = append(m.Calls.SetExit, struct {
		RunId string
		Exit  domain.RunExit
	}{
		RunId: runId,
		Exit:  exit,
	})
	if m.Impl.SetExit != nil {
		return m.Impl.SetExit(ctx, runId, exit)
	}

	panic(errors.New("it should no be called"))

}

func (m *RunInterface) Find(ctx context.Context, query domain.RunFindQuery) ([]string, error) {
	m.Calls.Find = append(m.Calls.Find, query)
	if m.Impl.Find != nil {
		return m.Impl.Find(ctx, query)
	}

	panic(errors.New("it should no be called"))
}

func (m *RunInterface) Get(ctx context.Context, runId []string) (map[string]domain.Run, error) {
	m.Calls.Get = append(m.Calls.Get, runId)
	if m.Impl.Get != nil {
		return m.Impl.Get(ctx, runId)
	}

	panic(errors.New("it should no be called"))
}

func (m *RunInterface) Delete(ctx context.Context, runId string) error {
	m.Calls.Delete = append(m.Calls.Delete, runId)
	if m.Impl.Delete != nil {
		return m.Impl.Delete(ctx, runId)
	}

	panic(errors.New("it should no be called"))
}

func (m *RunInterface) DeleteWorker(ctx context.Context, runId string) error {
	m.Calls.DeleteWorker = append(m.Calls.DeleteWorker, runId)
	if m.Impl.DeleteWorker != nil {
		return m.Impl.DeleteWorker(ctx, runId)
	}

	panic(errors.New("it should no be called"))
}

func (m *RunInterface) Retry(ctx context.Context, runId string) error {
	if m.Impl.Retry != nil {
		return m.Impl.Retry(ctx, runId)
	}

	panic(errors.New("it should no be called"))
}
