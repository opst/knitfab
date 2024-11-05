package mocks

import (
	"context"
	"errors"
	"time"

	"github.com/opst/knitfab/pkg/domain"
	kdbdata "github.com/opst/knitfab/pkg/domain/data/db"
	dbmock "github.com/opst/knitfab/pkg/domain/internal/db/mock"
)

type DataInterface struct {
	Impl struct {
		Get                func(context.Context, []string) (map[string]domain.KnitData, error)
		Find               func(context.Context, []domain.Tag, *time.Time, *time.Time) ([]string, error)
		UpdateTag          func(context.Context, string, domain.TagDelta) error
		NewAgent           func(context.Context, string, domain.DataAgentMode, time.Duration) (domain.DataAgent, error)
		RemoveAgent        func(context.Context, string) error
		PickAndRemoveAgent func(context.Context, domain.DataAgentCursor, func(domain.DataAgent) (bool, error)) (domain.DataAgentCursor, error)
		GetAgentName       func(context.Context, string, []domain.DataAgentMode) ([]string, error)
	}
	Calls struct {
		Get  dbmock.CallLog[struct{ KnitId []string }]
		Find dbmock.CallLog[struct {
			Tags  []domain.Tag
			Since *time.Time
			Until *time.Time
		}]
		Updatetag dbmock.CallLog[struct {
			KnitId string
			Delta  domain.TagDelta
		}]
		NewAgent dbmock.CallLog[struct {
			KnitId                string
			Mode                  domain.DataAgentMode
			LifecycleSuspendUntil time.Duration
		}]
		RemoveAgent        dbmock.CallLog[struct{ Name string }]
		PickAndRemoveAgent dbmock.CallLog[struct {
			Cursor domain.DataAgentCursor
		}]
		GetAgentName dbmock.CallLog[struct {
			KnitId string
			Modes  []domain.DataAgentMode
		}]
	}
}

func NewDataInterface() *DataInterface {
	return &DataInterface{}
}

var _ kdbdata.DataInterface = &DataInterface{}

func (di *DataInterface) Get(ctx context.Context, knitId []string) (map[string]domain.KnitData, error) {
	di.Calls.Get = append(di.Calls.Get, struct{ KnitId []string }{KnitId: knitId})
	if di.Impl.Get != nil {
		return di.Impl.Get(ctx, knitId)
	}
	panic(errors.New("it should no be called"))
}

func (di *DataInterface) Find(ctx context.Context, tags []domain.Tag, since *time.Time, until *time.Time) ([]string, error) {
	di.Calls.Find = append(di.Calls.Find, struct {
		Tags  []domain.Tag
		Since *time.Time
		Until *time.Time
	}{
		Tags: tags, Since: since, Until: until,
	})
	if di.Impl.Find != nil {
		return di.Impl.Find(ctx, tags, since, until)
	}
	panic(errors.New("it should no be called"))
}

func (di *DataInterface) UpdateTag(ctx context.Context, knitId string, delta domain.TagDelta) error {
	di.Calls.Updatetag = append(di.Calls.Updatetag, struct {
		KnitId string
		Delta  domain.TagDelta
	}{
		KnitId: knitId, Delta: delta,
	})
	if di.Impl.UpdateTag != nil {
		return di.Impl.UpdateTag(ctx, knitId, delta)
	}
	panic(errors.New("it should not be called"))
}

func (di *DataInterface) NewAgent(ctx context.Context, knitId string, mode domain.DataAgentMode, lifecycleSuspend time.Duration) (domain.DataAgent, error) {
	di.Calls.NewAgent = append(di.Calls.NewAgent, struct {
		KnitId                string
		Mode                  domain.DataAgentMode
		LifecycleSuspendUntil time.Duration
	}{
		KnitId: knitId, Mode: mode, LifecycleSuspendUntil: lifecycleSuspend,
	})
	if di.Impl.NewAgent != nil {
		return di.Impl.NewAgent(ctx, knitId, mode, lifecycleSuspend)
	}
	panic(errors.New("it should not be called"))
}

func (di *DataInterface) RemoveAgent(ctx context.Context, name string) error {
	di.Calls.RemoveAgent = append(di.Calls.RemoveAgent, struct{ Name string }{Name: name})
	if di.Impl.NewAgent != nil {
		return di.Impl.RemoveAgent(ctx, name)
	}
	panic(errors.New("it should not be called"))
}

func (di *DataInterface) PickAndRemoveAgent(ctx context.Context, cursor domain.DataAgentCursor, f func(domain.DataAgent) (bool, error)) (domain.DataAgentCursor, error) {
	di.Calls.PickAndRemoveAgent = append(di.Calls.PickAndRemoveAgent, struct {
		Cursor domain.DataAgentCursor
	}{
		Cursor: cursor,
	})
	if di.Impl.PickAndRemoveAgent != nil {
		return di.Impl.PickAndRemoveAgent(ctx, cursor, f)
	}
	panic(errors.New("it should not be called"))
}

func (di *DataInterface) GetAgentName(ctx context.Context, knitId string, modes []domain.DataAgentMode) ([]string, error) {
	di.Calls.GetAgentName = append(di.Calls.GetAgentName, struct {
		KnitId string
		Modes  []domain.DataAgentMode
	}{
		KnitId: knitId, Modes: modes,
	})
	if di.Impl.GetAgentName != nil {
		return di.Impl.GetAgentName(ctx, knitId, modes)
	}
	panic(errors.New("it should not be called"))
}
