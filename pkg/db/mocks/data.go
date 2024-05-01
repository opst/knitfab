package mocks

import (
	"context"
	"errors"
	"time"

	kdb "github.com/opst/knitfab/pkg/db"
)

type DataInterface struct {
	Impl struct {
		Get                      func(context.Context, []string) (map[string]kdb.KnitData, error)
		GetKnitIdByDataFindQuery func(context.Context, []kdb.Tag, string, string) ([]string, error)
		UpdateTag                func(context.Context, string, kdb.TagDelta) error
		NewAgent                 func(context.Context, string, kdb.DataAgentMode, time.Duration) (kdb.DataAgent, error)
		RemoveAgent              func(context.Context, string) error
		PickAndRemoveAgent       func(context.Context, kdb.DataAgentCursor, func(kdb.DataAgent) (bool, error)) (kdb.DataAgentCursor, error)
		GetAgentName             func(context.Context, string, []kdb.DataAgentMode) ([]string, error)
	}
	Calls struct {
		Get                      CallLog[struct{ KnitId []string }]
		GetKnitIdByDataFindQuery CallLog[struct {
			Tags     []kdb.Tag
			Since    string
			Duration string
		}]
		Updatetag CallLog[struct {
			KnitId string
			Delta  kdb.TagDelta
		}]
		NewAgent CallLog[struct {
			KnitId                string
			Mode                  kdb.DataAgentMode
			LifecycleSuspendUntil time.Duration
		}]
		RemoveAgent        CallLog[struct{ Name string }]
		PickAndRemoveAgent CallLog[struct {
			Cursor kdb.DataAgentCursor
		}]
		GetAgentName CallLog[struct {
			KnitId string
			Modes  []kdb.DataAgentMode
		}]
	}
}

func NewDataInterface() *DataInterface {
	return &DataInterface{}
}

var _ kdb.DataInterface = &DataInterface{}

func (di *DataInterface) Get(ctx context.Context, knitId []string) (map[string]kdb.KnitData, error) {
	di.Calls.Get = append(di.Calls.Get, struct{ KnitId []string }{KnitId: knitId})
	if di.Impl.Get != nil {
		return di.Impl.Get(ctx, knitId)
	}
	panic(errors.New("it should no be called"))
}

func (di *DataInterface) GetKnitIdByDataFindQuery(ctx context.Context, tags []kdb.Tag, since string, duration string) ([]string, error) {
	di.Calls.GetKnitIdByDataFindQuery = append(di.Calls.GetKnitIdByDataFindQuery, struct {
		Tags     []kdb.Tag
		Since    string
		Duration string
	}{
		Tags: tags, Since: since, Duration: duration,
	})
	if di.Impl.GetKnitIdByDataFindQuery != nil {
		return di.Impl.GetKnitIdByDataFindQuery(ctx, tags, since, duration)
	}
	panic(errors.New("it should no be called"))
}

func (di *DataInterface) UpdateTag(ctx context.Context, knitId string, delta kdb.TagDelta) error {
	di.Calls.Updatetag = append(di.Calls.Updatetag, struct {
		KnitId string
		Delta  kdb.TagDelta
	}{
		KnitId: knitId, Delta: delta,
	})
	if di.Impl.UpdateTag != nil {
		return di.Impl.UpdateTag(ctx, knitId, delta)
	}
	panic(errors.New("it should not be called"))
}

func (di *DataInterface) NewAgent(ctx context.Context, knitId string, mode kdb.DataAgentMode, lifecycleSuspend time.Duration) (kdb.DataAgent, error) {
	di.Calls.NewAgent = append(di.Calls.NewAgent, struct {
		KnitId                string
		Mode                  kdb.DataAgentMode
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

func (di *DataInterface) PickAndRemoveAgent(ctx context.Context, cursor kdb.DataAgentCursor, f func(kdb.DataAgent) (bool, error)) (kdb.DataAgentCursor, error) {
	di.Calls.PickAndRemoveAgent = append(di.Calls.PickAndRemoveAgent, struct {
		Cursor kdb.DataAgentCursor
	}{
		Cursor: cursor,
	})
	if di.Impl.PickAndRemoveAgent != nil {
		return di.Impl.PickAndRemoveAgent(ctx, cursor, f)
	}
	panic(errors.New("it should not be called"))
}

func (di *DataInterface) GetAgentName(ctx context.Context, knitId string, modes []kdb.DataAgentMode) ([]string, error) {
	di.Calls.GetAgentName = append(di.Calls.GetAgentName, struct {
		KnitId string
		Modes  []kdb.DataAgentMode
	}{
		KnitId: knitId, Modes: modes,
	})
	if di.Impl.GetAgentName != nil {
		return di.Impl.GetAgentName(ctx, knitId, modes)
	}
	panic(errors.New("it should not be called"))
}
