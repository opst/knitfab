package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
)

var ErrUnknownDataAgentMode = errors.New("unknown data agent mode")

type DataAgentMode string

var (
	DataAgentRead  DataAgentMode = "read"
	DataAgentWrite DataAgentMode = "write"
)

func (d DataAgentMode) String() string {
	return string(d)
}

func AsDataAgentMode(s string) (DataAgentMode, error) {
	switch DataAgentMode(s) {
	case DataAgentRead:
		return DataAgentRead, nil
	case DataAgentWrite:
		return DataAgentWrite, nil
	default:
		return DataAgentMode(s), fmt.Errorf("%w: %s", ErrUnknownDataAgentMode, s)
	}
}

type DataInterface interface {
	// Retreive data identified by knitId
	//
	// args:
	//     - ctx: context
	//     - string: knitId
	//
	// returns:
	//     - map[string]KnitData : mapping from KnitId to KnitData
	//     - error
	//
	Get(context.Context, []string) (map[string]KnitData, error)

	// Retrieve KnitId of the data that contains all specified tags and range of updated time.
	//
	// args:
	//     - ctx: context
	//     - []Tag: specified tags
	//     - string: start of the time range
	//     - string: duration of the time range
	//
	// returns:
	//     - []string: Knitid of the data that meets the conditions
	//     - error
	//
	Find(context.Context, []Tag, string, string) ([]string, error)

	// update tags on data.
	//
	// Args
	//
	// - context.Context
	//
	// - string : knitId of target data
	//
	// - TagDelta : tags adding/removing from data
	//
	// Return
	//
	// - error
	UpdateTag(context.Context, string, TagDelta) error

	// Create and occupy a new DataAgent for the KnitData.
	//
	// Args
	//
	// - ctx context.Context
	//
	// - knitId string : knitId of target data
	//
	// - mode DataAgentMode: mode of the DataAgent
	//
	// - houskeepDelay: duration to wait before housekeeping.
	// This duration can be truncated at some resolution when stored into database.
	//
	// You should start DataAgent on kubernetes by the duration,
	// or the DataAgent record is removed by housekeeping.
	//
	// Return
	//
	// - DataAgent : DataAgent for the KnitData
	//
	// - error
	NewAgent(ctx context.Context, knitId string, mode DataAgentMode, housekeepDelay time.Duration) (DataAgent, error)

	// Remove a DataAgent from the Data Agent regisrty.
	//
	// Args
	//
	// - ctx context.Context
	//
	// - name string : name of the DataAgent
	//
	// Return
	//
	// - error
	RemoveAgent(ctx context.Context, name string) error

	// Pick and remove a DataAgent from the Data Agent regisrty.
	//
	// This method picks DataAgent exceeding "housekeep after" time limit,
	// and remove it if the function f returns true without error.
	//
	// Args
	//
	// - ctx context.Context
	//
	// - curosr DataAgentCursor : cursor where pick DataAgent last time
	//
	// - f func(DataAgent) (removeOk bool, err error) : function to determine whether to remove the DataAgent
	//
	// Return
	//
	// - DataAgentCursor : cursor where pick DataAgent this time.
	// If no DataAgent is picked, the cursor is not changed.
	//
	// - error
	PickAndRemoveAgent(ctx context.Context, curosr DataAgentCursor, f func(DataAgent) (removeOk bool, err error)) (DataAgentCursor, error)

	// Get names of DataAgents for the KnitData.
	//
	// Args
	//
	// - ctx context.Context
	//
	// - knitId string : knitId of target data
	//
	// - modes []DataAgentMode : modes of the DataAgents to be queried
	//
	// Return
	//
	// - []string : names of DataAgents with the modes
	//
	GetAgentName(ctx context.Context, knitId string, modes []DataAgentMode) ([]string, error)
}

type TagDelta struct {
	Remove []Tag
	Add    []Tag
}

func (td *TagDelta) Equal(other *TagDelta) bool {
	return cmp.SliceContentEqWith(td.Remove, other.Remove, func(a, b Tag) bool { return a.Equal(&b) }) &&
		cmp.SliceContentEqWith(td.Add, other.Add, func(a, b Tag) bool { return a.Equal(&b) })
}

type KnitDataBody struct {
	KnitId    string
	VolumeRef string
	Tags      *TagSet
}

func (kbd *KnitDataBody) Equal(o *KnitDataBody) bool {
	if (kbd == nil) || (o == nil) {
		return (kbd == nil) && (o == nil)
	}

	return kbd.KnitId == o.KnitId &&
		kbd.VolumeRef == o.VolumeRef &&
		kbd.Tags.Equal(o.Tags)
}

func (kbody *KnitDataBody) Fulfilled() bool {
	return kbody != nil && kbody.KnitId != "" && kbody.VolumeRef != ""
}

type KnitData struct {
	KnitDataBody
	Upsteram    Dependency
	Downstreams []Dependency
	NominatedBy []Nomination
}

func (d *KnitData) Equal(other *KnitData) bool {
	return d.KnitDataBody.Equal(&other.KnitDataBody) &&
		d.Tags.Equal(other.Tags)
}

type Dependency struct {
	MountPoint
	RunBody
}

type DataAgent struct {
	Name         string
	Mode         DataAgentMode
	KnitDataBody KnitDataBody
}

func (da *DataAgent) Equal(other *DataAgent) bool {
	return da.Name == other.Name &&
		da.Mode == other.Mode &&
		da.KnitDataBody.Equal(&other.KnitDataBody)
}

type DataAgentCursor struct {
	// the name of Data Agent which is picked last time
	Head string

	// the interval to pick same Data Agent
	Debounce time.Duration
}
