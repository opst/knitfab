package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/opst/knitfab/pkg/utils/cmp"
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

// TagDelta represents the intent updating tags on data.
//
// It contains tags to be added and removed.
//
// If the same key is in both Add and Remove or RemoveKey, Remove and RemoveKey are applied first.
type TagDelta struct {
	Remove    []Tag
	RemoveKey []string
	Add       []Tag
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
