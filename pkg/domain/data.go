package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/opst/knitfab/pkg/utils/cmp"
)

var ErrUnknownDataAgentMode = errors.New("unknown data agent mode")
var ErrDataInUse = errors.New("data is in use")
var ErrDataIsPurged = errors.New("data is purged")

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
	VolumeRef *string
	Tags      *TagSet
}

func (kdb *KnitDataBody) Equal(o *KnitDataBody) bool {
	if (kdb == nil) || (o == nil) {
		return (kdb == nil) && (o == nil)
	}

	if kdb.VolumeRef == nil || o.VolumeRef == nil {
		return kdb.VolumeRef == o.VolumeRef
	}
	if *kdb.VolumeRef != *o.VolumeRef {
		return false
	}

	return kdb.KnitId == o.KnitId &&
		kdb.Tags.Equal(o.Tags)
}

func (kdb KnitDataBody) String() string {
	volumeRef := "(nil)"
	if kdb.VolumeRef != nil {
		volumeRef = *kdb.VolumeRef
	}
	return fmt.Sprintf("KnitDataBody{KnitId: %s, VolumeRef: %s, Tags: %s}", kdb.KnitId, volumeRef, kdb.Tags)
}

func (kbody *KnitDataBody) Fulfilled() bool {
	return kbody != nil && kbody.KnitId != "" && kbody.VolumeRef != nil
}

type KnitData struct {
	KnitDataBody
	Upstream    DataSource
	Downstreams []DataSink
	NominatedBy []Nomination
}

func (d *KnitData) Equal(other *KnitData) bool {
	return d.KnitDataBody.Equal(&other.KnitDataBody) &&
		d.Upstream.Equal(&other.Upstream) &&
		cmp.SliceContentEqWith(d.Downstreams, other.Downstreams, func(a, b DataSink) bool { return a.Equal(&b) }) &&
		cmp.SliceContentEqWith(d.NominatedBy, other.NominatedBy, func(a, b Nomination) bool { return a.Equal(&b) })
}

func (d KnitData) String() string {
	return fmt.Sprintf(
		"KnitData{KnitDataBody: %s, Upstream: %+v, Downstreams: %v, NominatedBy: %v}",
		d.KnitDataBody, d.Upstream, d.Downstreams, d.NominatedBy,
	)
}

type DataSource struct {
	LogPoint   *LogPoint
	MountPoint *MountPoint
	RunBody    RunBody
}

func (ds *DataSource) Equal(other *DataSource) bool {
	if ds == nil || other == nil {
		return ds == other
	}
	return ds.LogPoint.Equal(other.LogPoint) &&
		ds.MountPoint.Equal(other.MountPoint) &&
		ds.RunBody.Equal(&other.RunBody)
}

type DataSink struct {
	MountPoint
	RunBody
}

func (ds *DataSink) Equal(other *DataSink) bool {
	if ds == nil || other == nil {
		return ds == other
	}
	return ds.MountPoint.Equal(&other.MountPoint) &&
		ds.RunBody.Equal(&other.RunBody)
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
