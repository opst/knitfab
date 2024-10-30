package tables

import (
	"time"

	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/db/postgres/marshal"
	"github.com/opst/knitfab/pkg/utils/cmp"
)

// golang representation of record of PostgresSQL tables
//
// some tables are omitted, because of its simpleness.

type TagKey struct {
	Id  int
	Key string
}
type TagValue struct {
	Id    int
	KeyId int
	Value string
}

type Data struct {
	KnitId    string
	VolumeRef string
	PlanId    string
	RunId     string
	OutputId  int
}

func (a *Data) Equal(b *Data) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

type DataTimeStamp struct {
	KnitId    string
	Timestamp time.Time
}

func (a *DataTimeStamp) Equal(b *DataTimeStamp) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	return a.KnitId == b.KnitId &&
		a.Timestamp.Equal(b.Timestamp)
}

type Nomination struct {
	KnitId  string
	InputId int
	Updated bool
}

func (a *Nomination) Equal(b *Nomination) bool {
	return a.KnitId == b.KnitId &&
		a.InputId == b.InputId &&
		a.Updated == b.Updated
}

type Plan struct {
	PlanId string
	Active bool
	Hash   string
}
type PlanResource struct {
	PlanId string
	Type   string
	Value  marshal.ResourceQuantity
}
type PlanImage struct {
	PlanId  string
	Image   string
	Version string
}

type PlanEntrypoint struct {
	PlanId     string
	Entrypoint []string
}

func (pent PlanEntrypoint) Equal(other PlanEntrypoint) bool {
	return pent.PlanId == other.PlanId && cmp.SliceEq(pent.Entrypoint, other.Entrypoint)
}

type PlanArgs struct {
	PlanId string
	Args   []string
}

func (pargs PlanArgs) Equal(other PlanArgs) bool {
	return pargs.PlanId == other.PlanId && cmp.SliceEq(pargs.Args, other.Args)
}

type PlanOnNode struct {
	PlanId string
	Mode   kdb.OnNodeMode
	Key    string
	Value  string
}
type PlanPseudo struct {
	PlanId string
	Name   string
}
type Input struct {
	InputId int
	PlanId  string
	Path    string
}
type Output struct {
	OutputId int
	PlanId   string
	Path     string
}
type Log struct {
	OutputId int
	PlanId   int
}
type Annotation struct {
	PlanId string
	Key    string
	Value  string
}
type ServiceAccount struct {
	PlanId         string
	ServiceAccount string
}

type Run struct {
	RunId                 string
	PlanId                string
	Status                kdb.KnitRunStatus
	LifecycleSuspendUntil time.Time
	UpdatedAt             time.Time
}

func (a *Run) Equal(b *Run) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	return a.RunId == b.RunId && a.Equiv(b)
}

func (a *Run) Equiv(b *Run) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.PlanId == b.PlanId &&
		a.Status == b.Status &&
		a.LifecycleSuspendUntil.Equal(b.LifecycleSuspendUntil) &&
		a.UpdatedAt.Equal(b.UpdatedAt)
}

type RunExit struct {
	RunId    string
	ExitCode uint8
	Message  string
}

type Worker struct {
	RunId string
	Name  string
}

type Assign struct {
	RunId   string
	InputId int
	PlanId  string
	KnitId  string
}

func (a *Assign) Equal(b *Assign) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	return *a == *b
}

type Garbage struct {
	KnitId    string
	VolumeRef string
}

func (a *Garbage) Equal(b *Garbage) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	return a.KnitId == b.KnitId &&
		a.VolumeRef == b.VolumeRef
}

type DataAgent struct {
	Name                  string
	Mode                  string
	KnitId                string
	LifecycleSuspendUntil time.Time
}

func (da *DataAgent) Equal(b *DataAgent) bool {
	if da == nil || b == nil {
		return da == nil && b == nil
	}
	return da.Name == b.Name &&
		da.Mode == b.Mode &&
		da.KnitId == b.KnitId &&
		da.LifecycleSuspendUntil.Equal(b.LifecycleSuspendUntil)
}
