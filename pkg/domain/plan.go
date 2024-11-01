package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/slices"
	kstr "github.com/opst/knitfab/pkg/utils/strings"
	"k8s.io/apimachinery/pkg/api/resource"
)

// System defined pseudo plan name
type PseudoPlanName string

func (p PseudoPlanName) String() string {
	return string(p)
}

const (
	// File upload plan
	Uploaded PseudoPlanName = "knit#uploaded"
	Imported PseudoPlanName = "knit#imported"
)

type Annotation struct {
	Key   string
	Value string
}

// AnnotationDelta is a struct that represents the changesets of annotations.
//
// Remove is applied first, and then Add is applied.
type AnnotationDelta struct {
	// Add is a list of annotations to be added.
	//
	// If Plan already has an annotation in Add, it will be ignored.
	Add []Annotation

	// Remove is a list of annotations to be removed.
	//
	// If Plan does not have an annotation in Remove, it will be ignored.
	Remove []Annotation

	// RemoveKey is a list of annotation keys to be removed.
	RemoveKey []string
}

// Main body of plan, describes "what it is".
//
// Use Plan if you need PlanBody & relationship with others.
type PlanBody struct {
	// identifier of the plan
	PlanId string

	// plan hash.
	//
	// other plan having another Hash, that is different from this.
	// Note that same hash DOES NOT guarantee equivarency of plans (hash can corride).
	Hash string

	// activeness of plan.
	//
	// If true, knit can make runs based on this plan.
	//
	// For pseudo-plan, this value is true always.
	Active bool

	// If this property is not nil, this plan has image.
	Image *ImageIdentifier

	// Entrypoint is the entrypoint of container image
	Entrypoint []string

	// Args is the arguments for the entrypoints
	Args []string

	// If this property is not nil, this plan is pseudo.
	Pseudo *PseudoPlanDetail

	// resource requirements for this plan.
	//
	// key is resource name (cpu, memory, ...), value is quantity.
	// Key and Value are follow k8s resource requirements specs.
	Resources map[string]resource.Quantity

	OnNode []OnNode

	// ServiceAccount is the name of the service agent that Workers of this Plan should use.
	ServiceAccount string

	// Annotations is a list of annotations for this Plan.
	Annotations []Annotation
}

// true iff pb and other are equal, means they represent same entity
func (pb *PlanBody) Equal(other *PlanBody) bool {
	return pb.PlanId == other.PlanId && pb.Equiv(other)
}

// true iff pb and other are equiverent, means they represent same except PlanId
func (pb *PlanBody) Equiv(other *PlanBody) bool {
	return pb.Active == other.Active &&
		pb.Hash == other.Hash &&
		cmp.SliceContentEq(pb.Entrypoint, other.Entrypoint) &&
		cmp.SliceContentEq(pb.Args, other.Args) &&
		pb.Image.Equal(other.Image) &&
		pb.Pseudo.Equal(other.Pseudo) &&
		cmp.SliceContentEq(pb.OnNode, other.OnNode) &&
		cmp.MapEqWith(pb.Resources, other.Resources, resource.Quantity.Equal) &&
		pb.ServiceAccount == other.ServiceAccount &&
		cmp.SliceContentEq(pb.Annotations, other.Annotations)
}

// how to schedule the run of this plan
type OnNodeMode string

func (on OnNodeMode) String() string {
	return string(on)
}

const (
	// this plan can run on node with label, but not guaranteed
	//
	// When creating a new run worker from plan with this mode,
	// the worker (as k8s pod) has
	//
	// - torelance: key=label:NoSchedule
	//
	MayOnNode OnNodeMode = "may"

	// this plan run on node with label as possible as it can
	//
	// When creating a new run worker from plan with this mode,
	// the worker (as k8s pod) has
	//
	// - torelance: key=label:NoSchedule
	//
	// - torelance: key=label:PreferNoSchedule
	//
	// - node affinity/preferredDuringSchedulingIgnoredDuringExecution: key:value
	//
	PreferOnNode OnNodeMode = "prefer"

	// this plan can not run on node without label
	//
	// When creating a new run worker from plan with this mode,
	// the worker (as k8s pod) has
	//
	// - torelance: key=label:NoSchedule
	//
	// - torelance: key=label:PreferNoSchedule
	//
	// - node affinity/requiredDuringSchedulingIgnoredDuringExecution: key:value
	//
	MustOnNode OnNodeMode = "must"
)

// kubernetes label
type OnNode struct {
	Mode OnNodeMode

	// key of label and taint in kubernetes
	Key string

	// value of label and taint in kubernetes
	Value string
}

func (o OnNode) String() string {
	return fmt.Sprintf("%s=%s:%s", o.Key, o.Value, o.Mode)
}

type Plan struct {
	PlanBody
	Inputs  []MountPoint
	Outputs []MountPoint
	Log     *LogPoint
}

func (p *Plan) String() string {

	inputs := slices.Map(p.Inputs, func(mp MountPoint) string { return mp.String() })
	outputs := slices.Map(p.Outputs, func(mp MountPoint) string { return mp.String() })

	return fmt.Sprintf(
		"Plan{PlanBody:%+v Inputs:{%+v} Outputs:{%+v} Log:%+v}",
		p.PlanBody,
		strings.Join(inputs, ", "),
		strings.Join(outputs, ", "),
		p.Log,
	)
}

// true iff p and other are equal, means they represent same entity.
func (p *Plan) Equal(other *Plan) bool {
	return p.PlanBody.Equal(&other.PlanBody) &&
		cmp.SliceContentEqWith(
			slices.RefOf(p.Inputs),
			slices.RefOf(other.Inputs),
			(*MountPoint).Equal,
		) &&
		cmp.SliceContentEqWith(
			slices.RefOf(p.Outputs),
			slices.RefOf(other.Outputs),
			(*MountPoint).Equal,
		) &&
		p.Log.Equal(other.Log)
}

// true iff p and other are equivarent, means they represent same except PlanId.
func (p *Plan) Equiv(other *Plan) bool {
	eqInputs := cmp.SliceContentEqWith(
		slices.RefOf(p.Inputs),
		slices.RefOf(other.Inputs),
		(*MountPoint).Equiv,
	)
	eqOutputs := cmp.SliceContentEqWith(
		slices.RefOf(p.Outputs),
		slices.RefOf(other.Outputs),
		(*MountPoint).Equiv,
	)
	return p.PlanBody.Equiv(&other.PlanBody) &&
		eqInputs &&
		eqOutputs &&
		p.Log.Equiv(other.Log)
}

// container image identifier
type ImageIdentifier struct {
	Image   string
	Version string
}

func (ii *ImageIdentifier) Fulfilled() bool {
	return ii != nil &&
		ii.Image != "" &&
		ii.Version != ""
}

func (ii ImageIdentifier) String() string {
	return fmt.Sprintf("%s:%s", ii.Image, ii.Version)
}

func (ii *ImageIdentifier) Equal(other *ImageIdentifier) bool {
	if (ii == nil) || (other == nil) {
		return (ii == nil) && (other == nil)
	}

	return ii.Image == other.Image && ii.Version == other.Version
}

type PseudoPlanDetail struct {
	Name PseudoPlanName
}

func (ppd *PseudoPlanDetail) Format(s fmt.State, r rune) {
	fmt.Fprintf(s, `Pseudo{Name:"%s"}`, ppd.Name)
}

func (ppd *PseudoPlanDetail) Equal(other *PseudoPlanDetail) bool {
	if (ppd == nil) || (other == nil) {
		return (ppd == nil) && (other == nil)
	}
	return ppd.Name == other.Name
}

// declearation of input/output of plan.
type MountPoint struct {
	// id in DB.
	Id int

	// location in filesystem
	Path string

	// tags set on this mountpoint
	Tags *TagSet
}

func (mp *MountPoint) String() string {
	return fmt.Sprintf("MountPoint{Id:%d Path:%s Tags:%+v}", mp.Id, mp.Path, mp.Tags.String())
}

// true if m and other are equal, means they are represents same entity.
func (m *MountPoint) Equal(other *MountPoint) bool {
	return m.Id == other.Id && m.Equiv(other)
}

// true if m and other are equiverent, means they represents same thing except MountPointId.
func (m *MountPoint) Equiv(other *MountPoint) bool {
	return m.Path == other.Path &&
		cmp.SliceContentEqWith(
			slices.RefOf(m.Tags.Slice()),
			slices.RefOf(other.Tags.Slice()),
			(*Tag).Equal,
		)
}

type PlanParam struct {
	Image          string
	Entrypoint     []string
	Args           []string
	Version        string
	Active         bool
	Inputs         []MountPointParam
	Outputs        []MountPointParam
	Log            *LogParam
	OnNode         []OnNode
	Resources      map[string]resource.Quantity
	ServiceAccount string
	Annotations    []Annotation
}

// validate parameters and create PlanSpec.
//
// return:
//
// - *PlanSpec: validated plan spec
//
// - error: validation error. if error is not nil, *PlanSpec is nil.
func (pp PlanParam) Validate() (*PlanSpec, error) {
	entrypoint := make([]string, len(pp.Entrypoint))
	copy(entrypoint, pp.Entrypoint)
	args := make([]string, len(pp.Args))
	copy(args, pp.Args)
	inputs := make([]MountPointParam, len(pp.Inputs))
	copy(inputs, pp.Inputs)
	outputs := make([]MountPointParam, len(pp.Outputs))
	copy(outputs, pp.Outputs)
	onNode := make([]OnNode, len(pp.OnNode))
	copy(onNode, pp.OnNode)
	annotations := make([]Annotation, len(pp.Annotations))
	copy(annotations, pp.Annotations)
	resources := make(map[string]resource.Quantity, len(pp.Resources))
	for k, v := range pp.Resources {
		resources[k] = v
	}

	// take snapshot to guard from changing pp.mountpoint after return this method.

	ret := &PlanSpec{
		image:          pp.Image,
		entrypoint:     entrypoint,
		args:           args,
		version:        pp.Version,
		active:         pp.Active,
		inputs:         inputs,
		outputs:        outputs,
		onNode:         onNode,
		log:            pp.Log,
		resources:      resources,
		serviceaccount: pp.ServiceAccount,
		annotations:    annotations,
	}
	if err := ret.Validate(); err != nil {
		return nil, err
	}
	return ret, nil
}

// Let you create PlanSpec as validated regardless its property.
//
// Use this only for testing.
func BypassValidation(hash string, err error, pp PlanParam) *PlanSpec {
	inputs := make([]MountPointParam, len(pp.Inputs))
	copy(inputs, pp.Inputs)
	outputs := make([]MountPointParam, len(pp.Outputs))
	copy(outputs, pp.Outputs)
	onNode := make([]OnNode, len(pp.OnNode))
	copy(onNode, pp.OnNode)
	annotations := make([]Annotation, len(pp.Annotations))
	copy(annotations, pp.Annotations)
	resources := make(map[string]resource.Quantity, len(pp.Resources))
	for k, v := range pp.Resources {
		resources[k] = v
	}
	// take snapshot to guard from changing pp.mountpoint after return this method.

	ps := &PlanSpec{
		image:      pp.Image,
		entrypoint: pp.Entrypoint,
		args:       pp.Args,
		version:    pp.Version,
		active:     pp.Active,
		inputs:     inputs,
		outputs:    outputs,
		log:        pp.Log,
		onNode:     onNode,
		hash:       hash,
		resources:  resources,

		serviceaccount: pp.ServiceAccount,
		annotations:    annotations,

		validated: true,
		vErr:      err,
	}
	return ps
}

// Plan to be created.
//
// PlanSpec represents a plan with image, not pseudo plan.
// Plans with image are only created by users. Pseudo plans are knit built-in.
//
// to instantiate this struct with validation, use the factory function `NewPlanSpec`.
type PlanSpec struct {
	image      string
	version    string
	active     bool
	entrypoint []string
	args       []string
	inputs     []MountPointParam
	outputs    []MountPointParam
	log        *LogParam
	onNode     []OnNode
	hash       string

	serviceaccount string
	annotations    []Annotation

	resources map[string]resource.Quantity

	validated bool
	vErr      error
}

func (ps *PlanSpec) Image() string {
	return ps.image
}

func (ps *PlanSpec) Entrypoint() []string {
	return ps.entrypoint
}

func (ps *PlanSpec) Args() []string {
	return ps.args
}

func (ps *PlanSpec) Version() string {
	return ps.version
}

func (ps *PlanSpec) Active() bool {
	return ps.active
}

func (ps *PlanSpec) Inputs() []MountPointParam {
	return ps.inputs
}

func (ps *PlanSpec) Outputs() []MountPointParam {
	return ps.outputs
}

func (ps *PlanSpec) Log() *LogParam {
	return ps.log
}

func (ps *PlanSpec) OnNode() []OnNode {
	return ps.onNode
}

func (ps *PlanSpec) Resources() map[string]resource.Quantity {
	return ps.resources
}

func (ps *PlanSpec) Annotations() []Annotation {
	return ps.annotations
}

func (ps *PlanSpec) ServiceAccount() string {
	return ps.serviceaccount
}

func (ps *PlanSpec) Equal(other *PlanSpec) bool {
	return ps.image == other.image &&
		ps.version == other.version &&
		ps.active == other.active &&
		cmp.SliceContentEqWith(
			ps.inputs, other.inputs, MountPointParam.Equal,
		) &&
		cmp.SliceContentEqWith(
			ps.outputs, other.outputs, MountPointParam.Equal,
		) &&
		ps.log.Equal(other.log) &&
		cmp.SliceContentEq(ps.onNode, other.onNode) &&
		cmp.MapEqWith(ps.resources, other.resources, resource.Quantity.Equal) &&
		ps.Hash() == other.Hash() &&
		ps.serviceaccount == other.serviceaccount &&
		cmp.SliceContentEq(ps.annotations, other.annotations)
}

// true, iff this PlanSpec is equiverent with `plan`. otherwise false.
func (ps *PlanSpec) EquivPlan(plan *Plan) bool {
	if ps.hash != plan.Hash {
		return false
	}

	planImage := plan.Image
	if planImage == nil || planImage.Image != ps.image || planImage.Version != ps.version {
		return false
	}

	if !ps.log.EquivLogPoint(plan.Log) {
		return false
	}

	if !cmp.SliceContentEqWith(
		ps.inputs, slices.RefOf(plan.Inputs), MountPointParam.EquivMountPoint,
	) {
		return false
	}
	if !cmp.SliceContentEqWith(
		ps.outputs, slices.RefOf(plan.Outputs), MountPointParam.EquivMountPoint,
	) {
		return false
	}

	if !cmp.SliceContentEq(ps.entrypoint, plan.Entrypoint) {
		return false
	}

	if !cmp.SliceContentEq(ps.args, plan.Args) {
		return false
	}

	return true
}

// run validation if not yet.
//
// It do validate upto once.
// If this is called twice or more, it returns value of the first time.
//
// When it returns nil, it means "this is valid" and implies also mountpoints are valid.
//
// # Return
//
// validation error when this is invalid. otherwise nil.
// If this has run validation at least once, it just returns the last result.
// errors can be returned are below:
//
// - `NewErrPlanNamelessImage`: Image or version of image is empty.
//
// - `NewErrOverlappedMountPoints`: some mountpoint is subdirectory of another
//
// - `NewErrTooManyLogs`: there are two or more log mountpoints in one plan
//
// - errors comes from `MountPointSpec.Validate()`
//
// - errors comes from `hash.Hash.Write`
func (ps *PlanSpec) Validate() error {
	if ps.vErr != nil {
		return ps.vErr
	}
	if ps.validated {
		return nil
	}

	return ps.validate()
}

var reLabelKey = regexp.MustCompile(`^([a-zA-Z0-9]([-a-zA-Z0-9]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([-a-zA-Z0-9]{0,61}[a-zA-Z0-9])?)*/)?[a-zA-Z0-9]([-a-zA-Z0-9]{0,61}[a-zA-Z0-9])?$`)
var reLabelVal = regexp.MustCompile(`^[a-zA-Z0-9]([-._a-zA-Z0-9]{0,61}[a-zA-Z0-9])?$`)

// run validation whether it have or have not been validated.
//
// # Return
//
// validation error when this is invalid. otherwise nil.
func (ps *PlanSpec) validate() error {
	record := func(err error) error {
		ps.vErr = err
		ps.validated = err == nil
		return err
	}

	ps.onNode = slices.Sorted(
		ps.onNode,
		func(a, b OnNode) bool {
			if a.Mode != b.Mode {
				return a.Mode < b.Mode
			}
			if a.Key != b.Key {
				return a.Key < b.Key
			}
			return a.Value < b.Value
		},
	)

	for _, on := range ps.onNode {
		// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
		if on.Key == "" {
			return record(fmt.Errorf("%w: key is empty", ErrInvalidOnNodeKey))
		}
		if !reLabelKey.MatchString(on.Key) {
			return record(fmt.Errorf(
				"%w: bad pattern: %s", ErrInvalidOnNodeKey, on.Key,
			))
		}
		if prefix, _, ok := strings.Cut(on.Key, "/"); ok && 253 < len(prefix) {
			return record(fmt.Errorf(
				"%w: too long (> 253 chars) prefix: %s",
				ErrInvalidOnNodeKey, on.Key,
			))
		}

		if on.Value == "" {
			return record(fmt.Errorf("%w: value is empty", ErrInvalidOnNodeValue))
		}
		if !reLabelVal.MatchString(on.Value) {
			return record(fmt.Errorf(
				"%w: bad pattern: %s", ErrInvalidOnNodeValue, on.Value,
			))
		}
	}

	if ps.image == "" || ps.version == "" {
		return record(NewErrPlanNamelessImage(ps.image + ":" + ps.version))
	}

	inputs := slices.Sorted(
		ps.inputs,
		func(a, b MountPointParam) bool { return a.Path < b.Path },
	)
	outputs := slices.Sorted(
		ps.outputs,
		func(a, b MountPointParam) bool { return a.Path < b.Path },
	)

	if len(inputs) == 0 {
		return record(fmt.Errorf("%w: no inputs", ErrUnreachablePlan))
	}

	for i := range inputs {
		in := inputs[i]
		if in.Path == "" {
			return record(NewErrBadMountpointPath(in.Path, "path is empty"))
		}
		in.Path = strings.TrimSuffix(in.Path, "/")
		inputs[i] = in
	}
	ps.inputs = inputs
	for i := range outputs {
		out := outputs[i]
		if out.Path == "" {
			return record(NewErrBadMountpointPath(out.Path, "path is empty"))
		}
		out.Path = strings.TrimSuffix(out.Path, "/")
		outputs[i] = out
	}
	ps.outputs = outputs

	for nth := range ps.inputs {
		in := ps.inputs[nth]
		if p := in.Path; !filepath.IsAbs(p) || filepath.Clean(p) != p {
			return record(NewErrBadMountpointPath(in.Path, "not absolute or not clean"))
		}

		if in.Tags.Len() == 0 {
			return record(NewErrBadMountpontTag(in.Path, "no tags for input"))
		}

		var knitId *Tag
		var timestamp *Tag
		for _, t := range in.Tags.SystemTag() {
			tag := t // copy value from loop variable
			switch tag.Key {
			case KeyKnitTransient:
				return record(NewErrBadMountpontTag(
					in.Path, `data with "knit#transient" are never used`,
				))
			case KeyKnitId:
				if knitId != nil && !knitId.Equal(&tag) {
					return record(NewErrBadMountpontTag(
						in.Path, `"knit#id:..." found twice (or more)`,
					))
				}
				knitId = &tag
			case KeyKnitTimestamp:
				if timestamp != nil && !timestamp.Equal(&tag) {
					return record(NewErrBadMountpontTag(
						in.Path, `"knit#timestamp:..." found twice (or more)`,
					))
				}
				timestamp = &tag
			default:
				return record(NewErrBadMountpontTag(
					in.Path, "unknown system tag: "+tag.String(),
				))
			}
		}

		for _, other := range inputs[nth+1:] {
			if pathOverlap(in.Path, other.Path) {
				return record(NewErrOverlappedMountpoints(in.Path, other.Path))
			}
		}
		for _, other := range outputs {
			if pathOverlap(in.Path, other.Path) {
				return record(NewErrOverlappedMountpoints(in.Path, other.Path))
			}
		}
	}

	for nth := range outputs {
		out := outputs[nth]
		if p := out.Path; !filepath.IsAbs(p) || filepath.Clean(p) != p {
			return record(NewErrBadMountpointPath(out.Path, "not absolute or not clean"))
		}
		if len(out.Tags.SystemTag()) != 0 {
			return record(NewErrBadMountpontTag(
				out.Path, `output cannot have tag starting with "knit#" (reserved by system)`,
			))
		}
		for _, other := range inputs {
			if pathOverlap(out.Path, other.Path) {
				return record(NewErrOverlappedMountpoints(out.Path, other.Path))
			}
		}
		for _, other := range outputs[nth+1:] {
			if pathOverlap(out.Path, other.Path) {
				return record(NewErrOverlappedMountpoints(out.Path, other.Path))
			}
		}
	}

	if ps.log != nil {
		if len(ps.log.Tags.SystemTag()) != 0 {
			return record(NewErrBadMountpontTag(
				"log", `log cannot have tag starting with "knit#" (reserved by system)`,
			))
		}
	}

	return record(nil)
}

func pathOverlap(a, b string) bool {
	a, b = filepath.ToSlash(a), filepath.ToSlash(b)
	return strings.HasPrefix(kstr.SuppySuffix(a, "/"), b) ||
		strings.HasPrefix(kstr.SuppySuffix(b, "/"), a)
}

// calcurate plan hash
//
// # Return
//
// calcurated hash in hex string
func (ps *PlanSpec) Hash() string {
	if ps.hash != "" {
		return ps.hash
	}

	shahash := sha256.New()
	shahash.Write([]byte(ps.image))
	shahash.Write([]byte(ps.version))

	for _, on := range ps.onNode {
		shahash.Write([]byte(on.String()))
	}

	for _, mp := range ps.inputs {
		shahash.Write([]byte(mp.Path))
		for _, t := range mp.Tags.Slice() {
			shahash.Write([]byte(t.String()))
		}
	}
	for _, mp := range ps.outputs {
		shahash.Write([]byte(mp.Path))
		for _, t := range mp.Tags.Slice() {
			shahash.Write([]byte(t.String()))
		}
	}
	if ps.log != nil {
		shahash.Write([]byte("/log"))
		for _, t := range ps.log.Tags.Slice() {
			shahash.Write([]byte(t.String()))
		}
	}
	if 0 < len(ps.entrypoint) {
		shahash.Write([]byte("[entrypoint]"))
		for _, epitem := range ps.entrypoint {
			shahash.Write([]byte(epitem))
		}
	}
	if 0 < len(ps.args) {
		shahash.Write([]byte("[args]"))
		for _, argitem := range ps.args {
			shahash.Write([]byte(argitem))
		}
	}

	ps.hash = hex.EncodeToString(shahash.Sum(nil))
	return ps.hash
}

type MountPointParam struct {

	// mount path for container
	Path string

	// tags for this mountpoint
	Tags *TagSet
}

func (mps MountPointParam) Equal(other MountPointParam) bool {
	return mps.Path == other.Path &&
		cmp.SliceContentEqWith(
			slices.RefOf(mps.Tags.Slice()), slices.RefOf(other.Tags.Slice()),
			(*Tag).Equal,
		)
}

func (mps MountPointParam) EquivMountPoint(mp *MountPoint) bool {
	return mps.Path == mp.Path &&
		cmp.SliceContentEqWith(
			slices.RefOf(mps.Tags.Slice()), slices.RefOf(mp.Tags.Slice()),
			(*Tag).Equal,
		)
}

type LogParam struct {
	// tags for this mountpoint
	Tags *TagSet
}

func (lp *LogParam) Equal(other *LogParam) bool {
	if (lp == nil) || (other == nil) {
		return (lp == nil) && (other == nil)
	}
	return cmp.SliceContentEqWith(
		slices.RefOf(lp.Tags.Slice()), slices.RefOf(other.Tags.Slice()),
		(*Tag).Equal,
	)
}

func (lp *LogParam) EquivLogPoint(other *LogPoint) bool {
	if (lp == nil) || (other == nil) {
		return (lp == nil) && (other == nil)
	}
	return lp.Equal(&LogParam{Tags: other.Tags})
}

type LogPoint struct {
	// mountpoint Id of this mountpont
	Id int

	// tags for this mountpoint
	Tags *TagSet
}

func (lp *LogPoint) String() string {
	if lp == nil {
		return "(LogPoint)(nil)"
	}
	return fmt.Sprintf("LogPoint{Id:%d Tags:%+v}", lp.Id, lp.Tags.String())
}

func (lp *LogPoint) Equal(other *LogPoint) bool {
	if (lp == nil) || (other == nil) {
		return (lp == nil) && (other == nil)
	}
	return lp.Id == other.Id && lp.Equiv(other)
}

func (lp *LogPoint) Equiv(other *LogPoint) bool {
	if (lp == nil) || (other == nil) {
		return (lp == nil) && (other == nil)
	}
	return cmp.SliceContentEqWith(
		slices.RefOf(lp.Tags.Slice()), slices.RefOf(other.Tags.Slice()),
		(*Tag).Equal,
	)
}

func NewErrCyclicPlan() error {
	return ErrCyclicPlan
}

// some mountpoint is subpath of other mountpoint
func NewErrOverlappedMountpoints(pathes ...string) error {
	return fmt.Errorf("%w: %s", ErrOverlappedMountpoints, strings.Join(pathes, ", "))
}

func NewErrBadMountpointPath(path string, reason string) error {
	return fmt.Errorf(`%w (path = %s) %s`, ErrBadMountpointPath, path, reason)
}

func NewErrEquivPlanExists(planId string) error {
	return &ErrEquivPlanExists{PlanId: planId}
}

func NewErrPlanNamelessImage(planName string) error {
	return fmt.Errorf("%w(%s)", ErrPlanNamelessImage, planName)
}

func NewErrBadMountpontTag(path string, reason string) error {
	return fmt.Errorf("%w (path = %s): %s", ErrBadMountpointTag, path, reason)
}

var (
	ErrInvalidPlan        = errors.New("plan spec is invalid")
	ErrConflictingPlan    = errors.New("plan spec is conflicting with other plan")
	ErrInvalidOnNodeKey   = fmt.Errorf("%w: on_node: invalid key", ErrInvalidPlan)
	ErrInvalidOnNodeValue = fmt.Errorf("%w: on_node: invalid value", ErrInvalidPlan)

	// path of mountpoint spec is not absolute or contains "../"
	ErrBadMountpointPath = fmt.Errorf("%w: bad mountpoint path", ErrInvalidPlan)

	// plan spec has no input
	ErrUnreachablePlan = fmt.Errorf("%w: unreachable plan", ErrInvalidPlan)

	// plan spec has no input
	ErrPlanNamelessImage = fmt.Errorf("%w: nameless or versionless image", ErrInvalidPlan)

	// plan spec has mountpoints which pathes are overlapped (one is subdirectory of another)
	ErrOverlappedMountpoints = fmt.Errorf("%w: mountpoints are overlapped", ErrInvalidPlan)

	// plan spec has mountpoints which has bad tag (not suitable for mode, or tag makes mountpoint unreachable)
	ErrBadMountpointTag = fmt.Errorf("%w: bad tag", ErrInvalidPlan)

	// if the plan is registered, plan dependencies make cycle, means it will leads infinity loop
	ErrCyclicPlan = fmt.Errorf("%w: plan's tag dependency makes cycle", ErrConflictingPlan)
)

// already there have been a plan which has same image:version and equiverent mountpoints.
//
// This error is also ErrConflictingPlan
type ErrEquivPlanExists struct {
	PlanId string
}

func (e *ErrEquivPlanExists) Error() string {
	return "there are equivalent plan: " + e.PlanId
}

func (e *ErrEquivPlanExists) Is(o error) bool {
	switch actual := o.(type) {
	case *ErrEquivPlanExists:
		return actual.PlanId == e.PlanId
	default:
		if errors.Is(o, ErrConflictingPlan) {
			return true
		}
		return false
	}
}
