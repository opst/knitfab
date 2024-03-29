package plans

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/logic"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/resource"
)

type Summary struct {
	PlanId string `json:"planId"`
	Image  *Image `json:"image,omitempty"`
	Name   string `json:"name,omitempty"`
}

func (s Summary) Equal(o Summary) bool {
	return s.PlanId == o.PlanId &&
		s.Image.Equal(o.Image) &&
		s.Name == o.Name
}

type Resources map[string]resource.Quantity

func (r Resources) Equal(o Resources) bool {
	return cmp.MapEq(r, o)
}

func (r Resources) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]resource.Quantity(r))
}

func (r Resources) MarshalYAML() (interface{}, error) {
	jsonMap := map[string]string{}
	jsonBytes, err := r.MarshalJSON()
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(jsonBytes, &jsonMap)
	if err != nil {
		return nil, err
	}
	return jsonMap, nil
}

func (r *Resources) UnmarshalYAML(node *yaml.Node) error {
	var m map[string]string
	if err := node.Decode(&m); err != nil {
		return err
	}

	jsonBytes, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := r.UnmarshalJSON(jsonBytes); err != nil {
		return err
	}

	return nil
}

func (r *Resources) UnmarshalJSON(b []byte) error {
	var m map[string]resource.Quantity
	err := json.Unmarshal(b, &m)
	if err != nil {
		return err
	}
	*r = Resources(m)
	return nil
}

type Detail struct {
	Summary
	// props in Summary will be flattened in json.
	//     see also: https://github.com/golang/go/issues/7230

	Inputs    []Mountpoint `json:"inputs"`
	Outputs   []Mountpoint `json:"outputs"`
	Log       *LogPoint    `json:"log,omitempty"`
	Active    bool         `json:"active"`
	OnNode    *OnNode      `json:"on_node,omitempty"`
	Resources Resources    `json:"resources,omitempty"`
}

func (d *Detail) Equal(o *Detail) bool {
	if (d == nil) || (o == nil) {
		return (d == nil) && (o == nil)
	}

	return d.Summary.Equal(o.Summary) &&
		d.Active == o.Active &&
		d.Log.Equal(o.Log) &&
		cmp.SliceContentEqWith(
			utils.RefOf(d.Inputs), utils.RefOf(o.Inputs),
			(*Mountpoint).Equal,
		) &&
		cmp.SliceContentEqWith(
			utils.RefOf(d.Outputs), utils.RefOf(o.Outputs),
			(*Mountpoint).Equal,
		)
}

type Mountpoint struct {
	Path string       `json:"path"`
	Tags []apitag.Tag `json:"tags"`
}

func ComposeMountpoint(mp kdb.MountPoint) Mountpoint {
	return Mountpoint{
		Path: mp.Path,
		Tags: utils.Map(mp.Tags.Slice(), apitag.Convert),
	}
}

func (m *Mountpoint) Equal(o *Mountpoint) bool {
	if (m == nil) || (o == nil) {
		return (m == nil) && (o == nil)
	}

	return m.Path == o.Path &&
		cmp.SliceContentEqWith(
			m.Tags, o.Tags,
			func(a, b apitag.Tag) bool { return a.Equal(&b) },
		)
}

type LogPoint struct {
	Tags []apitag.Tag
}

func (lp *LogPoint) Equal(o *LogPoint) bool {
	if (lp == nil) || (o == nil) {
		return (lp == nil) && (o == nil)
	}
	return cmp.SliceContentEqWith(
		utils.RefOf(lp.Tags), utils.RefOf(o.Tags),
		(*apitag.Tag).Equal,
	)
}

func (lp *LogPoint) String() string {
	return fmt.Sprintf("{Tags: %+v}", lp.Tags)
}

type Image struct {
	Repository string
	Tag        string
}

func (i *Image) Equal(o *Image) bool {
	if (i == nil) || (o == nil) {
		return (i == nil) && (o == nil)
	}
	return i.Repository == o.Repository &&
		i.Tag == o.Tag
}

// parse string as Image Tag, and upgate itself.
//
// this spec is based on docker image tag spec[^1].
//
// [^1]: https://docs.docker.com/engine/reference/commandline/tag/#description
func (i *Image) Parse(s string) error {
	// [<repository>[:<port>]/]<name>:<tag>

	ref, err := name.NewTag(s, name.WithDefaultRegistry(""))
	if err != nil {
		return err
	}

	i.Repository = ref.Repository.Name()
	i.Tag = ref.TagStr()
	return nil
}

func (i *Image) marshal() string {
	if i.Repository == "" && i.Tag == "" {
		return ""
	}
	return fmt.Sprintf(`%s:%s`, i.Repository, i.Tag)
}

func (i Image) MarshalJSON() ([]byte, error) {
	b := bytes.NewBufferString(`"`)
	b.WriteString(i.marshal())
	b.WriteString(`"`)
	return b.Bytes(), nil
}

func (i Image) MarshalYAML() (interface{}, error) {
	n := yaml.Node{
		Kind:  yaml.ScalarNode,
		Value: i.marshal(),
		Style: yaml.DoubleQuotedStyle,
	}
	return n, nil
}

func (i *Image) UnmarshalYAML(node *yaml.Node) error {
	expr := new(string)
	err := node.Decode(expr)
	if err != nil {
		return err
	}
	return i.Parse(*expr)
}

func (i *Image) UnmarshalJSON(b []byte) error {
	expr := new(string)
	err := json.Unmarshal(b, expr)
	if err != nil {
		return err
	}
	return i.Parse(*expr)
}

func (i *Image) String() string {
	return i.marshal()
}

type OnSpecLabel struct {
	Key   string
	Value string
}

func (l OnSpecLabel) String() string {
	return fmt.Sprintf("%s=%s", l.Key, l.Value)
}

func (l *OnSpecLabel) Parse(s string) error {

	k, v, ok := strings.Cut(s, "=")
	if !ok {
		return fmt.Errorf("label format error (should be key=value): %s", s)
	}

	l.Key = k
	l.Value = v
	return nil
}

func (l OnSpecLabel) MarshalJSON() ([]byte, error) {
	b := bytes.NewBufferString(`"`)
	b.WriteString(l.String())
	b.WriteString(`"`)
	return b.Bytes(), nil
}

func (l OnSpecLabel) MarshalYAML() (interface{}, error) {
	n := yaml.Node{
		Kind:  yaml.ScalarNode,
		Value: l.String(),
		Style: yaml.DoubleQuotedStyle,
	}
	return n, nil
}

func (l *OnSpecLabel) UnmarshalJSON(value []byte) error {
	expr := new(string)
	err := json.Unmarshal(value, expr)
	if err != nil {
		return err
	}
	return l.Parse(*expr)
}

func (l *OnSpecLabel) UnmarshalYAML(node *yaml.Node) error {
	expr := new(string)
	err := node.Decode(expr)
	if err != nil {
		return err
	}
	return l.Parse(*expr)
}

type OnNode struct {
	May    []OnSpecLabel `json:"may,omitempty" yaml:"may,omitempty"`
	Prefer []OnSpecLabel `json:"prefer,omitempty" yaml:"prefer,omitempty"`
	Must   []OnSpecLabel `json:"must,omitempty" yaml:"must,omitempty"`
}

type PlanSpec struct {
	Image     Image        `json:"image" yaml:"image"`
	Inputs    []Mountpoint `json:"inputs" yaml:"inputs"`
	Outputs   []Mountpoint `json:"outputs" yaml:"outputs"`
	Log       *LogPoint    `json:"log,omitempty" yaml:"log,omitempty"`
	OnNode    *OnNode      `json:"on_node,omitempty" yaml:"on_node,omitempty"`
	Resources Resources    `json:"resources,omitempty" yaml:"resources,omitempty"`
	Active    *bool        `json:"active" yaml:"active,omitempty"`
}

func (ps *PlanSpec) Equal(o *PlanSpec) bool {
	if (ps == nil) || (o == nil) {
		return (ps == nil) && (o == nil)
	}

	return ps.Image.Equal(&o.Image) &&
		ps.Log.Equal(o.Log) &&
		cmp.MapEq(ps.Resources, o.Resources) &&
		cmp.SliceContentEqWith(
			utils.RefOf(ps.Inputs), utils.RefOf(o.Inputs),
			(*Mountpoint).Equal,
		) &&
		cmp.SliceContentEqWith(
			utils.RefOf(ps.Outputs), utils.RefOf(o.Outputs),
			(*Mountpoint).Equal,
		)
}

func ComposeDetail(plan kdb.Plan) Detail {
	var log *LogPoint
	if plan.Log != nil {
		log = &LogPoint{Tags: utils.Map(plan.Log.Tags.Slice(), apitag.Convert)}
	}

	var onNode *OnNode
	if 0 < len(plan.OnNode) {
		onNode = &OnNode{}
		for _, on := range plan.OnNode {
			switch on.Mode {
			case kdb.MayOnNode:
				onNode.May = append(onNode.May, OnSpecLabel{Key: on.Key, Value: on.Value})
			case kdb.PreferOnNode:
				onNode.Prefer = append(onNode.Prefer, OnSpecLabel{Key: on.Key, Value: on.Value})
			case kdb.MustOnNode:
				onNode.Must = append(onNode.Must, OnSpecLabel{Key: on.Key, Value: on.Value})
			}
		}
	}

	return Detail{
		Summary:   ComposeSummary(plan.PlanBody),
		Active:    plan.Active,
		Inputs:    utils.Map(plan.Inputs, ComposeMountpoint),
		Outputs:   utils.Map(plan.Outputs, ComposeMountpoint),
		Resources: Resources(plan.Resources),
		Log:       log,
		OnNode:    onNode,
	}
}

func ComposeSummary(planBody kdb.PlanBody) Summary {
	rst := Summary{
		PlanId: planBody.PlanId,
	}
	if i := planBody.Image; i != nil {
		rst.Image = &Image{Repository: i.Image, Tag: i.Version}
	}
	if p := planBody.Pseudo; p != nil {
		rst.Name = p.Name.String()
	}

	return rst
}

var ErrNilArgument = errors.New("nil is prohibited")

// ResourceLimitChange is a change of resource limit of plan.
type ResourceLimitChange struct {

	// Resource to be set.
	Set Resources `json:"set,omitempty" yaml:"set,omitempty"`

	// Resource types to be unset.
	//
	// If same type Set and Unset, Unset is affected.
	Unset []string `json:"unset,omitempty" yaml:"unset,omitempty"`
}

type FindArgs struct {
	Active   logic.Ternary
	ImageVer kdb.ImageIdentifier
	InTag    []kdb.Tag
	OutTag   []kdb.Tag
}

func (s *FindArgs) Equal(d *FindArgs) bool {

	if !cmp.SliceContentEqWith(utils.RefOf(s.InTag), utils.RefOf(d.InTag), (*kdb.Tag).Equal) {
		return false
	}
	if !cmp.SliceContentEqWith(utils.RefOf(s.OutTag), utils.RefOf(d.OutTag), (*kdb.Tag).Equal) {
		return false
	}

	return s.Active == d.Active && s.ImageVer == d.ImageVer
}
