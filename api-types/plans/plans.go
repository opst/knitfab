package plans

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/opst/knitfab-api-types/internal/utils/cmp"
	"github.com/opst/knitfab-api-types/tags"
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

func (d Detail) Equal(o Detail) bool {
	logEq := d.Log == nil && o.Log == nil || (d.Log != nil && o.Log != nil && d.Log.Equal(*o.Log))
	onnodeEq := d.OnNode == nil && o.OnNode == nil || (d.OnNode != nil && o.OnNode != nil && d.OnNode.Equal(*o.OnNode))

	return d.Summary.Equal(o.Summary) &&
		d.Active == o.Active &&
		logEq && onnodeEq &&
		cmp.SliceEqualUnordered(d.Inputs, o.Inputs) &&
		cmp.SliceEqualUnordered(d.Outputs, o.Outputs)
}

type Mountpoint struct {
	Path string     `json:"path"`
	Tags []tags.Tag `json:"tags"`
}

func (m Mountpoint) Equal(o Mountpoint) bool {
	return m.Path == o.Path && cmp.SliceEqualUnordered(m.Tags, o.Tags)
}

type LogPoint struct {
	Tags []tags.Tag
}

func (lp LogPoint) Equal(o LogPoint) bool {
	return cmp.SliceEqualUnordered(lp.Tags, o.Tags)
}

func (lp LogPoint) String() string {
	return fmt.Sprintf("{Tags: %+v}", lp.Tags)
}

type OnNode struct {
	May    []OnSpecLabel `json:"may,omitempty" yaml:"may,omitempty"`
	Prefer []OnSpecLabel `json:"prefer,omitempty" yaml:"prefer,omitempty"`
	Must   []OnSpecLabel `json:"must,omitempty" yaml:"must,omitempty"`
}

func (o OnNode) Equal(oo OnNode) bool {
	return cmp.SliceEqualUnordered(o.May, oo.May) &&
		cmp.SliceEqualUnordered(o.Prefer, oo.Prefer) &&
		cmp.SliceEqualUnordered(o.Must, oo.Must)
}

type OnSpecLabel struct {
	Key   string
	Value string
}

func (l OnSpecLabel) String() string {
	return fmt.Sprintf("%s=%s", l.Key, l.Value)
}

func (l OnSpecLabel) Equal(o OnSpecLabel) bool {
	return l.Key == o.Key && l.Value == o.Value
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

type Resources map[string]resource.Quantity

func (r Resources) Equal(o Resources) bool {
	return cmp.MapEqual(r, o)
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

type PlanSpec struct {
	Image     Image        `json:"image" yaml:"image"`
	Inputs    []Mountpoint `json:"inputs" yaml:"inputs"`
	Outputs   []Mountpoint `json:"outputs" yaml:"outputs"`
	Log       *LogPoint    `json:"log,omitempty" yaml:"log,omitempty"`
	OnNode    *OnNode      `json:"on_node,omitempty" yaml:"on_node,omitempty"`
	Resources Resources    `json:"resources,omitempty" yaml:"resources,omitempty"`
	Active    *bool        `json:"active" yaml:"active,omitempty"`
}

func (ps PlanSpec) Equal(o PlanSpec) bool {
	activeEq := ps.Active == nil && o.Active == nil || (ps.Active != nil && o.Active != nil && *ps.Active == *o.Active)
	logEq := ps.Log == nil && o.Log == nil || (ps.Log != nil && o.Log != nil && ps.Log.Equal(*o.Log))
	onNodeEq := ps.OnNode == nil && o.OnNode == nil || (ps.OnNode != nil && o.OnNode != nil && ps.OnNode.Equal(*o.OnNode))

	return ps.Image.Equal(&o.Image) &&
		logEq && onNodeEq && activeEq &&
		cmp.MapEqual(ps.Resources, o.Resources) &&
		cmp.SliceEqualUnordered(ps.Inputs, o.Inputs) &&
		cmp.SliceEqualUnordered(ps.Outputs, o.Outputs)
}

// ResourceLimitChange is a change of resource limit of plan.
type ResourceLimitChange struct {

	// Resource to be set.
	Set Resources `json:"set,omitempty" yaml:"set,omitempty"`

	// Resource types to be unset.
	//
	// If same type Set and Unset, Unset is affected.
	Unset []string `json:"unset,omitempty" yaml:"unset,omitempty"`
}
