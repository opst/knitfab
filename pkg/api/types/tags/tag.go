package tags

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"gopkg.in/yaml.v3"
)

const (
	KeyKnitId        = kdb.KeyKnitId
	KeyKnitTimestamp = kdb.KeyKnitTimestamp
	KeyKnitTransient = kdb.KeyKnitTransient
)

type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type UserTag Tag

func Convert(dbtag kdb.Tag) Tag {
	return Tag(dbtag)
}

func (t Tag) AsUserTag(ut *UserTag) bool {
	if strings.HasPrefix(t.Key, kdb.SystemTagPrefix) {
		return false
	}
	*ut = UserTag(t)
	return true
}

// parse string value as Tag
//
// # Args
//
// - string: "KEY:VALUE" formatted string. If not, it returns error.
func (t *Tag) Parse(s string) error {
	k, v, ok := strings.Cut(s, ":")
	if !ok {
		return fmt.Errorf("tag parse error: %s :no key", s)
	}
	_t, err := kdb.NewTag(strings.TrimSpace(k), strings.TrimSpace(v))
	if err != nil {
		return fmt.Errorf("tag parse error: %s: %s", s, err)
	}
	t.Key = _t.Key
	t.Value = _t.Value

	return nil
}

// parse string value as UserTag
//
// # Args
//
// - string: "KEY:VALUE" formatted string. If not, it returns error.
// If KEY part is started with "knit#", it returns error.
func (ut *UserTag) Parse(s string) error {
	t := &Tag{}
	if err := t.Parse(s); err != nil {
		return err
	}
	if strings.HasPrefix(t.Key, kdb.SystemTagPrefix) {
		return fmt.Errorf(`tag key "%s..." is reserved for system tags`, kdb.SystemTagPrefix)
	}
	*ut = UserTag(*t)
	return nil
}

func (t *Tag) unarshal(dat map[string]interface{}) error {
	if dat == nil {
		return errors.New("tag is nil")
	}

	// check key
	bkey, ok := dat["key"]
	if !ok {
		return errors.New(`field "key" is missing`)
	}
	if bkey == nil {
		return errors.New(`field "key"'s value is missing`)
	}
	key, ok := bkey.(string)
	if !ok {
		return errors.New(`field "key"'s value is invalid`)
	}
	t.Key = key

	// check value
	bvalue, ok := dat["value"]
	if !ok {
		return errors.New(`field "value" is missing`)
	}
	if bvalue == nil {
		return errors.New(`field "value"'s value is missing`)
	}
	value, ok := bvalue.(string)
	if !ok {
		return errors.New(`field "value"'s value is invalid`)
	}
	t.Value = value

	return nil
}

func (t *Tag) UnmarshalJSON(data []byte) error {
	{
		s := new(string)
		if err := json.Unmarshal(data, s); err == nil {
			return t.Parse(*s)
		}
	}

	var dat map[string]interface{}
	if err := json.Unmarshal(data, &dat); err != nil {
		return errors.New(`failed to parse Tag`)
	}

	return t.unarshal(dat)
}

func (t *Tag) UnmarshalYAML(n *yaml.Node) error {
	{
		s := new(string)
		if err := n.Decode(s); err == nil {
			return t.Parse(*s)
		}
	}

	var dat map[string]interface{}
	if err := n.Decode(&dat); err != nil {
		return errors.New(`failed to parse Tag`)
	}
	return t.unarshal(dat)
}

func (t Tag) marshal() string {
	return t.String()
}

func (ut Tag) MarshalJSON() ([]byte, error) {
	return []byte(`"` + ut.marshal() + `"`), nil
}

func (ut Tag) MarshalYAML() (interface{}, error) {
	n := yaml.Node{
		Kind:  yaml.ScalarNode,
		Value: ut.marshal(),
		Style: yaml.DoubleQuotedStyle,
	}
	return n, nil
}

func (ut *UserTag) UnmarshalJSON(data []byte) error {
	t := &Tag{}
	if err := t.UnmarshalJSON(data); err != nil {
		return err
	}
	if strings.HasPrefix(t.Key, kdb.SystemTagPrefix) {
		return fmt.Errorf(`tag key "%s..." is reserved for system tags`, kdb.SystemTagPrefix)
	}
	*ut = UserTag(*t)
	return nil
}

func (t Tag) String() string {
	return t.Key + ":" + t.Value
}

func (u *UserTag) Equal(o *UserTag) bool {
	ut, ot := Tag(*u), Tag(*o)
	return ut.Equal(&ot)
}

func (a *Tag) Equal(b *Tag) bool {
	if a.Key != b.Key {
		return false
	}

	if a.Key != KeyKnitTimestamp {
		return a.Value == b.Value
	}

	vA, errA := rfctime.ParseRFC3339DateTime(a.Value)
	vB, errB := rfctime.ParseRFC3339DateTime(b.Value)

	return (errA == nil) && (errB == nil) &&
		vA.Equiv(vB)
}

type Change struct {
	AddTags    []UserTag `json:"add"`
	RemoveTags []UserTag `json:"remove"`
}

func (c *Change) Equal(o *Change) bool {

	return cmp.SliceContentEq(c.AddTags, o.AddTags) && cmp.SliceContentEq(c.RemoveTags, o.RemoveTags)
}
