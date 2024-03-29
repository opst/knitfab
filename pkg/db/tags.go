package db

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/rfctime"
)

const (
	// TimestampValueFormat          string = "2006-01-02T15:04:05.999-07:00"
	SystemTagPrefix              string = "knit#"
	KeyKnitId                    string = SystemTagPrefix + "id"
	KeyKnitTimestamp             string = SystemTagPrefix + "timestamp"
	KeyKnitTransient             string = SystemTagPrefix + "transient"
	ValueKnitTransientFailed     string = "failed"
	ValueKnitTransientProcessing string = "processing"
)

var (
	ErrUnacceptableTag    = errors.New(`the tag is not acceptable`)
	ErrBadFormatTimestamp = fmt.Errorf(
		`%w (bad format knit#timestamp)`, ErrUnacceptableTag,
	)
	ErrUnknownSystemTag = fmt.Errorf(`%w (unknown system tag)`, ErrUnacceptableTag)
)

// create an error of ErrNotAcceptableTag.
//
// args:
//   - path: place where the system tag is passed
//   - why: the reason why the tag is not acceptable
//   - tag: the not-acceptable tag
//
// returns:
//
//	new error value wrapping `ErrNotAcceptableTag`
func NewErrUnacceptableTag(path string, reason string) error {
	return fmt.Errorf("%w (path = %s): %s", ErrUnacceptableTag, path, reason)
}

// format of "knit#timestamp" is wrong and cannot be parsed.
//
// this error is also a `NotAcceptableTag`.
//
// args:
//   - value: given value as "knit#timestamp".
func NewErrBadFormatKnitTimestamp(value string) error {
	return fmt.Errorf("%w: %s", ErrBadFormatTimestamp, value)
}

// create an error of ErrSystemTagIsNotAcceptable.
//
// args:
//   - where: place where the system tag is passed
func NewErrUnknownSystemTag(tag *Tag) error {
	return fmt.Errorf("%w: %s", ErrUnknownSystemTag, tag.String())
}

type Tag struct {
	Key   string
	Value string
}

// construct Tag instance
func NewTag(key, value string) (Tag, error) {
	if strings.HasPrefix(key, SystemTagPrefix) {
		switch key {
		case KeyKnitTimestamp:
			_, err := rfctime.ParseRFC3339DateTime(value)
			if err != nil {
				return Tag{}, fmt.Errorf(
					"tag parse error: %s: %s is not timestamp", KeyKnitTimestamp, value,
				)
			}
		case KeyKnitTransient:
			switch value {
			case ValueKnitTransientProcessing, ValueKnitTransientFailed:
				// pass
			default:
				return Tag{}, fmt.Errorf(
					`tag parse error: "%s" should be one of "%s" or "%s"`,
					KeyKnitTransient, ValueKnitTransientProcessing, ValueKnitTransientFailed,
				)
			}
		}
	}
	return Tag{Key: key, Value: value}, nil
}

func ParseTag(expression string) (Tag, error) {
	parts := strings.SplitN(expression, ":", 1)
	if len(parts) < 2 {
		return Tag{}, fmt.Errorf(
			`parse error: %s is not tag (not containing ":" )`, expression,
		)
	}

	k, v := parts[0], parts[1]
	return NewTag(k, v)
}

// construct Tag instance which has key "knit#timestamp" from time.Time
func NewTimestampTag(timestamp time.Time) Tag {
	return Tag{
		Key:   KeyKnitTimestamp,
		Value: rfctime.RFC3339(timestamp).String(),
	}
}

func (t Tag) String() string {
	if t.Key != KeyKnitTimestamp {
		return fmt.Sprintf("%s:%s", t.Key, t.Value)
	}
	ts, err := rfctime.ParseRFC3339DateTime(t.Value)
	if err != nil {
		return fmt.Sprintf("%s:%s", t.Key, t.Value)
	}
	val := ts.Time().UTC().Format(rfctime.RFC3339DateTimeFormat)
	return fmt.Sprintf("%s:%s", t.Key, val)
}

func (t *Tag) IsSystemTag() bool {
	return strings.HasPrefix(t.Key, SystemTagPrefix)
}

func (t *Tag) IsUserTag() bool {
	return !t.IsSystemTag()
}

func (t *Tag) Equal(other *Tag) bool {
	if t.Key != other.Key {
		return false
	}
	if t.Key != KeyKnitTimestamp {
		return t.Value == other.Value
	}

	tvTime, tErr := rfctime.ParseRFC3339DateTime(t.Value)
	ovTime, oErr := rfctime.ParseRFC3339DateTime(other.Value)
	if (tErr == nil) != (oErr == nil) {
		return false
	}
	if tErr == nil && oErr == nil {
		return tvTime.Equiv(ovTime)
	}

	return t.Value == other.Value
}

// ordering of tag.
func (a Tag) less(b Tag) bool {
	if a.Key != b.Key {
		return a.Key < b.Key
	}
	return a.Value < b.Value
}

// set of `Tag`
//
// Instance of `TagSet` should be created with `NewTagSet`.
//
// `TagSet` uses `sync.Mutex` value for stable `Normalize`.
// Do not copy `TagSet` by assign/dereference (`go vet` will warn you).
// alternately do `NewTagSet(tagset.Slice())`.
type TagSet struct {
	tags []Tag

	normalized bool // this is latch. once get true, not be false again.
	// zerovalue of TagSet is "not normalized".
	// Each methods of TagSet should do `Normalize` at first.

	m sync.Mutex
}

// convert slice of Tags to TagSet, and `Normalize` it.
func NewTagSet(tags []Tag) *TagSet {
	return (&TagSet{tags: tags}).Normalize()
}

func (ts *TagSet) String() string {
	ts.normalize()
	tags := utils.Map(
		ts.tags,
		func(t Tag) string { return t.String() },
	)
	return "TagSet{" + strings.Join(tags, ", ") + "}"
}

func (ts *TagSet) Equal(other *TagSet) bool {
	ts.Normalize()
	other.Normalize()

	return cmp.SliceEqWith(
		ts.Slice(), other.Slice(),
		func(a, b Tag) bool { return a.Equal(&b) },
	)
}

// Normalize TagSet
//
// - dedupe tags
// - sort by its (key, value) pair
//
// knit#timestamp:
//
// "knit#timestamp" tag is special case.
//
// - for deduping, they are compaird by its timestmap instant, not stirng.
// - for sorting, they are compairs by its string expression.
//
// so, `Normalize` will handles
// "knit#timestamp: 2022-07-15T12:34:56.888-00:00" and
// "knit#timestamp: 2022-07-15T15:34:56.888-03:00" are "duplicated",
// and drop them except one found first.
//
// This function works once. If you call this twice or more, this does nothing.
//
// Note: `NewTagSet` calls this. You do not need to call this explicitly in most cases.
func (ts *TagSet) Normalize() *TagSet {
	if ts == nil {
		return nil
	}
	if ts.normalized { // if it's ok, there are no need to wait lock
		return ts
	}
	ts.m.Lock()
	defer ts.m.Unlock()
	if ts.normalized { // other thread may run Normalize while this thread is waiting.
		return ts
	}

	return ts.normalize()
}

func (ts *TagSet) Format(s fmt.State, r rune) {
	tags := ts.Slice()
	if len(ts.tags) == 0 {
		fmt.Fprint(s, "TagSet[(empty)]")
	}
	fmt.Fprintf(s, "TagSet[%s", tags[0])
	for _, t := range tags[1:] {
		fmt.Fprintf(s, ", %s", t)
	}
	fmt.Fprint(s, "]")
}

func (ts *TagSet) normalize() *TagSet {
	if len(ts.tags) <= 1 {
		ts.normalized = true
		return ts
	}

	known := map[Tag]struct{}{}
	knownTimestamp := map[int64]struct{}{} // set of unix micro
	normalized := make([]Tag, 0, len(ts.tags))
	// NOTE: it can cause over allocation when ts.tags has many equal tags.

	for _, t := range ts.tags {
		if _, ok := known[t]; ok {
			continue
		}
		if t.Key == KeyKnitTimestamp {
			ts, err := rfctime.ParseRFC3339DateTime(t.Value)
			if err == nil {
				um := ts.Time().UnixMicro()
				// time.Time cannot be used as key as it is.
				// see: https://pkg.go.dev/time#Time
				if _, ok := knownTimestamp[um]; ok {
					continue
				}
				knownTimestamp[um] = struct{}{}
			}
		}

		// mark t as known
		known[t] = struct{}{}

		pos := utils.BinarySearch(normalized, t, Tag.less)
		// insert item into slice at pos.
		//
		// before append:
		//                       pos
		//                        v
		// []Tag{      ..., a, b, c, d, e, ...    }
		//       [...--[:pos+1]----)
		//                        [---[pos:]----...)
		//
		// after append:
		//                     comes from
		//      [...---[:pos+1]----|---[pos:]---...)
		// []Tag{      ..., a, b, c, c, d, e, ... }
		//                        ^
		//                       pos
		//
		// insert:
		// []Tag{      ..., a, b, t, c, d, e, ... }
		//                        ^
		//                    pos (updated)
		//

		normalized = append(normalized[:pos+1], normalized[pos:]...)
		normalized[pos] = t
	}

	ts.tags = normalized
	ts.normalized = true
	return ts
}

// convert TagSet into slice.
//
// the slice is a normalized snapshot.
// any operations for the slice do not effect to TagSet itself.
func (ts *TagSet) Slice() []Tag {
	if ts == nil {
		return []Tag{}
	}

	ts.Normalize()

	c := make([]Tag, len(ts.tags))
	copy(c, ts.tags)
	return c
}

// get size of TagSet
//
// `ts.Len()` is almost shorthand of `len(ts.Slice())`
// (but more effective since bypassing copying).
func (ts *TagSet) Len() int {
	ts.Normalize()

	return len(ts.tags)
}

// filter system tags only.
func (ts *TagSet) SystemTag() []Tag {
	if ts == nil {
		return []Tag{}
	}
	ts.Normalize()

	systemtags := make([]Tag, 0, 3)
	for _, t := range ts.tags {
		if t.IsSystemTag() {
			systemtags = append(systemtags, t)
		}
	}
	return systemtags // sorted & deduped, since it is created with filtering TagSet.
}

// filter user tags only.
func (ts *TagSet) UserTag() []Tag {
	if ts == nil {
		return []Tag{}
	}
	ts.Normalize()

	usertags := make([]Tag, 0, 5)
	for _, t := range ts.tags {
		if t.IsUserTag() {
			usertags = append(usertags, t)
		}
	}
	return usertags // sorted & deduped, since it is created with filtering TagSet.
}

type DataChangeResult struct {
	KnitId string
	Tags   []*Tag
}

func (dcr *DataChangeResult) Equal(other *DataChangeResult) bool {
	return dcr.KnitId == other.KnitId &&
		cmp.SliceContentEqWith(dcr.Tags, other.Tags, (*Tag).Equal)
}
