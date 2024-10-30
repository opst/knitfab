package db_test

import (
	"testing"

	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils/cmp"
)

func TestTagSet_New(t *testing.T) {
	t.Run("when creating TagSet, the TagSet shoud be deduped and sorted", func(t *testing.T) {
		input := []kdb.Tag{
			{Key: "foo", Value: "bar"},
			{Key: "fizz", Value: "quux"},
			{Key: "foo", Value: "bar"},
			{Key: "fizz", Value: "baz"},
		}
		output := kdb.NewTagSet(input)
		expected := []kdb.Tag{
			{Key: "fizz", Value: "baz"},
			{Key: "fizz", Value: "quux"},
			{Key: "foo", Value: "bar"},
		}

		if !cmp.SliceEq(output.Slice(), expected) {
			t.Errorf("not deduped/sorted: %#v", output)
		}
	})

	t.Run("when creating TagSet with multiple knit#timestamp, the TagSet shoud be deduped by time instant", func(t *testing.T) {
		input := []kdb.Tag{
			// there are 3 instance:
			//   - (a) : 2022-08-15T00:11:22+00:00
			//   - (b) : 2022-08-15T01:11:22+00:00
			//   - (c) : 2022-08-15T03:11:22+00:00
			// and, (x), not time instance
			{Key: kdb.KeyKnitTimestamp, Value: "2022-08-15T04:11:22+03:00"}, // (b)
			{Key: kdb.KeyKnitTimestamp, Value: "2022-08-15T00:11:22+00:00"}, // (a)
			{Key: kdb.KeyKnitTimestamp, Value: "2022-08-15T05:11:22+02:00"}, // (c)
			{Key: kdb.KeyKnitTimestamp, Value: "flying lightspeed"},         // (x)
			{Key: kdb.KeyKnitTimestamp, Value: "2022-08-15T01:11:22+01:00"}, // (a)
			{Key: kdb.KeyKnitTimestamp, Value: "2022-08-15T01:11:22+00:00"}, // (b)
		}
		output := kdb.NewTagSet(input)
		expected := []kdb.Tag{
			{Key: kdb.KeyKnitTimestamp, Value: "2022-08-15T00:11:22+00:00"}, // (a)
			{Key: kdb.KeyKnitTimestamp, Value: "2022-08-15T04:11:22+03:00"}, // (b)
			{Key: kdb.KeyKnitTimestamp, Value: "2022-08-15T05:11:22+02:00"}, // (c)
			{Key: kdb.KeyKnitTimestamp, Value: "flying lightspeed"},         // (x)
		}

		if !cmp.SliceEq(output.Slice(), expected) {
			t.Errorf("not deduped/sorted: %#v", output)
		}
	})
}

func TestTagSet_SystemTags_and_UserTags(t *testing.T) {

	for name, testcase := range map[string]struct {
		testee             *kdb.TagSet
		expectedSystemTags []kdb.Tag
		expectedUserTags   []kdb.Tag
	}{
		"for a given TagSet which has user tags only": {
			testee: kdb.NewTagSet(
				[]kdb.Tag{
					{Key: "a", Value: "1"},
					{Key: "b", Value: "2"},
					{Key: "c", Value: "3"},
				},
			),
			expectedSystemTags: []kdb.Tag{},
			expectedUserTags: []kdb.Tag{
				{Key: "a", Value: "1"},
				{Key: "b", Value: "2"},
				{Key: "c", Value: "3"},
			},
		},
		"for a given tagset which has system tags only": {
			testee: kdb.NewTagSet(
				[]kdb.Tag{
					{Key: kdb.KeyKnitId, Value: "some-knit-id"},
					{Key: kdb.KeyKnitTimestamp, Value: "2022-04-01T12:34:56+00:00"},
				},
			),
			expectedSystemTags: []kdb.Tag{
				{Key: kdb.KeyKnitId, Value: "some-knit-id"},
				{Key: kdb.KeyKnitTimestamp, Value: "2022-04-01T12:34:56+00:00"},
			},
			expectedUserTags: []kdb.Tag{},
		},
		"for a given tagset which has system tags and user tags": {
			testee: kdb.NewTagSet(
				[]kdb.Tag{
					{Key: "a", Value: "1"},
					{Key: "b", Value: "2"},
					{Key: "c", Value: "3"},
					{Key: kdb.KeyKnitId, Value: "some-knit-id"},
					{Key: kdb.KeyKnitTimestamp, Value: "2022-04-01T12:34:56+00:00"},
				},
			),
			expectedSystemTags: []kdb.Tag{
				{Key: kdb.KeyKnitId, Value: "some-knit-id"},
				{Key: kdb.KeyKnitTimestamp, Value: "2022-04-01T12:34:56+00:00"},
			},
			expectedUserTags: []kdb.Tag{
				{Key: "a", Value: "1"},
				{Key: "b", Value: "2"},
				{Key: "c", Value: "3"},
			},
		},
	} {
		testee := testcase.testee
		t.Run(name+", when call SystemTag(), it should return containing system tags", func(t *testing.T) {
			actual := testee.SystemTag()
			if !cmp.SliceEq(actual, testcase.expectedSystemTags) {
				t.Errorf("system tags are wrong: (actual, expected) = (%#v, %#v)", actual, testcase.expectedSystemTags)
			}
		})

		t.Run(name+", when call UserTag(), it should return containing user tags", func(t *testing.T) {
			actual := testee.UserTag()
			if !cmp.SliceEq(actual, testcase.expectedUserTags) {
				t.Errorf("system tags are wrong: (actual, expected) = (%#v, %#v)", actual, testcase.expectedUserTags)
			}
		})
	}
}
