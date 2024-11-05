package domain_test

import (
	"testing"

	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/cmp"
)

func TestTagSet_New(t *testing.T) {
	t.Run("when creating TagSet, the TagSet shoud be deduped and sorted", func(t *testing.T) {
		input := []domain.Tag{
			{Key: "foo", Value: "bar"},
			{Key: "fizz", Value: "quux"},
			{Key: "foo", Value: "bar"},
			{Key: "fizz", Value: "baz"},
		}
		output := domain.NewTagSet(input)
		expected := []domain.Tag{
			{Key: "fizz", Value: "baz"},
			{Key: "fizz", Value: "quux"},
			{Key: "foo", Value: "bar"},
		}

		if !cmp.SliceEq(output.Slice(), expected) {
			t.Errorf("not deduped/sorted: %#v", output)
		}
	})

	t.Run("when creating TagSet with multiple knit#timestamp, the TagSet shoud be deduped by time instant", func(t *testing.T) {
		input := []domain.Tag{
			// there are 3 instance:
			//   - (a) : 2022-08-15T00:11:22+00:00
			//   - (b) : 2022-08-15T01:11:22+00:00
			//   - (c) : 2022-08-15T03:11:22+00:00
			// and, (x), not time instance
			{Key: domain.KeyKnitTimestamp, Value: "2022-08-15T04:11:22+03:00"}, // (b)
			{Key: domain.KeyKnitTimestamp, Value: "2022-08-15T00:11:22+00:00"}, // (a)
			{Key: domain.KeyKnitTimestamp, Value: "2022-08-15T05:11:22+02:00"}, // (c)
			{Key: domain.KeyKnitTimestamp, Value: "flying lightspeed"},         // (x)
			{Key: domain.KeyKnitTimestamp, Value: "2022-08-15T01:11:22+01:00"}, // (a)
			{Key: domain.KeyKnitTimestamp, Value: "2022-08-15T01:11:22+00:00"}, // (b)
		}
		output := domain.NewTagSet(input)
		expected := []domain.Tag{
			{Key: domain.KeyKnitTimestamp, Value: "2022-08-15T00:11:22+00:00"}, // (a)
			{Key: domain.KeyKnitTimestamp, Value: "2022-08-15T04:11:22+03:00"}, // (b)
			{Key: domain.KeyKnitTimestamp, Value: "2022-08-15T05:11:22+02:00"}, // (c)
			{Key: domain.KeyKnitTimestamp, Value: "flying lightspeed"},         // (x)
		}

		if !cmp.SliceEq(output.Slice(), expected) {
			t.Errorf("not deduped/sorted: %#v", output)
		}
	})
}

func TestTagSet_SystemTags_and_UserTags(t *testing.T) {

	for name, testcase := range map[string]struct {
		testee             *domain.TagSet
		expectedSystemTags []domain.Tag
		expectedUserTags   []domain.Tag
	}{
		"for a given TagSet which has user tags only": {
			testee: domain.NewTagSet(
				[]domain.Tag{
					{Key: "a", Value: "1"},
					{Key: "b", Value: "2"},
					{Key: "c", Value: "3"},
				},
			),
			expectedSystemTags: []domain.Tag{},
			expectedUserTags: []domain.Tag{
				{Key: "a", Value: "1"},
				{Key: "b", Value: "2"},
				{Key: "c", Value: "3"},
			},
		},
		"for a given tagset which has system tags only": {
			testee: domain.NewTagSet(
				[]domain.Tag{
					{Key: domain.KeyKnitId, Value: "some-knit-id"},
					{Key: domain.KeyKnitTimestamp, Value: "2022-04-01T12:34:56+00:00"},
				},
			),
			expectedSystemTags: []domain.Tag{
				{Key: domain.KeyKnitId, Value: "some-knit-id"},
				{Key: domain.KeyKnitTimestamp, Value: "2022-04-01T12:34:56+00:00"},
			},
			expectedUserTags: []domain.Tag{},
		},
		"for a given tagset which has system tags and user tags": {
			testee: domain.NewTagSet(
				[]domain.Tag{
					{Key: "a", Value: "1"},
					{Key: "b", Value: "2"},
					{Key: "c", Value: "3"},
					{Key: domain.KeyKnitId, Value: "some-knit-id"},
					{Key: domain.KeyKnitTimestamp, Value: "2022-04-01T12:34:56+00:00"},
				},
			),
			expectedSystemTags: []domain.Tag{
				{Key: domain.KeyKnitId, Value: "some-knit-id"},
				{Key: domain.KeyKnitTimestamp, Value: "2022-04-01T12:34:56+00:00"},
			},
			expectedUserTags: []domain.Tag{
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
