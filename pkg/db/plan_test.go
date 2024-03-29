package db_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	kdb "github.com/opst/knitfab/pkg/db"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestPlanParam_Validation(t *testing.T) {

	sha256hash := func(source ...string) string {
		t.Helper()
		buf := sha256.New()
		for _, s := range source {
			buf.Write([]byte(s))
		}
		return hex.EncodeToString(buf.Sum(nil))
	}

	type then struct {
		hash string
		err  error
	}
	theory := func(when kdb.PlanParam, then then) func(*testing.T) {
		return func(t *testing.T) {
			actual, err := when.Validate()
			if !errors.Is(err, then.err) {
				t.Fatalf(
					"error is not expected type (actual, expected) = (%+v, %+v)",
					err, then.err,
				)
			}
			if then.err != nil {
				return
			}

			expected := kdb.BypassValidation(then.hash, then.err, when)
			if !actual.Equal(expected) {
				t.Errorf(
					"not match:\n- actual   : %+v\n- expecetd : %+v",
					*actual, *expected,
				)
			}
		}
	}

	t.Run("when it is passed valid parameters, it creates PlanSpec", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "foo", Value: "bar"},
					}),
				},
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "/out/result/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
				{
					Path: "/out/result/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "another", Value: "tag"},
					}),
				},
			},
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("1Gi"),
			},
			Log: &kdb.LogParam{
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "fizz", Value: "bazz"},
					{Key: "type", Value: "log"},
				}),
			},
			OnNode: []kdb.OnNode{
				{Mode: kdb.MayOnNode, Key: "simple-label", Value: "simple-value"},
				{Mode: kdb.PreferOnNode, Key: "my-cluster.example.com/prefixed-label", Value: "value._-prefixed"},
			},
		},
		then{
			hash: sha256hash(
				"repo.invalid/image-name", "v0.0-alpha",
				"simple-label=simple-value:may",
				"my-cluster.example.com/prefixed-label=value._-prefixed:prefer",
				"/in/data/1", "some:tag",
				"/in/data/2", "foo:bar",
				"/out/result/1", "fizz:bazz",
				"/out/result/2", "another:tag",
				"/log", "fizz:bazz", "type:log",
			),
		},
	))

	t.Run("when it is passed valid parameters (output & log have no tags), it creates PlanSpec", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "foo", Value: "bar"},
					}),
				},
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "/out/result/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
				{
					Path: "/out/result/2",
					Tags: kdb.NewTagSet([]kdb.Tag{}),
				},
			},
			Log: &kdb.LogParam{
				Tags: kdb.NewTagSet([]kdb.Tag{}),
			},
		},
		then{
			hash: sha256hash(
				"repo.invalid/image-name", "v0.0-alpha",
				"/in/data/1", "some:tag",
				"/in/data/2", "foo:bar",
				"/out/result/1", "fizz:bazz",
				"/out/result/2",
				"/log",
			),
		},
	))

	t.Run("when it is passed valid parameters (no log), it creates PlanSpec", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "foo", Value: "bar"},
					}),
				},
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "/out/result/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
				{
					Path: "/out/result/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "another", Value: "tag"},
					}),
				},
			},
		},
		then{
			hash: sha256hash(
				"repo.invalid/image-name", "v0.0-alpha",
				"/in/data/1", "some:tag",
				"/in/data/2", "foo:bar",
				"/out/result/1", "fizz:bazz",
				"/out/result/2", "another:tag",
			),
		},
	))

	t.Run("when it is passed valid parameters (no output), it creates PlanSpec", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitTimestamp, Value: "2021-10-11T12:13:14+00:00"},
						{Key: kdb.KeyKnitId, Value: "some-knit-id"},
						{Key: "foo", Value: "bar"},
					}),
				},
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{}, // empty
			Log: &kdb.LogParam{
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "fizz", Value: "bazz"},
					{Key: "type", Value: "log"},
				}),
			},
		},
		then{
			hash: sha256hash(
				"repo.invalid/image-name", "v0.0-alpha",
				"/in/data/1", "some:tag",
				"/in/data/2", "foo:bar", "knit#id:some-knit-id", "knit#timestamp:2021-10-11T12:13:14+00:00",
				"/log", "fizz:bazz", "type:log",
			),
		},
	))

	t.Run("when it is passed too long OnNode key (not prefixed), it returns error", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitTimestamp, Value: "2021-10-11T12:13:14+00:00"},
						{Key: kdb.KeyKnitId, Value: "some-knit-id"},
						{Key: "foo", Value: "bar"},
					}),
				},
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{}, // empty
			OnNode: []kdb.OnNode{
				{
					Mode:  kdb.PreferOnNode,
					Key:   "a" + strings.Repeat("0123456789", 6) + "bcd",
					Value: "simple-value",
				},
			},
		},
		then{
			err: kdb.ErrInvalidOnNodeKey,
		},
	))

	t.Run("when it is passed too long OnNode key (prefixed), it returns error", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitTimestamp, Value: "2021-10-11T12:13:14+00:00"},
						{Key: kdb.KeyKnitId, Value: "some-knit-id"},
						{Key: "foo", Value: "bar"},
					}),
				},
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{}, // empty
			OnNode: []kdb.OnNode{
				{
					Mode:  kdb.PreferOnNode,
					Key:   ".example.com/a" + strings.Repeat("0123456789", 6) + "bcd",
					Value: "simple-value",
				},
			},
		},
		then{
			err: kdb.ErrInvalidOnNodeKey,
		},
	))

	t.Run("when it is passed too long OnNode key prefix, it returns error", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitTimestamp, Value: "2021-10-11T12:13:14+00:00"},
						{Key: kdb.KeyKnitId, Value: "some-knit-id"},
						{Key: "foo", Value: "bar"},
					}),
				},
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{}, // empty
			OnNode: []kdb.OnNode{
				{
					Mode: kdb.PreferOnNode,
					//     vvvvv 3 +  8 * 31 (= 250) vvvvvv      +   3 = 254 > 253
					Key:   "aa." + strings.Repeat("example.", 31) + "com/abcd",
					Value: "simple-value",
				},
			},
		},
		then{
			err: kdb.ErrInvalidOnNodeKey,
		},
	))

	t.Run("when it is passed too long OnNode key prefix segment, it returns error", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitTimestamp, Value: "2021-10-11T12:13:14+00:00"},
						{Key: kdb.KeyKnitId, Value: "some-knit-id"},
						{Key: "foo", Value: "bar"},
					}),
				},
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{}, // empty
			OnNode: []kdb.OnNode{
				{
					Mode:  kdb.PreferOnNode,
					Key:   "a" + strings.Repeat("0123456789", 6) + "bcd.com/abcd",
					Value: "simple-value",
				},
			},
		},
		then{
			err: kdb.ErrInvalidOnNodeKey,
		},
	))

	t.Run("when it is passed OnNode key with empty name, it returns error", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitTimestamp, Value: "2021-10-11T12:13:14+00:00"},
						{Key: kdb.KeyKnitId, Value: "some-knit-id"},
						{Key: "foo", Value: "bar"},
					}),
				},
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{}, // empty
			OnNode: []kdb.OnNode{
				{
					Mode:  kdb.PreferOnNode,
					Key:   "example.com/",
					Value: "simple-value",
				},
			},
		},
		then{
			err: kdb.ErrInvalidOnNodeKey,
		},
	))

	t.Run("when it is passed too long OnNode value, it returns error", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitTimestamp, Value: "2021-10-11T12:13:14+00:00"},
						{Key: kdb.KeyKnitId, Value: "some-knit-id"},
						{Key: "foo", Value: "bar"},
					}),
				},
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{}, // empty
			OnNode: []kdb.OnNode{
				{
					Mode:  kdb.PreferOnNode,
					Key:   "label",
					Value: "a" + strings.Repeat("0123456789", 6) + "bcd",
				},
			},
		},
		then{
			err: kdb.ErrInvalidOnNodeValue,
		},
	))

	t.Run("when it is passed empty OnNode value, it returns error", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitTimestamp, Value: "2021-10-11T12:13:14+00:00"},
						{Key: kdb.KeyKnitId, Value: "some-knit-id"},
						{Key: "foo", Value: "bar"},
					}),
				},
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{}, // empty
			OnNode: []kdb.OnNode{
				{
					Mode:  kdb.PreferOnNode,
					Key:   "label",
					Value: "",
				},
			},
		},
		then{
			err: kdb.ErrInvalidOnNodeValue,
		},
	))

	t.Run("when it has no inputs, it causes ErrUnreachablePlan", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Outputs: []kdb.MountPointParam{
				{
					Path: "/out/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "foo", Value: "bar"},
					}),
				},
				{
					Path: "/out/result/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
			},
		},
		then{err: kdb.ErrUnreachablePlan},
	))

	t.Run("when it's input has non-absolute path, it causes ErrBadMountpointPath", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "relative/path",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "foo", Value: "bar"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "/out/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
			},
		},
		then{err: kdb.ErrBadMountpointPath},
	))
	t.Run("when it's output has non-absolute path, it causes ErrBadMountpointPath", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "foo", Value: "bar"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "relative/path",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
			},
		},
		then{err: kdb.ErrBadMountpointPath},
	))
	t.Run("when it's input has empty path, it causes ErrBadMountpointPath", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "foo", Value: "bar"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "/out/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
			},
		},
		then{err: kdb.ErrBadMountpointPath},
	))
	t.Run("when it's output has empty path, it causes ErrBadMountpointPath", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "foo", Value: "bar"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
			},
		},
		then{err: kdb.ErrBadMountpointPath},
	))

	t.Run("when it has different knit#id tags in input, it causes ErrBadMountpointTag", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitId, Value: "knit-1"},
						{Key: kdb.KeyKnitId, Value: "knit-2"},
					}),
				},
			},
		},
		then{err: kdb.ErrBadMountpointTag},
	))

	t.Run("when it's input has different knit#timestamp tags, it causes ErrBadMountpointTag", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitTimestamp, Value: "2021-10-11T12:13:14+00:00"},
						{Key: kdb.KeyKnitTimestamp, Value: "2021-10-11T12:13:14+01:00"},
					}),
				},
			},
		},
		then{err: kdb.ErrBadMountpointTag},
	))

	t.Run("when it's input has knit#transient: to-be-processing tags, it causes ErrBadMountpointTag", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
					}),
				},
			},
		},
		then{err: kdb.ErrBadMountpointTag},
	))

	t.Run("when it's input has knit#transient: failed tags, it causes ErrBadMountpointTag", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientFailed},
					}),
				},
			},
		},
		then{err: kdb.ErrBadMountpointTag},
	))

	t.Run("when it's input has knit#transient: unknwon-value tags, it causes ErrBadMountpointTag", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.KeyKnitTransient, Value: "unknwon-value"},
					}),
				},
			},
		},
		then{err: kdb.ErrBadMountpointTag},
	))

	t.Run("when it's input has unknwon system tag, it causes ErrBadMountpointTag", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: kdb.SystemTagPrefix + "unknwon", Value: "value"},
					}),
				},
			},
		},
		then{err: kdb.ErrBadMountpointTag},
	))

	t.Run("when it's input has no tag, it causes ErrBadMountpointTag", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{}),
				},
			},
		},
		then{err: kdb.ErrBadMountpointTag},
	))

	t.Run("when it's output has system tag, it causes ErrBadMountpointTag", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "foo", Value: "bar"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "/out/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
						{Key: kdb.SystemTagPrefix + "something", Value: "foo"},
					}),
				},
			},
		},
		then{err: kdb.ErrBadMountpointTag},
	))

	t.Run("when it's log has system tag, it causes ErrBadMountpointTag", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "foo", Value: "bar"},
					}),
				},
			},
			Log: &kdb.LogParam{
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "fizz", Value: "bazz"},
					{Key: kdb.SystemTagPrefix + "something", Value: "foo"},
				}),
			},
		},
		then{err: kdb.ErrBadMountpointTag},
	))

	t.Run("when it's mountpoints have overlapping path (input-input), it causes ErrOverlappedMountpoints", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "/out/notlog",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
			},
			Log: &kdb.LogParam{
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "foo", Value: "bar"},
				}),
			},
		},
		then{err: kdb.ErrOverlappedMountpoints},
	))
	t.Run("when it's mountpoints have overlapping path (output-output), it causes ErrOverlappedMountpoints", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
				{
					Path: "/in/data/2",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "/out/data",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
				{
					Path: "/out/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
			},
			Log: &kdb.LogParam{
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "foo", Value: "bar"},
				}),
			},
		},
		then{err: kdb.ErrOverlappedMountpoints},
	))
	t.Run("when it's mountpoints have overlapping path (input-output; input is parent), it causes ErrOverlappedMountpoints", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
				{
					Path: "/path/data/",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "/path/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
				{
					Path: "/out/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
			},
			Log: &kdb.LogParam{
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "foo", Value: "bar"},
				}),
			},
		},
		then{err: kdb.ErrOverlappedMountpoints},
	))
	t.Run("when it's mountpoints have overlapping path (input-output; output is parent), it causes ErrOverlappedMountpoints", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
				{
					Path: "/path/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "/path/data/1",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
				{
					Path: "/out/data",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
			},
			Log: &kdb.LogParam{
				Tags: kdb.NewTagSet([]kdb.Tag{
					{Key: "foo", Value: "bar"},
				}),
			},
		},
		then{err: kdb.ErrOverlappedMountpoints},
	))

	t.Run("when it is passed empty image name, it causes ErrPlanNamelessImage", theory(
		kdb.PlanParam{
			Image:   "repo.invalid/image-name",
			Version: "",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "/out/notlog",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
			},
		},
		then{err: kdb.ErrPlanNamelessImage},
	))

	t.Run("when it is passed empty version, it causes ErrPlanNamelessImage", theory(
		kdb.PlanParam{
			Image:   "",
			Version: "v0.0-alpha",
			Active:  true,
			Inputs: []kdb.MountPointParam{
				{
					Path: "/in/data",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "some", Value: "tag"},
					}),
				},
			},
			Outputs: []kdb.MountPointParam{
				{
					Path: "/out/notlog",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "fizz", Value: "bazz"},
					}),
				},
			},
		},
		then{err: kdb.ErrPlanNamelessImage},
	))
}
