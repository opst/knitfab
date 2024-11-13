package find_test

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	kargs "github.com/opst/knitfab/pkg/utils/args"
	"github.com/opst/knitfab/pkg/utils/slices"
	"github.com/youta-t/flarc"

	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	mock "github.com/opst/knitfab/cmd/knit/rest/mock"

	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	data_find "github.com/opst/knitfab/cmd/knit/subcommands/data/find"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestFindDataCommand(t *testing.T) {
	type when struct {
		flag         data_find.Flag
		presentation []data.Detail
		err          error
	}
	type then struct {
		err       error
		tags      []tags.Tag
		transient data_find.TransientValue
		since     *time.Time
		duration  *time.Duration
	}

	presentationItems := []data.Detail{
		{
			KnitId: "sample-knit-id",
			Tags: []tags.Tag{
				{Key: "knit#id", Value: "sample-knit-id"},
				{Key: "knit#transient", Value: "processing"},
				{Key: "foo", Value: "bar"},
				{Key: "baz", Value: "quux"},
			},
			Upstream: data.CreatedFrom{
				Run: runs.Summary{
					RunId: "sample-run-id", Status: string(domain.Running),
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-01-08T00:12:34+00:00",
					)).OrFatal(t),
					Plan: plans.Summary{
						PlanId: "sample-plan-id", Name: "knit#upload",
					},
				},
				Mountpoint: &plans.Mountpoint{Path: "/out"},
			},
			Downstreams: []data.AssignedTo{},
		},
	}

	theory := func(when when, then then) func(*testing.T) {
		return func(t *testing.T) {
			client := try.To(krst.NewClient(
				&kprof.KnitProfile{ApiRoot: "http://api.knit.invalid"},
			)).OrFatal(t)

			mockFindData := func(
				_ context.Context, _ *log.Logger, _ krst.KnitClient,
				q data_find.Query,
			) ([]data.Detail, error) {
				if !cmp.SliceContentEqWith(q.Tags, then.tags, tags.Tag.Equal) {
					t.Errorf(
						"wrong tags are passed into client:\nactual = %+v\nexpected = %+v",
						q.Tags, then.tags,
					)
				}

				if q.Transient != then.transient {
					t.Errorf(
						"wrong transient flag is passed into client:\nactual = %+v\nexpected = %+v",
						q.Transient, then.transient,
					)
				}

				if then.since != nil {
					if q.Since == nil || !q.Since.Equal(*then.since) {
						t.Errorf(
							"wrong since is passed into client:\nactual = %+v\nexpected = %+v",
							q.Since, then.since,
						)
					}
				} else {
					if q.Since != nil {
						t.Errorf(
							"since should not be passed into client:\nactual = %+v\nexpected = nil",
							q.Since,
						)
					}
				}

				if then.duration != nil {
					if q.Duration == nil || *q.Duration != *then.duration {
						t.Errorf(
							"wrong duration is passed into client:\nactual = %+v\nexpected = %+v",
							q.Duration, then.duration,
						)
					}
				} else {
					if q.Duration != nil {
						t.Errorf(
							"duration should not be passed into client:\nactual = %+v\nexpected = nil",
							q.Duration,
						)
					}
				}

				return when.presentation, when.err
			}

			stdout := new(strings.Builder)

			testee := data_find.Task(mockFindData)
			ctx := context.Background()
			actual := testee(
				ctx, logger.Null(), *kenv.New(), client,
				commandline.MockCommandline[data_find.Flag]{
					Fullname_: "knit data find",
					Stdout_:   stdout,
					Flags_:    when.flag,
					Args_:     nil,
				},
				[]any{},
			)

			if !errors.Is(actual, then.err) {
				t.Errorf(
					"wrong status: (actual, expected) != (%d, %d)",
					actual, then.err,
				)
			}

			if then.err == nil {
				actual := []data.Detail{}
				if err := json.Unmarshal([]byte(stdout.String()), &actual); err != nil {
					t.Fatal(err)
				}
				if !cmp.SliceContentEqWith(actual, when.presentation, data.Detail.Equal) {
					t.Errorf(
						"wrong result:\nactual   = %+v\nexpected = %+v",
						actual, when.presentation,
					)
				}
			}
		}
	}

	t.Run("when tags are passed, it should call task with all tags", theory(
		when{
			flag: data_find.Flag{
				Tags: &kargs.Tags{
					{Key: "foo", Value: "bar"},
					{Key: "baz", Value: "quux"},
				},
				Transient: "both",
			},
			presentation: presentationItems,
		},
		then{
			err: nil,
			tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "baz", Value: "quux"},
			},
			transient: data_find.TransientAny,
		},
	))

	t.Run("when '--transient yes' is passed, it should call task with TransientOnly", theory(
		when{
			flag: data_find.Flag{
				Transient: "yes",
			},
			presentation: presentationItems,
		},
		then{
			err:       nil,
			tags:      []tags.Tag{},
			transient: data_find.TransientOnly,
		},
	))

	t.Run("when '--transient true' is passed, it should call task with TransientOnly", theory(
		when{
			flag: data_find.Flag{
				Transient: "true",
			},
			presentation: presentationItems,
		},
		then{
			err:       nil,
			tags:      []tags.Tag{},
			transient: data_find.TransientOnly,
		},
	))

	t.Run("when '--transient no' is passed, it should call task with TransientExclude", theory(
		when{
			flag: data_find.Flag{
				Tags: &kargs.Tags{
					{Key: "foo", Value: "bar"},
					{Key: "baz", Value: "quux"},
				},
				Transient: "no",
			},
			presentation: presentationItems,
		},
		then{
			err: nil,
			tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "baz", Value: "quux"},
			},
			transient: data_find.TransientExclude,
		},
	))

	t.Run("when '--transient any' is passed, it should call task with TransientAny", theory(
		when{
			flag: data_find.Flag{
				Transient: "both",
			},
			presentation: presentationItems,
		},
		then{
			err:       nil,
			tags:      []tags.Tag{},
			transient: data_find.TransientAny,
		},
	))
	t.Run("when tags and --transient yes are passed, it should call task with all tags and TransientOnly", theory(
		when{
			flag: data_find.Flag{
				Tags: &kargs.Tags{
					{Key: "foo", Value: "bar"},
					{Key: "baz", Value: "quux"},
				},
				Transient: "yes",
			},
			presentation: presentationItems,
		},
		then{
			err: nil,
			tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "baz", Value: "quux"},
			},
			transient: data_find.TransientOnly,
		},
	))
	t.Run("when tags and --transient true are passed, it should call task with all tags and TransientOnly", theory(
		when{
			flag: data_find.Flag{
				Tags: &kargs.Tags{
					{Key: "foo", Value: "bar"},
					{Key: "baz", Value: "quux"},
				},
				Transient: "true",
			},

			presentation: presentationItems,
		},
		then{
			err: nil,
			tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "baz", Value: "quux"},
			},
			transient: data_find.TransientOnly,
		},
	))

	t.Run("when tags and --transient no are passed, it should call task with all tags and TransientExclude", theory(
		when{
			flag: data_find.Flag{
				Tags: &kargs.Tags{
					{Key: "foo", Value: "bar"},
					{Key: "example", Value: "tag"},
				},
				Transient: "no",
			},
			presentation: presentationItems,
		},
		then{
			err: nil,
			tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: "example", Value: "tag"},
			},
			transient: data_find.TransientExclude,
		},
	))
	t.Run("when tags and --transient false are passed, it should call task with all tags and TransientExclude", theory(
		when{
			flag: data_find.Flag{
				Tags: &kargs.Tags{
					{Key: "foo", Value: "bar"},
					{Key: domain.KeyKnitId, Value: "some-knit-id"},
				},
				Transient: "false",
			},
			presentation: presentationItems,
		},
		then{
			err: nil,
			tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: domain.KeyKnitId, Value: "some-knit-id"},
			},
			transient: data_find.TransientExclude,
		},
	))

	{
		timestamp := try.To(rfctime.ParseRFC3339DateTime("2024-04-22T00:00:00.000+09:00")).OrFatal(t).Time()
		d := 2 * time.Hour
		duration := new(kargs.OptionalDuration)
		duration.Set(d.String())

		s := kargs.LooseRFC3339(timestamp)
		since := &kargs.OptionalLooseRFC3339{}
		since.Set(s.String())
		t.Run("when since and duration are passed, it should call task with since and duration", theory(
			when{
				flag: data_find.Flag{
					Transient: "both",
					Since:     since,
					Duration:  duration,
				},
				presentation: presentationItems,
			},
			then{
				err:       nil,
				tags:      []tags.Tag{},
				transient: data_find.TransientAny,
				since:     &timestamp,
				duration:  &d,
			},
		))

		t.Run("when since is not specified and duration is specified, it should return ErrUage", theory(
			when{
				flag: data_find.Flag{
					Duration: duration,
				},
				presentation: presentationItems,
			},
			then{
				err:      flarc.ErrUsage,
				duration: &d,
			},
		))
	}

	t.Run("when task returns no data, it should be done", theory(
		when{
			flag: data_find.Flag{
				Tags: &kargs.Tags{
					{Key: "foo", Value: "bar"},
					{Key: domain.KeyKnitId, Value: "some-knit-id"},
				},
				Transient: "both",
			},
			presentation: []data.Detail{},
		},
		then{
			err: nil,
			tags: []tags.Tag{
				{Key: "foo", Value: "bar"},
				{Key: domain.KeyKnitId, Value: "some-knit-id"},
			},
			transient: data_find.TransientAny,
		},
	))

	{
		err := errors.New("fake error")
		t.Run("when task returns error, it should return with error", theory(
			when{
				flag: data_find.Flag{
					Tags: &kargs.Tags{
						{Key: "foo", Value: "bar"},
						{Key: domain.KeyKnitId, Value: "some-knit-id"},
					},
					Transient: "both",
				},
				err: err,
			},
			then{
				err: err,
				tags: []tags.Tag{
					{Key: "foo", Value: "bar"},
					{Key: domain.KeyKnitId, Value: "some-knit-id"},
				},
				transient: data_find.TransientAny,
			},
		))
	}

	t.Run("when it is passed --transient with wrong value, it should return with ErrUsage", theory(
		when{
			flag: data_find.Flag{
				Transient: "wrong-value",
			},
			presentation: presentationItems,
		},
		then{
			err: flarc.ErrUsage,
		},
	))
}

func TestFindData(t *testing.T) {

	notTransient1 := data.Detail{
		KnitId: "item-1",
		Tags: []tags.Tag{
			{Key: "foo", Value: "bar"},
			{Key: "fizz", Value: "bazz"},
			{Key: "knit#id", Value: "item-1"},
			{Key: "knit#timestamp", Value: "2022-08-01T12:34:56+00:00"},
		},
		Upstream: data.CreatedFrom{
			Run: runs.Summary{
				RunId: "run-1", Status: string(domain.Done),
				UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
					"2022-08-01T12:34:56+00:00",
				)).OrFatal(t),
				Plan: plans.Summary{PlanId: "plan-1", Name: "knit#upload"},
			},
			Mountpoint: &plans.Mountpoint{Path: "/out"},
		},
		Downstreams: []data.AssignedTo{},
		Nomination: []data.NominatedBy{
			{
				Plan:       plans.Summary{PlanId: "plan-2"},
				Mountpoint: plans.Mountpoint{Path: "/in/data-1"},
			},
		},
	}

	notTransient2 := data.Detail{
		KnitId: "item-2",
		Tags: []tags.Tag{
			{Key: "foo", Value: "bar"},
			{Key: "fizz", Value: "bazz"},
			{Key: "knit#id", Value: "item-2"},
			{Key: "knit#timestamp", Value: "2022-08-02T12:34:56+00:00"},
		},
		Upstream: data.CreatedFrom{
			Run: runs.Summary{
				RunId: "run-2", Status: string(domain.Done),
				UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
					"2022-08-01T12:34:56+00:00",
				)).OrFatal(t),
				Plan: plans.Summary{
					PlanId: "plan-3",
					Image:  &plans.Image{Repository: "knit.image.repo.invalid/trainer", Tag: "v1"},
				},
			},
			Mountpoint: &plans.Mountpoint{Path: "/out"},
		},
		Downstreams: []data.AssignedTo{
			{
				Run: runs.Summary{
					RunId: "run-3", Status: string(domain.Running),
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-08-01T12:34:56+00:00",
					)).OrFatal(t),
					Plan: plans.Summary{
						PlanId: "plan-4",
						Image:  &plans.Image{Repository: "knit.image.repo.invalid/evaluator", Tag: "v1"},
					},
				},
				Mountpoint: plans.Mountpoint{Path: "/in/model"},
			},
		},
		Nomination: []data.NominatedBy{
			{
				Plan: plans.Summary{
					PlanId: "plan-4",
					Image:  &plans.Image{Repository: "knit.image.repo.invalid/evaluator", Tag: "v1"},
				},
				Mountpoint: plans.Mountpoint{Path: "/in/model"},
			},
		},
	}

	transientProcessing := data.Detail{
		KnitId: "item-3",
		Tags: []tags.Tag{
			{Key: "foo", Value: "bar"},
			{Key: "fizz", Value: "bazz"},
			{Key: domain.KeyKnitId, Value: "item-1"},
			{Key: domain.KeyKnitTransient, Value: domain.ValueKnitTransientProcessing},
		},
		Upstream: data.CreatedFrom{
			Run: runs.Summary{
				RunId: "run-3", Status: string(domain.Running),
				UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
					"2022-08-01T12:43:56+00:00",
				)).OrFatal(t),
				Plan: plans.Summary{PlanId: "plan-1", Name: "knit#upload"},
			},
			Mountpoint: &plans.Mountpoint{Path: "/out"},
		},
		Downstreams: []data.AssignedTo{},
		Nomination:  []data.NominatedBy{},
	}
	transientFailed := data.Detail{
		KnitId: "item-4",
		Tags: []tags.Tag{
			{Key: "foo", Value: "bar"},
			{Key: "fizz", Value: "bazz"},
			{Key: domain.KeyKnitId, Value: "item-4"},
			{Key: domain.KeyKnitTransient, Value: domain.ValueKnitTransientFailed},
		},
		Upstream: data.CreatedFrom{
			Run: runs.Summary{
				RunId: "run-4", Status: string(domain.Failed),
				UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
					"2022-08-01T12:34:56+00:00",
				)).OrFatal(t),
				Plan: plans.Summary{
					PlanId: "plan-3",
					Image:  &plans.Image{Repository: "knit.image.repo.invalid/trainer", Tag: "v1"},
				},
			},
			Mountpoint: &plans.Mountpoint{Path: "/out"},
		},
		Downstreams: []data.AssignedTo{},
		Nomination: []data.NominatedBy{
			{
				Plan: plans.Summary{
					PlanId: "plan-4",
					Image:  &plans.Image{Repository: "knit.image.repo.invalid/evaluator", Tag: "v1"},
				},
				Mountpoint: plans.Mountpoint{Path: "/in/model"},
			},
		},
	}

	type when struct {
		tags          []tags.Tag
		transientFlag data_find.TransientValue
		since         *time.Time
		duration      *time.Duration
	}

	for name, testcase := range map[string]struct {
		// all data responded from server
		given []data.Detail

		when when

		// data needed in output of command
		then []string
	}{
		`no data are given by server. when it is passed "TransientAny", it returns empry`: {
			given: []data.Detail{},
			when: when{
				tags:          []tags.Tag{{Key: "foo", Value: "bar"}, {Key: "fizz", Value: "quux"}},
				transientFlag: data_find.TransientAny,
			},
			then: []string{},
		},
		`no data are given by server. when it is passed "TransientOnly", it returns empry`: {
			given: []data.Detail{},
			when: when{
				tags:          []tags.Tag{{Key: "foo", Value: "beep"}},
				transientFlag: data_find.TransientOnly,
			},
			then: []string{},
		},
		`no data are given by server. when it is passed "TransientExclude", it returns empry`: {
			given: []data.Detail{},
			when: when{
				tags:          []tags.Tag{},
				transientFlag: data_find.TransientExclude,
			},
			then: []string{},
		},

		`only non-transient data are given by server. when it is passed tags and "TransientAny", it returns all items in presentation form`: {
			given: []data.Detail{notTransient1, notTransient2},
			when: when{
				tags:          []tags.Tag{{Key: "foo", Value: "bar"}},
				transientFlag: data_find.TransientAny,
			},
			then: []string{notTransient1.KnitId, notTransient2.KnitId},
		},
		`only non-transient data are given by server. when it is passed tags and "TransientOnly", it returns empty`: {
			given: []data.Detail{notTransient1, notTransient2},
			when: when{
				tags:          []tags.Tag{},
				transientFlag: data_find.TransientOnly,
			},
			then: []string{},
		},
		`only non-transient data are given by server. when it is passed tags and "TransientExclude", it returns all items in presentation form`: {
			given: []data.Detail{notTransient1, notTransient2},
			when: when{
				tags:          []tags.Tag{{Key: "foo", Value: "bar"}, {Key: "fizz", Value: "bazz"}},
				transientFlag: data_find.TransientExclude,
			},
			then: []string{notTransient2.KnitId, notTransient1.KnitId},
		},

		`only transient data are given by server. when it is passed tags and "TransientAny", it returns all items in presentation form`: {
			given: []data.Detail{transientProcessing, transientFailed},
			when: when{
				tags:          []tags.Tag{},
				transientFlag: data_find.TransientAny,
			},
			then: []string{transientProcessing.KnitId, transientFailed.KnitId},
		},
		`only transient data are given by server. when it is passed tags and "TransientOnly", it returns all items in presentation form`: {
			given: []data.Detail{transientProcessing, transientFailed},
			when: when{
				tags:          []tags.Tag{{Key: "fizz", Value: "bazz"}},
				transientFlag: data_find.TransientOnly,
			},
			then: []string{transientProcessing.KnitId, transientFailed.KnitId},
		},
		`only transient data are given by server. when it is passed tags and "TransientExcept", it returns empty`: {
			given: []data.Detail{transientProcessing, transientFailed},
			when: when{
				tags:          []tags.Tag{{Key: "foo", Value: "bar"}, {Key: "fizz", Value: "bazz"}},
				transientFlag: data_find.TransientExclude,
			},
			then: []string{},
		},

		`both transient and non-transient data are given by server. when it is passed tags and "TransientAny", it returns all items in presentation form`: {
			given: []data.Detail{
				notTransient1, notTransient2, transientProcessing, transientFailed,
			},
			when: when{
				tags:          []tags.Tag{{Key: "foo", Value: "bar"}, {Key: "fizz", Value: "bazz"}},
				transientFlag: data_find.TransientAny,
			},
			then: []string{
				notTransient1.KnitId, notTransient2.KnitId,
				transientProcessing.KnitId, transientFailed.KnitId,
			},
		},
		`both transient and non-transient data are given by server. when it is passed tags and "TransientOnly", it returns items with "knit#transient" in presentation form`: {
			given: []data.Detail{
				notTransient1, notTransient2,
				transientProcessing, transientFailed,
			},
			when: when{
				tags:          []tags.Tag{{Key: "foo", Value: "bar"}, {Key: "fizz", Value: "bazz"}},
				transientFlag: data_find.TransientOnly,
			},
			then: []string{
				transientProcessing.KnitId, transientFailed.KnitId,
			},
		},
		`both transient and non-transient data are given by server. when it is passed tags and "TransientExclude", it returns items wihtout "knit#transient" in presentation form`: {
			given: []data.Detail{
				notTransient1, notTransient2,
				transientProcessing, transientFailed,
			},
			when: when{
				tags:          []tags.Tag{{Key: "foo", Value: "bar"}, {Key: "fizz", Value: "bazz"}},
				transientFlag: data_find.TransientExclude,
			},
			then: []string{notTransient1.KnitId, notTransient2.KnitId},
		},

		`when since and duration are passed, it should call task with since and duration`: {
			given: []data.Detail{notTransient1, notTransient2},
			when: when{
				tags:          []tags.Tag{{Key: "foo", Value: "bar"}, {Key: "fizz", Value: "bazz"}},
				transientFlag: data_find.TransientAny,
				since:         pointer.Ref(try.To(rfctime.ParseRFC3339DateTime("2024-04-22T00:00:00.000+09:00")).OrFatal(t).Time()),
				duration:      pointer.Ref(2 * time.Hour),
			},
			then: []string{notTransient1.KnitId, notTransient2.KnitId},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			logger := logger.Null()
			mock := mock.New(t)
			mock.Impl.FindData = func(ctx context.Context, tags []tags.Tag, s *time.Time, d *time.Duration) ([]data.Detail, error) {

				if !cmp.SliceContentEq(tags, testcase.when.tags) {
					t.Errorf(
						"wrong tags are passed into client:\nactual = %+v\nexpected = %+v",
						t, testcase.when.tags,
					)
				}

				if want := testcase.when.since; want == nil {
					if s != nil {
						t.Errorf(
							"since should not be passed into client:\nactual = %+v\nexpected = nil",
							s,
						)
					}
				} else {
					if s == nil || !s.Equal(*want) {
						t.Errorf(
							"wrong since is passed into client:\nactual = %+v\nexpected = %+v",
							s, want,
						)
					}
				}

				if want := testcase.when.duration; want == nil {
					if d != nil {
						t.Errorf(
							"duration should not be passed into client:\nactual = %+v\nexpected = nil",
							d,
						)
					}
				} else {
					if d == nil || *d != *want {
						t.Errorf(
							"wrong duration is passed into client:\nactual = %+v\nexpected = %+v",
							d, want,
						)
					}
				}

				return testcase.given, nil
			}

			actual := try.To(data_find.FindData(
				ctx, logger, mock,
				data_find.Query{
					Tags:      testcase.when.tags,
					Transient: testcase.when.transientFlag,
					Since:     testcase.when.since,
					Duration:  testcase.when.duration,
				},
			)).OrFatal(t)

			{
				given := slices.ToMap(testcase.given, func(d data.Detail) string { return d.KnitId })
				actual := slices.ToMap(actual, func(d data.Detail) string { return d.KnitId })

				// are requied ids satisfied?
				if !cmp.SliceContentEq(
					slices.KeysOf(actual), testcase.then,
				) {
					t.Errorf(
						"unmatch: unexpected knit ids are remained: (actual, expeted) = (%+v, %+v)",
						slices.KeysOf(actual), testcase.then,
					)
				}

				// and, these are same as responded ones?
				if !cmp.MapLeqWith(
					actual, given, // actual ⊆ given
					data.Detail.Equal,
				) {
					t.Errorf("wrong result:\nactual   = %+v\nexpected = %+v", actual, testcase.then)
				}

			}

			if len(mock.Calls.FindData) != 1 {
				t.Fatalf(
					"FindData is called too much or less: (actual, expected) = (%d, 1)",
					len(mock.Calls.FindData),
				)
			}
			if !cmp.SliceContentEq(mock.Calls.FindData[0].Tags, testcase.when.tags) {
				t.Errorf(
					"wrong tags are passed into client:\nactual = %+v\nexpected = %+v",
					mock.Calls.FindData[0].Tags, testcase.when.tags,
				)
			}
		})
	}

	t.Run("when client returns error, it returns the error as is", func(t *testing.T) {
		ctx := context.Background()
		logger := logger.Null()
		expectedError := errors.New("fake error")

		mock := mock.New(t)
		mock.Impl.FindData = func(ctx context.Context, t []tags.Tag, s *time.Time, d *time.Duration) ([]data.Detail, error) {
			return nil, expectedError
		}

		since := try.To(rfctime.ParseRFC3339DateTime("2024-04-22T00:00:00.000+09:00")).OrFatal(t).Time()
		duration := time.Duration(2 * time.Hour)

		actual, err := data_find.FindData(
			ctx, logger, mock,
			data_find.Query{
				Tags:      []tags.Tag{},
				Transient: data_find.TransientAny,
				Since:     &since,
				Duration:  &duration,
			},
		)

		if len(actual) != 0 {
			t.Errorf("unexpected value is returned: %+v", actual)
		}

		if !errors.Is(err, expectedError) {
			t.Errorf("returned error is not expected one: %+v", err)
		}

	})
}
