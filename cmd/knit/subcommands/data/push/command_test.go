package push_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	kenv "github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest"
	rmock "github.com/opst/knitfab/cmd/knit/rest/mock"
	data_push "github.com/opst/knitfab/cmd/knit/subcommands/data/push"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	apidata "github.com/opst/knitfab/pkg/api/types/data"
	"github.com/opst/knitfab/pkg/api/types/plans"
	"github.com/opst/knitfab/pkg/api/types/runs"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestPush(t *testing.T) {
	t.Run("push one data source to knit.", func(t *testing.T) {
		// prepare for test
		mock := rmock.New(t)
		logger := logger.Null()
		tmp := t.TempDir()

		// create dummy data path
		path1 := filepath.Join(tmp, "data1")
		try.To(os.Create(path1)).OrFatal(t).Close()

		env := kenv.KnitEnv{
			Tag: []apitag.Tag{
				{Key: "project", Value: "knitfab"},
			},
		}

		stdout := new(strings.Builder)
		testee := data_push.New(
			data_push.WithProgressOut(io.Discard),
			data_push.WithOutput(stdout),
		)

		mock.Impl.PostData = func(_ context.Context, source string, dereference bool) rest.Progress[*apidata.Detail] {
			if dereference {
				t.Errorf("unexpected dereference flag")
			}
			done := make(chan struct{})
			estDone := make(chan struct{})
			close(done)
			close(estDone)
			return &rmock.MockedPostDataProgress{
				EstimatedTotalSize_: 100,
				ProgressedSize_:     100,
				ProgressingFile_:    "source",
				Result_: &apidata.Detail{
					KnitId: "1234",
					Tags: []apitag.Tag{
						{Key: apitag.KeyKnitId, Value: "1234"},
						{Key: apitag.KeyKnitTimestamp, Value: "2022-10-11T12:13:14+00:00"},
					},
					Upstream: apidata.AssignedTo{
						Run: runs.Summary{
							RunId: "run#1", Status: "done",
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14+00:00",
							)).OrFatal(t),
							Plan: plans.Summary{
								PlanId: "uploaded", Name: "knit#uploaded",
							},
						},
						Mountpoint: plans.Mountpoint{Path: "/out"},
					},
				},
				ResultOk_: true,
				Done_:     done,
				Sent_:     estDone,
			}
		}

		outputData := &apidata.Detail{
			KnitId: "1234",
			Tags: []apitag.Tag{
				{Key: apitag.KeyKnitId, Value: "1234"},
				{Key: apitag.KeyKnitTimestamp, Value: "2022-10-11T12:13:14+00:00"},
				{Key: "project", Value: "knitfab"},
				{Key: "type", Value: "image"},
				{Key: "format", Value: "png"},
			},
			Upstream: apidata.AssignedTo{
				Run: runs.Summary{
					RunId: "run#1", Status: "done",
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14+00:00",
					)).OrFatal(t),
					Plan: plans.Summary{
						PlanId: "uploaded", Name: "knit#uploaded",
					},
				},
				Mountpoint: plans.Mountpoint{Path: "/out"},
			},
		}
		mock.Impl.PutTagsForData = func(knitId string, argtags apitag.Change) (*apidata.Detail, error) {
			return outputData, nil
		}

		err := testee.Execute(
			context.Background(),
			logger, env, mock,
			usage.FlagSet[data_push.Flags]{
				Flags: data_push.Flags{
					Tag: []apitag.Tag{
						{Key: "type", Value: "image"},
						{Key: "format", Value: "png"},
					},
				},
				Args: map[string][]string{
					data_push.ARG_SOURCE: {tmp},
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		{
			expected := []rmock.PostDataArgs{{Source: tmp}}
			if actual := mock.Calls.PostData; !cmp.SliceContentEq(actual, expected) {
				t.Errorf(
					"unexpected post data args:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		}

		{
			expected := []rmock.PutTagsForDataArgs{
				{
					KnitId: "1234",
					Tags: apitag.Change{
						AddTags: []apitag.UserTag{
							{Key: "project", Value: "knitfab"},
							{Key: "type", Value: "image"},
							{Key: "format", Value: "png"},
						},
					},
				},
			}

			if actual := mock.Calls.PutTagsForData; !cmp.SliceContentEqWith(
				actual, expected, func(a, b rmock.PutTagsForDataArgs) bool {
					return a.KnitId == b.KnitId && a.Tags.Equal(&b.Tags)
				},
			) {
				t.Errorf(
					"unexpected put tags args:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		}

		{
			actual := new(apidata.Detail)
			content := stdout.String()
			if err := json.Unmarshal([]byte(content), actual); err != nil {
				t.Fatal(err)
			}
			if !actual.Equal(outputData) {
				t.Errorf(
					"unexpected output:\n===actual===\n%+v\n===expected===\n%+v",
					actual, outputData,
				)
			}
		}
	})

	t.Run("push two data source with -n", func(t *testing.T) {
		// prepare for test
		mock := rmock.New(t)
		logger := logger.Null()
		tmpA := t.TempDir()
		tmpB := t.TempDir()

		// create dummy data path
		path1 := filepath.Join(tmpA, "data1")
		try.To(os.Create(path1)).OrFatal(t).Close()
		path2 := filepath.Join(tmpB, "data2")
		try.To(os.Create(path2)).OrFatal(t).Close()

		env := kenv.KnitEnv{
			Tag: []apitag.Tag{
				{Key: "project", Value: "knitfab"},
			},
		}

		stdout := new(strings.Builder)
		testee := data_push.New(
			data_push.WithProgressOut(io.Discard),
			data_push.WithOutput(stdout),
		)

		nth := 0
		mock.Impl.PostData = func(_ context.Context, source string, dereference bool) rest.Progress[*apidata.Detail] {
			if dereference {
				t.Errorf("unexpected dereference flag")
			}
			nth += 1
			closed := make(chan struct{})
			close(closed)
			return &rmock.MockedPostDataProgress{
				EstimatedTotalSize_: 100,
				ProgressedSize_:     100,
				ProgressingFile_:    "source",
				Result_: &apidata.Detail{
					KnitId: fmt.Sprintf("1234_%d", nth),
					Tags: []apitag.Tag{
						{Key: apitag.KeyKnitId, Value: "1234"},
						{Key: apitag.KeyKnitTimestamp, Value: "2022-10-11T12:13:14+00:00"},
					},
					Upstream: apidata.AssignedTo{
						Run: runs.Summary{
							RunId: "run#1", Status: "done",
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14+00:00",
							)).OrFatal(t),
							Plan: plans.Summary{
								PlanId: "uploaded", Name: "knit#uploaded",
							},
						},
						Mountpoint: plans.Mountpoint{Path: "/out"},
					},
				},
				ResultOk_: true,
				Done_:     closed,
				Sent_:     closed,
			}
		}

		outputData := &apidata.Detail{
			KnitId: "1234",
			Tags: []apitag.Tag{
				{Key: apitag.KeyKnitId, Value: "1234"},
				{Key: apitag.KeyKnitTimestamp, Value: "2022-10-11T12:13:14+00:00"},
				{Key: "project", Value: "knitfab"},
				{Key: "type", Value: "image"},
				{Key: "format", Value: "png"},
			},
			Upstream: apidata.AssignedTo{
				Run: runs.Summary{
					RunId: "run#1", Status: "done",
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14+00:00",
					)).OrFatal(t),
					Plan: plans.Summary{
						PlanId: "uploaded", Name: "knit#uploaded",
					},
				},
				Mountpoint: plans.Mountpoint{Path: "/out"},
			},
		}
		mock.Impl.PutTagsForData = func(knitId string, argtags apitag.Change) (*apidata.Detail, error) {
			return outputData, nil
		}

		err := testee.Execute(
			context.Background(),
			logger, env, mock,
			usage.FlagSet[data_push.Flags]{
				Flags: data_push.Flags{
					Tag: []apitag.Tag{
						{Key: "type", Value: "image"},
						{Key: "format", Value: "png"},
					},
					Name: true,
				},
				Args: map[string][]string{
					data_push.ARG_SOURCE: {tmpA, tmpB},
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		{
			expected := []rmock.PostDataArgs{{Source: tmpA}, {Source: tmpB}}
			if actual := mock.Calls.PostData; !cmp.SliceContentEq(actual, expected) {
				t.Errorf(
					"unexpected post data args:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		}

		{
			expected := []rmock.PutTagsForDataArgs{
				{
					KnitId: "1234_1",
					Tags: apitag.Change{
						AddTags: []apitag.UserTag{
							{Key: "project", Value: "knitfab"},
							{Key: "type", Value: "image"},
							{Key: "format", Value: "png"},
							{Key: "name", Value: path.Base(tmpA)},
						},
					},
				},
				{
					KnitId: "1234_2",
					Tags: apitag.Change{
						AddTags: []apitag.UserTag{
							{Key: "project", Value: "knitfab"},
							{Key: "type", Value: "image"},
							{Key: "format", Value: "png"},
							{Key: "name", Value: path.Base(tmpB)},
						},
					},
				},
			}

			if actual := mock.Calls.PutTagsForData; !cmp.SliceContentEqWith(
				actual, expected, func(a, b rmock.PutTagsForDataArgs) bool {
					return a.KnitId == b.KnitId && a.Tags.Equal(&b.Tags)
				},
			) {
				t.Errorf(
					"unexpected put tags args:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		}
	})

	t.Run("push one data source with -n", func(t *testing.T) {
		// prepare for test
		mock := rmock.New(t)
		logger := logger.Null()
		tmp := t.TempDir()

		// create dummy data path
		path1 := filepath.Join(tmp, "data1")
		try.To(os.Create(path1)).OrFatal(t).Close()

		env := kenv.KnitEnv{
			Tag: []apitag.Tag{
				{Key: "project", Value: "knitfab"},
			},
		}

		stdout := new(strings.Builder)
		testee := data_push.New(
			data_push.WithProgressOut(io.Discard),
			data_push.WithOutput(stdout),
		)

		mock.Impl.PostData = func(_ context.Context, source string, dereference bool) rest.Progress[*apidata.Detail] {

			if dereference {
				t.Errorf("unexpected dereference flag")
			}
			closed := make(chan struct{})
			close(closed)
			return &rmock.MockedPostDataProgress{
				EstimatedTotalSize_: 100,
				ProgressedSize_:     100,
				ProgressingFile_:    "source",
				Result_: &apidata.Detail{
					KnitId: "1234",
					Tags: []apitag.Tag{
						{Key: apitag.KeyKnitId, Value: "1234"},
						{Key: apitag.KeyKnitTimestamp, Value: "2022-10-11T12:13:14+00:00"},
					},
					Upstream: apidata.AssignedTo{
						Run: runs.Summary{
							RunId: "run#1", Status: "done",
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14+00:00",
							)).OrFatal(t),
							Plan: plans.Summary{
								PlanId: "uploaded", Name: "knit#uploaded",
							},
						},
						Mountpoint: plans.Mountpoint{Path: "/out"},
					},
				},
				ResultOk_: true,
				Done_:     closed,
				Sent_:     closed,
			}
		}

		outputData := &apidata.Detail{
			KnitId: "1234",
			Tags: []apitag.Tag{
				{Key: apitag.KeyKnitId, Value: "1234"},
				{Key: apitag.KeyKnitTimestamp, Value: "2022-10-11T12:13:14+00:00"},
				{Key: "project", Value: "knitfab"},
				{Key: "type", Value: "image"},
				{Key: "format", Value: "png"},
			},
			Upstream: apidata.AssignedTo{
				Run: runs.Summary{
					RunId: "run#1", Status: "done",
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14+00:00",
					)).OrFatal(t),
					Plan: plans.Summary{
						PlanId: "uploaded", Name: "knit#uploaded",
					},
				},
				Mountpoint: plans.Mountpoint{Path: "/out"},
			},
		}
		mock.Impl.PutTagsForData = func(knitId string, argtags apitag.Change) (*apidata.Detail, error) {
			return outputData, nil
		}

		err := testee.Execute(
			context.Background(),
			logger, env, mock,
			usage.FlagSet[data_push.Flags]{
				Flags: data_push.Flags{
					Tag: []apitag.Tag{
						{Key: "type", Value: "image"},
						{Key: "format", Value: "png"},
					},
					Name: true,
				},
				Args: map[string][]string{
					data_push.ARG_SOURCE: {tmp},
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		{
			expected := []rmock.PostDataArgs{{Source: tmp}}
			if actual := mock.Calls.PostData; !cmp.SliceContentEq(actual, expected) {
				t.Errorf(
					"unexpected post data args:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		}

		{
			expected := []rmock.PutTagsForDataArgs{
				{
					KnitId: "1234",
					Tags: apitag.Change{
						AddTags: []apitag.UserTag{
							{Key: "project", Value: "knitfab"},
							{Key: "type", Value: "image"},
							{Key: "format", Value: "png"},
							{Key: "name", Value: path.Base(tmp)},
						},
					},
				},
			}

			if actual := mock.Calls.PutTagsForData; !cmp.SliceContentEqWith(
				actual, expected, func(a, b rmock.PutTagsForDataArgs) bool {
					return a.KnitId == b.KnitId && a.Tags.Equal(&b.Tags)
				},
			) {
				t.Errorf(
					"unexpected put tags args:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		}

		{
			actual := new(apidata.Detail)
			content := stdout.String()
			if err := json.Unmarshal([]byte(content), actual); err != nil {
				t.Fatal(err)
			}
			if !actual.Equal(outputData) {
				t.Errorf(
					"unexpected output:\n===actual===\n%+v\n===expected===\n%+v",
					actual, outputData,
				)
			}
		}
	})

	t.Run("push one data source with -L", func(t *testing.T) {
		// prepare for test
		mock := rmock.New(t)
		logger := logger.Null()
		tmp := t.TempDir()

		// create dummy data path
		path1 := filepath.Join(tmp, "data1")
		try.To(os.Create(path1)).OrFatal(t).Close()

		env := kenv.KnitEnv{
			Tag: []apitag.Tag{
				{Key: "project", Value: "knitfab"},
			},
		}

		stdout := new(strings.Builder)
		testee := data_push.New(
			data_push.WithProgressOut(io.Discard),
			data_push.WithOutput(stdout),
		)

		mock.Impl.PostData = func(_ context.Context, source string, dereference bool) rest.Progress[*apidata.Detail] {

			if !dereference {
				t.Errorf("unexpected dereference flag")
			}

			closed := make(chan struct{})
			close(closed)
			return &rmock.MockedPostDataProgress{
				EstimatedTotalSize_: 100,
				ProgressedSize_:     100,
				ProgressingFile_:    "source",
				Result_: &apidata.Detail{
					KnitId: "1234",
					Tags: []apitag.Tag{
						{Key: apitag.KeyKnitId, Value: "1234"},
						{Key: apitag.KeyKnitTimestamp, Value: "2022-10-11T12:13:14+00:00"},
					},
					Upstream: apidata.AssignedTo{
						Run: runs.Summary{
							RunId: "run#1", Status: "done",
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-10-11T12:13:14+00:00",
							)).OrFatal(t),
							Plan: plans.Summary{
								PlanId: "uploaded", Name: "knit#uploaded",
							},
						},
						Mountpoint: plans.Mountpoint{Path: "/out"},
					},
				},
				ResultOk_: true,
				Done_:     closed,
				Sent_:     closed,
			}
		}

		outputData := &apidata.Detail{
			KnitId: "1234",
			Tags: []apitag.Tag{
				{Key: apitag.KeyKnitId, Value: "1234"},
				{Key: apitag.KeyKnitTimestamp, Value: "2022-10-11T12:13:14+00:00"},
				{Key: "project", Value: "knitfab"},
				{Key: "type", Value: "image"},
				{Key: "format", Value: "png"},
			},
			Upstream: apidata.AssignedTo{
				Run: runs.Summary{
					RunId: "run#1", Status: "done",
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14+00:00",
					)).OrFatal(t),
					Plan: plans.Summary{
						PlanId: "uploaded", Name: "knit#uploaded",
					},
				},
				Mountpoint: plans.Mountpoint{Path: "/out"},
			},
		}
		mock.Impl.PutTagsForData = func(knitId string, argtags apitag.Change) (*apidata.Detail, error) {
			return outputData, nil
		}

		err := testee.Execute(
			context.Background(),
			logger, env, mock,
			usage.FlagSet[data_push.Flags]{
				Flags: data_push.Flags{
					Tag: []apitag.Tag{
						{Key: "type", Value: "image"},
						{Key: "format", Value: "png"},
					},
					Dereference: true,
				},
				Args: map[string][]string{
					data_push.ARG_SOURCE: {tmp},
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		{
			expected := []rmock.PostDataArgs{{Source: tmp}}
			if actual := mock.Calls.PostData; !cmp.SliceContentEq(actual, expected) {
				t.Errorf(
					"unexpected post data args:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		}

		{
			expected := []rmock.PutTagsForDataArgs{
				{
					KnitId: "1234",
					Tags: apitag.Change{
						AddTags: []apitag.UserTag{
							{Key: "project", Value: "knitfab"},
							{Key: "type", Value: "image"},
							{Key: "format", Value: "png"},
						},
					},
				},
			}

			if actual := mock.Calls.PutTagsForData; !cmp.SliceContentEqWith(
				actual, expected, func(a, b rmock.PutTagsForDataArgs) bool {
					return a.KnitId == b.KnitId && a.Tags.Equal(&b.Tags)
				},
			) {
				t.Errorf(
					"unexpected put tags args:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		}

		{
			actual := new(apidata.Detail)
			content := stdout.String()
			if err := json.Unmarshal([]byte(content), actual); err != nil {
				t.Fatal(err)
			}
			if !actual.Equal(outputData) {
				t.Errorf(
					"unexpected output:\n===actual===\n%+v\n===expected===\n%+v",
					actual, outputData,
				)
			}
		}
	})

	t.Run("push no data source to knit.", func(t *testing.T) {
		// prepare for test
		mock := rmock.New(t)
		logger := logger.Null()

		env := kenv.KnitEnv{
			Tag: []apitag.Tag{
				{Key: "project", Value: "knitfab"},
			},
		}

		testee := data_push.New(
			data_push.WithProgressOut(io.Discard),
		)

		err := testee.Execute(
			context.Background(),
			logger, env, mock,
			usage.FlagSet[data_push.Flags]{
				Flags: data_push.Flags{
					Tag: []apitag.Tag{
						{Key: "type", Value: "image"},
						{Key: "format", Value: "png"},
					},
				},
				Args: map[string][]string{
					data_push.ARG_SOURCE: {},
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		{
			if actual := mock.Calls.PostData; len(actual) != 0 {
				t.Errorf(
					"unexpected post data args (should be empty):\n===actual===\n%+v",
					actual,
				)
			}
		}

		{
			if actual := mock.Calls.PutTagsForData; len(actual) != 0 {
				t.Errorf(
					"unexpected put tags args (should be empty):\n===actual===\n%+v",
					actual,
				)
			}
		}
	})
}
