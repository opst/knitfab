package pull_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	mock "github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/youta-t/flarc"

	data_pull "github.com/opst/knitfab/cmd/knit/subcommands/data/pull"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	"github.com/opst/knitfab/pkg/utils/cmp"
)

type MockFileEntry struct {
	Header tar.Header
	Body   string
}

func (m MockFileEntry) Equal(o MockFileEntry) bool {
	return m.Header.Name == o.Header.Name &&
		m.Header.Mode == o.Header.Mode &&
		m.Body == o.Body
}

func TestCommand_with_x(t *testing.T) {

	type when struct {
		knitId   string
		subdir   string
		contents []MockFileEntry
		err      error
	}
	type then struct {
		err error
	}

	theory := func(when when, then then) func(t *testing.T) {
		return func(t *testing.T) {
			logger := logger.Null()
			ctx := context.Background()
			client := mock.New(t)
			client.Impl.GetData = func(ctx context.Context, knitid string, handler func(krst.FileEntry) error) error {
				if when.err != nil {
					return when.err
				}
				for _, entry := range when.contents {
					e := krst.FileEntry{
						Header: entry.Header,
						Body:   bytes.NewReader([]byte(entry.Body)),
					}
					if err := handler(e); err != nil {
						return err
					}
				}
				return nil
			}

			root := t.TempDir()
			dest := root
			if when.subdir != "" {
				dest = filepath.Join(dest, when.subdir)
			}

			testee := data_pull.Task

			stdout := new(strings.Builder)

			actualErr := testee(
				ctx, logger, *kenv.New(), client,
				commandline.MockCommandline[data_pull.Flags]{
					Fullname_: "knit data pull",
					Stdout_:   stdout,
					Stderr_:   io.Discard,
					Flags_:    data_pull.Flags{Extract: true},
					Args_: map[string][]string{
						data_pull.ARG_KNIT_ID: {when.knitId},
						data_pull.ARG_DEST:    {dest},
					},
				},
				[]any{},
			)

			if !errors.Is(actualErr, then.err) {
				t.Errorf("err: (actual, expected) = (%d, %d)", actualErr, then.err)
			}
			if actualErr != nil {
				return
			}

			if !cmp.SliceContentEq(client.Calls.GetData, []string{when.knitId}) {
				t.Errorf(
					"client.Calls.GetData:\n===actual===\n%+v\n===expected===\n%+v",
					client.Calls.GetData, []string{when.knitId},
				)
			}

			actualFiles := []MockFileEntry{}
			expectedPath := filepath.Join(dest, when.knitId)
			err := filepath.Walk(expectedPath, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() {
					return nil
				}
				rel, err := filepath.Rel(expectedPath, path)
				if err != nil {
					return err
				}
				f, err := os.Open(path)
				if err != nil {
					return err
				}
				defer f.Close()
				buf, err := io.ReadAll(f)
				if err != nil {
					return err
				}
				actualFiles = append(actualFiles, MockFileEntry{
					Header: tar.Header{
						Name: rel,
						Mode: int64(info.Mode()),
						Size: info.Size(),
					},
					Body: string(buf),
				})

				return nil
			})

			if err != nil {
				t.Fatal(err)
			}

			if !cmp.SliceContentEqWith(actualFiles, when.contents, MockFileEntry.Equal) {
				t.Errorf(
					"actualFiles:\n===actual===\n%+v\n===expected===\n%+v",
					actualFiles, when.contents,
				)
			}
		}
	}

	t.Run("it download data as a extracted file", theory(
		when{
			knitId: "some-knit-id",
			contents: []MockFileEntry{
				{
					Header: tar.Header{
						Name: "file1",
						Size: 7,
						Mode: 0644,
					},
					Body: "content1",
				},
				{
					Header: tar.Header{
						Name: "dir2/file2-1",
						Size: 14,
						Mode: 0744,
					},
					Body: "dir2, file2-1",
				},
			},
		},
		then{err: nil},
	))

	t.Run("it download data as a file supplying missing directory", theory(
		when{
			knitId: "some-knit-id",
			subdir: "sub",
			contents: []MockFileEntry{
				{
					Header: tar.Header{
						Name: "file1",
						Size: 7,
						Mode: 0644,
					},
					Body: "content1",
				},
				{
					Header: tar.Header{
						Name: "dir2/file2-1",
						Size: 14,
						Mode: 0744,
					},
					Body: "dir2, file2-1",
				},
			},
		},
		then{err: nil},
	))

	{
		expectedErr := errors.New("some error")
		t.Run("when client errors, command should error", theory(
			when{
				knitId: "some-knit-id",
				err:    expectedErr,
			},
			then{err: expectedErr},
		))
	}

	t.Run("when -x is passed with output -, command should cause usage error", func(t *testing.T) {
		logger := logger.Null()
		ctx := context.Background()
		client := mock.New(t)

		testee := data_pull.Task

		stdout := new(strings.Builder)

		actualErr := testee(
			ctx, logger, *kenv.New(), client,
			commandline.MockCommandline[data_pull.Flags]{
				Fullname_: "knit data pull",
				Stdout_:   stdout,
				Stderr_:   io.Discard,
				Flags_:    data_pull.Flags{Extract: true},
				Args_: map[string][]string{
					data_pull.ARG_KNIT_ID: {"knit-id"},
					data_pull.ARG_DEST:    {"-"},
				},
			},
			[]any{},
		)

		if !errors.Is(actualErr, flarc.ErrUsage) {
			t.Errorf("err: (actual, expected) = (%d, %d)", actualErr, flarc.ErrUsage)
		}
	})
}

func TestCommand_without_x(t *testing.T) {
	type When struct {
		knitId  string
		subpath string
		payload string
		err     error
	}
	type Then struct {
		err error
	}

	theory := func(when When, then Then) func(t *testing.T) {
		return func(t *testing.T) {
			logger := logger.Null()
			ctx := context.Background()
			client := mock.New(t)
			client.Impl.GetDataRaw = func(ctx context.Context, knitid string, handler func(io.Reader) error) error {
				if when.err != nil {
					return when.err
				}
				return handler(bytes.NewReader([]byte(when.payload)))
			}

			dest := t.TempDir()
			if when.subpath != "" {
				dest = filepath.Join(dest, when.subpath)
			}
			testee := data_pull.Task

			stdout := new(strings.Builder)

			actualErr := testee(
				ctx, logger, *kenv.New(), client,
				commandline.MockCommandline[data_pull.Flags]{
					Fullname_: "knit data pull",
					Stdout_:   stdout,
					Stderr_:   io.Discard,
					Flags_:    data_pull.Flags{},
					Args_: map[string][]string{
						data_pull.ARG_KNIT_ID: {when.knitId},
						data_pull.ARG_DEST:    {dest},
					},
				},
				[]any{},
			)

			if !errors.Is(actualErr, then.err) {
				t.Errorf("err: (actual, expected) = (%d, %d)", actualErr, then.err)
			}
			if actualErr != nil {
				return
			}

			if !cmp.SliceContentEq(client.Calls.GetDataRaw, []string{when.knitId}) {
				t.Errorf(
					"client.Calls.GetDataRaw:\n===actual===\n%+v\n===expected===\n%+v",
					client.Calls.GetDataRaw, []string{when.knitId},
				)
			}

			expectedPath := filepath.Join(dest, when.knitId) + ".tar.gz"
			actualPayload := try.To(os.ReadFile(expectedPath)).OrFatal(t)
			if actual := string(actualPayload); actual != when.payload {
				t.Errorf("actual: %s, expected: %s", actual, when.payload)
			}
		}
	}

	t.Run("it download data as a file", theory(
		When{
			knitId:  "some-knit-id",
			payload: "some payload",
		},
		Then{err: nil},
	))

	t.Run("it download data as a file into new directory", theory(
		When{
			knitId:  "some-knit-id",
			payload: "some payload",
			subpath: "sub",
		},
		Then{err: nil},
	))

	{
		expectedErr := errors.New("some error")
		t.Run("when client errors, command should error", theory(
			When{
				knitId: "some-knit-id",
				err:    expectedErr,
			},
			Then{err: expectedErr},
		))
	}

	t.Run("if destination is - , it write to output buffer", func(t *testing.T) {
		{
			logger := logger.Null()
			ctx := context.Background()

			payload := "payload content\n"
			client := mock.New(t)
			client.Impl.GetDataRaw = func(ctx context.Context, knitid string, handler func(io.Reader) error) error {
				return handler(bytes.NewReader([]byte(payload)))
			}

			stdout := new(bytes.Buffer)

			testee := data_pull.Task
			actualErr := testee(
				ctx, logger, *kenv.New(), client,
				commandline.MockCommandline[data_pull.Flags]{
					Fullname_: "knit data pull",
					Stdout_:   stdout,
					Stderr_:   io.Discard,
					Flags_:    data_pull.Flags{},
					Args_: map[string][]string{
						data_pull.ARG_KNIT_ID: {"knit-id"},
						data_pull.ARG_DEST:    {"-"},
					},
				},
				[]any{},
			)
			if actualErr != nil {
				t.Fatal(actualErr)
			}

			if !cmp.SliceContentEq(client.Calls.GetDataRaw, []string{"knit-id"}) {
				t.Errorf(
					"client.Calls.GetDataRaw:\n===actual===\n%+v\n===expected===\n%+v",
					client.Calls.GetDataRaw, []string{"knit-id"},
				)
			}

			if actual := stdout.String(); actual != payload {
				t.Errorf("actual: %s, expected: %s", actual, payload)
			}
		}
	})
}
