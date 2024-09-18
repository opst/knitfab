package show_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	apitag "github.com/opst/knitfab-api-types/tags"
	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/rest/mock"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	run_show "github.com/opst/knitfab/cmd/knit/subcommands/run/show"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestShowCommand(t *testing.T) {
	rundata := runs.Detail{
		Summary: runs.Summary{
			RunId:  "test-runId",
			Status: "done",
			Plan: plans.Summary{
				PlanId: "test-Id",
				Image: &plans.Image{
					Repository: "test-Image", Tag: "test-Version",
				},
				Name: "test-Name",
			},
			UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
				"2022-04-02T12:00:00+00:00",
			)).OrFatal(t),
		},
		Inputs: []runs.Assignment{
			{
				Mountpoint: plans.Mountpoint{
					Path: "/in/1",
					Tags: []apitag.Tag{
						{Key: "type", Value: "raw data"},
						{Key: "format", Value: "rgb image"},
					},
				},
				KnitId: "test-knitId-a",
			},
		},
		Outputs: []runs.Assignment{
			{
				Mountpoint: plans.Mountpoint{
					Path: "/out/2",
					Tags: []apitag.Tag{
						{Key: "type", Value: "training data"},
						{Key: "format", Value: "mask"},
					},
				},
				KnitId: "test-knitId-b",
			}},
		Log: &runs.LogSummary{
			LogPoint: plans.LogPoint{
				Tags: []apitag.Tag{
					{Key: "type", Value: "log"},
					{Key: "format", Value: "jsonl"},
				},
			},
			KnitId: "test-knitId",
		},
	}

	type when struct {
		flags            run_show.Flags
		runId            string
		run              runs.Detail
		funcForInfoError error
		funcForLogError  error
	}

	type then struct {
		err error
	}

	theory := func(when when, then then) func(*testing.T) {
		return func(t *testing.T) {
			profile := &kprof.KnitProfile{ApiRoot: "http://api.knit.invalid"}
			client := try.To(krst.NewClient(profile)).OrFatal(t)

			funcForInfo := func(
				ctx context.Context,
				client krst.KnitClient,
				runId string,
			) (runs.Detail, error) {
				if runId != when.runId {
					t.Errorf("unexpected runId: %s", runId)
				}
				return when.run, when.funcForInfoError
			}
			funcForLog := func(
				ctx context.Context,
				client krst.KnitClient,
				runId string,
				follow bool,
			) error {
				if runId != when.runId {
					t.Errorf("unexpected runId: %s", runId)
				}
				if follow != when.flags.Follow {
					t.Errorf("unexpected follow: %t", follow)
				}
				return when.funcForLogError
			}

			testee := run_show.Task(funcForInfo, funcForLog)

			stdout := new(strings.Builder)
			stderr := new(strings.Builder)

			ctx := context.Background()
			err := testee(
				ctx,
				logger.Null(),
				*kenv.New(),
				client,
				commandline.MockCommandline[run_show.Flags]{
					Fullname_: "knit run show",
					Stdout_:   stdout,
					Stderr_:   stderr,
					Flags_:    when.flags,
					Args_: map[string][]string{
						run_show.ARG_RUNID: {when.runId},
					},
				},
				[]any{},
			)

			if !errors.Is(err, then.err) {
				t.Errorf(
					"wrong status: (actual, expected) != (%d, %d)",
					err, then.err,
				)
			}
		}
	}

	t.Run("when call without args, it should success", theory(
		when{
			runId:            "test-runId",
			run:              rundata,
			funcForInfoError: nil,
			funcForLogError:  nil,
		},

		then{
			err: nil,
		},
	))
	t.Run("when call with --log, it should success", theory(
		when{
			flags:            run_show.Flags{Log: true},
			runId:            "test-runId",
			run:              rundata,
			funcForInfoError: nil,
			funcForLogError:  nil,
		},
		then{
			err: nil,
		},
	))
	t.Run("when call with --follow, it should success", theory(
		when{
			runId:            "test-runId",
			run:              rundata,
			funcForInfoError: nil,
			funcForLogError:  nil,
		},

		then{
			err: nil,
		},
	))
	t.Run("when called with --log --follow, it should success", theory(
		when{
			flags:            run_show.Flags{Log: true, Follow: true},
			runId:            "test-runId",
			run:              rundata,
			funcForInfoError: nil,
			funcForLogError:  nil,
		},
		then{
			err: nil,
		},
	))
	{
		err := errors.New("fake error")
		t.Run("when --log is not specified and the function for information causes error, it should return error", theory(
			when{
				runId:            "test-runId",
				run:              rundata,
				funcForInfoError: err,
				funcForLogError:  nil,
			},
			then{
				err: err,
			},
		))
		t.Run("when --log is specified and the function for log causes error, it should return error", theory(
			when{
				flags:            run_show.Flags{Log: true},
				runId:            "test-runId",
				run:              rundata,
				funcForInfoError: nil,
				funcForLogError:  err,
			},
			then{
				err: err,
			},
		))
	}
}

func TestRunShowRunforInfo(t *testing.T) {
	t.Run("When client does not cause any error, it should return the runId returned by client as is", func(t *testing.T) {
		ctx := context.Background()
		mock := mock.New(t)
		expectedValue := runs.Detail{
			Summary: runs.Summary{
				RunId:  "test-runId",
				Status: "done",
				Plan: plans.Summary{
					PlanId: "test-Id",
					Image: &plans.Image{
						Repository: "test-Image", Tag: "test-Version",
					},
					Name: "test-Name",
				},
				UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
					"2022-04-02T12:00:00+00:00",
				)).OrFatal(t),
			},
			Inputs: []runs.Assignment{
				{
					Mountpoint: plans.Mountpoint{
						Path: "/in/1",
						Tags: []apitag.Tag{
							{Key: "type", Value: "raw data"},
							{Key: "format", Value: "rgb image"},
						},
					},
					KnitId: "test-knitId-a",
				},
			},
			Outputs: []runs.Assignment{
				{
					Mountpoint: plans.Mountpoint{
						Path: "/out/2",
						Tags: []apitag.Tag{
							{Key: "type", Value: "training data"},
							{Key: "format", Value: "mask"},
						},
					},
					KnitId: "test-knitId-b",
				},
			},
			Log: &runs.LogSummary{
				LogPoint: plans.LogPoint{
					Tags: []apitag.Tag{
						{Key: "type", Value: "log"},
						{Key: "format", Value: "jsonl"},
					},
				},
				KnitId: "test-knitId",
			},
		}
		mock.Impl.GetRun = func(ctx context.Context, runId string) (runs.Detail, error) {
			return expectedValue, nil
		}

		actual := try.To(run_show.RunShowRunforInfo(ctx, mock, "test-runId")).OrFatal(t)
		if !actual.Equal(expectedValue) {
			t.Errorf("response is not equal (actual,expected): %v,%v", actual, expectedValue)
		}
	})

	t.Run("when client returns error, it should return the error as is", func(t *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("fake error")

		mock := mock.New(t)
		mock.Impl.GetRun = func(ctx context.Context, runId string) (runs.Detail, error) {
			return runs.Detail{}, expectedError
		}

		actual, err := run_show.RunShowRunforInfo(ctx, mock, "test-Id")

		expectedValue := runs.Detail{}
		if !actual.Equal(expectedValue) {
			t.Errorf("response is not equal (actual,expected): %v,%v", actual, expectedValue)
		}
		if !errors.Is(err, expectedError) {
			t.Errorf("returned error is not expected one: %+v", err)
		}
	})
}

func TestRunShowRunforLog(t *testing.T) {
	t.Run("when client does not cause any error, it should return the content returned by client as is (non-follow)", func(t *testing.T) {
		ctx := context.Background()
		fakeResponse := []byte("response payload")
		mock := mock.New(t)
		mock.Impl.GetRunLog = func(ctx context.Context, runId string, follow bool) (io.ReadCloser, error) {
			if runId != "test-Id" {
				t.Errorf("unexpected runId: got = %s, want = test-Id", runId)
			}
			if follow {
				t.Errorf("unexpected follow: %t", follow)
			}
			return io.NopCloser(bytes.NewBuffer(fakeResponse)), nil
		}

		//backup the existing Stdout
		Stdout := os.Stdout
		//restore the output destination
		defer func() {
			os.Stdout = Stdout
		}()

		pr, pw, _ := os.Pipe()
		os.Stdout = pw
		defer pw.Close()

		err := run_show.RunShowRunforLog(ctx, mock, "test-Id", false)
		if err != nil {
			t.Fatalf("RunShowRunforLog returns error unexpectedly: %s (%+v)", err.Error(), err)
		}
		pw.Close() //the object will block the process until it is closed.

		buf := bytes.Buffer{}
		io.Copy(&buf, pr)
		if !bytes.Equal(buf.Bytes(), fakeResponse) {
			t.Errorf("unexpected content: (actual, expeceted) = (%s, %s)", buf.Bytes(), fakeResponse)
		}
	})

	t.Run("when client returns error, it should return the error as is (non-follow)", func(t *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("fake error")

		mock := mock.New(t)
		mock.Impl.GetRunLog = func(ctx context.Context, runId string, follow bool) (io.ReadCloser, error) {
			if runId != "test-Id" {
				t.Errorf("unexpected runId: %s", runId)
			}
			if follow {
				t.Errorf("unexpected follow: %t", follow)
			}
			return nil, expectedError
		}

		err := run_show.RunShowRunforLog(ctx, mock, "test-Id", false)
		if !errors.Is(err, expectedError) {
			t.Errorf("returned error is not expected one: %+v", err)
		}
	})

	t.Run("when client does not cause any error, it should return the content returned by client as is (follow)", func(t *testing.T) {
		ctx := context.Background()
		fakeResponse := []byte("response payload")
		mock := mock.New(t)
		mock.Impl.GetRunLog = func(ctx context.Context, runId string, follow bool) (io.ReadCloser, error) {
			if runId != "test-Id" {
				t.Errorf("unexpected runId: %s", runId)
			}
			if !follow {
				t.Errorf("unexpected follow: %t", follow)
			}
			return io.NopCloser(bytes.NewBuffer(fakeResponse)), nil
		}

		//backup the existing Stdout
		Stdout := os.Stdout
		//restore the output destination
		defer func() {
			os.Stdout = Stdout
		}()

		pr, pw, _ := os.Pipe()
		os.Stdout = pw
		defer pw.Close()

		err := run_show.RunShowRunforLog(ctx, mock, "test-Id", true)
		if err != nil {
			t.Fatalf("RunShowRunforLog returns error unexpectedly: %s (%+v)", err.Error(), err)
		}
		pw.Close() //the object will block the process until it is closed.

		buf := bytes.Buffer{}
		io.Copy(&buf, pr)
		if !bytes.Equal(buf.Bytes(), fakeResponse) {
			t.Errorf("unexpected content: (actual, expeceted) = (%s, %s)", buf.Bytes(), fakeResponse)
		}
	})

	t.Run("when client returns error, it should return the error as is (follow)", func(t *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("fake error")

		mock := mock.New(t)
		mock.Impl.GetRunLog = func(ctx context.Context, runId string, follow bool) (io.ReadCloser, error) {
			if runId != "test-Id" {
				t.Errorf("unexpected runId: %s", runId)
			}
			if !follow {
				t.Errorf("unexpected follow: %t", follow)
			}
			return nil, expectedError
		}

		err := run_show.RunShowRunforLog(ctx, mock, "test-Id", true)
		if !errors.Is(err, expectedError) {
			t.Errorf("returned error is not expected one: %+v", err)
		}
	})
}
