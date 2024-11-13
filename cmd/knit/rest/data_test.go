package rest_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opst/knitfab-api-types/data"
	apierr "github.com/opst/knitfab-api-types/errors"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	kprof "github.com/opst/knitfab/cmd/knit/config/profiles"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/archive"
	"github.com/opst/knitfab/pkg/utils/cmp"
	kio "github.com/opst/knitfab/pkg/utils/io"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestPostData(t *testing.T) {

	type When struct {
		dereference bool
	}
	type Then struct {
		archiveAsLike string
	}

	type TarEntry struct {
		Header  *tar.Header
		Content []byte
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {

			response := data.Detail{
				KnitId: "1234",
				Tags:   []tags.Tag{{Key: "keydata", Value: "valdata"}},
				Upstream: data.CreatedFrom{
					Run: runs.Summary{
						RunId: "parent-run", Status: string(domain.Done),
						UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
							"2022-04-02T12:00:00+00:00",
						)).OrFatal(t),
						Plan: plans.Summary{
							PlanId: "plan-trainer",
							Image:  &plans.Image{Repository: "image-name", Tag: "v0.0.1"},
						},
					},
					Mountpoint: &plans.Mountpoint{
						Path: "/out/model",
						Tags: []tags.Tag{
							{Key: "type", Value: "model-paramter"},
							{Key: "task", Value: "LLM"},
						},
					},
				},
				Downstreams: []data.AssignedTo{
					{
						Run: runs.Summary{
							RunId: "evaluator-1-1", Status: string(domain.Done),
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-04-03T12:00:00+00:00",
							)).OrFatal(t),
							Plan: plans.Summary{
								PlanId: "plan-evaluate",
								Image:  &plans.Image{Repository: "child-image-1", Tag: "0.1-alpha"},
							},
						},
						Mountpoint: plans.Mountpoint{
							Path: "/in/model",
							Tags: []tags.Tag{
								{Key: "type", Value: "model-paramter"},
								{Key: "task", Value: "LLM"},
							},
						},
					},
				},
				Nomination: []data.NominatedBy{
					{
						Mountpoint: plans.Mountpoint{Path: "/in/model"},
						Plan: plans.Summary{
							PlanId: "plan-evaluate",
							Image:  &plans.Image{Repository: "child-image-1", Tag: "0.1-alpha"},
						},
					},
				},
			}

			gotContent := map[string]TarEntry{}

			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Error("unexpected http method")
				}
				if r.Header.Get("Content-Type") != "application/tar+gzip" {
					t.Error("unmatch header Content-Type.")
				}
				defer r.Body.Close()

				hreader := kio.NewMD5Reader(r.Body)
				gzreader := try.To(gzip.NewReader(hreader)).OrFatal(t)
				defer gzreader.Close()
				tarreader := tar.NewReader(gzreader)
				for {
					h, err := tarreader.Next()
					if errors.Is(err, io.EOF) {
						break
					} else if err != nil {
						t.Fatal(err)
					}
					content := try.To(io.ReadAll(tarreader)).OrFatal(t)
					gotContent[h.Name] = TarEntry{Header: h, Content: content}
				}

				checksum := r.Trailer.Get("x-checksum-md5")
				if checksum != hex.EncodeToString(hreader.Sum()) {
					t.Error("unmatch checksum.")
				}

				w.WriteHeader(http.StatusOK)
				m := try.To(json.Marshal(response)).OrFatal(t)
				w.Write(m)
			})

			ts := httptest.NewServer(h)
			defer ts.Close()

			profile := kprof.KnitProfile{ApiRoot: ts.URL}
			testee := try.To(krst.NewClient(&profile)).OrFatal(t)

			root := "./testdata/data/root"
			prog := testee.PostData(context.Background(), root, when.dereference)
			<-prog.Done()
			if err := prog.Error(); err != nil {
				t.Fatalf("unexpected result. error occured: %s", err)
			}
			gotResponse, ok := prog.Result()
			if !ok {
				t.Fatalf("unexpected result. it not failed: %s", prog.Error())
			}
			if !gotResponse.Equal(response) {
				t.Errorf("unexpected response: %v", gotResponse)
			}

			err := filepath.Walk(then.archiveAsLike, func(path string, _ os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				relpath, err := filepath.Rel(then.archiveAsLike, path)
				if err != nil {
					return err
				}
				stat := try.To(os.Lstat(path)).OrFatal(t)
				if stat.IsDir() {
					return nil
				}
				got, ok := gotContent[relpath]
				if !ok {
					t.Errorf("unexpected file:%s\n", relpath)
					return nil
				}

				if stat.Mode()&os.ModeSymlink != 0 {
					if got.Header.Typeflag != tar.TypeSymlink {
						t.Errorf("unexpected file type:%s\n", relpath)
					}
					linkname := try.To(os.Readlink(path)).OrFatal(t)
					if linkname != got.Header.Linkname {
						t.Errorf("unexpected link name:%s\n", relpath)
					}
					return nil
				}

				if stat.Mode() != os.FileMode(got.Header.Mode) {
					t.Errorf("unexpected file mode:%s\n", relpath)
				}
				if relpath != got.Header.Name {
					t.Errorf("unexpected file name:%s\n", relpath)
				}
				wantContent := try.To(os.ReadFile(path)).OrFatal(t)
				if !bytes.Equal(wantContent, got.Content) {
					t.Errorf("unexpected file content:%s\n", relpath)
				}

				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("when `PostData` is completed, it returns data in response (dereference link)", theory(
		When{dereference: true},
		Then{archiveAsLike: "./testdata/data/root-dereferenced"},
	))

	t.Run("when `PostData` is completed, it returns data in response (not dereference link)", theory(
		When{dereference: false},
		Then{archiveAsLike: "./testdata/data/root"},
	))

	t.Run("it fails to `PostData` when an invalid url is given. ", func(t *testing.T) {

		// prepare for the tests
		profile := kprof.KnitProfile{ApiRoot: "http://test.invalid"}

		ci := try.To(krst.NewClient(&profile)).OrFatal(t)

		tmp := try.To(os.MkdirTemp("", "knittest")).OrFatal(t)
		defer os.RemoveAll(tmp)

		content := make([]byte, 256)
		rand.Read(content)

		f := try.To(os.Create(filepath.Join(tmp, "pushdata"))).OrFatal(t)
		defer f.Close()

		f.Write(content)

		prog := ci.PostData(context.Background(), tmp, false)
		<-prog.Done()
		if err := prog.Error(); err == nil {
			t.Error("unexpected result. an error should be occured.")
		}
		_, ok := prog.Result()
		if ok {
			t.Error("unexpected result. channel is not closed.")
		}
	})

	t.Run("it fails to `PostData` when response body is unexpected format.", func(t *testing.T) {

		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			if r.Method != http.MethodPost {
				t.Error("unexpected http method")
			}
			if r.Header.Get("Content-Type") != "application/tar+gzip" {
				t.Error("unmatch header Content-Type.")
			}
			w.WriteHeader(http.StatusOK)
			w.Write(nil)
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		// prepare for the tests
		profile := kprof.KnitProfile{ApiRoot: ts.URL}

		ci := try.To(krst.NewClient(&profile)).OrFatal(t)

		tmp := t.TempDir()
		content := make([]byte, 256)
		rand.Read(content)

		f := try.To(os.Create(filepath.Join(tmp, "pushdata"))).OrFatal(t)
		defer f.Close()

		f.Write(content)

		prog := ci.PostData(context.Background(), tmp, false)
		<-prog.Done()
		if err := prog.Error(); err == nil {
			t.Error("unexpected result. an error should be occured.")
		}
		_, ok := prog.Result()
		if ok {
			t.Error("unexpected result. channel is not closed.")
		}
	})

	t.Run("it fails to `PostData`, when it is received a response with status code 500.", func(t *testing.T) {
		severError := "internal server error. unexpected error occured!!!!"
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			if r.Method != http.MethodPost {
				t.Error("unexpected http method")
			}
			if r.Header.Get("Content-Type") != "application/tar+gzip" {
				t.Error("unmatch header Content-Type.")
			}
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(severError))
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		// prepare for the tests
		profile := kprof.KnitProfile{ApiRoot: ts.URL}

		ci := try.To(krst.NewClient(&profile)).OrFatal(t)

		tmp := t.TempDir()

		content := make([]byte, 256)
		rand.Read(content)

		f := try.To(os.Create(filepath.Join(tmp, "pushdata"))).OrFatal(t)
		defer f.Close()

		f.Write(content)

		prog := ci.PostData(context.Background(), tmp, false)
		<-prog.Done()
		if err := prog.Error(); err == nil {
			t.Error("unexpected result. an error should be occured.")
		}
		_, ok := prog.Result()
		if ok {
			t.Error("unexpected result. channel is not closed.")
		}
	})

	t.Run("it fails to `PostData`, when it tries to send unexisting directory", func(t *testing.T) {
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Errorf("request should not be sent.")

			data := data.Summary{
				KnitId: "1234",
				Tags:   []tags.Tag{{Key: "keydata", Value: "valdata"}},
			}
			respBody := try.To(json.Marshal(data)).OrFatal(t)

			w.WriteHeader(http.StatusOK)
			w.Write(respBody)
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		// prepare for the tests
		profile := kprof.KnitProfile{ApiRoot: ts.URL}

		ci := try.To(krst.NewClient(&profile)).OrFatal(t)

		tmp := t.TempDir()
		prog := ci.PostData(context.Background(), filepath.Join(tmp, "no-such-directory"), false)
		<-prog.Done()
		if err := prog.Error(); err == nil {
			t.Error("unexpected result. an error should be occured.")
		}
		_, ok := prog.Result()
		if ok {
			t.Error("unexpected result. it is not failed")
		}
	})
}

func TestPutTagshWithKnitId(t *testing.T) {
	t.Run("when `PutTag` is completed it returns registered tag information.", func(t *testing.T) {

		expectedAddTags := []tags.UserTag{
			{Key: "tagkey1", Value: "tagval1"},
			{Key: "tagkey2", Value: "tagval2"},
		}
		expectedRemoveTags := []tags.UserTag{
			{Key: "tagkeyrem1", Value: "tagvalrem1"},
			{Key: "tagkeyrem2", Value: "tagvalrem2"},
		}
		expectedTags := []tags.Tag{
			{Key: "tagkey1", Value: "tagval1"},
			{Key: "tagkey2", Value: "tagval2"},
			{Key: "tagkeyrem1", Value: "tagvalrem1"},
			{Key: "tagkeyrem2", Value: "tagvalrem2"},
		}

		expected := data.Summary{KnitId: "1234", Tags: expectedTags}
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			if r.Method != http.MethodPut {
				t.Error("unexpected http method")
			}

			if r.Header.Get("Content-Type") != "application/json" {
				t.Error("unmatch header Content-Type.")
			}

			tags := tags.Change{}
			b := try.To(io.ReadAll(r.Body)).OrFatal(t)
			// compare request body
			if err := json.Unmarshal(b, &tags); err != nil {
				t.Error(err)
			}

			if !cmp.SliceContentEq(tags.AddTags, expectedAddTags) {
				t.Errorf("unmatch add tags:%s, expected:%s", tags, expectedAddTags)
			}

			if !cmp.SliceContentEq(tags.RemoveTags, expectedRemoveTags) {
				t.Errorf("unmatch remove tags:%s, expected:%s", tags, expectedRemoveTags)
			}
			// set response body
			body := try.To(json.Marshal(expected)).OrFatal(t)
			w.WriteHeader(http.StatusOK)
			w.Write(body)
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		// prepare for the tests
		profile := kprof.KnitProfile{ApiRoot: ts.URL}

		ci := try.To(krst.NewClient(&profile)).OrFatal(t)

		tags := tags.Change{}
		tags.AddTags = expectedAddTags
		tags.RemoveTags = expectedRemoveTags
		res := try.To(ci.PutTagsForData("1234", tags)).OrFatal(t)

		if expected.KnitId != res.KnitId {
			t.Errorf("unmatch knit id:%s, expected:%s", expected.KnitId, res.KnitId)
		}

		if !cmp.SliceContentEq(res.Tags, expected.Tags) {
			t.Errorf("unmatch request tags:%s, expected:%s", tags, expected)
		}
	})

	t.Run("it fails to `PutTag`, when it is given  a invalid url.", func(t *testing.T) {

		// prepare for the tests
		profile := kprof.KnitProfile{ApiRoot: "http://test.invalid"}

		ci := try.To(krst.NewClient(&profile)).OrFatal(t)

		tags := tags.Change{
			AddTags: []tags.UserTag{
				{Key: "tagkey1", Value: "tagval1"},
				{Key: "tagkey2", Value: "tagval2"},
			},
		}
		if _, err := ci.PutTagsForData("1234", tags); err == nil {
			t.Error("unexpected result. an error should be occured.")
		}

	})

	t.Run("it fails to `PutTag`, when it is received a invalid response body.", func(t *testing.T) {

		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut {
				t.Error("unexpected http method")
			}

			if r.Header.Get("Content-Type") != "application/json" {
				t.Error("unmatch header Content-Type.")
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("{{aa}"))
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		// prepare for the tests
		profile := kprof.KnitProfile{ApiRoot: ts.URL}

		ci := try.To(krst.NewClient(&profile)).OrFatal(t)

		tags := tags.Change{
			AddTags: []tags.UserTag{
				{Key: "tagkey1", Value: "tagval1"},
				{Key: "tagkey2", Value: "tagval2"},
			},
		}
		if _, err := ci.PutTagsForData("1234", tags); err == nil {
			t.Error("unexpected result. an error should be occured.")
		}

	})

	t.Run("it fails to `PutTag`, when it is received a response with status code 500.", func(t *testing.T) {
		severError := "internal server error. unexpected error occured!!!!"
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut {
				t.Error("unexpected http method")
			}

			if r.Header.Get("Content-Type") != "application/json" {
				t.Error("unmatch header Content-Type.")
			}

			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(severError))
		})
		ts := httptest.NewServer(h)
		defer ts.Close()

		// prepare for the tests
		profile := kprof.KnitProfile{ApiRoot: ts.URL}

		ci := try.To(krst.NewClient(&profile)).OrFatal(t)

		tags := tags.Change{
			AddTags: []tags.UserTag{
				{Key: "tagkey1", Value: "tagval1"},
				{Key: "tagkey2", Value: "tagval2"},
			},
		}
		if _, err := ci.PutTagsForData("1234", tags); err == nil {
			t.Error("unexpected result. an error should be occured.")
		}

	})
}

func TestFindData(t *testing.T) {
	t.Run("a server responding successfully	is given", func(t *testing.T) {
		handlerFactory := func(t *testing.T, resp []data.Detail) (http.Handler, func() *http.Request) {
			var request *http.Request
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Helper()

				request = r

				w.Header().Add("Content-Type", "application/json")

				buf := try.To(json.Marshal(resp)).OrFatal(t)
				w.Write(buf)
			})
			return h, func() *http.Request { return request }
		}

		type when struct {
			tags     []tags.Tag
			since    *time.Time
			duration *time.Duration
		}

		type then struct {
			tagsInQuery   []string
			sinceQuery    string
			durationQuery string
		}

		type testcase struct {
			when when
			then then
		}

		since := try.To(rfctime.ParseRFC3339DateTime("2024-04-22T12:34:56.987654321+07:00")).OrFatal(t).Time()
		duration := time.Duration(2 * time.Hour)

		for name, testcase := range map[string]testcase{
			"when query with no tags, server receives empty query string": {
				when: when{},
				then: then{
					tagsInQuery:   []string{},
					sinceQuery:    "",
					durationQuery: "",
				},
			},
			"when query with tags, server receives them": {
				when: when{
					tags: []tags.Tag{
						{Key: "key-a", Value: "value/a"},
						{Key: "type", Value: "unknown?"},
						{Key: "knit#id", Value: "some-knit-id"},
						{Key: "owner", Value: "100% our-team&client, of cource!"},
					},
					since:    &since,
					duration: &duration,
				},
				then: then{
					// metachar: '/' '#' '?' '&' '%', ' '(space) and ',
					tagsInQuery: []string{
						"key-a:value/a",
						"type:unknown?",
						"knit#id:some-knit-id",
						"owner:100% our-team&client, of cource!",
					},
					sinceQuery:    "2024-04-22T12:34:56.987654321+07:00",
					durationQuery: "2h0m0s",
				},
			},
		} {
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				response := []data.Detail{} //empty. this is out of scope of these testcases.

				handler, getLastRequest := handlerFactory(t, response)
				ts := httptest.NewServer(handler)
				defer ts.Close()

				// prepare for the tests
				profile := kprof.KnitProfile{ApiRoot: ts.URL}

				when := testcase.when
				then := testcase.then

				testee := try.To(krst.NewClient(&profile)).OrFatal(t)
				result := try.To(testee.FindData(ctx, when.tags, when.since, when.duration)).OrFatal(t)

				if !cmp.SliceContentEqWith(result, response, data.Detail.Equal) {
					t.Errorf(
						"response is wrong:\n- actual:\n%#v\n- expected:\n%#v",
						result, response,
					)
				}

				actualMethod := getLastRequest().Method
				if actualMethod != http.MethodGet {
					t.Errorf("wrong HTTP method: %s (!= %s )", actualMethod, http.MethodGet)
				}

				actualTags := getLastRequest().URL.Query()["tag"]
				if !cmp.SliceContentEq(actualTags, then.tagsInQuery) {
					t.Errorf(
						"query tags is wrong:\n- actual  : %s\n- expected: %s",
						actualTags, then.tagsInQuery,
					)
				}

			})
		}

		t.Run("when server returns data, it returns that as is", func(t *testing.T) {
			ctx := context.Background()

			expectedResponse := []data.Detail{
				{
					KnitId: "model-parameter-1",
					Tags: []tags.Tag{
						{Key: "knit#id", Value: "model-parameter-1"},
						{Key: "knit#timestamp", Value: "2022-04-01T12:34:56+00:00"},
						{Key: "type", Value: "model-paramter"},
						{Key: "task", Value: "LLM"},
						{Key: "tag-a", Value: "value-a"},
					},
					Upstream: data.CreatedFrom{
						Run: runs.Summary{
							RunId: "parent-run", Status: string(domain.Done),
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-04-02T12:00:00+00:00",
							)).OrFatal(t),
							Plan: plans.Summary{
								PlanId: "plan-trainer",
								Image:  &plans.Image{Repository: "image-name", Tag: "v0.0.1"},
							},
						},
						Mountpoint: &plans.Mountpoint{
							Path: "/out/model",
							Tags: []tags.Tag{
								{Key: "type", Value: "model-paramter"},
								{Key: "task", Value: "LLM"},
							},
						},
					},
					Downstreams: []data.AssignedTo{
						{
							Run: runs.Summary{
								RunId: "evaluator-1-1", Status: string(domain.Done),
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
									"2022-04-03T12:00:00+00:00",
								)).OrFatal(t),
								Plan: plans.Summary{
									PlanId: "plan-evaluate",
									Image:  &plans.Image{Repository: "child-image-1", Tag: "0.1-alpha"},
								},
							},
							Mountpoint: plans.Mountpoint{
								Path: "/in/model",
								Tags: []tags.Tag{
									{Key: "type", Value: "model-paramter"},
									{Key: "task", Value: "LLM"},
								},
							},
						},
						{
							Run: runs.Summary{
								RunId: "evaluator-1-2", Status: string(domain.Running),
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
									"2022-04-04T12:00:00+00:00",
								)).OrFatal(t),
								Plan: plans.Summary{
									PlanId: "plan-evaluate",
									Image:  &plans.Image{Repository: "child-image-1", Tag: "0.1-alpha"},
								},
							},
							Mountpoint: plans.Mountpoint{
								Path: "/in/model",
								Tags: []tags.Tag{
									{Key: "task", Value: "LLM"},
								},
							},
						},
					},
					Nomination: []data.NominatedBy{
						{
							Mountpoint: plans.Mountpoint{Path: "/in/model"},
							Plan: plans.Summary{
								PlanId: "plan-evaluate",
								Image:  &plans.Image{Repository: "child-image-1", Tag: "0.1-alpha"},
							},
						},
					},
				},
				{
					KnitId: "model-parameter-2",
					Tags: []tags.Tag{
						{Key: "knit#id", Value: "model-parameter-2"},
						{Key: "knit#timestamp", Value: "2022-07-20T13:57:20+00:00"},
						{Key: "tag-a", Value: "value-a"},
						{Key: "tag-b", Value: "value-b"},
					},
					Upstream: data.CreatedFrom{
						Run: runs.Summary{
							RunId: "parent-run", Status: string(domain.Done),
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
								"2022-04-02T12:00:00+00:00",
							)).OrFatal(t),
							Plan: plans.Summary{
								PlanId: "plan-trainer",
								Image:  &plans.Image{Repository: "image-name", Tag: "v0.0.1"},
							},
						},
						Mountpoint: &plans.Mountpoint{
							Path: "/out/model",
							Tags: []tags.Tag{
								{Key: "type", Value: "model-paramter"},
								{Key: "task", Value: "LLM"},
							},
						},
					},
					Downstreams: []data.AssignedTo{
						{
							Run: runs.Summary{
								RunId: "evaluator-2-1", Status: string(domain.Running),
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
									"2022-04-02T12:30:00+00:00",
								)).OrFatal(t),
								Plan: plans.Summary{
									PlanId: "plan-evaluate",
									Image:  &plans.Image{Repository: "child-image-1", Tag: "0.1-alpha"},
								},
							},
							Mountpoint: plans.Mountpoint{
								Path: "/in/model",
								Tags: []tags.Tag{
									{Key: "task", Value: "LLM"},
								},
							},
						},
						{
							Run: runs.Summary{
								RunId: "evaluator-2-2", Status: string(domain.Running),
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
									"2022-04-02T12:30:00+00:00",
								)).OrFatal(t),
								Plan: plans.Summary{
									PlanId: "plan-evaluate",
									Image:  &plans.Image{Repository: "child-image-1", Tag: "0.1-alpha"},
								},
							},
							Mountpoint: plans.Mountpoint{
								Path: "/in/model",
								Tags: []tags.Tag{
									{Key: "task", Value: "LLM"},
								},
							},
						},
						{
							Run: runs.Summary{
								RunId: "done-hook", Status: string(domain.Done),
								UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
									"2022-04-02T12:30:00+00:00",
								)).OrFatal(t),
								Plan: plans.Summary{
									PlanId: "notifier",
									Image:  &plans.Image{Repository: "slack-notifier", Tag: "beta"},
								},
							},
							Mountpoint: plans.Mountpoint{
								Path: "/in/trigger",
								Tags: []tags.Tag{
									{Key: "type", Value: "model-paramter"},
									{Key: "task", Value: "LLM"},
								},
							},
						},
					},
				},
			}

			handler, _ := handlerFactory(t, expectedResponse)

			ts := httptest.NewServer(handler)
			defer ts.Close()

			// prepare for the tests
			profile := kprof.KnitProfile{ApiRoot: ts.URL}
			queryTags := []tags.Tag{
				{Key: "tag-a", Value: "value-a"},
			}

			since := try.To(rfctime.ParseRFC3339DateTime("2024-04-22T12:34:56.987654321+07:00")).OrFatal(t).Time()
			duration := time.Duration(2 * time.Hour)

			testee := try.To(krst.NewClient(&profile)).OrFatal(t)
			actualResponse := try.To(testee.FindData(ctx, queryTags, &since, &duration)).OrFatal(t)

			if !cmp.SliceContentEqWith(actualResponse, expectedResponse, data.Detail.Equal) {
				t.Errorf(
					"response is in unexpected form:\n===actual===\n%+v\n===expected===\n%+v",
					actualResponse, expectedResponse,
				)
			}
		})
	})

	t.Run("a server responding with error is given", func(t *testing.T) {
		handerFactory := func(t *testing.T, status int, message string) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Helper()
				w.Header().Add("Content-Type", "application/json")
				w.WriteHeader(status)

				buf := try.To(json.Marshal(
					apierr.ErrorMessage{Reason: message},
				)).OrFatal(t)
				w.Write(buf)
			})
		}

		for _, status := range []int{http.StatusBadRequest, http.StatusInternalServerError} {
			t.Run(fmt.Sprintf("when server responding with %d, it returns error", status), func(t *testing.T) {
				ctx := context.Background()
				handler := handerFactory(t, status, "something wrong")

				ts := httptest.NewServer(handler)
				defer ts.Close()

				// prepare for the tests
				profile := kprof.KnitProfile{ApiRoot: ts.URL}

				testee := try.To(krst.NewClient(&profile)).OrFatal(t)

				queryTags := []tags.Tag{
					{Key: "tag-a", Value: "value-a"},
				}

				since := try.To(rfctime.ParseRFC3339DateTime("2024-04-22T12:34:56.987654321+07:00")).OrFatal(t).Time()
				duration := time.Duration(2 * time.Hour)

				if _, err := testee.FindData(ctx, queryTags, &since, &duration); err == nil {
					t.Errorf("no error occured")
				}

			})
		}
	})

}

func TestGetData(t *testing.T) {
	var targz []byte
	contents := map[string]string{}
	func() {
		// create tar archive with gzip
		temp := t.TempDir()
		{
			path := filepath.Join(temp, "data1")
			relpath := try.To(filepath.Rel(temp, path)).OrFatal(t)
			f1 := try.To(os.Create(path)).OrFatal(t)
			defer f1.Close()
			content := "content1"
			io.WriteString(f1, content)
			contents[relpath] = content
		}
		{
			path := filepath.Join(temp, "data2")
			relpath := try.To(filepath.Rel(temp, path)).OrFatal(t)
			f2 := try.To(os.Create(path)).OrFatal(t)
			defer f2.Close()
			content := "content2"
			io.WriteString(f2, content)
			contents[relpath] = content
		}
		{
			path := filepath.Join(temp, "dir1")
			if err := os.Mkdir(path, 0755); err != nil {
				t.Fatal(err)
			}
			path = filepath.Join(path, "data3")
			relpath := try.To(filepath.Rel(temp, path)).OrFatal(t)
			f3 := try.To(os.Create(path)).OrFatal(t)
			defer f3.Close()
			content := "content3"
			io.WriteString(f3, content)
			contents[relpath] = content
		}

		ctx := context.Background()
		buf := new(bytes.Buffer)
		gz := gzip.NewWriter(buf)
		defer func() {
			if err := gz.Close(); err != nil {
				t.Fatal(err)
			}
			targz = buf.Bytes()
		}()
		p := archive.GoTar(ctx, temp, gz)
		<-p.Done()
		if err := p.Error(); err != nil {
			t.Fatal(err)
		}
	}()

	t.Run("when server response with 200 in chunked, it returns the stream in response", func(t *testing.T) {

		knitId := "someKnitId"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("request is not GET /api/data/:knitId (actual method = %s)", r.Method)
			}
			if !strings.HasSuffix(r.URL.Path, "/data/"+knitId) {
				t.Errorf("request is not GET /api/data/:knitId (actual path = %s)", r.URL.Path)
			}

			w.Header().Add("Transfer-Encoding", "chunked")
			w.Header().Add("Trailer", "x-checksum-md5")
			w.WriteHeader(http.StatusOK)

			hasher := md5.New()
			hasher.Write(targz)
			checksum := hex.EncodeToString(hasher.Sum(nil))
			w.Header().Add("x-checksum-md5", checksum)

			w.Write(targz)
		}))
		defer server.Close()

		prof := kprof.KnitProfile{ApiRoot: server.URL}

		testee := try.To(krst.NewClient(&prof)).OrFatal(t)

		ctx := context.Background()
		err := testee.GetData(ctx, knitId, func(fe krst.FileEntry) error {
			head := fe.Header
			actual, err := io.ReadAll(fe.Body)
			if err != nil {
				return err
			}
			if a := string(actual); a != contents[head.Name] {
				t.Errorf(
					"unmatch content: %s\n===actual===\n%s\n===expected===\n%s",
					head.Name, a, contents[head.Name],
				)
			}

			return nil

		})
		if err != nil {
			t.Fatalf("GetData has returned error: %s", err)
		}
	})

	t.Run("When checksum in response is wrong, promise resolves with ErrChecksumUnmatch error ", func(t *testing.T) {
		knitId := "someKnitId"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("request is not GET /api/data/:knitId (actual method = %s)", r.Method)
			}
			if !strings.HasSuffix(r.URL.Path, "/data/"+knitId) {
				t.Errorf("request is not GET /api/data/:knitId (actual path = %s)", r.URL.Path)
			}

			w.Header().Add("Transfer-Encoding", "chunked")
			w.Header().Add("Trailer", "x-checksum-md5")
			w.WriteHeader(http.StatusOK)

			hasher := md5.New()
			hasher.Write([]byte("wrong:"))
			hasher.Write(targz)
			checksum := hasher.Sum(nil)
			w.Header().Add("x-checksum-md5", hex.EncodeToString(checksum))

			w.Write(targz)
		}))
		defer server.Close()

		prof := kprof.KnitProfile{ApiRoot: server.URL}

		testee := try.To(krst.NewClient(&prof)).OrFatal(t)

		ctx := context.Background()
		err := testee.GetData(ctx, knitId, func(fe krst.FileEntry) error {
			head := fe.Header
			actual, err := io.ReadAll(fe.Body)
			if err != nil {
				return err
			}
			if a := string(actual); a != contents[head.Name] {
				t.Errorf(
					"unmatch content: %s\n===actual===\n%s\n===expected===\n%s",
					head.Name, a, contents[head.Name],
				)
			}

			return nil
		})
		if !errors.Is(err, krst.ErrChecksumUnmatch) {
			t.Fatalf(
				"GetData has returned unexpected error: %s (want %s)",
				err, krst.ErrChecksumUnmatch,
			)
		}
	})

	t.Run("When send request to invalid host, it returns error", func(t *testing.T) {
		knitId := "someKnitId"
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		prof := kprof.KnitProfile{ApiRoot: "http://test.invalid"}

		testee := try.To(krst.NewClient(&prof)).OrFatal(t)

		err := testee.GetData(ctx, knitId, func(fe krst.FileEntry) error {
			t.Error("callback is called")
			return nil
		})

		if err == nil {
			t.Errorf("GetData does not return error")
		}
	})

	t.Run("When handler returns error, it returns error", func(t *testing.T) {
		knitId := "someKnitId"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("request is not GET /api/data/:knitId (actual method = %s)", r.Method)
			}
			if !strings.HasSuffix(r.URL.Path, "/data/"+knitId) {
				t.Errorf("request is not GET /api/data/:knitId (actual path = %s)", r.URL.Path)
			}

			w.Header().Add("Transfer-Encoding", "chunked")
			w.Header().Add("Trailer", "x-checksum-md5")
			w.WriteHeader(http.StatusOK)

			hasher := md5.New()
			hasher.Write([]byte("wrong:"))
			hasher.Write(targz)
			checksum := hasher.Sum(nil)
			w.Header().Add("x-checksum-md5", hex.EncodeToString(checksum))

			w.Write(targz)
		}))
		defer server.Close()

		prof := kprof.KnitProfile{ApiRoot: server.URL}
		testee := try.To(krst.NewClient(&prof)).OrFatal(t)

		expectedErr := errors.New("some error")
		ctx := context.Background()
		err := testee.GetData(ctx, knitId, func(fe krst.FileEntry) error {
			return expectedErr
		})

		if !errors.Is(err, expectedErr) {
			t.Errorf(
				"unexpected error is returned:\n- actual: %s\n- expected: %s",
				err, expectedErr,
			)
		}
	})
}

func TestGetDataRaw(t *testing.T) {
	t.Run("when server response with 200 in chunked", func(t *testing.T) {

		knitId := "someKnitId"
		payload := []byte("response payload")

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("request is not GET /api/data/:knitId (actual method = %s)", r.Method)
			}
			if !strings.HasSuffix(r.URL.Path, "/data/"+knitId) {
				t.Errorf("request is not GET /api/data/:knitId (actual path = %s)", r.URL.Path)
			}

			w.Header().Add("Transfer-Encoding", "chunked")
			w.Header().Add("Trailer", "x-checksum-md5")
			w.WriteHeader(http.StatusOK)

			hasher := md5.New()
			hasher.Write(payload)
			checksum := hex.EncodeToString(hasher.Sum(nil))
			w.Header().Add("x-checksum-md5", checksum)

			w.Write(payload)
		}))
		defer server.Close()

		prof := kprof.KnitProfile{ApiRoot: server.URL}

		testee := try.To(krst.NewClient(&prof)).OrFatal(t)

		ctx := context.Background()
		err := testee.GetDataRaw(ctx, knitId, func(r io.Reader) error {
			actual, err := io.ReadAll(r)
			if err != nil {
				return err
			}
			if !bytes.Equal(actual, payload) {
				t.Errorf(
					"unmatch content:\n- actual: %s\n- expected: %s",
					string(actual), string(payload),
				)
			}

			return nil

		})
		if err != nil {
			t.Errorf("GetData has returned error: %s", err)
		}
	})

	t.Run("when server response with 200 in chunked with wrong checksum", func(t *testing.T) {

		knitId := "someKnitId"
		payload := []byte("response payload")

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("request is not GET /api/data/:knitId (actual method = %s)", r.Method)
			}
			if !strings.HasSuffix(r.URL.Path, "/data/"+knitId) {
				t.Errorf("request is not GET /api/data/:knitId (actual path = %s)", r.URL.Path)
			}

			w.Header().Add("Transfer-Encoding", "chunked")
			w.Header().Add("Trailer", "x-checksum-md5")
			w.WriteHeader(http.StatusOK)

			hasher := md5.New()
			hasher.Write([]byte("wrong "))
			hasher.Write(payload)
			checksum := hex.EncodeToString(hasher.Sum(nil))
			w.Header().Add("x-checksum-md5", checksum)

			w.Write(payload)
		}))
		defer server.Close()

		prof := kprof.KnitProfile{ApiRoot: server.URL}

		testee := try.To(krst.NewClient(&prof)).OrFatal(t)

		ctx := context.Background()
		err := testee.GetDataRaw(ctx, knitId, func(r io.Reader) error {
			actual, err := io.ReadAll(r)
			if err != nil {
				return err
			}
			if !bytes.Equal(actual, payload) {
				t.Errorf(
					"unmatch content:\n- actual: %s\n- expected: %s",
					string(actual), string(payload),
				)
			}

			return nil

		})
		if !errors.Is(err, krst.ErrChecksumUnmatch) {
			t.Errorf(
				"GetData returns unexpected error: %s (want: %s)",
				err, krst.ErrChecksumUnmatch,
			)
		}
	})

	t.Run("When handler returns error, it returns error", func(t *testing.T) {
		knitId := "someKnitId"
		payload := []byte("response payload")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("request is not GET /api/data/:knitId (actual method = %s)", r.Method)
			}
			if !strings.HasSuffix(r.URL.Path, "/data/"+knitId) {
				t.Errorf("request is not GET /api/data/:knitId (actual path = %s)", r.URL.Path)
			}

			w.Header().Add("Transfer-Encoding", "chunked")
			w.Header().Add("Trailer", "x-checksum-md5")
			w.WriteHeader(http.StatusOK)

			hasher := md5.New()
			hasher.Write([]byte("wrong "))
			hasher.Write(payload)
			checksum := hex.EncodeToString(hasher.Sum(nil))
			w.Header().Add("x-checksum-md5", checksum)

			w.Write(payload)
		}))
		defer server.Close()

		prof := kprof.KnitProfile{ApiRoot: server.URL}

		testee := try.To(krst.NewClient(&prof)).OrFatal(t)

		ctx := context.Background()
		expectedErr := errors.New("some error")
		err := testee.GetDataRaw(ctx, knitId, func(r io.Reader) error {
			return expectedErr

		})
		if !errors.Is(err, expectedErr) {
			t.Errorf(
				"GetData returns unexpected error: %s (want: %s)",
				err, expectedErr,
			)
		}
	})

	t.Run("When send request to invalid host, it returns error", func(t *testing.T) {
		knitId := "someKnitId"
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		prof := kprof.KnitProfile{ApiRoot: "http://test.invalid"}

		testee := try.To(krst.NewClient(&prof)).OrFatal(t)

		err := testee.GetDataRaw(ctx, knitId, func(r io.Reader) error {
			t.Error("callback is called")
			return nil
		})

		if err == nil {
			t.Errorf("GetData does not return error")
		}
	})
}
