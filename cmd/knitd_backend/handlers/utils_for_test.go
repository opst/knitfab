package handlers_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/cmp"
	kio "github.com/opst/knitfab/pkg/utils/io"
	"github.com/opst/knitfab/pkg/workloads/dataagt"
	"github.com/opst/knitfab/pkg/workloads/k8s"
	"github.com/opst/knitfab/pkg/workloads/worker"
)

type responseDescriptor struct {
	code    int
	header  map[string][]string
	body    []byte
	trailer *map[string][]string
}

// Write response as its description
func (r *responseDescriptor) Write(resp http.ResponseWriter) {
	for name, vs := range r.header {
		for _, v := range vs {
			resp.Header().Add(name, v)
		}
	}
	resp.WriteHeader(r.code)
	resp.Write(r.body)
	if r.trailer == nil {
		return
	}

	for name, vs := range *r.trailer {
		for _, v := range vs {
			resp.Header().Add(name, v)
		}
	}
}

func (r *responseDescriptor) WriteAsTarGzContainsSingleFile(fileName string, resp http.ResponseWriter) {
	chw := kio.NewMD5Writer(resp)

	for name, vs := range r.header {
		for _, v := range vs {
			resp.Header().Add(name, v)
		}
	}
	resp.WriteHeader(r.code)

	resp.Header().Add("trailer", "gzip")
	if r.trailer != nil {
		for name, vs := range *r.trailer {
			for _, v := range vs {
				resp.Header().Add("trailer", name)
				resp.Header().Add(name, v)
			}
		}
	}

	gz := gzip.NewWriter(chw)
	defer gz.Close()
	tarWriter := tar.NewWriter(gz)
	defer tarWriter.Close()

	if err := tarWriter.WriteHeader(&tar.Header{
		Name:    fileName,
		Mode:    int64(1),
		ModTime: time.Date(2023, time.March, 8, 01, 02, 03, 0, time.UTC),
		Size:    int64(len(r.body)),
	}); err != nil {
		return
	}
	if _, err := io.Copy(tarWriter, bytes.NewReader(r.body)); err != nil {
		return
	}
	resp.Header().Add("x-checksum-md5", hex.EncodeToString(chw.Sum()))

}

type requestSnapshot struct {
	header  map[string][]string
	body    []byte
	chunked bool
	trailer map[string][]string
}

// capture http request to requestSnapshot
//
// keys of header and trailer are lower-cased.
func Read(req *http.Request) (*requestSnapshot, error) {
	header := map[string][]string{}
	for name, vs := range req.Header {
		n := strings.ToLower(name)
		entry, ok := header[n]
		if !ok {
			header[n] = []string{}
			entry = header[n]
		}
		header[n] = append(entry, vs...)
	}

	var chunked bool
	if _, ok := utils.First(req.TransferEncoding, func(s string) bool { return s == "chunked" }); ok {
		chunked = true
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	trailer := map[string][]string{}
	for name, vs := range req.Trailer {
		n := strings.ToLower(name)
		entry, ok := trailer[n]
		if !ok {
			trailer[n] = []string{}
			entry = trailer[n]
		}

		trailer[n] = append(entry, vs...)
	}

	return &requestSnapshot{
		header:  header,
		body:    body,
		trailer: trailer,
		chunked: chunked,
	}, nil
}

type respmatch struct {
	code        bool
	header      bool
	body        bool
	trailer     bool
	bodyPayload []byte
}

func (m *respmatch) Match() bool {
	return m.code && m.header && m.body && m.trailer
}

func ResponseEq(resp httptest.ResponseRecorder, expected responseDescriptor) (*respmatch, error) {
	bodypayload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	match := &respmatch{
		code:        resp.Result().StatusCode == expected.code,
		header:      cmp.MapGeqWith(resp.Result().Header, expected.header, cmp.SliceContentEq[string]),
		body:        bytes.Equal(bodypayload, expected.body),
		bodyPayload: bodypayload,
	}
	if expected.trailer == nil {
		match.trailer = len(resp.Result().Trailer) == 0
	} else {
		match.trailer = (resp.Result().Trailer != nil && cmp.MapGeqWith(
			resp.Result().Trailer,
			*expected.trailer,
			cmp.SliceContentEq[string],
		))
	}

	return match, nil
}

type CallLog[T any] struct {
	Args []T
}

func (l CallLog[T]) Times() uint {
	return uint(len(l.Args))
}

type MockedDataagt struct {
	Impl struct {
		Namespace func() string
		Name      func() string
		Host      func() string
		IP        func() string
		Port      func(name string) int32
		Close     func() error
		APIPort   func() int32
		URL       func() string
		Mode      func() kdb.DataAgentMode
		KnitID    func() string
		VolumeRef func() string
		String    func() string
	}
	Calls struct {
		Namespace CallLog[any]
		Name      CallLog[any]
		Host      CallLog[any]
		IP        CallLog[any]
		Port      CallLog[string]
		Close     CallLog[any]
		APIPort   CallLog[any]
		URL       CallLog[any]
		Mode      CallLog[any]
		KnitID    CallLog[any]
		VolumeRef CallLog[any]
		String    CallLog[any]
	}
}

func NewMockedDataagt(svr *httptest.Server) *MockedDataagt {
	da := &MockedDataagt{}

	// svr.URL is shaped in "<SCHEME>://<HOST>:<PORT>"
	_, endpoint, _ := strings.Cut(svr.URL, "://")
	host, port, _ := strings.Cut(endpoint, ":")
	porti, err := strconv.Atoi(port)
	if err != nil {
		panic(err)
	}
	da.Impl.Host = func() string { return host }
	da.Impl.APIPort = func() int32 { return int32(porti) }
	da.Impl.URL = func() string { return fmt.Sprintf("http://%s:%d/", host, int32(porti)) }
	return da
}

// create Dataagt points "example.invalid:80".
func NewBrokenDataagt() *MockedDataagt {
	da := &MockedDataagt{}
	da.Impl.Host = func() string { return "example.invalid" }
	da.Impl.APIPort = func() int32 { return int32(80) }
	da.Impl.URL = func() string { return fmt.Sprintf("http://%s:%d/", da.Host(), da.APIPort()) }

	return da
}

var _ dataagt.Dataagt = &MockedDataagt{}

func (m *MockedDataagt) Namespace() string {
	m.Calls.Namespace.Args = append(m.Calls.Namespace.Args, nil)
	if m.Impl.Namespace != nil {
		return m.Impl.Namespace()
	}
	panic(errors.New("it should not be called"))
}

func (m *MockedDataagt) Name() string {
	m.Calls.Name.Args = append(m.Calls.Name.Args, nil)
	if m.Impl.Name != nil {
		return m.Impl.Name()
	}
	panic(errors.New("it should not be called"))
}

func (m *MockedDataagt) Host() string {
	m.Calls.Host.Args = append(m.Calls.Host.Args, nil)
	if m.Impl.Host != nil {
		return m.Impl.Host()
	}
	panic(errors.New("it should not be called"))
}

func (m *MockedDataagt) IP() string {
	m.Calls.IP.Args = append(m.Calls.IP.Args, nil)
	if m.Impl.IP != nil {
		return m.Impl.IP()
	}
	panic(errors.New("it should not be called"))
}

func (m *MockedDataagt) Port(name string) int32 {
	m.Calls.Port.Args = append(m.Calls.Port.Args, name)
	if m.Impl.Port != nil {
		return m.Impl.Port(name)
	}
	panic(errors.New("it should not be called"))
}

func (m *MockedDataagt) Close() error {
	m.Calls.Close.Args = append(m.Calls.Close.Args, nil)
	if m.Impl.Close != nil {
		return m.Impl.Close()
	}
	panic(errors.New("it should not be called"))
}

func (m *MockedDataagt) APIPort() int32 {
	m.Calls.APIPort.Args = append(m.Calls.APIPort.Args, nil)
	if m.Impl.APIPort != nil {
		return m.Impl.APIPort()
	}
	panic(errors.New("it should not be called"))
}

func (m *MockedDataagt) URL() string {
	m.Calls.URL.Args = append(m.Calls.URL.Args, nil)
	if m.Impl.URL != nil {
		return m.Impl.URL()
	}
	panic(errors.New("it should not be called"))
}

func (m *MockedDataagt) Mode() kdb.DataAgentMode {
	m.Calls.Mode.Args = append(m.Calls.Mode.Args, nil)
	if m.Impl.Mode != nil {
		return m.Impl.Mode()
	}
	panic(errors.New("it should not be called"))
}

func (m *MockedDataagt) KnitID() string {
	m.Calls.KnitID.Args = append(m.Calls.KnitID.Args, nil)
	if m.Impl.KnitID != nil {
		return m.Impl.KnitID()
	}
	panic(errors.New("it should not be called"))
}

func (m *MockedDataagt) VolumeRef() string {
	m.Calls.VolumeRef.Args = append(m.Calls.VolumeRef.Args, nil)
	if m.Impl.VolumeRef != nil {
		return m.Impl.VolumeRef()
	}
	panic(errors.New("it should not be called"))
}

func (m *MockedDataagt) String() string {
	m.Calls.String.Args = append(m.Calls.String.Args, nil)
	if m.Impl.String != nil {
		return m.Impl.String()
	}
	panic(errors.New("it should not be called"))
}

type mockWorker struct {
	Impl struct {
		RunId     func() string
		JobStatus func() k8s.JobStatus
		ExitCode  func() (uint8, string, bool)
		Log       func(context.Context) (io.ReadCloser, error)
		Close     func() error
	}
	Calls struct {
		RunId     CallLog[any]
		JobStatus CallLog[any]
		ExitCode  CallLog[any]
		Log       CallLog[any]
		Close     CallLog[any]
	}
}

func NewMockWorker() *mockWorker {
	return &mockWorker{}
}

var _ worker.Worker = &mockWorker{}

func (m *mockWorker) RunId() string {
	m.Calls.RunId.Args = append(m.Calls.RunId.Args, nil)
	if m.Impl.RunId != nil {
		return m.Impl.RunId()
	}
	panic(errors.New("it should not be called"))
}

func (m *mockWorker) JobStatus(ctx context.Context) k8s.JobStatus {
	m.Calls.JobStatus.Args = append(m.Calls.JobStatus.Args, nil)
	if m.Impl.JobStatus != nil {
		return m.Impl.JobStatus()
	}
	panic(errors.New("it should not be called"))
}

func (m *mockWorker) Log(ctx context.Context) (io.ReadCloser, error) {
	m.Calls.Log.Args = append(m.Calls.Log.Args, nil)
	if m.Impl.Log != nil {
		return m.Impl.Log(ctx)
	}
	panic(errors.New("it should not be called"))
}

func (m *mockWorker) Close() error {
	m.Calls.Close.Args = append(m.Calls.Close.Args, nil)
	if m.Impl.Close != nil {
		return m.Impl.Close()
	}
	panic(errors.New("it should not be called"))
}
