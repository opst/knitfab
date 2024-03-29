package rest

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	apidata "github.com/opst/knitfab/pkg/api/types/data"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	"github.com/opst/knitfab/pkg/archive"
	kio "github.com/opst/knitfab/pkg/io"
)

var (
	ErrChecksumUnmatch = errors.New("checksum unmatch")
)

type Progress[T any] interface {
	// EstimatedTotalSize returns the total size of files to be archived.
	//
	// This is estimated and not compressed size.
	EstimatedTotalSize() int64

	// ProgressedSize returns the size of archived files.
	//
	// This size is updated during archiving.
	//
	// This is raw (not compressed) size.
	ProgressedSize() int64

	// ProgressingFile returns the file name which is currently being archived.
	ProgressingFile() string

	// Error returns error caused during archiving.
	Error() error

	// Result returns the result of the operation.
	//
	// # Returns
	//
	// - T: the result of the operation.
	//
	// - bool: true if the operation has been done.
	Result() (T, bool)

	// Done returns a channel which is closed when progressing task is over.
	Done() <-chan struct{}

	// Sent returns a chennel which is closed when the data is sent to the server.
	Sent() <-chan struct{}
}

type progress struct {
	p        archive.Progress
	e        error
	result   *apidata.Detail
	resultOk bool
	done     chan struct{}
	sent     chan struct{}
}

func (p *progress) EstimatedTotalSize() int64 {
	return p.p.EstimatedTotalSize()
}

func (p *progress) ProgressedSize() int64 {
	return p.p.ProgressedSize()
}

func (p *progress) ProgressingFile() string {
	return p.p.ProgressingFile()
}

func (p *progress) Error() error {
	if err := p.p.Error(); err != nil {
		return err
	}
	return p.e
}

func (p *progress) Result() (*apidata.Detail, bool) {
	return p.result, p.resultOk
}

func (p *progress) Done() <-chan struct{} {
	return p.done
}

func (p *progress) Sent() <-chan struct{} {
	return p.sent
}

func (c *client) PostData(ctx context.Context, source string, dereference bool) Progress[*apidata.Detail] {
	ctx, cancel := context.WithCancel(ctx)

	started := false
	r, w := io.Pipe()
	defer func() {
		if !started {
			r.Close()
			w.Close()
		}
	}()

	md5writer := kio.NewMD5Writer(w)
	gzwriter := gzip.NewWriter(md5writer)
	taropts := []archive.TarOption{}
	if dereference {
		taropts = append(taropts, archive.FollowSymlinks())
	}
	prog := &progress{
		sent: make(chan struct{}, 1),
		done: make(chan struct{}, 1),
		p:    archive.GoTar(ctx, source, gzwriter, taropts...),
	}

	treader := kio.NewTriggerReader(r)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apipath("data"), treader)
	if err != nil {
		cancel()
		prog.e = err
		return prog
	}
	treader.OnEnd(func() {
		req.Trailer.Add("x-checksum-md5", hex.EncodeToString(md5writer.Sum()))
		close(prog.sent)
	})

	req.Trailer = http.Header{}
	req.Header.Add("Content-Type", "application/tar+gzip")
	req.Header.Add("Transfer-Encoding", "chunked")
	req.Header.Add("Trailer", "x-checksum-md5")

	go func() {
		<-prog.p.Done()
		if err := prog.p.Error(); err != nil {
			cancel()
		}
		gzwriter.Close()
		w.Close()
	}()

	started = true
	go func() {
		defer close(prog.done)
		defer r.Close()

		// send data to api/data.
		resp, err := c.httpclient.Do(req)
		if err != nil {
			prog.e = err
			return
		}
		defer resp.Body.Close()

		res := &apidata.Detail{}
		if err := unmarshalJsonResponse(
			resp, res,
			MessageFor{
				Status4xx: fmt.Sprintf("sending data is rejected by server (status code = %d)", resp.StatusCode),
				Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
			},
		); err != nil {
			prog.e = err
			return
		}

		prog.result = res
		prog.resultOk = true
	}()

	return prog
}

func (c *client) PutTagsForData(knitId string, tags apitag.Change) (*apidata.Detail, error) {

	reqBody, err := json.Marshal(tags)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPut, c.apipath("data", knitId), bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	res := apidata.Detail{}
	if err := unmarshalJsonResponse(
		resp, &res,
		MessageFor{
			Status4xx: fmt.Sprintf("tagging data is rejected by server (status code = %d)", resp.StatusCode),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return nil, err
	}

	return &res, nil
}

func (ci *client) GetDataRaw(ctx context.Context, knitId string, handler func(io.Reader) error) error {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, ci.apipath("data", knitId), nil,
	)
	if err != nil {
		return err
	}

	resp, err := ci.httpclient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	r, err := unmarshalStreamResponse(
		resp,
		MessageFor{
			Status4xx: fmt.Sprintf("tagging data is rejected by server (status code = %d)", resp.StatusCode),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	)
	if err != nil {
		return err
	}

	chr := kio.NewMD5Reader(r)
	tr := kio.NewTriggerReader(chr)
	var hasherr error
	tr.OnEnd(func() {
		serverChecksum := resp.Trailer.Get("x-checksum-md5")
		if serverChecksum == "" {
			err = fmt.Errorf("%w: server response is incompleted", ErrChecksumUnmatch)
			return
		}

		actualChecksum := hex.EncodeToString(chr.Sum())
		if serverChecksum == actualChecksum {
			return
		}
		hasherr = fmt.Errorf(
			"%w: server sent: %s, calcurated: %s",
			ErrChecksumUnmatch, serverChecksum, actualChecksum,
		)
	})

	if err := handler(tr); err != nil {
		return err
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		// drain rest of the entry.
		return err
	}

	return hasherr

}

type FileEntry struct {
	// Header is the header of the entry.
	Header tar.Header

	// Content of file.
	Body io.Reader
}

// download data
func (ci *client) GetData(ctx context.Context, knitid string, handler func(FileEntry) error) error {
	return ci.GetDataRaw(ctx, knitid, func(r io.Reader) error {
		gzr, err := gzip.NewReader(r)
		if err != nil {
			return err
		}
		defer gzr.Close()

		tarr := tar.NewReader(gzr)
		for {
			hdr, err := tarr.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			if err := handler(FileEntry{Header: *hdr, Body: tarr}); err != nil {
				return err
			}

			// drain rest of the entry.
			if _, err := io.Copy(io.Discard, tarr); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *client) FindData(ctx context.Context, tags []apitag.Tag) ([]apidata.Detail, error) {

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apipath("data"), nil)
	if err != nil {
		return nil, err
	}
	tagcount := len(tags)
	if 0 < tagcount {
		q := req.URL.Query()
		for _, t := range tags {
			q.Add("tag", fmt.Sprintf("%s:%s", t.Key, t.Value))
		}
		req.URL.RawQuery = q.Encode()
	}

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	dataMetas := make([]apidata.Detail, 0, 5)
	if err := unmarshalJsonResponse(
		resp, &dataMetas,
		MessageFor{
			Status4xx: fmt.Sprintf("[BUG] client is not compatible with the server (status code = %d)", resp.StatusCode),
			Status5xx: fmt.Sprintf("server error (status code = %d)", resp.StatusCode),
		},
	); err != nil {
		return nil, err
	}

	return dataMetas, nil
}
