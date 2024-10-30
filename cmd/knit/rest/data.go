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
	"time"

	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/pkg/utils/archive"
	kio "github.com/opst/knitfab/pkg/utils/io"
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
	result   *data.Detail
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

func (p *progress) Result() (*data.Detail, bool) {
	return p.result, p.resultOk
}

func (p *progress) Done() <-chan struct{} {
	return p.done
}

func (p *progress) Sent() <-chan struct{} {
	return p.sent
}

func (c *client) PostData(sendingCtx context.Context, source string, dereference bool) Progress[*data.Detail] {
	sendingCtx, cancel := context.WithCancel(sendingCtx)

	r, w := io.Pipe()

	md5writer := kio.NewMD5Writer(w)
	gzwriter := gzip.NewWriter(md5writer)
	taropts := []archive.TarOption{}
	if dereference {
		taropts = append(taropts, archive.FollowSymlinks())
	}
	prog := &progress{
		sent: make(chan struct{}, 1),
		done: make(chan struct{}, 1),
		p:    archive.GoTar(sendingCtx, source, gzwriter, taropts...),
	}

	if err := prog.Error(); err != nil {
		cancel()
		prog.e = fmt.Errorf("failed to archive %s: %w", source, err)
		close(prog.done)
		return prog
	}

	treader := kio.NewTriggerReader(r)
	req, err := http.NewRequestWithContext(sendingCtx, http.MethodPost, c.apipath("data"), treader)
	if err != nil {
		cancel()
		prog.e = err
		return prog
	}
	treader.OnEnd(func() {
		req.Trailer.Add("x-checksum-md5", hex.EncodeToString(md5writer.Sum()))
	})

	req.Trailer = http.Header{}
	req.Header.Add("Content-Type", "application/tar+gzip")
	req.Header.Add("Transfer-Encoding", "chunked")
	req.Header.Add("Trailer", "x-checksum-md5")

	go func() {
		select {
		case <-prog.p.Done():
		case <-sendingCtx.Done():
		}
		if err := prog.Error(); err == nil {
			gzwriter.Close()
		}
		w.Close()
		close(prog.sent)
	}()

	go func() {
		defer close(prog.done)
		defer cancel()

		// send data to api/data.
		resp, err := c.httpclient.Do(req)
		if err != nil {
			prog.e = err
			return
		}
		defer resp.Body.Close()

		res := &data.Detail{}
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

func (c *client) PutTagsForData(knitId string, tags tags.Change) (*data.Detail, error) {

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

	res := data.Detail{}
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

func (c *client) FindData(ctx context.Context, tags []tags.Tag, since *time.Time, duration *time.Duration) ([]data.Detail, error) {

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apipath("data"), nil)
	if err != nil {
		return nil, err
	}

	// set query values
	q := req.URL.Query()

	if since != nil {
		q.Add("since", since.Format(rfctime.RFC3339DateTimeFormatZ))
	}

	if duration != nil {
		q.Add("duration", duration.String())
	}

	for _, t := range tags {
		q.Add("tag", t.String())
	}
	req.URL.RawQuery = q.Encode()

	resp, err := c.httpclient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	dataMetas := make([]data.Detail, 0, 5)
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
