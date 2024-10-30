package analyzer

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/opst/knitfab/pkg/cmp"
)

type TaggedConfig struct {
	Tags   []string
	Config Config
}

func (tc TaggedConfig) Equal(o TaggedConfig) bool {
	return cmp.SliceEq(tc.Tags, o.Tags) && tc.Config.Equal(o.Config)
}

type peekReader struct {
	peeking bool
	r       io.Reader
	head    byte
}

func (pr *peekReader) Read(p []byte) (n int, err error) {
	if pr.peeking {
		p[0] = pr.head
		pr.peeking = false
		return 1, nil
	}
	return pr.r.Read(p)
}

func (pr *peekReader) Peek() (byte, error) {
	if pr.peeking {
		return pr.head, nil
	}
	var b [1]byte
	n, err := pr.r.Read(b[:])
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, io.EOF
	}
	pr.peeking = true
	pr.head = b[0]
	return pr.head, nil
}

// Analyze reads a tar stream of an OCI image and returns found configurations (with its tag, if any).
func Analyze(ctx context.Context, stream io.Reader) ([]TaggedConfig, error) {
	// Parse OCI Image tar stream IN ONE PASS.
	//
	// To do that, read blobs one by one and parse them into Config.

	images := map[string]Image{}
	manifests := []DockerManifest{}

	tr := tar.NewReader(stream)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if hdr.Name == "manifest.json" {
			if err := json.NewDecoder(tr).Decode(&manifests); err != nil {
				return nil, err
			}

			continue
		}

		// Some tarballs DO NOT have `/blob` dir, so here, we analyze all files in the tarball.
		// (for example, images made by go-containerregistry/pkg/v1/tarball .)
		//
		// To read image config file ( https://github.com/opencontainers/image-spec/blob/main/config.md ),
		// we do file-sniffing by checking if the file is JSON and its format is known or not.
		if !hdr.FileInfo().IsDir() {
			var img Image
			r := &peekReader{r: tr}
			p, err := r.Peek()
			if err != nil {
				return nil, err
			}
			if p != '{' {
				continue // sniffing; skip non-JSON file contains object
			}
			if err := json.NewDecoder(r).Decode(&img); err != nil {
				if juterr := new(json.UnmarshalTypeError); errors.As(err, &juterr) {
					continue // skip non-Image file
				}
				if jsynerr := new(json.SyntaxError); errors.As(err, &jsynerr) {
					continue // skip non-JSON file
				}
				return nil, err
			}
			if img.IsValid() {
				images[hdr.Name] = img
			}
		}
	}

	imagesWithTag := map[string]*TaggedConfig{}
	for name, img := range images {
		imagesWithTag[name] = &TaggedConfig{
			Tags:   []string{},
			Config: img.Config,
		}
	}

	for _, manifest := range manifests {
		name := manifest.Config
		iwt, ok := imagesWithTag[name]
		if !ok {
			continue
		}

		iwt.Tags = append(iwt.Tags, manifest.RepoTags...)
		imagesWithTag[name] = iwt
	}

	var result []TaggedConfig
	for _, iwt := range imagesWithTag {
		result = append(result, *iwt)
	}

	return result, nil
}
