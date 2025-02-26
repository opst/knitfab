package archive

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

type Progress interface {
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

	// Done returns a channel which is closed when archiving is done.
	Done() <-chan struct{}

	// EstimateDone returns a channel which is closed when EstimatedTotalSize is calcurated.
	EstimateDone() <-chan struct{}
}

type progress struct {
	totalSize int64
	doneSize  int64
	file      string
	err       error
	done      chan struct{}
	estDone   chan struct{}
}

func (m *progress) EstimatedTotalSize() int64 {
	return m.totalSize
}

func (m *progress) ProgressedSize() int64 {
	return m.doneSize
}

func (m *progress) ProgressingFile() string {
	return m.file
}

func (m *progress) Error() error {
	return m.err
}

func (m *progress) Done() <-chan struct{} {
	return m.done
}

func (m *progress) EstimateDone() <-chan struct{} {
	return m.estDone
}

type tarOption struct {
	followSymlinks bool
}

type TarOption func(*tarOption) *tarOption

func FollowSymlinks() TarOption {
	return func(o *tarOption) *tarOption {
		o.followSymlinks = true
		return o
	}
}

// GoTar archives files under root into dest in background goroutine.
//
// # Args
//
// - ctx context.Context: context to be used for archiving.
//
// - root string: root directory where it collects files from.
//
// - dest io.Writer: where tar stream is to be written.
// If dest is io.WriteCloser, it is closed when archiving is done.
//
// # Example
//
//	func main() {
//	    ctx, cancel := signal.NotifyContext(
//	        context.Background(), os.Interrupt, os.Kill,
//	    )
//	    defer cancel()
//
//	    dest, err := os.Create(os.Args[2])
//	    if err != nil {
//	        panic(err)
//	    }
//	    defer dest.Close()
//
//	    monitor, err := archive.GoTar(ctx, os.Args[1], dest)
//	    if err != nil {
//	        panic(err)
//	    }
//
//	    for {
//	        select {
//	        case <-ctx.Done():
//	            return
//	        case <-monitor.Done():
//	            if err := monitor.Error(); err != nil {
//	                fmt.Println(err)
//	            }
//	            return
//	        case <-time.After(1 * time.Second):
//	            fmt.Printf(
//	                "progress: %d/%d (%s)\n",
//	                monitor.ProgressedSize(),
//	                monitor.EstimateTotalSize(),
//	                monitor.ProgressingFile(),
//	            )
//	        }
//	    }
//	    fmt.Println("done")
//	}
//
// # Returns
//
// - Monitor: monitor object to watch the progress of archiving.
//
// - error: error caused during start archiving.
func GoTar(ctx context.Context, root string, dest io.Writer, options ...TarOption) Progress {

	opt := &tarOption{}
	for _, o := range options {
		opt = o(opt)
	}

	started := false
	prog := &progress{
		done:    make(chan struct{}),
		estDone: make(chan struct{}),
	}

	defer func() {
		if !started {
			close(prog.estDone)
			close(prog.done)
		}
	}()
	absroot, err := filepath.Abs(root)
	if err != nil {
		prog.err = err
		return prog
	}

	if _, err := os.Stat(absroot); err != nil {
		prog.err = err
		return prog
	}

	go func() {
		// estimate size

		defer close(prog.estDone)
		if err := findFiles(absroot, opt.followSymlinks, func(_ string, info fs.FileInfo) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if prog.err != nil {
				return prog.err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			prog.totalSize += info.Size()
			return nil

		}); err != nil {
			prog.err = err
			return
		}
	}()

	started = true
	go func() {
		defer close(prog.done)
		defer func() {
			switch pan := recover().(type) {
			case nil:
				// ok
			case error:
				prog.err = pan
			case string:
				prog.err = fmt.Errorf("%s", pan)
			default:
				prog.err = fmt.Errorf("%v", pan)
			}
		}()

		tarWriter := tar.NewWriter(dest)
		writer := &reportingWriter{dest: tarWriter, prog: prog}
		var err error
		defer func() {
			if err == nil && recover() == nil {
				tarWriter.Close()
			}
		}()

		prog.err = findFiles(
			absroot, opt.followSymlinks,
			func(fullpath string, fi fs.FileInfo) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				if prog.err != nil {
					return prog.err
				}

				relpath, err := filepath.Rel(absroot, fullpath)
				if err != nil {
					return err
				}
				prog.file = relpath

				linkname := ""
				if fi.Mode()&os.ModeSymlink != 0 {
					ln, err := os.Readlink(fullpath)
					if err != nil {
						return err
					}
					linkname = ln
				}

				hdr, err := tar.FileInfoHeader(fi, linkname)
				if err != nil {
					return err
				}
				hdr.Name = relpath

				if err := tarWriter.WriteHeader(hdr); err != nil {
					return err
				}

				if fi.Mode().IsRegular() {
					fp, err := ctxOpen(ctx, fullpath)
					if err != nil {
						return err
					}
					defer fp.Close()
					if _, err := io.Copy(writer, fp); err != nil {
						return err
					}
				}

				return nil
			},
		)
	}()

	return prog
}

func GoUntar(ctx context.Context, src io.Reader, dest string) Progress {
	prog := &progress{
		done:    make(chan struct{}),
		estDone: make(chan struct{}),
	}

	start := false
	defer func() {
		if !start {
			close(prog.estDone)
			close(prog.done)
		}
	}()

	go func() {
		defer close(prog.done)
		defer close(prog.estDone)
		tarr := tar.NewReader(src)
		carr := &ctxReader{ctx: ctx, r: tarr}
		for {
			select {
			case <-ctx.Done():
				prog.err = ctx.Err()
				return
			default:
			}

			hdr, err := tarr.Next()
			if err == io.EOF {
				return
			}
			if err != nil {
				prog.err = err
				return
			}

			if hdr.Name == "" {
				continue
			}

			fullpath := filepath.Join(dest, hdr.Name)
			prog.file = hdr.Name

			{
				d := filepath.Dir(fullpath)
				if err := os.MkdirAll(d, 0766); err != nil {
					prog.err = err
					return
				}
			}

			if hdr.Typeflag == tar.TypeSymlink {
				if err := os.Symlink(hdr.Linkname, fullpath); err != nil {
					prog.err = err
				}
				continue
			}

			if hdr.Typeflag != tar.TypeReg {
				continue
			}

			func() {
				fp, err := os.OpenFile(fullpath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(hdr.Mode))
				if err != nil {
					prog.err = err
					return
				}
				defer fp.Close()

				repw := &reportingWriter{dest: fp, prog: prog}
				if _, err := io.Copy(repw, carr); err != nil {
					prog.err = err
					return
				}
			}()
		}
	}()
	start = true

	return prog
}

func findFiles(from string, followLink bool, callback func(string, fs.FileInfo) error) error {
	stat, err := os.Lstat(from)
	if err != nil {
		return err
	}

	via := map[string]struct{}{}
	if stat.Mode()&os.ModeSymlink != 0 && followLink {
		s, err := os.Stat(from)
		if err != nil {
			return err
		}
		stat = s

		rpath, err := filepath.EvalSymlinks(from)
		if err != nil {
			return err
		}
		via[rpath] = struct{}{}
	}

	if !stat.IsDir() {
		return callback(from, stat)
	}

	return findFilesInDirectory(from, followLink, via, callback)
}

func findFilesInDirectory(from string, followLink bool, viaSymlink map[string]struct{}, callback func(string, fs.FileInfo) error) error {
	entries, err := os.ReadDir(from)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		err := func() error {
			fullpath := filepath.Join(from, entry.Name())
			stat, err := os.Lstat(fullpath)
			if err != nil {
				return err
			}

			if stat.Mode()&os.ModeSymlink != 0 && followLink {
				realpath, err := filepath.EvalSymlinks(fullpath)
				if err != nil {
					return err
				}
				if _, ok := viaSymlink[realpath]; ok {
					return ErrLoopSymlink
				}
				viaSymlink[realpath] = struct{}{}
				defer func() {
					delete(viaSymlink, realpath)
				}()

				s, err := os.Stat(fullpath)
				if err != nil {
					return err
				}
				stat = s
			}

			if stat.IsDir() {
				if err := findFilesInDirectory(
					fullpath, followLink, viaSymlink, callback,
				); err != nil {
					return err
				}
				return nil
			}

			return callback(fullpath, stat)
		}()

		if err != nil {
			return err
		}
	}
	return nil
}

var ErrLoopSymlink = errors.New("symlink loop detected")

// open file as long as ctx is alive.
func ctxOpen(ctx context.Context, p string) (io.ReadCloser, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	return &ctxReader{ctx: ctx, r: f}, nil
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *ctxReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		r.Close()
		return 0, r.ctx.Err()
	default:
	}
	return r.r.Read(p)
}

func (r *ctxReader) Close() error {
	if closer, ok := r.r.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

type reportingWriter struct {
	dest io.Writer
	prog Progress
}

func (w *reportingWriter) Write(p []byte) (int, error) {
	n, err := w.dest.Write(p)
	w.prog.(*progress).doneSize += int64(n)
	return n, err
}

type walkBreak struct {
	error string
}

func (w walkBreak) Error() string {
	return w.error
}

func WalkBreak() walkBreak {
	return walkBreak{}
}

// handler of tar entry.
//
// args:
//   - header: header of tar entry
//   - payload: `io.Reader` points the content of the tar entry.
//   - err: error happens when get a tar entry.
//     err is never `io.EOF`.
//     Because walking focuses each entries, not whole tar file.
//
// return:
//
//	any error which caused in a handler.
//	You can early terminate with return `WalkBreak()`
type TarWalker func(header *tar.Header, payload io.Reader, err error) error

// traverse tar entry.
//
// args:
//   - from io.Reader: Reader object refers *.tar.gz stream.
//     This function does not close `from`.
//   - walker TarWalker: tar entry handler.
//
// return: error, caused reading tar.gz or returned by walker.
//
//	If nothing happens, it returns `nil`.
func TarGzWalk(from io.Reader, walker TarWalker) error {
	gzin, err := gzip.NewReader(from)
	if err != nil {
		return err
	}
	defer gzin.Close()

	tarin := tar.NewReader(gzin)
	for {
		header, err := tarin.Next()
		if err == io.EOF {
			return nil
		}
		err = walker(header, tarin, err)
		if err == nil {
			continue
		}
		switch err.(type) {
		case walkBreak:
			return nil
		default:
			return err
		}
	}
}
