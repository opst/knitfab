package pull

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/cheggaaa/pb/v3"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/knitcmd"
	kpath "github.com/opst/knitfab/pkg/utils/path"
	"github.com/youta-t/flarc"
)

type Command struct{}

type Flags struct {
	Extract bool `flag:"extract" alias:"x" help:"extract files from tar.gz archive"`
}

const (
	ARG_KNIT_ID = "KNIT_ID"
	ARG_DEST    = "DEST"
)

func New() (flarc.Command, error) {
	return flarc.NewCommand(
		"Pull (download) Data from Knitfab to your local filesystem.",
		Flags{
			Extract: false,
		},
		flarc.Args{
			{
				Name: ARG_KNIT_ID, Required: true,
				Help: `Data identifier. This is same as the Tag "knit#id" of Data.`,
			},
			{
				Name: ARG_DEST, Required: false,
				Help: `
directory where a downloaded Data will be located at.
If the directory does not exist, it will be created.
If you set "-", the Data will be written to stdout (not applicable with -x).
Default: current directory ".".
`,
			},
		},
		knitcmd.NewTask(Task),
		flarc.WithDescription(`
Example
-------

Pull Data "knit#id:foobar" as "./foobar.tar.gz":
	{{ .Command }} foobar

Pull Data "knit#id:foobar" into "./foobar" directory, and extract it:
	{{ .Command }} -x foobar

Pull Data "knit#id:foobar" into "/somewhere/foobar" directory, and extract it:
	{{ .Command }} -x foobar /somewhere

Pull Data to stdout (-x is not allowed):
	{{ .Command }} foobar -


(directory will be created if not exists)
`),
	)
}

const noBar pb.ProgressBarTemplate = `{{with string . "prefix"}}{{.}} {{end}}{{counters . }} {{with string . "suffix"}} {{.}}{{end}}`

func Task(
	ctx context.Context,
	l *log.Logger,
	e kenv.KnitEnv,
	c krst.KnitClient,
	cl flarc.Commandline[Flags],
	_ []any,
) error {
	args := cl.Args()
	knitId := args[ARG_KNIT_ID][0]

	dest := "."
	if 0 < len(args[ARG_DEST]) {
		dest = args[ARG_DEST][0]
	}

	writeDefault := false
	if dest == "-" {
		writeDefault = true
	}

	dest, err := kpath.Resolve(dest)
	if err != nil {
		return fmt.Errorf("path resolving error for '%s': %w", dest, err)
	}
	dest = filepath.Clean(dest)
	dest = filepath.Join(dest, knitId)

	flags := cl.Flags()
	if !flags.Extract {
		dest = dest + ".tar.gz"
		err = c.GetDataRaw(ctx, knitId, func(r io.Reader) error {
			if writeDefault {
				_, err := io.Copy(cl.Stdout(), r)
				return err
			}

			d := filepath.Dir(dest)
			if err := os.MkdirAll(d, os.FileMode(0777)); err != nil {
				return err
			}
			f, err := os.OpenFile(dest, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(0666))
			if err != nil {
				return err
			}
			defer f.Close()

			bar := noBar.New(-1)
			bar.SetWriter(cl.Stderr())
			bar.Set("prefix", fmt.Sprintf("Downloading to %s:", ellipsis(dest, 60)))
			bar.Start()
			w := bar.NewProxyWriter(f)
			defer w.Close()
			if _, err := io.Copy(w, r); err != nil {
				return err
			}
			return nil
		})
	} else if writeDefault {
		return fmt.Errorf("%w: cannot extract Data to stdout (-)", flarc.ErrUsage)
	} else {
		bar := noBar.New(-1)
		bar.SetWriter(cl.Stderr())
		bar.Start()

		err = c.GetData(ctx, knitId, func(fe krst.FileEntry) error {
			fdest := filepath.Join(dest, fe.Header.Name)
			d := filepath.Dir(fdest)
			if err := os.MkdirAll(d, os.FileMode(0777)); err != nil {
				return err
			}
			if fe.Header.Typeflag == tar.TypeSymlink {
				return os.Symlink(fe.Header.Linkname, fdest)
			}

			f, err := os.OpenFile(fdest, os.O_CREATE|os.O_RDWR|os.O_TRUNC, fe.Header.FileInfo().Mode())
			if err != nil {
				return err
			}
			defer f.Close()
			bar.Set("prefix", fmt.Sprintf("extracting: %s into %s: ", ellipsis(fe.Header.Name, 20), ellipsis(dest, 60)))

			w := bar.NewProxyWriter(f) // do not close. won't Finish the bar here.
			if _, err := io.Copy(w, fe.Body); err != nil {
				return err
			}

			return nil
		})
		bar.Set("prefix", "done.: ")
		bar.Finish()
	}

	if errors.Is(err, krst.ErrChecksumUnmatch) {
		return errors.New("[WARN] checksum unmatch: Your Data is saved, but it may be corrupted")
	}

	return err
}

func ellipsis(s string, length int) string {
	if len(s) <= length {
		return s
	}

	l := len(s)
	return "[...]" + s[l-length+5:]
}
