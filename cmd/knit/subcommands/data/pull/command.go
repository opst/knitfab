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
	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/pkg/commandline/usage"
	kpath "github.com/opst/knitfab/pkg/utils/path"
)

type Command struct {
	progressOutput io.Writer
	defaultOutput  io.Writer
}

type Flags struct {
	Extract bool `flag:"extract,short=x,help=extract files from tar.gz archive"`
}

func WithProgressOutput(w io.Writer) func(com *Command) *Command {
	return func(com *Command) *Command {
		com.progressOutput = w
		return com
	}
}

func WithOutput(w io.Writer) func(com *Command) *Command {
	return func(com *Command) *Command {
		com.defaultOutput = w
		return com
	}
}

func New(taskOption ...func(com *Command) *Command) kcmd.KnitCommand[Flags] {
	command := &Command{
		progressOutput: os.Stderr,
		defaultOutput:  os.Stdout,
	}

	for _, opt := range taskOption {
		command = opt(command)
	}
	return command
}

const (
	ARG_KNIT_ID = "KNIT_ID"
	ARG_DEST    = "DEST"
)

func (*Command) Usage() usage.Usage[Flags] {
	return usage.New(
		Flags{},
		usage.Args{
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
	)
}

func (*Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "download Data from knitfab to your local filesystem",
		Example: `
Pull Data "knit#id:foobar" as "./foobar.tar.gz":
	{{ .Command }} foobar

Pull Data "knit#id:foobar" into "./foobar" directory, and extract it:
	{{ .Command }} -x foobar

Pull Data "knit#id:foobar" into "/somewhere/foobar" directory, and extract it:
	{{ .Command }} -x foobar /somewhere

Pull Data to stdout (-x is not allowed):
	{{ .Command }} foobar -


(directory will be created if not exists)
`,
	}
}

func (cmd *Command) Name() string {
	return "pull"
}

func (cmd *Command) Synopsis() string {
	return "download Data from knit to your local filesystem"
}

const noBar pb.ProgressBarTemplate = `{{with string . "prefix"}}{{.}} {{end}}{{counters . }} {{with string . "suffix"}} {{.}}{{end}}`

func (cmd *Command) Execute(
	ctx context.Context,
	l *log.Logger,
	e kenv.KnitEnv,
	c krst.KnitClient,
	f usage.FlagSet[Flags],
) error {
	knitId := f.Args[ARG_KNIT_ID][0]

	dest := "."
	if 0 < len(f.Args[ARG_DEST]) {
		dest = f.Args[ARG_DEST][0]
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

	if !f.Flags.Extract {
		dest = dest + ".tar.gz"
		err = c.GetDataRaw(ctx, knitId, func(r io.Reader) error {
			if writeDefault {
				_, err := io.Copy(cmd.defaultOutput, r)
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
			bar.SetWriter(cmd.progressOutput)
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
		return fmt.Errorf("%w: cannot extract Data to stdout (-)", kcmd.ErrUsage)
	} else {
		bar := noBar.New(-1)
		bar.SetWriter(cmd.progressOutput)
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
