package push

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	kflg "github.com/opst/knitfab/pkg/commandline/flag"
	"github.com/opst/knitfab/pkg/commandline/usage"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"

	pb "github.com/cheggaaa/pb/v3"
)

type Flags struct {
	Tag         kflg.Tags `flag:"tag,short=t,metavar=KEY:VALUE...,help=Tags to be put on Data. It can be specified multiple times."`
	Name        bool      `flag:"name,short=n,help=add tag name:<source>"`
	Dereference bool      `flag:"dereference,short=L,help=Symlinks are followed and it stores target files of links. Otherwise symlinks are stored as such."`
}

type Command struct {
	progressOut io.Writer
	output      io.Writer
}

type Option func(*Command) *Command

func WithProgressOut(w io.Writer) Option {
	return func(c *Command) *Command {
		c.progressOut = w
		return c
	}
}

func WithOutput(w io.Writer) Option {
	return func(c *Command) *Command {
		c.output = w
		return c
	}
}

func New(opt ...Option) kcmd.KnitCommand[Flags] {
	c := &Command{
		progressOut: os.Stderr,
		output:      os.Stdout,
	}

	for _, o := range opt {
		c = o(c)
	}

	return c
}
func (cmd *Command) Name() string {
	return "push"
}

const ARG_SOURCE = "source"

func (*Command) Usage() usage.Usage[Flags] {
	return usage.New(
		Flags{},
		usage.Args{
			{
				Name: ARG_SOURCE, Repeatable: true, Required: true,
				Help: `Data directory to be pushed to knitfab`,
			},
		},
	)
}

func (*Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "push (register) Data to knitfab",
		Example: `
To register directory "./data/train" to knit:

	{{ .Command }} ./data/train

To register directory "./data/train" with tag to knitfab:

	{{ .Command }} --tag "type:training-data" ./data/train
	{{ .Command }} -t "type:training-data" ./data/train  (equivelent to above)

To register directories:

	{{ .Command }} ./data/train ./data/test

For each example, Tags in knitenv file are also added to the Data.
`,
		Detail: `
Register Data in your local directory to knit.

Tags are added to the registered Data.
You can specify Tags with --tag (or -t) option and --name (or -n) option.
If you specify --name option, the Tag "name:<SOURCE_DIRECOTRYNAME>" is added to the Data.

And, knitenv Tags are also added to the Data, implicitly.
`,
	}
}

func (cmd *Command) Execute(
	ctx context.Context,
	l *log.Logger,
	e kenv.KnitEnv,
	c krst.KnitClient,
	flags usage.FlagSet[Flags],
) error {
	tags := make(map[apitag.UserTag]struct{}, len(flags.Flags.Tag))
	for _, t := range flags.Flags.Tag {
		if ut := new(apitag.UserTag); t.AsUserTag(ut) {
			tags[*ut] = struct{}{}
		} else {
			return fmt.Errorf("%w: Tag starting %s is reserved", kcmd.ErrUsage, kdb.SystemTagPrefix)
		}
	}

	for _, t := range e.Tags() {
		if ut := new(apitag.UserTag); t.AsUserTag(ut) {
			tags[*ut] = struct{}{}
		}
	}

	toBeNamed := flags.Flags.Name
	total := len(flags.Args[ARG_SOURCE])
	for n, s := range flags.Args[ARG_SOURCE] {
		if _, err := os.Stat(s); err != nil {
			l.Printf("%s: %s -- skipped", err, s)
			continue
		}

		t := utils.KeysOf(tags)
		if toBeNamed {
			t = append(t, apitag.UserTag{Key: "name", Value: filepath.Base(s)})
		}

		prog := c.PostData(ctx, s, flags.Flags.Dereference)

		bar := pb.New64(prog.EstimatedTotalSize())
		bar.Set(pb.Bytes, true)
		bar.SetWriter(cmd.progressOut)
		if err := bar.Err(); err != nil {
			return err
		}

		bar.Start()
		l.Printf("[[%d/%d]] sending... %s\n", n+1, total, s)
		for {
			select {
			case <-time.NewTimer(1 * time.Second).C:
				bar.SetTotal(prog.EstimatedTotalSize())
				bar.SetCurrent(prog.ProgressedSize())
				bar.Set("prefix", ellipsis(prog.ProgressingFile(), 60)+":")
				continue
			case <-prog.Sent():
				bar.SetTotal(prog.EstimatedTotalSize())
				bar.SetCurrent(prog.ProgressedSize())
				bar.Set("prefix", "")
			}
			break
		}
		bar.Finish()
		select {
		case <-time.NewTimer(1 * time.Second).C:
			l.Println("waiting server...")
		case <-prog.Done():
		}
		<-prog.Done()
		if err := prog.Error(); err != nil {
			return err
		}

		knitData, ok := prog.Result()
		if !ok {
			return fmt.Errorf("[ERROR] failed to register %s", s)
		}

		l.Printf(
			"registered: %s -> %s:%s",
			s, kdb.KeyKnitId, knitData.KnitId,
		)

		// tagging
		tagChange := apitag.Change{AddTags: t}
		l.Println("tagging...")
		res, err := c.PutTagsForData(knitData.KnitId, tagChange)
		if err != nil {
			buf, _err := json.MarshalIndent(knitData, "", "    ")
			if _err != nil {
				return err
			}

			l.Printf(
				"[[%d/%d]] [WARN] partially done: %s -> %s:%s (but not Tagged)",
				n+1, total, s, kdb.KeyKnitId, res.KnitId,
			)
			cmd.output.Write(buf)
			return err
		}

		buf, err := json.MarshalIndent(res, "", "    ")
		if err != nil {
			return err
		}
		l.Printf(
			"[[%d/%d]] [OK] done: %s -> %s:%s",
			n+1, total, s, kdb.KeyKnitId, res.KnitId,
		)
		cmd.output.Write(buf)
	}

	return nil
}

func ellipsis(s string, length int) string {
	if len(s) <= length {
		return s
	}
	l := len(s)
	return "[...]" + s[l-length+5:]
}
