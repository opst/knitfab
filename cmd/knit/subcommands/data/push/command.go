package push

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	kflg "github.com/opst/knitfab/pkg/commandline/flag"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/youta-t/flarc"

	pb "github.com/cheggaaa/pb/v3"
)

type Flags struct {
	Tag         *kflg.Tags `flag:"tag" alias:"t" metavar:"KEY:VALUE..." help:"Tags to be put on Data. It can be specified multiple times."`
	Name        bool       `flag:"name" alias:"n" help:"add tag name:<source>"`
	Dereference bool       `flag:"dereference" short:"L" help:"Symlinks are followed and it stores target files of links. Otherwise symlinks are stored as such."`
}

const ARG_SOURCE = "source"

func New() (flarc.Command, error) {
	return flarc.NewCommand(
		"Push (register) Data to Knitfab.",
		Flags{
			Tag:         &kflg.Tags{},
			Name:        false,
			Dereference: false,
		},
		flarc.Args{
			{
				Name: ARG_SOURCE, Repeatable: true, Required: true,
				Help: `Data directory to be pushed to knitfab`,
			},
		},
		common.NewTask(Task),
		flarc.WithDescription(`
Register Data in your local directory to knit.

Tags are added to the registered Data.
You can specify Tags with --tag (or -t) option and --name (or -n) option.
If you specify --name option, the Tag "name:<SOURCE_DIRECOTRYNAME>" is added to the Data.

And, knitenv Tags are also added to the Data, implicitly.

Example
-------

To register directory "./data/train" to knit:

	{{ .Command }} ./data/train

To register directory "./data/train" with tag to knitfab:

	{{ .Command }} --tag "type:training-data" ./data/train
	{{ .Command }} -t "type:training-data" ./data/train  (equivelent to above)

To register directories:

	{{ .Command }} ./data/train ./data/test

For each example, Tags in knitenv file are also added to the Data.
`,
		),
	)
}

func Task(
	ctx context.Context,
	l *log.Logger,
	e kenv.KnitEnv,
	c krst.KnitClient,
	cl flarc.Commandline[Flags],
	_ []any,
) error {
	flags := cl.Flags()
	rawtags := kflg.Tags{}
	if flags.Tag != nil {
		rawtags = *flags.Tag
	}
	tags := make(map[apitag.UserTag]struct{}, len(rawtags))
	for _, t := range rawtags {
		if ut := new(apitag.UserTag); t.AsUserTag(ut) {
			tags[*ut] = struct{}{}
		} else {
			return fmt.Errorf("%w: Tag starting %s is reserved", flarc.ErrUsage, kdb.SystemTagPrefix)
		}
	}

	for _, t := range e.Tags() {
		if ut := new(apitag.UserTag); t.AsUserTag(ut) {
			tags[*ut] = struct{}{}
		}
	}

	toBeNamed := flags.Name

	args := cl.Args()
	total := len(args[ARG_SOURCE])
	for n, s := range args[ARG_SOURCE] {
		if _, err := os.Stat(s); err != nil {
			l.Printf("%s: %s -- skipped", err, s)
			continue
		}

		t := utils.KeysOf(tags)
		if toBeNamed {
			t = append(t, apitag.UserTag{Key: "name", Value: filepath.Base(s)})
		}

		prog := c.PostData(ctx, s, flags.Dereference)

		bar := pb.New64(prog.EstimatedTotalSize())
		bar.Set(pb.Bytes, true)
		bar.SetWriter(cl.Stderr())
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
			case <-prog.Done():
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
			cl.Stdout().Write(buf)
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
		cl.Stdout().Write(buf)
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
