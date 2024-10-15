package tag

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	apitag "github.com/opst/knitfab-api-types/tags"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	kflg "github.com/opst/knitfab/pkg/commandline/flag"
	"github.com/youta-t/flarc"
)

type Flag struct {
	AddTag    *kflg.Tags `flag:"add" metavar:"KEY:VALUE..." help:"add Tags to Data. Repeatable."`
	RemoveTag *kflg.Tags `flag:"remove" metavar:"KEY:VALUE..." help:"remove Tags from Data. Repeatable."`
	RemoveKey []string   `flag:"remove-key" metavar:"KEY..." help:"remove Tags by key. Repeatable"`
}

var ARG_KNITID = "KNIT_ID"

func New() (flarc.Command, error) {
	return flarc.NewCommand(
		"Add and/or remove Tags on Data in knitfab.",
		Flag{
			AddTag:    &kflg.Tags{},
			RemoveTag: &kflg.Tags{},
		},
		flarc.Args{
			{
				Name: ARG_KNITID, Required: true,
				Help: "the Knit Id of Data to be Tagged.",
			},
		},
		common.NewTask(Task),
		flarc.WithDescription(`
Add and/or remove Tags on Data in knitfab.

If the same Tag is specified in both add and remove, the Tag will be added. (remove first, then add)
`),
	)
}

func Task(
	ctx context.Context,
	l *log.Logger,
	e kenv.KnitEnv,
	c krst.KnitClient,
	cl flarc.Commandline[Flag],
	_ []any,
) error {
	args := cl.Args()
	knitId := args[ARG_KNITID][0]

	flags := cl.Flags()
	change := apitag.Change{
		AddTags:    []apitag.UserTag{},
		RemoveTags: []apitag.UserTag{},
		RemoveKey:  flags.RemoveKey,
	}

	if flags.AddTag != nil {
		for _, t := range *flags.AddTag {
			if ut := new(apitag.UserTag); !t.AsUserTag(ut) {
				return fmt.Errorf(
					"%w: tag key %s is reserved for system tags", flarc.ErrUsage, t.Key,
				)
			} else {
				change.AddTags = append(change.AddTags, *ut)
			}
		}
	}
	if flags.RemoveTag != nil {
		for _, t := range *flags.RemoveTag {
			if ut := new(apitag.UserTag); !t.AsUserTag(ut) {
				return fmt.Errorf(
					"%w: tag key %s is reserved for system tags", flarc.ErrUsage, t.Key,
				)
			} else {
				change.RemoveTags = append(change.RemoveTags, *ut)
			}
		}
	}

	if err := UpdateTag(ctx, l, c, knitId, change); err != nil {
		return err
	}

	return nil
}

func UpdateTag(
	ctx context.Context,
	logger *log.Logger,
	ci krst.KnitClient,
	knitid string,
	change apitag.Change,
) error {

	logger.Printf("tagging to knit#id:%s", knitid)
	res, err := ci.PutTagsForData(knitid, change)
	if err != nil {
		buf, _err := json.MarshalIndent(change, "", "    ")
		if _err != nil {
			return fmt.Errorf("unexpected error: %w", err)
		}
		logger.Printf("failed to update tag for knit#id:%s.\nrequested tags change :\n%s\n", knitid, string(buf))
		return err
	}

	buf, err := json.MarshalIndent(res, "", "    ")
	if err != nil {
		return fmt.Errorf("unexpected error: %w", err)
	}
	logger.Printf("[OK] Tags are updated for data knit#id:%s\n%s\n", res.KnitId, string(buf))

	return nil
}
