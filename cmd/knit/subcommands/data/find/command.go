package find

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	apidata "github.com/opst/knitfab/pkg/api/types/data"
	apitag "github.com/opst/knitfab/pkg/api/types/tags"
	kflag "github.com/opst/knitfab/pkg/commandline/flag"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"github.com/opst/knitfab/pkg/utils"
	ptr "github.com/opst/knitfab/pkg/utils/pointer"
)

type TransientValue int

const (
	TransientAny TransientValue = iota
	TransientOnly
	TransientExclude
)

var ErrUnknownTransientFlag = fmt.Errorf("%w: unknown --transient value", kcmd.ErrUsage)

func NewErrUnknwonTransientFlag(actualValue string) error {
	return fmt.Errorf("%w: %s", ErrUnknownTransientFlag, actualValue)
}

type Flag struct {
	Tags      *kflag.Tags         `flag:"tag,short=t,metavar=KEY:VALUE...,help=Find Data with this Tag. Repeatable."`
	Transient string              `flag:"transient,metavar=both|yes|true|no|false,help=yes|true (transient Data only) / no|false (non transient Data only) / both"`
	Since     *kflag.LooseRFC3339 `flag:"since,help=Find Data only updated at this time or later."`
	Duration  time.Duration       `flag:"duration,help=Find Data only updated at a time earlier than the sum of since and duration."`
}

type Command struct {
	task func(
		context.Context,
		*log.Logger,
		krst.KnitClient,
		[]apitag.Tag,
		TransientValue,
		*time.Time,
		*time.Duration,
	) ([]apidata.Detail, error)
}

func WithTask(
	task func(
		context.Context,
		*log.Logger,
		krst.KnitClient,
		[]apitag.Tag,
		TransientValue,
		*time.Time,
		*time.Duration,
	) ([]apidata.Detail, error),
) func(*Command) *Command {
	return func(dfc *Command) *Command {
		dfc.task = task
		return dfc
	}
}

func New(
	options ...func(*Command) *Command,
) kcmd.KnitCommand[Flag] {
	return utils.ApplyAll(
		&Command{task: RunFindData},
		options...,
	)
}

func (cmd *Command) Name() string {
	return "find"
}

func (*Command) Usage() usage.Usage[Flag] {
	return usage.New(
		Flag{
			Tags:      &kflag.Tags{},
			Transient: "both",
			Since:     &kflag.LooseRFC3339{},
			Duration:  0,
		},
		usage.Args{},
	)
}

func (cmd *Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "Find Data that satisfy all specified conditions.",
		Detail: `
Find Data that satisfy all specified conditions.

'Tag' can be specified multiple times to search for Data that have all the specified Tags.

'Since' and 'duration' are used to specify the time range to search for Data.

Since targets Data that have been updated at equal to or later than since.
The since can be described in RFC3339 date-time format, and it is also possible to omit 
sub-seconds,seconds, minutes, and hours, in the description.
If the time zone is omitted, the local time zone is applied. 
When including a date and time, the following characters are allowed as delimiters between the date and time: "T" or space.

Duration is a flag used in conjunction with Since. 
It targets Data for search that have been updated at a time earlier than the sum of since and duration.
Duration can be described in Go's time.Duration type.
Examples of duration are "300ms", "1.5h" or "2h45m". 
`,
		Example: `
Finding Data with tag "key1:value1":

	{{ .Command }} --tag key1:value1

Finding Data specified by "knit#id:foobar":

	{{ .Command }} --tag "knit#id:foobar"

Finding Data with tag "key1:value1" AND "key2:value2":

	{{ .Command }} --tag key1:value1 --tag key2:value2

	(this does not find Data has only "key1:value1" or "key2:value2". needs both.)

Finding Data which is ready to use with Tag "key1:value1":

	{{ .Command }} --tag key1:value1 --transient no
	{{ .Command }} --tag key1:value1 --transient false

	(both above are equivalent)

Finding Data out of use with Tag "tag1:value1":

	{{ .Command }} --tag key1:value1 --transient yes
	{{ .Command }} --tag key1:value1 --transient true

	(both above are equivalent)

Finding all Data:

	{{ .Command }}
`,
	}
}

func (cmd *Command) Execute(
	ctx context.Context,
	l *log.Logger,
	e kenv.KnitEnv,
	c krst.KnitClient,
	flags usage.FlagSet[Flag],
) error {

	tags := []apitag.Tag{}
	if flags.Flags.Tags != nil {
		tags = *flags.Flags.Tags
	}

	transientFlag := TransientAny
	switch flags.Flags.Transient {
	case "yes", "true":
		transientFlag = TransientOnly
	case "no", "false":
		transientFlag = TransientExclude
	case "both":
		// default value.
	default:
		return NewErrUnknwonTransientFlag(flags.Flags.Transient)
	}

	since := time.Time(ptr.SafeDeref(flags.Flags.Since))
	duration := flags.Flags.Duration
	if since == (time.Time{}) && duration != 0 {
		return fmt.Errorf("%w: since and duration must be specified together", kcmd.ErrUsage)
	}

	data, err := cmd.task(ctx, l, c, tags, transientFlag, &since, &duration)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "    ")
	if err := enc.Encode(data); err != nil {
		l.Panicf("fail to dump found Data")
	}

	return nil
}

// find data from knit api
//
// args:
//   - ctx: context.Context
//   - logger: *log.Logger
//   - client: client to be used for sending request to knit API
//   - tags: query. RunFindData finds Data which have all of tags.
//   - transientFlag: restriction of output.
//     when... TransientAny, each data is returned wheather it has "knit#transient" tag or not.
//     TransientOnly, returned data will be restricted to ones with `knit#transint` tag.
//     TransientExclude, returned data will be restricted to ones without `knit#transint` tag.
//
// returns:
//   - []presentation.Data: found data. they are re-formatted for printing to console.
//   - error
func RunFindData(
	ctx context.Context,
	logger *log.Logger,
	client krst.KnitClient,
	tags []apitag.Tag,
	transientFlag TransientValue,
	since *time.Time,
	duration *time.Duration,
) ([]apidata.Detail, error) {

	result, err := client.FindData(ctx, tags, since, duration)
	if err != nil {
		return nil, err
	}

	isTransient := func(d apidata.Detail) bool {
		_, ok := utils.First(d.Tags, func(t apitag.Tag) bool {
			return t.Key == "knit#transient"
		})
		return ok
	}
	filter := isTransient

	switch transientFlag {
	case TransientAny:
		filter = func(apidata.Detail) bool { return true }
	case TransientOnly:
		// noop. filter is "isTransient", already.
	case TransientExclude:
		filter = func(d apidata.Detail) bool { return !isTransient(d) }
	}

	satisfied, _ := utils.Group(result, filter)

	return satisfied, nil
}
