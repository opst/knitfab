package init

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"

	"github.com/google/subcommands"
	"gopkg.in/yaml.v3"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	prof "github.com/opst/knitfab/cmd/knit/config/profiles"
	"github.com/opst/knitfab/pkg/commandline/flag/flagger"
	"github.com/opst/knitfab/pkg/commandline/usage"
)

type Command struct {
	commonFlags *flagger.Flagger[kcmd.CommonFlags]
	parent      string
}

func (c *Command) SetParent(p string) {
	c.parent = p
}

func New(commonFlags kcmd.CommonFlags) subcommands.Command {
	return &Command{
		commonFlags: flagger.New(commonFlags),
	}
}

var _ subcommands.Command = &Command{}

func (cmd *Command) Name() string {
	return "init"
}

const ARG_KNIT_PROFILE_FILE = "KNIT_PROFILE_FILE"

func (c *Command) usage() usage.Usage[struct{}] {
	return usage.New(
		struct{}{},
		usage.Args{
			{
				Name: ARG_KNIT_PROFILE_FILE, Required: true,
				Help: "filepath to knitprofile file, which you received from your admin.",
			},
		},
	)
}

func (*Command) help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "initialize this directory as knitfab-powerd project.",
		Detail: `
Register a new knitprofile into your profile store.

"knitprofile" is a file which contains information about knitfab cluster.
"{{ .Command }}" register the given knitprofile into your profile store.

The name of the profile is given by "--profile" ( default: current filepath ).
`,
	}
}

func (cmd *Command) Synopsis() string {
	return cmd.help().Synopsis
}

func (cmd *Command) Usage() string {
	command := cmd.Name()
	if cmd.parent != "" {
		command = cmd.parent + " " + command
	}

	return kcmd.BuildUsageMessage(command, cmd.help(), cmd.usage(), cmd.commonFlags.Flags...)
}

func (cmd *Command) SetFlags(f *flag.FlagSet) {
	cmd.commonFlags.SetFlags(f)
}

func (cmd *Command) Execute(ctx context.Context, f *flag.FlagSet, arg ...interface{}) subcommands.ExitStatus {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	for _, elem := range arg {
		switch e := elem.(type) {
		case *log.Logger:
			logger = e
		}
	}

	args, err := cmd.usage().Parse(f.Args())
	if err != nil {
		logger.Printf("failed to parse flags : %s", err)
		return subcommands.ExitUsageError
	}

	cf := cmd.commonFlags.Values

	profFile := args.Args[ARG_KNIT_PROFILE_FILE][0]

	profStore, err := prof.LoadProfileStore(cf.ProfileStore)
	if errors.Is(err, prof.ErrProfileStoreNotFound) {
		// ok.
		profStore = prof.ProfileStore{}
	} else if err != nil {
		logger.Printf(
			"failed to load profile store (%s) : %s", cf.ProfileStore, err,
		)
		return subcommands.ExitFailure
	}

	profName := cf.Profile
	newProf := new(prof.KnitProfile)
	{
		content, err := os.ReadFile(profFile)
		if err != nil {
			logger.Printf("failed to read profile file (%s) : %s", profFile, err)
			return subcommands.ExitFailure
		}

		if err := yaml.Unmarshal(content, newProf); err != nil {
			logger.Printf("failed to parse profile file (%s) : %s", profFile, err)
			return subcommands.ExitFailure
		}
	}
	if err := newProf.Verify(); err != nil {
		logger.Printf("%s: %s", profFile, err)
		return subcommands.ExitFailure
	}

	profStore[profName] = newProf
	if err := profStore.Save(cf.ProfileStore); err != nil {
		logger.Printf(
			"failed to save profile store (%s) : %s",
			cf.ProfileStore, err,
		)
		return subcommands.ExitFailure
	}
	logger.Printf(
		"profile %s is saved to %s", profName, cf.ProfileStore,
	)

	{
		f, err := os.OpenFile(".knitprofile", os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(0600))
		if err != nil {
			logger.Printf("failed to open .knitprofile : %s", err)
			return subcommands.ExitFailure
		}
		defer f.Close()
		f.Write([]byte(profName))
	}

	return subcommands.ExitSuccess
}
