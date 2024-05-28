package init

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/youta-t/flarc"
	"gopkg.in/yaml.v3"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	prof "github.com/opst/knitfab/cmd/knit/config/profiles"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/knitcmd"
)

const ARG_KNIT_PROFILE_FILE = "KNIT_PROFILE_FILE"

func New() (flarc.Command, error) {
	return flarc.NewCommand(
		"Initialize this directory as Knitfab-powerd project.",
		struct{}{},
		flarc.Args{
			{
				Name: ARG_KNIT_PROFILE_FILE, Required: true,
				Help: "filepath to knitprofile file, which you received from your admin.",
			},
		},
		knitcmd.NewTaskWithCommonFlag(func(
			ctx context.Context,
			logger *log.Logger,
			cf kcmd.CommonFlags,
			cl flarc.Commandline[struct{}],
			params []any,
		) error {
			profFile := cl.Args()[ARG_KNIT_PROFILE_FILE][0]

			profStore, err := prof.LoadProfileStore(cf.ProfileStore)
			if errors.Is(err, prof.ErrProfileStoreNotFound) {
				// ok.
				profStore = prof.ProfileStore{}
			} else if err != nil {
				return fmt.Errorf(
					"failed to load profile store (%s) : %w", cf.ProfileStore, err,
				)
			}

			profName := cf.Profile
			newProf := new(prof.KnitProfile)
			{
				content, err := os.ReadFile(profFile)
				if err != nil {
					return fmt.Errorf("failed to read profile file (%s) : %w", profFile, err)
				}

				if err := yaml.Unmarshal(content, newProf); err != nil {
					return fmt.Errorf("failed to parse profile file (%s) : %w", profFile, err)
				}
			}
			if err := newProf.Verify(); err != nil {
				return fmt.Errorf("%s: %w", profFile, err)
			}

			profStore[profName] = newProf
			if err := profStore.Save(cf.ProfileStore); err != nil {
				return fmt.Errorf(
					"failed to save profile store (%s) : %w",
					cf.ProfileStore, err,
				)
			}
			logger.Printf(
				"profile %s is saved to %s", profName, cf.ProfileStore,
			)

			{
				f, err := os.OpenFile(".knitprofile", os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(0600))
				if err != nil {
					return fmt.Errorf("failed to open .knitprofile : %w", err)
				}
				defer f.Close()
				f.Write([]byte(profName))
			}
			return nil
		}),
		flarc.WithDescription(`
Register a new knitprofile into your profile store.

"knitprofile" is a file which contains information about knitfab cluster.
"{{ .Command }}" register the given knitprofile into your profile store.

The name of the profile is given by "--profile" ( default: current filepath ).
`,
		),
	)
}
