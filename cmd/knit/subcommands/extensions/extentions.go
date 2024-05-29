package extensions

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	"github.com/youta-t/flarc"
)

type ExtentionCommand struct {
	Name string
	Path string
}

func FindSubcommand(prefix string) []ExtentionCommand {
	subcommands := []ExtentionCommand{}

	pathes := strings.Split(os.Getenv("PATH"), string(os.PathListSeparator))
	knwon := map[string]struct{}{}

	for _, p := range pathes {
		if p == "" {
			p = "."
		}
		files, err := os.ReadDir(p)
		if err != nil {
			continue
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasPrefix(f.Name(), prefix) {
				continue
			}
			abspath, err := exec.LookPath(filepath.Join(p, f.Name()))
			if err != nil {
				continue
			}
			name := strings.TrimPrefix(f.Name(), prefix)
			if _, ok := knwon[name]; ok {
				continue
			}
			for _, executableExt := range []string{".exe", ".bat", ".cmd", ".com"} {
				if strings.HasSuffix(name, executableExt) {
					name = strings.TrimSuffix(name, executableExt)
					break
				}
			}
			subcommands = append(
				subcommands,
				ExtentionCommand{Name: name, Path: abspath},
			)
			knwon[name] = struct{}{}
		}
	}

	return subcommands
}

const PARAMS = "PARAMS"

func New(ext ExtentionCommand) (flarc.Command, error) {
	return flarc.NewCommand(
		fmt.Sprintf("(= %s)", ext.Path),
		struct{}{},
		flarc.Args{
			{
				Name: PARAMS, Required: false, Repeatable: true,
				Help: "parameters for the extention command",
			},
		},
		common.NewTaskWithCommonFlag(Task(ext)),
	)
}

func Task(ext ExtentionCommand) common.KnitTaskWithCommonFlag[struct{}] {
	return func(
		ctx context.Context,
		logger *log.Logger,
		cf common.CommonFlags,
		cl flarc.Commandline[struct{}],
		params []any,
	) error {
		args := cl.Args()[PARAMS]
		cmd := exec.Command(ext.Path, args...)
		cmd.Stdin = cl.Stdin()
		cmd.Stdout = cl.Stdout()
		cmd.Stderr = cl.Stderr()
		environ := append(
			os.Environ(),
			"KNIT_PROFILE="+cf.Profile,
			"KNIT_PROFILE_STORE="+cf.ProfileStore,
		)
		cmd.Env = environ
		err := cmd.Run()
		if err != nil {
			return err
		}
		cmd.Wait()

		return nil
	}
}
