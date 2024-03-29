package show

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	kcmd "github.com/opst/knitfab/cmd/knit/commandline/command"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	apirun "github.com/opst/knitfab/pkg/api/types/runs"
	"github.com/opst/knitfab/pkg/commandline/usage"
	"github.com/opst/knitfab/pkg/utils"
)

type Command struct {
	task *struct {
		forInfo ForInfo
		forLog  ForLog
	}
}

type ForInfo func(
	ctx context.Context,
	client krst.KnitClient,
	runId string,
) (apirun.Detail, error)

type ForLog func(
	ctx context.Context,
	client krst.KnitClient,
	runId string,
	follow bool,
) error

type Flags struct {
	Log    bool `flag:",help=display the log of that Run"`
	Follow bool `flag:",sho¥rt=f,help=follow log if Run is running"`
}

func WithRunner(
	funcForInfo ForInfo, funcForLog ForLog,
) func(*Command) *Command {
	return func(dfc *Command) *Command {
		dfc.task = &struct {
			forInfo ForInfo
			forLog  ForLog
		}{
			forInfo: funcForInfo,
			forLog:  funcForLog,
		}
		return dfc
	}
}

func New(
	options ...func(*Command) *Command,
) kcmd.KnitCommand[Flags] {
	return utils.ApplyAll(
		&Command{
			task: &struct {
				forInfo ForInfo
				forLog  ForLog
			}{
				forInfo: RunShowRunforInfo,
				forLog:  RunShowRunforLog,
			},
		},
		options...,
	)
}

const ARG_RUNID = "RUN_ID"

func (cmd *Command) Usage() usage.Usage[Flags] {
	return usage.New(
		Flags{},
		usage.Args{
			{
				Name: ARG_RUNID, Required: true,
				Help: "Id of the Run Id to be shown",
			},
		},
	)
}

func (cmd *Command) Name() string {
	return "show"
}

func (cmd *Command) Help() kcmd.Help {
	return kcmd.Help{
		Synopsis: "Return the Run information for the specified Run Id.",
		Detail: `
Return the Run information for the specified Run Id.

when --log is passed, it display the log of that Run on the console.
`,
	}
}

func (cmd *Command) Execute(
	ctx context.Context,
	l *log.Logger,
	e kenv.KnitEnv,
	client krst.KnitClient,
	f usage.FlagSet[Flags],
) error {
	runId := f.Args[ARG_RUNID][0]

	if !f.Flags.Log {
		data, err := cmd.task.forInfo(ctx, client, runId)
		if err != nil {
			return fmt.Errorf("%w: Run Id:%s", err, runId)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "    ")
		if err := enc.Encode(data); err != nil {
			l.Panicf("fail to dump found Run")
		}
	} else {
		if err := cmd.task.forLog(ctx, client, runId, f.Flags.Follow); err != nil {
			return err
		}
	}
	return nil
}

func RunShowRunforInfo(
	ctx context.Context, client krst.KnitClient, runId string,
) (apirun.Detail, error) {
	result, err := client.GetRun(ctx, runId)
	if err != nil {
		return apirun.Detail{}, err
	}
	return result, nil
}

func RunShowRunforLog(
	ctx context.Context, client krst.KnitClient, runId string, follow bool,
) error {
	r, err := client.GetRunLog(ctx, runId, follow)
	if err != nil {
		return err
	}
	defer r.Close()

	_, err = io.Copy(os.Stdout, r)
	if err != nil {
		return err
	}
	return nil
}
