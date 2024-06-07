package show

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/opst/knitfab/cmd/knit/env"
	krst "github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/cmd/knit/subcommands/common"
	apirun "github.com/opst/knitfab/pkg/api/types/runs"
	"github.com/youta-t/flarc"
)

type Option struct {
	showInfo ShowInfo
	showLog  ShowLog
}

type ShowInfo func(
	ctx context.Context,
	client krst.KnitClient,
	runId string,
) (apirun.Detail, error)

type ShowLog func(
	ctx context.Context,
	client krst.KnitClient,
	runId string,
	follow bool,
) error

type Flags struct {
	Log    bool `flag:"log" help:"display the log of that Run"`
	Follow bool `flag:"follow" alias:"f" help:"follow log if Run is running"`
}

func WithRunner(
	showInfo ShowInfo, showLog ShowLog,
) func(*Option) *Option {
	return func(dfc *Option) *Option {
		dfc.showInfo = showInfo
		dfc.showLog = showLog
		return dfc
	}
}

const ARG_RUNID = "RUN_ID"

func New(
	options ...func(*Option) *Option,
) (flarc.Command, error) {
	option := &Option{
		showInfo: RunShowRunforInfo,
		showLog:  RunShowRunforLog,
	}

	for _, opt := range options {
		option = opt(option)
	}

	return flarc.NewCommand(
		"Return the Run information for the specified Run Id.",
		Flags{
			Log:    false,
			Follow: false,
		},
		flarc.Args{
			{
				Name: ARG_RUNID, Required: true,
				Help: "Id of the Run Id to be shown",
			},
		},
		common.NewTask(Task(option.showInfo, option.showLog)),
		flarc.WithDescription(`
Return the Run information for the specified Run Id.

when --log is passed, it display the log of that Run on the console.
`),
	)
}

func Task(showInfo ShowInfo, showLog ShowLog) common.Task[Flags] {
	return func(
		ctx context.Context,
		logger *log.Logger,
		knitEnv env.KnitEnv,
		client krst.KnitClient,
		cl flarc.Commandline[Flags],
		params []any,
	) error {
		runId := cl.Args()[ARG_RUNID][0]

		flags := cl.Flags()
		if !flags.Log {
			data, err := showInfo(ctx, client, runId)
			if err != nil {
				return fmt.Errorf("%w: Run Id:%s", err, runId)
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "    ")
			if err := enc.Encode(data); err != nil {
				logger.Panicf("fail to dump found Run")
			}
		} else {
			if err := showLog(ctx, client, runId, flags.Follow); err != nil {
				return err
			}
		}
		return nil
	}
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
