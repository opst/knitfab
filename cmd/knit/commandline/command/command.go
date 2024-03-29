package command

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/google/subcommands"
	"github.com/opst/knitfab/cmd/knit/config"
	"github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/cmd/knit/rest"
	"github.com/opst/knitfab/pkg/commandline/flag/flagger"
	"github.com/opst/knitfab/pkg/commandline/usage"
)

type usagePlaceHolder struct {
	// full command name
	Command string
}

func (uph usagePlaceHolder) Fill(tpl string) (string, error) {
	tplExecuter, err := template.New("").Parse(tpl)
	if err != nil {
		return "", err
	}
	sb := new(strings.Builder)
	if err := tplExecuter.Execute(sb, uph); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// Help message components.
type Help struct {
	// short description of the command.
	Synopsis string

	// example of the command.
	Example string

	// long description of the command.
	Detail string
}

// Command is a command definition of knitfab CLI.
//
// To convert KnitCommand to subcommands.Command, use Build.
type KnitCommand[T any] interface {
	// Execute executes the command as its entrypoint.
	//
	// # Args
	//
	// - *log.Logger: logger to be used in the command.
	//
	// - env.KnitEnv: knitenv to be used in the command.
	//
	// - rest.KnitClient: client to be used in the command.
	//
	// - usage.FlagSet[T]: parsed flags and arguments.
	// It is output of Usage().Parse().
	//
	// # Returns
	//
	// - error: error if any.
	//
	// ErrUsage is returned if the command is invoked with invalid flags/arguments.
	Execute(ctx context.Context, l *log.Logger, e env.KnitEnv, c rest.KnitClient, flags usage.FlagSet[T]) error

	// Command name
	//
	// This method is expected to return same value in each call.
	Name() string

	// Command flags and arguments.
	//
	// This method is expected to return same value in each call.
	Usage() usage.Usage[T]

	// Help message components.
	//
	// This method is expected to return same value in each call.
	Help() Help
}

// ErrUsage is returned when the command is invoked with invalid flags/arguments.
//
// Return this from Execute.
var ErrUsage = errors.New("usage error")

type CommonFlags struct {
	Profile      string `flag:",help=knitprofile name to use"`
	ProfileStore string `flag:",help=path to knitprofile store file"`
	Env          string `flag:",help=path to knitenv file"`
}

type commonFlagDetection struct {
	home string
}

type CommonFlagDetectionOption func(*commonFlagDetection) *commonFlagDetection

func WithHome(home string) CommonFlagDetectionOption {
	return func(opt *commonFlagDetection) *commonFlagDetection {
		opt.home = home
		return opt
	}
}

func DefaultCommonFlags(from string, opt ...CommonFlagDetectionOption) (CommonFlags, error) {
	detparam := commonFlagDetection{
		home: "",
	}
	for _, o := range opt {
		detparam = *o(&detparam)
	}

	home := detparam.home
	if home == "" {
		_home, err := os.UserHomeDir()
		if err != nil {
			_home = ""
		}
		home = _home
	}

	if _from, err := filepath.Abs(from); err == nil {
		from = _from
	}

	profile := from

	profileFound := false
	envFound := false
	env := path.Join(from, "knitenv")
	for searchpath := from; ; {
		candidate := path.Join(searchpath, ".knitprofile")
		if !profileFound {
			if s, err := os.Stat(candidate); err == nil && s.Mode().IsRegular() {
				_profile, err := os.ReadFile(candidate)
				if err != nil {
					return CommonFlags{}, err
				}
				profileFound = true
				if p := strings.Split(string(_profile), "\n"); 0 < len(p) {
					profile = strings.TrimSpace(p[0])
				}
			}
		}
		if !envFound {
			candidate := path.Join(searchpath, "knitenv")
			if s, err := os.Stat(candidate); err == nil && s.Mode().IsRegular() {
				envFound = true
				env = candidate
			}
		}

		if profileFound && envFound {
			break
		}

		next := path.Dir(searchpath)
		if next == searchpath {
			break
		}
		searchpath = next
	}

	return CommonFlags{
		Profile:      profile,
		ProfileStore: path.Join(home, ".knit", "profile"),
		Env:          env,
	}, nil
}

type CommonFlagOption func(*CommonFlags) *CommonFlags

func WithProfile(profile string, store string) CommonFlagOption {
	return func(opt *CommonFlags) *CommonFlags {
		opt.Profile = profile
		opt.ProfileStore = store
		return opt
	}
}

func WithEnv(env string) CommonFlagOption {
	return func(opt *CommonFlags) *CommonFlags {
		opt.Env = env
		return opt
	}
}

// Build builds subcommands.Command from KnitCommand.
func Build[T any](kc KnitCommand[T], commonFlags CommonFlags) subcommands.Command {
	cf := flagger.New(commonFlags)

	// command specific flags
	f := kc.Usage()

	return &command[T]{c: kc, f: f, cf: cf}
}

// command is a wrapper of KnitCommand to be used as subcommands.Command.
type command[T any] struct {
	c      KnitCommand[T]
	f      usage.Usage[T]
	cf     *flagger.Flagger[CommonFlags]
	parent string
}

func (c *command[T]) SetParent(parent string) {
	c.parent = parent
}

func (c *command[T]) Name() string {
	return c.c.Name()
}

func (c *command[T]) Synopsis() string {
	return c.c.Help().Synopsis
}

func (c *command[T]) Usage() string {
	name := c.Name()
	if c.parent != "" {
		name = c.parent + " " + name
	}
	return BuildUsageMessage(
		name, c.c.Help(), c.c.Usage(), c.cf.Flags...,
	)
}

func BuildUsageMessage[T any](command string, help Help, usage usage.Usage[T], otherFlags ...flagger.Flag) string {
	indent := func(s string) string {
		return "  " + strings.ReplaceAll(s, "\n", "\n  ")
	}

	message := []string{"Usage: " + command + " " + usage.String()}

	if help.Detail != "" {
		message = append(
			message,
			"",
			indent(strings.TrimSpace(help.Detail)),
		)
	} else {
		message = append(
			message,
			"",
			indent(strings.TrimSpace(help.Synopsis)),
		)
	}

	if help.Example != "" {
		message = append(
			message,
			"",
			"Example:",
			indent(strings.TrimSpace(help.Example)),
		)
	}
	if args := usage.Args(); 0 < len(args) {
		message = append(
			message,
			"",
			"Arguments:",
		)
		for _, arg := range args {
			s := fmt.Sprintf("%s\n%s", arg.Name, strings.TrimSpace(arg.Help))
			s = strings.ReplaceAll(s, "\n", "\n	")
			message = append(message, indent(s))
		}
	}
	if 0 < len(usage.Flags())+len(otherFlags) {
		message = append(
			message,
			"",
			"Flags:",
			"",
			// subcommand's help command will show flags.
		)
	}
	tpl := strings.Join(message, "\n")

	plh := usagePlaceHolder{Command: command}
	text, err := plh.Fill(tpl)
	if err != nil {
		return tpl + "(templating error: " + err.Error() + ")\n"
	}
	return text
}

func (c *command[T]) SetFlags(f *flag.FlagSet) {
	c.f.SetFlags(f)
	c.cf.SetFlags(f)
}

func (c *command[T]) Execute(
	ctx context.Context,
	f *flag.FlagSet,
	args ...interface{},
) subcommands.ExitStatus {
	logger, _, ok := extract[*log.Logger](args)
	if !ok {
		return subcommands.ExitFailure
	}

	logger = log.New(
		logger.Writer(),
		"["+strings.TrimSpace(logger.Prefix())+" "+c.c.Name()+"] ",
		logger.Flags(),
	)

	commonOption := c.cf.Values
	prof, err := config.LoadProfileStore(commonOption.ProfileStore)
	if err != nil {
		if errors.Is(err, config.ErrProfileStoreNotFound) {
			logger.Printf(
				"knitprofile store (%s) not found.\nPlease try `knit init` first. Ask your admin to get knitprofile.",
				commonOption.ProfileStore,
			)
			return subcommands.ExitFailure
		}
		logger.Printf(
			"knitprofile (%s) can not be loaded: %s",
			commonOption.ProfileStore, err,
		)
		return subcommands.ExitFailure
	}

	profile, ok := prof[commonOption.Profile]
	if !ok {
		logger.Printf(
			"knitprofile (%s) not found in %s. Please try `knit init` first. Ask your admin to get knitprofile.",
			commonOption.Profile, commonOption.ProfileStore,
		)
		return subcommands.ExitFailure
	}

	client, err := rest.NewClient(profile)
	if err != nil {
		logger.Printf(
			"knitprofile (%s in %s) may be broken: %s\n\nRemove it and try `knit init` again. Ask your admin to get knitprofile.",
			commonOption.Profile, commonOption.ProfileStore,
			err,
		)
		return subcommands.ExitFailure
	}

	// parse positional arguments
	flg, err := c.f.Parse(f.Args())
	if err != nil {
		logger.Println(err)
		if p, _, ok := extract[*subcommands.Commander](args); ok {
			p.ExplainCommand(os.Stderr, c)
		}

		return subcommands.ExitUsageError
	}

	e, err := env.LoadKnitEnv(commonOption.Env)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			e = env.New()
		} else {
			logger.Println(err)
			return subcommands.ExitFailure
		}
	}

	if err := c.c.Execute(ctx, logger, *e, client, flg); err != nil {
		logger.Println(err)
		if errors.Is(err, ErrUsage) {
			if p, _, ok := extract[*subcommands.Commander](args); ok {
				p.ExplainCommand(os.Stderr, c)
			}

			return subcommands.ExitUsageError
		}
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}

// KnitCommander is a command group of Knit CLI.
//
// This is also a subcommands.Command.
type KnitCommander struct {
	name    string
	help    Help
	command []subcommands.Command
	parent  string
}

// NewCommander builds new KnitCommander.
//
// To add subcommand, use Register.
//
// # Args
//
// - name: name of this subcommand group.
//
// - help: help message components.
func NewCommander(name string, help Help) *KnitCommander {
	return &KnitCommander{
		name:    name,
		help:    help,
		command: []subcommands.Command{},
	}
}

func (kc *KnitCommander) SetParent(parent string) {
	kc.parent = parent
}

func (kc *KnitCommander) Name() string {
	return kc.name
}

func (*KnitCommander) SetFlags(*flag.FlagSet) {
	// noop
}

func (k *KnitCommander) Synopsis() string {
	return k.help.Synopsis
}

func (k *KnitCommander) Usage() string {
	s := k.help.Synopsis
	if k.help.Detail != "" {
		s = k.help.Detail
	}
	s = strings.TrimSpace(s)

	usage := []string{s}

	if len(k.command) != 0 {
		usage = append(
			usage,
			"",
			"Subcommands:",
		)
		for _, cmd := range k.command {
			usage = append(
				usage,
				fmt.Sprintf("\t%s\t%s", cmd.Name(), cmd.Synopsis()),
			)
		}
	}

	plh := usagePlaceHolder{Command: k.name}
	if k.parent != "" {
		plh.Command = k.parent + " " + plh.Command
	}
	tpl := strings.Join(usage, "\n") + "\n\n"
	text, err := plh.Fill(tpl)
	if err != nil {
		return tpl + "(templating error: " + err.Error() + ")\n"
	}
	return text
}

func (kcg *KnitCommander) Register(cmd subcommands.Command) {
	kcg.command = append(kcg.command, cmd)
}

func (kc *KnitCommander) Execute(
	ctx context.Context,
	f *flag.FlagSet,
	args ...interface{},
) subcommands.ExitStatus {
	logger, rest, ok := extract[*log.Logger](args)
	if !ok {
		return subcommands.ExitFailure
	}
	_, rest, _ = extract[*subcommands.Commander](rest)

	l := log.New(
		logger.Writer(),
		strings.TrimSpace(logger.Prefix())+" "+kc.name,
		logger.Flags(),
	)

	commander := subcommands.NewCommander(f, kc.name)
	commander.Register(subcommands.HelpCommand(), "help")
	commander.Register(subcommands.FlagsCommand(), "help")
	commander.Register(subcommands.CommandsCommand(), "help")
	for _, cmd := range kc.command {
		if c, ok := cmd.(interface{ SetParent(string) }); ok {
			c.SetParent(kc.parent + " " + kc.name)
		}
		commander.Register(cmd, "")
	}
	args = append([]any{l, commander}, rest...)

	return commander.Execute(ctx, args...)
}

func extract[T any](args []any) (T, []any, bool) {
	var value T
	var rest []any
	for i, arg := range args {
		if v, ok := arg.(T); ok {
			value = v
			rest = append(rest, args[i+1:]...)
			return value, rest, true
		}
		rest = append(rest, arg)
	}
	return value, rest, false
}
