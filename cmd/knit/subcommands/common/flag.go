package common

import (
	"os"
	"path"
	"path/filepath"
	"strings"
)

// ErrUsage is returned when the command is invoked with invalid flags/arguments.
//
// Return this from Execute.
// var ErrUsage = flarc.ErrUsage

type CommonFlags struct {
	Profile      string `flag:"profile" help:"knitprofile name to use"`
	ProfileStore string `flag:"profile-store" help:"path to knitprofile store file"`
	Env          string `flag:"env" help:"path to knitenv file"`
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

func Flags(from string, opt ...CommonFlagDetectionOption) (CommonFlags, error) {
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
