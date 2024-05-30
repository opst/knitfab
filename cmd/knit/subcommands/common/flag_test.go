package common_test

import (
	"path/filepath"
	"testing"

	common "github.com/opst/knitfab/cmd/knit/subcommands/common"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestDefaultCommonFlags(t *testing.T) {
	t.Run("it returns default value from given directory", func(t *testing.T) {
		cf := try.To(common.Flags(
			"./testdata/current",
			common.WithHome("./testdata/home"),
		)).OrFatal(t)

		if try.To(filepath.Abs(cf.ProfileStore)).OrFatal(t) != try.To(filepath.Abs("./testdata/home/.knit/profile")).OrFatal(t) {
			t.Errorf("wrong profile store: %s", cf.ProfileStore)
		}

		if cf.Profile != "test" {
			t.Errorf("wrong profile: %s", cf.Profile)
		}

		if cf.Env != try.To(filepath.Abs("./testdata/current/knitenv")).OrFatal(t) {
			t.Errorf("wrong env: %s", cf.Env)
		}
	})

	t.Run("it returns default value from ancestors of given directory", func(t *testing.T) {
		cf := try.To(common.Flags(
			"./testdata/current/children/folder",
			common.WithHome("./testdata/home"),
		)).OrFatal(t)

		if try.To(filepath.Abs(cf.ProfileStore)).OrFatal(t) != try.To(filepath.Abs("./testdata/home/.knit/profile")).OrFatal(t) {
			t.Errorf("wrong profile store: %s", cf.ProfileStore)
		}

		if cf.Profile != "test" {
			t.Errorf("wrong profile: %s", cf.Profile)
		}

		if cf.Env != try.To(filepath.Abs("./testdata/current/knitenv")).OrFatal(t) {
			t.Errorf("wrong env: %s", cf.Env)
		}
	})
}
