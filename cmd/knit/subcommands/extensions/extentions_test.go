package extensions_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	common "github.com/opst/knitfab/cmd/knit/subcommands/common"
	"github.com/opst/knitfab/cmd/knit/subcommands/extensions"
	"github.com/opst/knitfab/cmd/knit/subcommands/internal/commandline"
	"github.com/opst/knitfab/cmd/knit/subcommands/logger"
	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestFindSubcommand(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	dir3 := t.TempDir()

	want := []extensions.ExtentionCommand{}
	parentCommand := "knit"

	// Create a file in dir1
	{
		// non executable
		filename1 := filepath.Join(dir1, "file1")
		f1 := try.To(os.Create(filename1)).OrFatal(t)
		f1.Close()

		// executable
		filename2 := filepath.Join(dir1, parentCommand+"-file2")
		f2 := try.To(os.Create(filename2)).OrFatal(t)
		f2.Close()
		if err := os.Chmod(filename2, 0700); err != nil {
			t.Fatal(err)
		}
		want = append(want, extensions.ExtentionCommand{
			Name: "file2",
			Path: try.To(filepath.Abs(filename2)).OrFatal(t),
		})

		filename3 := filepath.Join(dir1, "not-"+parentCommand+"-file2")
		f3 := try.To(os.Create(filename3)).OrFatal(t)
		f3.Close()
		if err := os.Chmod(filename3, 0700); err != nil {
			t.Fatal(err)
		}
		// should not be detected because of the prefix
	}

	// Create a file in dir2
	{
		// non executable
		filename1 := filepath.Join(dir2, "file1")
		f1 := try.To(os.Create(filename1)).OrFatal(t)
		f1.Close()

		// windows executable extentions are trimmed
		for _, ext := range []string{"exe", "bat", "cmd", "com"} {
			// executable
			name := "with_suffix_" + ext
			filename2 := filepath.Join(dir2, "knit-"+name+"."+ext)
			f2 := try.To(os.Create(filename2)).OrFatal(t)
			f2.Close()
			if err := os.Chmod(filename2, 0700); err != nil {
				t.Fatal(err)
			}
			want = append(want, extensions.ExtentionCommand{
				Name: name,
				Path: try.To(filepath.Abs(filename2)).OrFatal(t),
			})
		}

		// normal extension is not trimmed
		filename3 := filepath.Join(dir2, "knit-file3.ext")
		f3 := try.To(os.Create(filename3)).OrFatal(t)
		f3.Close()
		if err := os.Chmod(filename3, 0700); err != nil {
			t.Fatal(err)
		}
		want = append(want, extensions.ExtentionCommand{
			Name: "file3.ext",
			Path: try.To(filepath.Abs(filename3)).OrFatal(t),
		})

	}
	// Create a file in dir3
	{
		// non executable
		filename1 := filepath.Join(dir3, "file1")
		f1 := try.To(os.Create(filename1)).OrFatal(t)
		f1.Close()

		// executable

		// conflicted subcommand name; should be ignored
		filename2 := filepath.Join(dir3, "knit-file2")
		f2 := try.To(os.Create(filename2)).OrFatal(t)
		f2.Close()
		if err := os.Chmod(filename2, 0700); err != nil {
			t.Fatal(err)
		}
	}

	t.Setenv(
		"PATH",
		strings.Join([]string{dir1, dir2, dir3, dir1}, string(os.PathListSeparator)),
	)
	got := extensions.FindSubcommand("knit-")

	if !cmp.SliceContentEq(want, got) {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestCommand(t *testing.T) {
	// to generate commands to be invoked in this test
	if err := exec.Command("go", "generate", "./...").Run(); err != nil {
		t.Fatal(err)
	}

	type Output struct {
		KnitProfile      string   `json:"KNIT_PROFILE"`
		KnitProfileStore string   `json:"KNIT_PROFILE_STORE"`
		Stdin            string   `json:"stdin"`
		Args             []string `json:"args"`
	}

	outputEq := func(a, b Output) bool {
		return a.KnitProfile == b.KnitProfile &&
			a.KnitProfileStore == b.KnitProfileStore &&
			a.Stdin == b.Stdin &&
			cmp.SliceEq(a.Args, b.Args)
	}

	type When struct {
		ExtCmd extensions.ExtentionCommand
		Args   []string
	}

	theory := func(when When, wantErr bool) func(t *testing.T) {
		return func(t *testing.T) {
			testee := extensions.Task(when.ExtCmd)

			ctx := context.Background()

			stdin := new(bytes.Buffer)
			stdout := new(bytes.Buffer)
			stderr := new(strings.Builder)

			stdin.WriteString("stdin message\n")

			err := testee(
				ctx,
				logger.Null(),
				common.CommonFlags{
					Profile:      "test-profile",
					ProfileStore: "test-profile-store",
				},
				commandline.MockCommandline[struct{}]{
					Fullname_: "knit extention",
					Args_: map[string][]string{
						extensions.PARAMS: when.Args,
					},
					Stdin_:  stdin,
					Stdout_: stdout,
					Stderr_: stderr,
				},
				[]any{},
			)

			if got := (err != nil); wantErr != got {
				t.Errorf("returned error: want = %v, but got = %v", wantErr, got)
			}

			gotStdout := Output{}
			if err := json.Unmarshal(stdout.Bytes(), &gotStdout); err != nil {
				t.Fatal(err)
			}

			wantStdout := Output{
				KnitProfile:      "test-profile",
				KnitProfileStore: "test-profile-store",
				Stdin:            "stdin message\n",
				Args:             when.Args,
			}

			if !outputEq(wantStdout, gotStdout) {
				t.Errorf("want %v, got %v", wantStdout, gotStdout)
			}

			gotStderr := stderr.String()
			wantStderr := "error message\n"
			if got := string(gotStderr); got != wantStderr {
				t.Errorf("want:\n%s\ngot:\n%s", wantStderr, got)
			}
		}
	}

	t.Run("subcommand successes", theory(
		When{
			ExtCmd: extensions.ExtentionCommand{
				Name: "success",
				Path: "./internal/knit-success/knit-success",
			},
			Args: []string{"-f", "--flag", "value", "arg1", "arg2"},
		},
		false,
	))

	t.Run("subcommand failures", theory(
		When{
			ExtCmd: extensions.ExtentionCommand{
				Name: "failed",
				Path: "./internal/knit-failed/knit-failed",
			},
			Args: []string{"-f", "--flag", "value", "arg1", "arg2"},
		},
		true,
	))
}
