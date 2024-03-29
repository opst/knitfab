package flagger_test

import (
	"flag"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/commandline/flag/flagger"
	internal "github.com/opst/knitfab/pkg/commandline/flag/flagger/internal"
)

func TestFlagger(t *testing.T) {

	t.Run("flagger with Flags parses flags", func(t *testing.T) {
		f := internal.Flags{
			BoolFlag:     false,
			IntFlag:      1,
			Int64Flag:    2,
			UintFlag:     3,
			Uint64Flag:   4,
			Float64Flag:  5.5,
			StrFlag:      "6",
			DurationFlag: 7,
			ValueFlag:    &internal.V{Value: "8"},
		}

		testee := flagger.New(f)

		fs := new(flag.FlagSet)
		if _, err := testee.SetFlags(fs); err != nil {
			t.Fatal(err)
		}

		if err := fs.Parse([]string{
			"--bool", // set true
			"--int", "10",
			"--int64", "20",
			"--uint", "30",
			"--uint64", "40",
			"--float64", "50.5",
			"--string", "60",
			"--duration", "7s",
			"--value", "eight",
		}); err != nil {
			t.Fatal(err)
		}

		if testee.Values.BoolFlag != true {
			t.Errorf("expected %v, got %v", true, testee.Values.BoolFlag)
		}
		if testee.Values.IntFlag != 10 {
			t.Errorf("expected %v, got %v", 10, testee.Values.IntFlag)
		}
		if testee.Values.Int64Flag != 20 {
			t.Errorf("expected %v, got %v", 20, testee.Values.Int64Flag)
		}
		if testee.Values.UintFlag != 30 {
			t.Errorf("expected %v, got %v", 30, testee.Values.UintFlag)
		}
		if testee.Values.Uint64Flag != 40 {
			t.Errorf("expected %v, got %v", 40, testee.Values.Uint64Flag)
		}
		if testee.Values.Float64Flag != 50.5 {
			t.Errorf("expected %v, got %v", 50.5, testee.Values.Float64Flag)
		}
		if testee.Values.StrFlag != "60" {
			t.Errorf("expected %v, got %v", "60", testee.Values.StrFlag)
		}
		if testee.Values.DurationFlag != 7*time.Second {
			t.Errorf("expected %v, got %v", 7*time.Second, testee.Values.DurationFlag)
		}
		if testee.Values.ValueFlag.Value != "eight" {
			t.Errorf("expected %v, got %v", "eight", testee.Values.ValueFlag.Value)
		}
	})

	t.Run("flagger with Flags parses flags in short option", func(t *testing.T) {
		f := internal.Flags{
			BoolFlag:     false,
			IntFlag:      1,
			Int64Flag:    2,
			UintFlag:     3,
			Uint64Flag:   4,
			Float64Flag:  5.5,
			StrFlag:      "6",
			DurationFlag: 7,
			ValueFlag:    &internal.V{Value: "8"},
		}

		testee := flagger.New(f)

		fs := new(flag.FlagSet)
		if _, err := testee.SetFlags(fs); err != nil {
			t.Fatal(err)
		}

		if err := fs.Parse([]string{
			"-b", // set true
			"-i", "10",
			"-l", "20",
			"-u", "30",
			"-U", "40",
			"-f", "50.5",
			"-s", "60",
			"-d", "7s",
			"-v", "eight",
		}); err != nil {
			t.Fatal(err)
		}

		if testee.Values.BoolFlag != true {
			t.Errorf("expected %v, got %v", true, testee.Values.BoolFlag)
		}
		if testee.Values.IntFlag != 10 {
			t.Errorf("expected %v, got %v", 10, testee.Values.IntFlag)
		}
		if testee.Values.Int64Flag != 20 {
			t.Errorf("expected %v, got %v", 20, testee.Values.Int64Flag)
		}
		if testee.Values.UintFlag != 30 {
			t.Errorf("expected %v, got %v", 30, testee.Values.UintFlag)
		}
		if testee.Values.Uint64Flag != 40 {
			t.Errorf("expected %v, got %v", 40, testee.Values.Uint64Flag)
		}
		if testee.Values.Float64Flag != 50.5 {
			t.Errorf("expected %v, got %v", 50.5, testee.Values.Float64Flag)
		}
		if testee.Values.StrFlag != "60" {
			t.Errorf("expected %v, got %v", "60", testee.Values.StrFlag)
		}
		if testee.Values.DurationFlag != 7*time.Second {
			t.Errorf("expected %v, got %v", 7*time.Second, testee.Values.DurationFlag)
		}
		if testee.Values.ValueFlag.Value != "eight" {
			t.Errorf("expected %v, got %v", "eight", testee.Values.ValueFlag.Value)
		}
	})

	t.Run("flagger with Flags parses flags in short option", func(t *testing.T) {
		f := internal.FlagsAllDefaulted{
			BoolFlag:     false,
			IntFlag:      1,
			Int64Flag:    2,
			UintFlag:     3,
			Uint64Flag:   4,
			Float64Flag:  5.5,
			StrFlag:      "6",
			DurationFlag: 7,
			ValueFlag:    &internal.V{Value: "8"},
		}

		testee := flagger.New(f)

		fs := new(flag.FlagSet)
		if _, err := testee.SetFlags(fs); err != nil {
			t.Fatal(err)
		}

		if err := fs.Parse([]string{
			"--bool-flag", // set true
			"--int-flag", "10",
			"--int64-flag", "20",
			"--uint-flag", "30",
			"--uint64-flag", "40",
			"--float64-flag", "50.5",
			"--str-flag", "60",
			"--duration-flag", "7s",
			"--value-flag", "eight",
		}); err != nil {
			t.Fatal(err)
		}

		if testee.Values.BoolFlag != true {
			t.Errorf("expected %v, got %v", true, testee.Values.BoolFlag)
		}
		if testee.Values.IntFlag != 10 {
			t.Errorf("expected %v, got %v", 10, testee.Values.IntFlag)
		}
		if testee.Values.Int64Flag != 20 {
			t.Errorf("expected %v, got %v", 20, testee.Values.Int64Flag)
		}
		if testee.Values.UintFlag != 30 {
			t.Errorf("expected %v, got %v", 30, testee.Values.UintFlag)
		}
		if testee.Values.Uint64Flag != 40 {
			t.Errorf("expected %v, got %v", 40, testee.Values.Uint64Flag)
		}
		if testee.Values.Float64Flag != 50.5 {
			t.Errorf("expected %v, got %v", 50.5, testee.Values.Float64Flag)
		}
		if testee.Values.StrFlag != "60" {
			t.Errorf("expected %v, got %v", "60", testee.Values.StrFlag)
		}
		if testee.Values.DurationFlag != 7*time.Second {
			t.Errorf("expected %v, got %v", 7*time.Second, testee.Values.DurationFlag)
		}
		if testee.Values.ValueFlag.Value != "eight" {
			t.Errorf("expected %v, got %v", "eight", testee.Values.ValueFlag.Value)
		}
	})
}
