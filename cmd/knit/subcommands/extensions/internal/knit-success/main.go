//go:generate go build -o knit-success .
package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
)

func main() {
	KNIT_PROFILE := os.Getenv("KNIT_PROFILE")
	KNIT_PROFILE_STORE := os.Getenv("KNIT_PROFILE_STORE")
	args := os.Args[1:]

	stdin, err := io.ReadAll(os.Stdin)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			panic(err)
		}
	}

	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
		"KNIT_PROFILE":       KNIT_PROFILE,
		"KNIT_PROFILE_STORE": KNIT_PROFILE_STORE,
		"stdin":              string(stdin),
		"args":               args,
	}); err != nil {
		panic(err)
	}

	if _, err := os.Stderr.WriteString("error message\n"); err != nil {
		panic(err)
	}

	os.Exit(0)
}