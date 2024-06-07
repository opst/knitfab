//go:generate go run github.com/Songmu/gocredits/cmd/gocredits@v0.3.0 -w
package main

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"

	"github.com/opst/knitfab/pkg/db/postgres"
	kio "github.com/opst/knitfab/pkg/io"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/youta-t/flarc"
)

type Flag struct {
	Host     string `flag:"host" help:"The host of the database."`
	Port     int    `flag:"port" help:"The port of the database."`
	User     string `flag:"user" help:"The user of the database."`
	Password string `flag:"pass" help:"The password of the database."`
	Database string `flag:"database" help:"The name of the database."`

	Schema  string `flag:"schema" help:"The path to the schema repository directory."`
	License bool   `flag:"license" help:"Print the license."`
}

const ARG_SCHEMA_DEST = "ARG_SCHEMA_DEST"

//go:embed CREDITS
var CREDITS string

func main() {
	logger := log.Default()
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt, os.Kill,
	)
	defer cancel()

	port := 5432
	if sp := os.Getenv("DB_PORT"); sp != "" {
		p, err := strconv.Atoi(sp)
		if err == nil {
			port = p
		}
	}

	cmd := try.To(flarc.NewCommand(
		"database schema upgrader",
		Flag{
			Host:     os.Getenv("DB_HOST"),
			Port:     port,
			User:     os.Getenv("DB_USER"),
			Password: os.Getenv("DB_PASSWORD"),
			Database: os.Getenv("DB_NAME"),

			Schema:  os.Getenv("KNIT_SCHEMA"),
			License: false,
		},
		flarc.Args{
			{
				Name: ARG_SCHEMA_DEST, Help: "The schema files are copied to these directories.",
				Required: false, Repeatable: false,
			},
		},
		func(ctx context.Context, c flarc.Commandline[Flag], a []any) error {
			flags := c.Flags()
			if flags.License {
				_, err := io.WriteString(c.Stdout(), CREDITS)
				return err
			}

			dest := c.Args()[ARG_SCHEMA_DEST]
			if len(dest) != 0 {
				logger.Println("copying schema files...")
				if err := kio.DirCopy(flags.Schema, dest[0]); err != nil {
					return err
				}
			}

			db, err := postgres.New(
				ctx,
				fmt.Sprintf(
					"postgres://%s:%s@%s:%d/%s",
					flags.User, flags.Password, flags.Host, flags.Port, flags.Database,
				),
				postgres.WithSchemaRepository(flags.Schema),
			)
			if err != nil {
				return err
			}

			return db.Schema().Upgrade(ctx)
		},
	)).OrFatal(logger)

	os.Exit(flarc.Run(ctx, cmd))
}
