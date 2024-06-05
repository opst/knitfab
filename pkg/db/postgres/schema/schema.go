package schema

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
)

type pgSchema struct {
	pool             kpool.Pool
	schemaRepository string
}

// New creates a new Schema.
//
// # Args
//
// - schemaRepository: The path to the schema repository directory.
func New(pool kpool.Pool, schemaRepository string) *pgSchema {
	return &pgSchema{
		pool:             pool,
		schemaRepository: schemaRepository,
	}
}

type version struct {
	Version int
	Root    string
}

func (v version) Apply(ctx context.Context, conn kpool.Queryer) error {
	return filepath.WalkDir(v.Root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".sql") {
			return nil
		}

		query, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		if _, err := conn.Exec(ctx, string(query)); err != nil {
			return err
		}
		return nil
	})
}

func (s *pgSchema) Version(ctx context.Context) (int, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Release()

	var version int
	if err := conn.QueryRow(
		ctx, `SELECT max("version") FROM "schema_version"`,
	).Scan(&version); err != nil {
		if pgerr := new(pgconn.PgError); errors.As(err, &pgerr) {
			if pgerr.Code == pgerrcode.UndefinedTable {
				return 0, nil
			}
		}
		return -1, err
	}

	return version, nil
}

func (s *pgSchema) Upgrade(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	schemaVersions, err := s.versions()
	if err != nil {
		return err
	}

	currentVersion, err := s.Version(ctx)
	if err != nil {
		return err
	}

	for _, v := range schemaVersions {
		if v.Version <= currentVersion {
			continue
		}
		if err := v.Apply(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.Exec(
			ctx, `DELETE FROM "schema_version"`,
		); err != nil {
			return err
		}
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO "schema_version" ("version") VALUES ($1)`,
			v.Version,
		); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

func (s *pgSchema) Context(ctx context.Context) (context.Context, context.CancelFunc) {
	cctx, can := context.WithCancelCause(ctx)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		can(err)
		return cctx, func() {}
	}
	if err := w.Add(s.schemaRepository); err != nil {
		can(err)
		return cctx, func() {}
	}

	checkVersion := func() {
		vs, err := s.versions()
		if err != nil {
			can(fmt.Errorf("failed to read schema repository: %w", err))
			return
		}

		currentVersion, err := s.Version(ctx)
		if err != nil {
			can(fmt.Errorf("failed to get current schema version: %w", err))
			return
		}

		for _, v := range vs {
			if currentVersion < v.Version {
				can(fmt.Errorf(
					"schema is outdated: %d (in db) < %d (in repository)",
					currentVersion, v.Version,
				))
				return
			}
		}
	}

	go func() {
		defer w.Close()

		for {
			select {
			case <-cctx.Done():
				return
			case ev := <-w.Events:
				if !ev.Has(fsnotify.Create) && !ev.Has(fsnotify.Remove) {
					continue
				}
				if s.schemaRepository != filepath.Dir(ev.Name) {
					continue
				}

				checkVersion()
			}
		}
	}()

	checkVersion()
	return cctx, func() { can(nil) }
}

// versions lookup the schema from the schema repository.
//
// # Returns
//
// - []version: The list of schema versions, sorted by version number.
//
// - error: The error if any.
func (s *pgSchema) versions() ([]version, error) {
	dir, err := os.ReadDir(s.schemaRepository)
	if err != nil {
		return nil, err
	}

	schemaVersions := make([]version, 0, len(dir))
	for _, entry := range dir {
		if !entry.IsDir() {
			continue
		}

		v, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}

		schemaVersions = append(schemaVersions, version{
			Version: v,
			Root:    filepath.Join(s.schemaRepository, entry.Name()),
		})
	}
	slices.SortFunc(
		schemaVersions,
		func(i, j version) int { return cmp.Compare(i.Version, j.Version) },
	)

	return schemaVersions, nil
}

func Null() *nullSchema {
	return &nullSchema{}
}

type nullSchema struct{}

func (nullSchema) Upgrade(ctx context.Context) error {
	return errors.New("no schema repository available")
}

func (nullSchema) Version(ctx context.Context) (int, error) {
	return -1, nil
}

func (nullSchema) Context(ctx context.Context) (context.Context, context.CancelFunc) {
	return ctx, func() {}
}
