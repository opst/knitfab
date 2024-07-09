package testenv

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"

	k8stestenv "github.com/opst/knitfab/pkg/workloads/k8s/testenv"
)

type pg struct {
	pool *pgxpool.Pool
	pf   k8stestenv.Portforwarding
}

func (p *pg) GetPool(ctx context.Context, t *testing.T) kpool.Pool {
	t.Cleanup(func() {
		t.Helper()
		ClearTables(ctx, p.pool, t)
	})

	ClearTables(ctx, p.pool, t)
	return kpool.Wrap(p.pool)
}

type pgNoClean struct {
	pool *pgxpool.Pool
	pf   k8stestenv.Portforwarding
}

func (p *pgNoClean) GetPool(ctx context.Context, t *testing.T) kpool.Pool {
	return kpool.Wrap(p.pool)
}

// PoolBroaker is a interface to get a pool.
type PoolBroaker interface {
	// GetPool returns a pool.
	//
	// Tables are cleaned up before returning and after t.
	GetPool(ctx context.Context, t *testing.T) kpool.Pool
}

type pgConnOptions struct {
	User         string
	Password     string
	Dbname       string
	DoNotCleanup bool
}

type PgConnOption func(*pgConnOptions) *pgConnOptions

func WithUser(user string) PgConnOption {
	return func(o *pgConnOptions) *pgConnOptions {
		o.User = user
		return o
	}
}

func WithPassword(password string) PgConnOption {
	return func(o *pgConnOptions) *pgConnOptions {
		o.Password = password
		return o
	}
}

func WithDbname(dbname string) PgConnOption {
	return func(o *pgConnOptions) *pgConnOptions {
		o.Dbname = dbname
		return o
	}
}

func WithDoNotCleanup() PgConnOption {
	return func(o *pgConnOptions) *pgConnOptions {
		o.DoNotCleanup = true
		return o
	}
}

// NewPoolBroaker returns a PoolBroaker.
//
// This function provides a postgres pool (via port-forwarding to pod).
//
// # Args
//
// - ctx: When this context is canceled, the database connection behind the pool will be lost
// (since  port-forwarding is stopped).
//
// - t: scope of the PoolBroaker.
// When this test is finished, the broaker will be shutdown.
func NewPoolBroaker(ctx context.Context, t *testing.T, options ...PgConnOption) PoolBroaker {
	t.Helper()

	// see template "pkg-db-postgres.yaml"
	namespace := k8stestenv.Namespace()
	postgresSvcName := "database"
	postgresPortName := "postgres"

	pgctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	pf, err := k8stestenv.Portforward(
		pgctx, namespace, postgresSvcName, postgresPortName,
		k8stestenv.WithLog(t),
	)
	if err != nil {
		cancel()
		t.Fatal(err)
	}

	return NewPoolBroakerWithForwarder(ctx, t, pf, options...)
}

func NewPoolBroakerWithForwarder(
	ctx context.Context,
	t *testing.T,
	pf k8stestenv.Portforwarding,
	options ...PgConnOption,
) PoolBroaker {
	t.Helper()
	t.Cleanup(func() {
		if err := pf.Err(); err != nil {
			t.Logf("error caused in port-forwarding: %v", err)
		}
	})

	opts := &pgConnOptions{
		// default values; see template "pkg-db-postgres.yaml"
		User:     "test-user",
		Password: "test-pass",
		Dbname:   "knit",
	}
	for _, o := range options {
		opts = o(opts)
	}

	pool, err := pgxpool.Connect(
		ctx,
		fmt.Sprintf(
			"postgres://%s:%s@%s/%s",
			opts.User, opts.Password, pf.LocalAddr(), opts.Dbname,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	if opts.DoNotCleanup {
		return &pgNoClean{pool: pool, pf: pf}
	} else {
		return &pg{pool: pool, pf: pf}
	}
}

func ClearTables(ctx context.Context, p *pgxpool.Pool, t *testing.T) {
	t.Helper()

	conn, err := p.Acquire(ctx)
	defer conn.Release()

	if err != nil {
		t.Errorf("fail to clean-up tables.: %v", err)
	}

	for _, command := range []string{
		`truncate "plan" RESTART IDENTITY cascade`,
		`truncate "knit_id" RESTART IDENTITY cascade`,
		`truncate "tag_key" RESTART IDENTITY cascade`,
		// by cascade, all row in tables should be deleted.
	} {
		_, err = conn.Exec(ctx, command)
		if err != nil {
			t.Errorf("fail to clean-up tables.: %v", err)
		}
	}
}
