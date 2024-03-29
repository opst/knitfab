package postgres

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
	kdb "github.com/opst/knitfab/pkg/db"
	kpgdata "github.com/opst/knitfab/pkg/db/postgres/data"
	kpggbg "github.com/opst/knitfab/pkg/db/postgres/garbage"
	kpgnom "github.com/opst/knitfab/pkg/db/postgres/nominator"
	kpgplan "github.com/opst/knitfab/pkg/db/postgres/plan"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
	kpgrun "github.com/opst/knitfab/pkg/db/postgres/run"
	xe "github.com/opst/knitfab/pkg/errors"
)

type knitDBPostgres struct {
	pool    *pgxpool.Pool
	data    kdb.DataInterface
	runs    kdb.RunInterface
	plan    kdb.PlanInterface
	garbage kdb.GarbageInterface
}

type Config struct {
	Nominator kpgnom.Nominator
}

func DefaultConfig() Config {
	return Config{
		Nominator: kpgnom.DefaultNominator(),
	}
}

type Option func(*Config) *Config

func WithNominator(nominator kpgnom.Nominator) Option {
	return func(c *Config) *Config {
		c.Nominator = nominator
		return c
	}
}

func New(ctx context.Context, url string, options ...Option) (kdb.KnitDatabase, error) {
	pool, err := pgxpool.Connect(ctx, url)
	if err != nil {
		return nil, xe.Wrap(err)
	}

	c := DefaultConfig()
	for _, option := range options {
		c = *option(&c)
	}

	p := kpool.Wrap(pool)
	return &knitDBPostgres{
		pool:    pool,
		data:    kpgdata.New(p, kpgdata.WithNominator(c.Nominator)),
		runs:    kpgrun.New(p, kpgrun.WithNominator(c.Nominator)),
		plan:    kpgplan.New(p, kpgplan.WithNominator(c.Nominator)),
		garbage: kpggbg.New(p),
	}, nil
}

func (k *knitDBPostgres) Data() kdb.DataInterface {
	return k.data
}

func (k *knitDBPostgres) Runs() kdb.RunInterface {
	return k.runs
}

func (k *knitDBPostgres) Plan() kdb.PlanInterface {
	return k.plan
}

func (k *knitDBPostgres) Garbage() kdb.GarbageInterface {
	return k.garbage
}

func (k *knitDBPostgres) Close() error {
	k.pool.Close()
	return nil
}
