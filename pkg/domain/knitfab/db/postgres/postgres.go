package postgres

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	kdata "github.com/opst/knitfab/pkg/domain/data/db"
	kpgdata "github.com/opst/knitfab/pkg/domain/data/db/postgres"
	kgarbage "github.com/opst/knitfab/pkg/domain/garbage/db"
	kpggbg "github.com/opst/knitfab/pkg/domain/garbage/db/postgres"
	kkeychain "github.com/opst/knitfab/pkg/domain/keychain/db"
	kpgkeychain "github.com/opst/knitfab/pkg/domain/keychain/db/postgres"
	dbInterface "github.com/opst/knitfab/pkg/domain/knitfab/db"
	kpgnom "github.com/opst/knitfab/pkg/domain/nomination/db/postgres"
	kplan "github.com/opst/knitfab/pkg/domain/plan/db"
	kpgplan "github.com/opst/knitfab/pkg/domain/plan/db/postgres"
	krun "github.com/opst/knitfab/pkg/domain/run/db"
	kpgrun "github.com/opst/knitfab/pkg/domain/run/db/postgres"
	kschema "github.com/opst/knitfab/pkg/domain/schema/db"
	kpgschema "github.com/opst/knitfab/pkg/domain/schema/db/postgres"
	xe "github.com/opst/knitfab/pkg/errors"
)

type knitDBPostgres struct {
	pool     *pgxpool.Pool
	data     kdata.DataInterface
	runs     krun.Interface
	plan     kplan.PlanInterface
	garbage  kgarbage.Interface
	keychain kkeychain.KeychainInterface
	schema   kschema.SchemaInterface
}

type Config struct {
	Nominator        kpgnom.Nominator
	SchemaRepository string
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

func WithSchemaRepository(repository string) Option {
	return func(c *Config) *Config {
		c.SchemaRepository = repository
		return c
	}
}

func New(
	ctx context.Context,
	url string,
	options ...Option,
) (dbInterface.KnitDatabase, error) {
	pool, err := pgxpool.Connect(ctx, url)
	if err != nil {
		return nil, xe.Wrap(err)
	}

	c := DefaultConfig()
	for _, option := range options {
		c = *option(&c)
	}

	p := kpool.Wrap(pool)
	var schema kschema.SchemaInterface = kpgschema.Null()
	if c.SchemaRepository != "" {
		schema = kpgschema.New(p, c.SchemaRepository)
	}

	return &knitDBPostgres{
		pool:     pool,
		data:     kpgdata.New(p, kpgdata.WithNominator(c.Nominator)),
		runs:     kpgrun.New(p, kpgrun.WithNominator(c.Nominator)),
		plan:     kpgplan.New(p, kpgplan.WithNominator(c.Nominator)),
		schema:   schema,
		garbage:  kpggbg.New(p),
		keychain: kpgkeychain.New(p),
	}, nil
}

func (k *knitDBPostgres) Data() kdata.DataInterface {
	return k.data
}

func (k *knitDBPostgres) Run() krun.Interface {
	return k.runs
}

func (k *knitDBPostgres) Plan() kplan.PlanInterface {
	return k.plan
}

func (k *knitDBPostgres) Garbage() kgarbage.Interface {
	return k.garbage
}

func (k *knitDBPostgres) Schema() kschema.SchemaInterface {
	return k.schema
}

func (k *knitDBPostgres) Keychain() kkeychain.KeychainInterface {
	return k.keychain
}

func (k *knitDBPostgres) Close() error {
	k.pool.Close()
	return nil
}
