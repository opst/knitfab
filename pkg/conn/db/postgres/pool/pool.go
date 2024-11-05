package pool

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/opst/knitfab/pkg/utils/slices"
)

// something begins SQL Transaction
//
// this is extracted interface from "pgxpool.Pool", "pgpool.Conn" or "pgx.Tx".
// when you need more details, see them.
type Begin interface {
	Begin(ctx context.Context) (Tx, error)
	// // When you need more methods found in pgx, add.
	// BeginFunc(ctx context.Context, f func(Tx) error) (err error)
	// // and more...
}

// something begins SQL Transaction with options.
//
// this is extracted interface from "pgxpool.Pool" or "pgpool.Conn".
// when you need more details, see them.
type BeginTx interface {
	Begin
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (Tx, error)
	// // When you need more methods found in pgx, add.
	// BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(Tx) error) error
	// // and more...
}

// something sending query with SQL.
//
// this is extracted interface from `pgxpool.Conn` and `pgx.Tx`
// When you need more details, see them.
type Queryer interface {
	// sending SQL Command which does not have any result rows.
	//
	// for more detail, see `pgxpool.Conn.Exec`
	Exec(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error)

	// sending SQL Command which has result rows.
	//
	// for more detail, see `pgxpool.Conn.Query`
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)

	// sending SQL Command which has just single result row.
	//
	// for more detail, see `pgxpool.Conn.QueryRow`
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row

	// // When you need more methods found in pgx, add.
	// QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error)
	// // and more ...
}

// interface extracted from `pgx.Tx`
//
// # note 1: `pgx.Tx` does NOT implement `Tx`
//
// because golang lacks covariance/contravariance in typing,
// `Tx` cannot be defined as generatization of `pgx.Tx`, directly.
//
// If you need to wrap `pgx.Tx` as `Tx`,
// you can use `Pool` or `Conn` in this package and call `Begin()` .
//
// # note 2: this is subset
//
// this interface is JUST A SUBSET likes `pgx.Tx`
//
// When you need more methods only `pgx.Tx` has, declare them.
type Tx interface {
	Queryer
	Begin

	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error

	Conn() *pgx.Conn
}

// thin wrapper of pgx.Tx as Tx
type pgxTx struct {
	base pgx.Tx
}

var _ Tx = &pgxTx{}

func (tx *pgxTx) Begin(ctx context.Context) (Tx, error) {
	new, err := tx.base.Begin(ctx)
	if new == nil {
		return nil, err
	}
	return &pgxTx{new}, err
}

func (tx *pgxTx) Commit(ctx context.Context) error {
	return tx.base.Commit(ctx)
}
func (tx *pgxTx) Rollback(ctx context.Context) error {
	return tx.base.Rollback(ctx)
}
func (tx *pgxTx) Exec(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error) {
	return tx.base.Exec(ctx, sql, arguments...)
}
func (tx *pgxTx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return tx.base.Query(ctx, sql, args...)
}
func (tx *pgxTx) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return tx.base.QueryRow(ctx, sql, args...)
}
func (tx *pgxTx) Conn() *pgx.Conn {
	return tx.base.Conn()
}

// interface extracted from `*pgxpool.Conn`
//
// # note 1: `*pgxpool.Conn` does NOT implement `Conn`
//
// because golang lacks covariance/contravariance in typing,
// `Conn` cannot be defined as a generatization of `pgxpool.Conn`, directly.
//
// If you need to wrap `pgx.Conn` as `Conn`,
// you can use `Pool` in this package and call `Acquire()`.
//
// # note 2: this is subset
//
// this interface is JUST A SUBSET like `*pgxpool.Conn`
//
// When you need more methods only `*pgxpool.Conn` has, declare them.
type Conn interface {
	BeginTx

	Release()
	Queryer
	Ping(ctx context.Context) error
	Conn() *pgx.Conn
}

// thin wrapper of pgxpool.Conn as Conn
type pgxPoolConn struct {
	base *pgxpool.Conn
}

var _ Conn = &pgxPoolConn{}

func (c *pgxPoolConn) Begin(ctx context.Context) (Tx, error) {
	tx, err := c.base.Begin(ctx)
	if tx == nil {
		return nil, err
	}
	return &pgxTx{tx}, err
}
func (c *pgxPoolConn) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (Tx, error) {
	tx, err := c.base.BeginTx(ctx, txOptions)
	if tx == nil {
		return nil, err
	}
	return &pgxTx{tx}, err
}
func (c *pgxPoolConn) Release() {
	c.base.Release()
}
func (c *pgxPoolConn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return c.base.Exec(ctx, sql, arguments...)
}
func (c *pgxPoolConn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return c.base.Query(ctx, sql, args...)
}
func (c *pgxPoolConn) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return c.base.QueryRow(ctx, sql, args...)
}
func (c *pgxPoolConn) Ping(ctx context.Context) error {
	return c.base.Ping(ctx)
}
func (c *pgxPoolConn) Conn() *pgx.Conn {
	return c.base.Conn()
}

// interface extracted from `*pgxpool.Pool`
//
// # note 1: `*pgxpool.Pool` does NOT implement `Pool`
//
// because golang lacks covariance/contravariance in typing,
// `Pool` cannot be defined as a generatization of `*pgxpool.Pool`, directly.
//
// If you need to wrap `*pgxpool.Pool` as `Pool`, you can `Wrap` it.
//
// # note 2: this is subset
//
// this interface is JUST A SUBSET like `*pgxpool.Pool`
//
// When you need more methods only `*pgxpool.Pool` has, declare them.
type Pool interface {
	BeginTx

	Acquire(ctx context.Context) (Conn, error)
	AcquireAllIdle(ctx context.Context) []Conn

	Config() *pgxpool.Config
	Ping(ctx context.Context) error
}

type pgxPool struct {
	base *pgxpool.Pool
}

var _ Pool = &pgxPool{}

func (p *pgxPool) Begin(ctx context.Context) (Tx, error) {
	tx, err := p.base.Begin(ctx)
	if tx == nil {
		return nil, err
	}
	return &pgxTx{tx}, err
}
func (p *pgxPool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (Tx, error) {
	tx, err := p.base.BeginTx(ctx, txOptions)
	if tx == nil {
		return nil, err
	}
	return &pgxTx{tx}, err
}
func (p *pgxPool) Acquire(ctx context.Context) (Conn, error) {
	conn, err := p.base.Acquire(ctx)
	if conn == nil {
		return nil, err
	}
	return &pgxPoolConn{conn}, err
}
func (p *pgxPool) AcquireFunc(ctx context.Context, f func(Conn) error) error {
	wf := func(pconn *pgxpool.Conn) error {
		return f(&pgxPoolConn{pconn})
	}
	return p.base.AcquireFunc(ctx, wf)
}
func (p *pgxPool) AcquireAllIdle(ctx context.Context) []Conn {
	return slices.Map(
		p.base.AcquireAllIdle(ctx),
		func(c *pgxpool.Conn) Conn { return &pgxPoolConn{c} },
	)
}
func (p *pgxPool) Config() *pgxpool.Config {
	return p.base.Config()
}
func (p *pgxPool) Ping(ctx context.Context) error {
	return p.base.Ping(ctx)
}

func Wrap(p *pgxpool.Pool) Pool {
	return &pgxPool{p}
}
