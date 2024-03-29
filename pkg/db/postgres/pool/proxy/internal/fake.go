package internal

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
)

//

type FakePool struct {
	NextBegin struct {
		Tx  kpool.Tx
		Err error
	}
	NextBeginTx struct {
		Tx  kpool.Tx
		Err error
	}
	NextAcquire struct {
		Conn kpool.Conn
		Err  error
	}
	NextAcquireAllIdle []kpool.Conn
	NextConfig         *pgxpool.Config
	NextPing           error
}

// parameter v is never read/overwritten. just a represent value of type T.
func zero[T any](T) T {
	return *new(T)
}

func (p *FakePool) Begin(ctx context.Context) (kpool.Tx, error) {
	defer func() {
		p.NextBegin = zero(p.NextBegin)
		p.NextBegin.Tx = &FakeTx{}
	}()
	return p.NextBegin.Tx, p.NextBegin.Err
}
func (p *FakePool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (kpool.Tx, error) {
	defer func() {
		p.NextBeginTx = zero(p.NextBeginTx)
		p.NextBeginTx.Tx = &FakeTx{}
	}()
	return p.NextBeginTx.Tx, p.NextBeginTx.Err
}
func (p *FakePool) Acquire(ctx context.Context) (kpool.Conn, error) {
	defer func() {
		p.NextAcquire = zero(p.NextAcquire)
		p.NextAcquire.Conn = &FakeConn{}
	}()
	return p.NextAcquire.Conn, p.NextAcquire.Err
}

func (p *FakePool) AcquireAllIdle(ctx context.Context) []kpool.Conn {
	defer func() {
		p.NextAcquireAllIdle = zero(p.NextAcquireAllIdle)
	}()
	return p.NextAcquireAllIdle
}
func (p *FakePool) Config() *pgxpool.Config {
	defer func() {
		p.NextConfig = zero(p.NextConfig)
	}()
	return p.NextConfig
}
func (p *FakePool) Ping(ctx context.Context) error {
	defer func() {
		p.NextPing = zero(p.NextPing)
	}()
	return p.NextPing
}

type FakeTx struct {
	NextBegin struct {
		Tx  kpool.Tx
		Err error
	}
	NextCommit   error
	NextRollback error
	NextExec     struct {
		CommandTag pgconn.CommandTag
		Err        error
	}
	NextQuery struct {
		Rows pgx.Rows
		Err  error
	}
	NextQueryRow pgx.Row
	NextConn     *pgx.Conn
}

func (tx *FakeTx) Begin(ctx context.Context) (kpool.Tx, error) {

	defer func() {
		tx.NextBegin = zero(tx.NextBegin)
		tx.NextBegin.Tx = &FakeTx{}
	}()

	return tx.NextBegin.Tx, tx.NextBegin.Err
}

func (tx *FakeTx) Commit(ctx context.Context) error {
	defer func() {
		tx.NextCommit = zero(tx.NextCommit)
	}()
	return tx.NextCommit
}
func (tx *FakeTx) Rollback(ctx context.Context) error {
	defer func() {
		tx.NextRollback = zero(tx.NextRollback)
	}()
	return tx.NextRollback
}
func (tx *FakeTx) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	defer func() {
		tx.NextExec = zero(tx.NextExec)
	}()
	return tx.NextExec.CommandTag, tx.NextExec.Err
}
func (tx *FakeTx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	defer func() {
		tx.NextQuery = zero(tx.NextQuery)
	}()
	return tx.NextQuery.Rows, tx.NextQuery.Err
}
func (tx *FakeTx) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	defer func() {
		tx.NextQueryRow = zero(tx.NextQueryRow)
	}()
	return tx.NextQueryRow
}
func (tx *FakeTx) Conn() *pgx.Conn {
	defer func() {
		tx.NextConn = zero(tx.NextConn)
	}()
	return tx.NextConn
}

type FakeConn struct {
	NextBegin struct {
		Tx  kpool.Tx
		Err error
	}
	NextBeginTx struct {
		Tx  kpool.Tx
		Err error
	}
	NextExec struct {
		CommandTag pgconn.CommandTag
		Err        error
	}
	NextQuery struct {
		Rows pgx.Rows
		Err  error
	}
	NextQueryRow pgx.Row
	NextConn     *pgx.Conn
	NextPing     error
}

func (c *FakeConn) Begin(ctx context.Context) (kpool.Tx, error) {
	defer func() {
		c.NextBegin = zero(c.NextBegin)
		c.NextBegin.Tx = &FakeTx{}
	}()
	return c.NextBegin.Tx, c.NextBegin.Err
}
func (c *FakeConn) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (kpool.Tx, error) {
	defer func() {
		c.NextBeginTx = zero(c.NextBeginTx)
		c.NextBeginTx.Tx = &FakeTx{}
	}()
	return c.NextBeginTx.Tx, c.NextBeginTx.Err
}
func (c *FakeConn) Release() {
}

func (c *FakeConn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	defer func() {
		c.NextExec = zero(c.NextExec)
	}()
	return c.NextExec.CommandTag, c.NextExec.Err
}
func (c *FakeConn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	defer func() {
		c.NextQuery = zero(c.NextQuery)
	}()
	return c.NextQuery.Rows, c.NextQuery.Err
}
func (c *FakeConn) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	defer func() {
		c.NextQueryRow = zero(c.NextQueryRow)
	}()
	return c.NextQueryRow
}
func (c *FakeConn) Ping(ctx context.Context) error {
	defer func() {
		c.NextPing = zero(c.NextPing)
	}()
	return c.NextPing
}
func (c *FakeConn) Conn() *pgx.Conn {
	defer func() {
		c.NextConn = zero(c.NextConn)
	}()
	return c.NextConn
}
