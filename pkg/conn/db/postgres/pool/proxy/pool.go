package proxy

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/utils"
)

type Callback func()

type event struct {
	before []Callback
	after  []Callback
	chain  *event
}

func (e *event) Before(cb ...Callback) *event {
	e.before = append(e.before, cb...)
	return e
}

func (e *event) After(cb ...Callback) *event {
	e.after = append(e.after, cb...)
	return e
}

func (e *event) Invoke(f func()) {
	e.invokeBefore()
	defer e.invokeAfter()
	f()
}

func (e *event) invokeBefore() {
	if e == nil {
		return
	}
	for _, cb := range e.before {
		cb()
	}
	e.chain.invokeBefore()
}

func (e *event) invokeAfter() {
	if e == nil {
		return
	}
	e.chain.invokeAfter()
	for _, cb := range e.after {
		cb()
	}
}

type SQLEvents struct {
	Query    *event
	Commit   *event
	Rollback *event
	ExitTx   *event
}

func (sq *SQLEvents) Events() *SQLEvents {
	return sq
}

func NewPgxEvents() *SQLEvents {
	query := new(event)
	commit := new(event)
	rollback := new(event)
	exitTx := new(event)

	commit.chain = exitTx
	rollback.chain = exitTx

	return &SQLEvents{
		Query:    query,
		Commit:   commit,
		Rollback: rollback,
		ExitTx:   exitTx,
	}
}

type sqlEventHost interface {
	Events() *SQLEvents
}

func WrapTx(tx kpool.Tx, ev sqlEventHost) *Tx {
	if tx == nil {
		return nil
	}
	return &Tx{Base: tx, events: ev.Events()}
}

func WrapConn(conn kpool.Conn, ev sqlEventHost) *ConnProxy {
	if conn == nil {
		return nil
	}
	return &ConnProxy{Base: conn, events: ev.Events()}
}

// Proxy object for knit/pkg/db/postgres/Pool .
//
// Pool can accept event handlers for events.
// Each of handlers will be invoked before or after event it relevants.
//
// # Defined events
//
// - commit   : send `COMMIT;` statement via `Commit` method
//
// - rollback : send `ROLLBACK;` statement via `Commit` method
//
// - exitTx   : means "exit transaction". This is also emitted when "commit" and/or "rollback" are.
//
// - query    : send other SQL statements for PostgresSQL Server. `Exec`, `Query` and its variants emit this event.
//
// # Naming Convention of Handlers
//
// You can register handlers with `On<EventName>` and `OnAfter<EventName>`.
//
// Callbacks added into `On<EventName>` (or `OnAfter<EventName>`) will
// be invoked before (or after) the event, vice versa.
//
// For example,
//
// - callbacks for `OnQuery` will be invoked before query.
//
// - callbacks for `OnAfterRollback` will be invoked after rollback.
//
// Note that handlers for `exitTx` event will be invoked on commit AND rollback.
// The order of event is:
//
// - before exitTx
//
// - before commit / before rollback
//
// - after commit / after rollback
//
// - after exitTx
//
// # Scope of Handlers
//
// Event handlers added for Pool are passed for Conn and/or Tx of itself.
//
// So, you add handlers once, they works on all of connections/transactions of the pool.
type Pool struct {
	Base   kpool.Pool
	events *SQLEvents
}

func (p *Pool) Events() *SQLEvents {
	return p.events
}

var _ kpool.Pool = &Pool{}

func (p *Pool) Acquire(ctx context.Context) (kpool.Conn, error) {
	conn, err := p.Base.Acquire(ctx)
	if w := WrapConn(conn, p); w != nil {
		return w, err
	}
	return nil, err
}
func (p *Pool) AcquireAllIdle(ctx context.Context) []kpool.Conn {
	return utils.Map(
		p.Base.AcquireAllIdle(ctx),
		func(c kpool.Conn) kpool.Conn { return WrapConn(c, p) },
	)
}
func (p *Pool) Begin(ctx context.Context) (kpool.Tx, error) {
	tx, err := p.Base.Begin(ctx)
	if w := WrapTx(tx, p); w != nil {
		return w, err
	}
	return nil, err
}
func (p *Pool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (kpool.Tx, error) {
	tx, err := p.Base.BeginTx(ctx, txOptions)
	if w := WrapTx(tx, p); w != nil {
		return w, err
	}
	return nil, err
}
func (p *Pool) Config() *pgxpool.Config {
	return p.Base.Config()
}
func (p *Pool) Ping(ctx context.Context) error {
	return p.Base.Ping(ctx)
}

func Wrap(p kpool.Pool) *Pool {
	return &Pool{Base: p, events: NewPgxEvents()}
}

type Tx struct {
	Base   kpool.Tx
	events *SQLEvents
}

func (tx *Tx) Events() *SQLEvents {
	return tx.events
}

var _ kpool.Tx = &Tx{}

func (tx *Tx) Begin(ctx context.Context) (kpool.Tx, error) {
	new, err := tx.Base.Begin(ctx)
	if w := WrapTx(new, tx); w != nil {
		return w, err
	}
	return nil, err
}

func (tx *Tx) Commit(ctx context.Context) (err error) {
	tx.events.Commit.Invoke(func() {
		err = tx.Base.Commit(ctx)
	})
	return
}
func (tx *Tx) Rollback(ctx context.Context) (err error) {
	tx.events.Rollback.Invoke(func() {
		err = tx.Base.Rollback(ctx)
	})
	return
}
func (tx *Tx) Exec(
	ctx context.Context, sql string, arguments ...interface{},
) (ctag pgconn.CommandTag, err error) {
	tx.events.Query.Invoke(func() {
		ctag, err = tx.Base.Exec(ctx, sql, arguments...)
	})
	return
}
func (tx *Tx) Query(
	ctx context.Context, sql string, args ...interface{},
) (r pgx.Rows, err error) {
	tx.events.Query.Invoke(func() {
		r, err = tx.Base.Query(ctx, sql, args...)
	})
	return
}
func (tx *Tx) QueryRow(
	ctx context.Context, sql string, args ...interface{},
) (r pgx.Row) {
	tx.events.Query.Invoke(func() {
		r = tx.Base.QueryRow(ctx, sql, args...)
	})
	return
}

func (tx *Tx) Conn() *pgx.Conn {
	return tx.Base.Conn()
}

type ConnProxy struct {
	Base   kpool.Conn
	events *SQLEvents
}

func (p *ConnProxy) Events() *SQLEvents {
	return p.events
}

var _ kpool.Conn = &ConnProxy{}

func (c *ConnProxy) Begin(ctx context.Context) (kpool.Tx, error) {
	tx, err := c.Base.Begin(ctx)
	if w := WrapTx(tx, c); w != nil {
		return w, err
	}
	return nil, err
}
func (c *ConnProxy) BeginTx(
	ctx context.Context, txOptions pgx.TxOptions,
) (kpool.Tx, error) {
	tx, err := c.Base.BeginTx(ctx, txOptions)
	if w := WrapTx(tx, c); w != nil {
		return w, err
	}
	return nil, err
}

func (c *ConnProxy) Release() {
	c.Base.Release()
}
func (c *ConnProxy) Exec(
	ctx context.Context, sql string, arguments ...interface{},
) (ctag pgconn.CommandTag, err error) {
	c.events.Query.Invoke(func() {
		ctag, err = c.Base.Exec(ctx, sql, arguments...)
	})
	return
}
func (c *ConnProxy) Query(
	ctx context.Context, sql string, args ...interface{},
) (rs pgx.Rows, err error) {
	c.events.Query.Invoke(func() {
		rs, err = c.Base.Query(ctx, sql, args...)
	})
	return
}
func (c *ConnProxy) QueryRow(
	ctx context.Context, sql string, args ...interface{},
) (r pgx.Row) {
	c.events.Query.Invoke(func() {
		r = c.Base.QueryRow(ctx, sql, args...)
	})
	return
}

func (c *ConnProxy) Ping(ctx context.Context) error {
	return c.Base.Ping(ctx)
}
func (c *ConnProxy) Conn() *pgx.Conn {
	return c.Base.Conn()
}
